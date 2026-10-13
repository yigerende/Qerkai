//go:build unit

package service

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type freshConnectionSocketDialer struct {
	url      string
	delegate openAIWSClientDialer
}

func (d *freshConnectionSocketDialer) Dial(ctx context.Context, _ string, headers http.Header, _ string) (openAIWSClientConn, int, http.Header, error) {
	return d.delegate.Dial(ctx, d.url, headers, "")
}

// Exercise the real WS reader, pool and SSE forwarding under concurrent load.
// Each logical request must fail once, dial a different socket, resend exactly
// the same tool-capable payload and return only the successful turn's usage.
func TestOpenAIWSFreshConnectionRetryConcurrentSockets(t *testing.T) {
	const requests = 40
	setFreshConnectionRetryForTest(t, true, 1)
	defer setTTFTModeForTest(t, OpenAITTFTModeNetwork)()
	var accepted atomic.Int64
	var mu sync.Mutex
	firstPayload := map[string]string{}
	firstConn := map[string]int64{}
	attempts := map[string]int{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		connID := accepted.Add(1)
		for {
			_, payload, err := conn.ReadMessage()
			if err != nil {
				return
			}
			key := gjson.GetBytes(payload, "prompt_cache_key").String()
			mu.Lock()
			attempts[key]++
			attempt := attempts[key]
			if attempt == 1 {
				firstPayload[key], firstConn[key] = string(payload), connID
			} else {
				if string(payload) != firstPayload[key] {
					t.Errorf("request payload changed during retry for %s", key)
				}
				if connID == firstConn[key] {
					t.Errorf("retry reused failed socket for %s", key)
				}
			}
			mu.Unlock()
			if err := conn.WriteMessage(websocket.TextMessage, freshConnectionCreated(int(connID))); err != nil {
				return
			}
			if attempt == 1 {
				if err := conn.WriteMessage(websocket.TextMessage, freshConnectionErrorEvent("error")); err != nil {
					return
				}
				continue
			}
			tool := []byte(`{"type":"response.output_item.added","sequence_number":1,"item":{"type":"function_call","call_id":"call_compile","name":"exec","arguments":"{\"command\":\"go test ./...\"}"}}`)
			if err := conn.WriteMessage(websocket.TextMessage, tool); err != nil {
				return
			}
			if err := conn.WriteMessage(websocket.TextMessage, freshConnectionCompleted(int(connID))); err != nil {
				return
			}
		}
	}))
	t.Cleanup(upstream.Close)
	svc, accountA := freshConnectionTestGateway(t, &freshConnectionSocketDialer{
		url: "ws" + strings.TrimPrefix(upstream.URL, "http"), delegate: newDefaultOpenAIWSClientDialer(),
	}, true)
	accountB := *accountA
	accountB.ID++
	accountB.Credentials = map[string]any{"access_token": "test-token-b", "chatgpt_account_id": "account-b"}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var completed atomic.Int64
	var wg sync.WaitGroup
	for i := range requests {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, rec := freshConnectionTestContext()
			c.Request = c.Request.WithContext(ctx)
			account := accountA
			if i%2 == 1 {
				account = &accountB
			}
			body := []byte(fmt.Sprintf(`{"model":"gpt-6.1-sol","stream":true,"prompt_cache_key":"concurrent_%d","input":"Implement and test a Go function","tools":[{"type":"function","name":"exec","parameters":{"type":"object","properties":{"command":{"type":"string"}}}}]}`, i))
			result, err := svc.Forward(ctx, c, account, body)
			if err != nil {
				t.Errorf("request %d failed: %v", i, err)
				return
			}
			if result == nil || result.Usage.InputTokens != 12 || result.Usage.OutputTokens != 3 {
				t.Errorf("request %d returned incorrect usage", i)
				return
			}
			if strings.Contains(rec.Body.String(), "server_error") || !strings.Contains(rec.Body.String(), "function_call") {
				t.Errorf("request %d returned incorrect SSE: %s", i, rec.Body.String())
				return
			}
			if strings.Count(rec.Body.String(), `"type":"response.created"`) != 1 || strings.Count(rec.Body.String(), `"type":"response.completed"`) != 1 {
				t.Errorf("request %d returned duplicate response notifications", i)
				return
			}
			if OpenAIUpstream5xxRetryCount(c) != 0 {
				t.Errorf("request %d polluted 502/503 retry count", i)
				return
			}
			completed.Add(1)
		}()
	}
	wg.Wait()
	require.EqualValues(t, requests, completed.Load())
	require.EqualValues(t, requests*2, accepted.Load())
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, attempts, requests)
	for key, count := range attempts {
		require.Equal(t, 2, count, key)
	}
}
