//go:build unit

package service

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestBPSConfiguredTTFTOverRealSocket(t *testing.T) {
	for _, transport := range []string{"http", "auto"} {
		for _, mode := range []string{OpenAITTFTModeNetwork, OpenAITTFTModeSemantic, OpenAITTFTModeVisible} {
			t.Run(transport+"/"+mode, func(t *testing.T) {
				defer setTTFTModeForTest(t, mode)()
				gateway, account := bpsTestGateway(t)
				gateway.settingService.settingRepo.(*settingRepoStub).values[SettingKeyOpenAITTFTMode] = mode
				// Resolve the real stored setting rather than injecting a fresh runtime cache.
				gatewayForwardingCache.Store(&cachedGatewayForwardingSettings{expiresAt: 1})
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				release := make(chan struct{})
				var releaseOnce sync.Once
				finishUpstream := func() { releaseOnce.Do(func() { close(release) }) }
				defer finishUpstream()
				created := make(chan struct{})
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					var emit func(map[string]any) error
					if transport == "auto" {
						conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
						if err != nil {
							return
						}
						defer conn.Close()
						if _, _, err := conn.ReadMessage(); err != nil {
							return
						}
						emit = func(event map[string]any) error { return conn.WriteJSON(event) }
					} else {
						_, _ = io.Copy(io.Discard, r.Body)
						w.Header().Set("Content-Type", "text/event-stream")
						emit = func(event map[string]any) error {
							raw, _ := json.Marshal(event)
							_, err := fmt.Fprintf(w, "data: %s\n\n", raw)
							w.(http.Flusher).Flush()
							return err
						}
					}
					// Real BPS notifications include a large tool/instruction catalog.
					if err := emit(map[string]any{"type": "response.created", "response": map[string]any{
						"id": "resp_bps", "model": "gpt-6-astra", "status": "in_progress", "output": []any{},
						"instructions": strings.Repeat("x", 100<<10),
					}}); err != nil {
						return
					}
					close(created)
					select {
					case <-release:
					case <-ctx.Done():
						return
					}
					for _, raw := range []string{
						`{"type":"response.output_item.added","output_index":0,"item":{"id":"msg_bps","type":"message","role":"assistant","status":"in_progress","content":[]}}`,
						`{"type":"response.content_part.added","output_index":0,"content_index":0,"item_id":"msg_bps","part":{"type":"output_text","text":"","annotations":[]}}`,
						`{"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"msg_bps","delta":"  hello\n"}`,
						`{"type":"response.completed","response":` + bpsTextFixture + `}`,
					} {
						var event map[string]any
						_ = json.Unmarshal([]byte(raw), &event)
						if emit(event) != nil {
							return
						}
					}
				}))
				defer upstream.Close()
				gateway.httpUpstream = &keeperHTTPStub{do: func(r *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
					return upstream.Client().Do(r)
				}}
				cfg := gateway.settingService.bpsSettings(ctx).settings
				cfg.UpstreamTransport, cfg.ResponsesURL = transport, upstream.URL+"/responses"
				gateway.settingService.bpsSettingsCache.Store(newCachedOpenAIBPS(cfg, time.Hour))
				results := make(chan *OpenAIForwardResult, 1)
				errorsC := make(chan error, 1)
				downstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					c, _ := gin.CreateTestContext(w)
					c.Request = r
					result, err := gateway.forwardBPS(r.Context(), c, account, []byte(`{"model":"gpt-6-astra","stream":true,"input":"hi"}`), gateway.settingService.bpsSettings(r.Context()), "responses", "", "")
					results <- result
					errorsC <- err
				}))
				defer downstream.Close()
				first := make(chan string, 1)
				readDone := make(chan error, 1)
				go func() {
					req, _ := http.NewRequestWithContext(ctx, http.MethodPost, downstream.URL, nil)
					resp, err := downstream.Client().Do(req)
					if err != nil {
						readDone <- err
						return
					}
					defer resp.Body.Close()
					scanner := bufio.NewScanner(resp.Body)
					scanner.Buffer(make([]byte, 4096), 1<<20)
					received := false
					for scanner.Scan() {
						if data, ok := extractOpenAISSEDataLine(scanner.Text()); ok && !received {
							received = true
							first <- gjson.Get(data, "type").String()
						}
					}
					readDone <- scanner.Err()
				}()
				select {
				case <-created:
				case <-ctx.Done():
					t.Fatal("upstream notification was not sent")
				}
				if mode == OpenAITTFTModeNetwork {
					select {
					case event := <-first:
						require.Equal(t, "response.created", event)
					case <-ctx.Done():
						t.Fatal("network notification was buffered until the answer")
					}
				} else {
					select {
					case event := <-first:
						t.Fatalf("mode %s unexpectedly released %s before output", mode, event)
					case <-time.After(100 * time.Millisecond):
					}
				}
				finishUpstream()
				require.NoError(t, <-readDone)
				require.NoError(t, <-errorsC)
				result := <-results
				require.NotNil(t, result.FirstTokenMs)
				t.Logf("transport=%s setting=%s first_token_ms=%d", transport, mode, *result.FirstTokenMs)
			})
		}
	}
}
