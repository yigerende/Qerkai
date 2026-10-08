//go:build unit

package service

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func bpsWriteWSText(conn *websocket.Conn, terminal string) error {
	var response map[string]any
	if err := json.Unmarshal([]byte(terminal), &response); err != nil {
		return err
	}
	message := response["output"].([]any)[0].(map[string]any)
	text := message["content"].([]any)[0].(map[string]any)["text"]
	events := []map[string]any{
		{"type": "response.created", "response": map[string]any{"id": response["id"], "model": response["model"], "status": "in_progress", "output": []any{}}},
		{"type": "response.output_item.added", "output_index": 0, "item": map[string]any{"id": message["id"], "type": "message", "role": "assistant", "status": "in_progress", "content": []any{}}},
		{"type": "response.content_part.added", "output_index": 0, "content_index": 0, "item_id": message["id"], "part": map[string]any{"type": "output_text", "text": "", "annotations": []any{}}},
		{"type": "response.output_text.delta", "output_index": 0, "content_index": 0, "item_id": message["id"], "delta": text},
		{"type": "response.completed", "response": response},
	}
	for _, event := range events {
		if err := conn.WriteJSON(event); err != nil {
			return err
		}
	}
	return nil
}

func bpsTestWSServer(t *testing.T, respond func(*http.Request, map[string]any) string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
		if !assert.NoError(t, err) {
			return
		}
		defer conn.Close()
		var payload map[string]any
		if !assert.NoError(t, conn.ReadJSON(&payload)) {
			return
		}
		assert.Equal(t, "response.create", payload["type"])
		assert.Equal(t, "gpt-6-astra", payload["model"])
		_ = bpsWriteWSText(conn, respond(r, payload))
	}))
	t.Cleanup(server.Close)
	return server
}

func TestBPSUpstreamTransportIndependentOfAccountWS(t *testing.T) {
	for _, transport := range []string{"http", "auto"} {
		for _, accountMode := range []string{"missing", "off", "ctx_pool", "passthrough", "force-http"} {
			t.Run(transport+"/"+accountMode, func(t *testing.T) {
				gateway, account := bpsTestGateway(t)
				account.Extra = map[string]any{}
				if accountMode != "missing" {
					account.Extra["openai_oauth_responses_websockets_v2_mode"] = accountMode
				}
				if accountMode == "force-http" {
					account.Extra["openai_ws_force_http"] = true
				}
				var wsCalls, httpCalls atomic.Int32
				server := bpsTestWSServer(t, func(r *http.Request, p map[string]any) string {
					wsCalls.Add(1)
					assert.Equal(t, "Bearer fixture-token", r.Header.Get("Authorization"))
					return bpsTextFixture
				})
				cfg := gateway.settingService.bpsSettings(context.Background()).settings
				cfg.UpstreamTransport, cfg.ResponsesURL = transport, server.URL+"/responses"
				gateway.settingService.bpsSettingsCache.Store(newCachedOpenAIBPS(cfg, time.Hour))
				gateway.httpUpstream = &keeperHTTPStub{do: func(*http.Request, string, int64, int) (*http.Response, error) {
					httpCalls.Add(1)
					return newJSONResponse(200, bpsTextFixture), nil
				}}
				for _, stream := range []bool{false, true} {
					c, rec := downstreamTestContext(11)
					result, err := gateway.Forward(context.Background(), c, account, []byte(fmt.Sprintf(`{"model":"gpt-6-astra","input":"hi","stream":%t}`, stream)))
					require.NoError(t, err, rec.Body.String())
					require.Equal(t, 8, result.Usage.CacheReadInputTokens)
					require.Equal(t, openAIBPSEndpoint, result.UpstreamEndpoint)
					require.False(t, result.OpenAIWSMode, "retain HTTP ingress billing identity")
					usage := &UsageLog{Stream: result.Stream, OpenAIWSMode: openAIUsageWSMode(result)}
					usage.SyncRequestTypeAndLegacyFields()
					wantType := RequestTypeSync
					if stream {
						wantType = RequestTypeStream
					}
					if transport == "auto" {
						wantType = RequestTypeWSV2
					}
					require.Equal(t, wantType, usage.RequestType)
					require.False(t, result.StateInjected)
					require.Zero(t, result.OpenAIUpstream5xxRetryCount)
				}
				if transport == "auto" {
					require.EqualValues(t, 2, wsCalls.Load())
					require.Zero(t, httpCalls.Load())
				} else {
					require.Zero(t, wsCalls.Load())
					require.EqualValues(t, 2, httpCalls.Load())
				}
			})
		}
	}
}

func TestBPSUsageTypeAfterHTTPFallback(t *testing.T) {
	for _, format := range []string{"responses", "chat", "anthropic"} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stream=%t", format, stream), func(t *testing.T) {
				gateway, account := bpsTestGateway(t)
				var handshakes, httpCalls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					handshakes.Add(1)
					http.Error(w, "WebSocket unavailable", http.StatusNotFound)
				}))
				defer server.Close()
				cfg := gateway.settingService.bpsSettings(context.Background()).settings
				cfg.UpstreamTransport, cfg.ResponsesURL = "auto", server.URL+"/responses"
				gateway.settingService.bpsSettingsCache.Store(newCachedOpenAIBPS(cfg, time.Hour))
				gateway.httpUpstream = &keeperHTTPStub{do: func(*http.Request, string, int64, int) (*http.Response, error) {
					httpCalls.Add(1)
					return newJSONResponse(http.StatusOK, bpsTextFixture), nil
				}}
				body := []byte(fmt.Sprintf(`{"model":"gpt-6-astra","input":"hi","messages":[{"role":"user","content":"hi"}],"max_tokens":100,"stream":%t}`, stream))
				c, rec := downstreamTestContext(11)
				result, err := gateway.forwardBPS(context.Background(), c, account, body, gateway.settingService.bpsSettings(context.Background()), format, "", "")
				require.NoError(t, err, rec.Body.String())
				require.False(t, result.OpenAIWSMode)
				usage := &UsageLog{Stream: result.Stream, OpenAIWSMode: openAIUsageWSMode(result)}
				usage.SyncRequestTypeAndLegacyFields()
				want := RequestTypeSync
				if stream {
					want = RequestTypeStream
				}
				require.Equal(t, want, usage.RequestType)
				require.EqualValues(t, 1, handshakes.Load())
				require.EqualValues(t, 1, httpCalls.Load())
			})
		}
	}
}

func TestBPSUpstreamWSCompatibilityAndQuality(t *testing.T) {
	for _, returnedModel := range []string{"gpt-6-astra", "gpt-5.6-luna"} {
		t.Run(returnedModel, func(t *testing.T) {
			gateway, account := bpsTestGateway(t)
			account.GroupIDs = []int64{11}
			server := bpsTestWSServer(t, func(r *http.Request, p map[string]any) string {
				return strings.ReplaceAll(strings.ReplaceAll(bpsTextFixture, `  hello\n`, "21"), "gpt-6-astra", returnedModel)
			})
			cfg := gateway.settingService.bpsSettings(context.Background()).settings
			cfg.UpstreamTransport, cfg.ResponsesURL = "auto", server.URL+"/responses"
			gateway.settingService.bpsSettingsCache.Store(newCachedOpenAIBPS(cfg, time.Hour))
			gateway.httpUpstream = &keeperHTTPStub{do: func(*http.Request, string, int64, int) (*http.Response, error) {
				return nil, fmt.Errorf("unexpected HTTP fallback")
			}}
			for _, format := range []string{"responses", "chat", "anthropic"} {
				for _, stream := range []bool{false, true} {
					body := []byte(fmt.Sprintf(`{"model":"gpt-6-astra","input":"hi","messages":[{"role":"user","content":"hi"}],"max_tokens":100,"stream":%t}`, stream))
					c, rec := downstreamTestContext(11)
					result, err := gateway.forwardBPS(context.Background(), c, account, body, gateway.settingService.bpsSettings(context.Background()), format, "", "")
					require.NoError(t, err, rec.Body.String())
					require.Equal(t, 8, result.Usage.CacheReadInputTokens)
					require.Contains(t, rec.Body.String(), "21")
					require.True(t, openAIUsageWSMode(result))
				}
			}
			s := &AccountQualityService{tests: &AccountTestService{accountRepo: &qualityAccountRepo{account: account}, httpUpstream: gateway.httpUpstream, bpsGateway: gateway, settingService: gateway.settingService}}
			observer := &upstreamResponseModelObserver{}
			answer, _, err := s.testAnswer(context.Background(), account.ID, DefaultAccountQualitySettings(), QualityQuestion{Prompt: "What is 17 plus 4?"}, observer)
			require.NoError(t, err)
			require.Equal(t, "21", answer)
			require.Equal(t, returnedModel, observer.Model(), "quality checks must observe the real upstream model")
		})
	}
}

func TestBPSUpstreamWSNetworkNotificationIsImmediate(t *testing.T) {
	defer setTTFTModeForTest(t, OpenAITTFTModeNetwork)()
	gatewayForwardingCache.Store(&cachedGatewayForwardingSettings{openAITTFTMode: OpenAITTFTModeNetwork, expiresAt: time.Now().Add(time.Hour).UnixNano()})
	gateway, account := bpsTestGateway(t)
	release := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
		if !assert.NoError(t, err) {
			return
		}
		defer conn.Close()
		if _, _, err := conn.ReadMessage(); !assert.NoError(t, err) {
			return
		}
		_ = conn.WriteJSON(map[string]any{"type": "response.created", "response": map[string]any{"id": "resp_bps", "model": "gpt-6-astra", "status": "in_progress", "output": []any{}}})
		select {
		case <-release:
		case <-ctx.Done():
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":`+bpsTextFixture+`}`))
	}))
	defer server.Close()
	cfg := gateway.settingService.bpsSettings(ctx).settings
	cfg.UpstreamTransport, cfg.ResponsesURL = "auto", server.URL+"/responses"
	gateway.settingService.bpsSettingsCache.Store(newCachedOpenAIBPS(cfg, time.Hour))
	gateway.httpUpstream = bpsFixtureHTTP(bpsTextFixture)
	flushed := make(chan string, 20)
	recorder := &networkTTFTRecorder{ResponseRecorder: httptest.NewRecorder(), onFlush: func(s string) { flushed <- s }}
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil).WithContext(ctx)
	done := make(chan error, 1)
	var result *OpenAIForwardResult
	go func() {
		var err error
		result, err = gateway.forwardBPS(ctx, c, account, []byte(`{"model":"gpt-6-astra","input":"hi","stream":true}`), gateway.settingService.bpsSettings(ctx), "responses", "", "")
		done <- err
	}()
	select {
	case output := <-flushed:
		require.Contains(t, output, `"type":"response.created"`)
		require.NotContains(t, output, "hello")
	case <-ctx.Done():
		t.Fatal("WS notification waited for the answer")
	}
	close(release)
	require.NoError(t, <-done)
	require.NotNil(t, result.FirstTokenMs)
	require.Equal(t, 1, strings.Count(recorder.Body.String(), `"type":"response.created"`))
}

func TestBPSUpstreamWSConcurrentStreams(t *testing.T) {
	for _, count := range []int{100, 500} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			gateway, first := bpsTestGateway(t)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			allArrived := make(chan struct{})
			release := make(chan struct{})
			second := *first
			second.ID = first.ID + 1
			second.Credentials = map[string]any{"access_token": "second-token", "chatgpt_account_id": "second-account"}
			accounts := []*Account{first, &second}
			var calls, active, peak atomic.Int32
			server := bpsTestWSServer(t, func(r *http.Request, p map[string]any) string {
				n := active.Add(1)
				defer active.Add(-1)
				for old := peak.Load(); n > old; old = peak.Load() {
					if peak.CompareAndSwap(old, n) {
						break
					}
				}
				identity := r.Header.Get("ChatGPT-Account-ID")
				if identity == "second-account" {
					assert.Equal(t, "Bearer second-token", r.Header.Get("Authorization"))
				} else {
					assert.Equal(t, "fixture-account", identity)
					assert.Equal(t, "Bearer fixture-token", r.Header.Get("Authorization"))
				}
				if calls.Add(1) == int32(count) {
					close(allArrived)
				}
				// Keep every request in flight until the full concurrency is reached.
				select {
				case <-release:
				case <-ctx.Done():
				}
				return strings.ReplaceAll(bpsTextFixture, `  hello\n`, identity)
			})
			cfg := gateway.settingService.bpsSettings(context.Background()).settings
			cfg.UpstreamTransport, cfg.ResponsesURL = "auto", server.URL+"/responses"
			gateway.settingService.bpsSettingsCache.Store(newCachedOpenAIBPS(cfg, time.Hour))
			gateway.httpUpstream = &keeperHTTPStub{do: func(*http.Request, string, int64, int) (*http.Response, error) {
				return nil, fmt.Errorf("unexpected HTTP fallback")
			}}
			ready := make(chan struct{})
			var ramp *time.Ticker
			if count > 100 {
				// Windows loopback rejects some SYNs in an instantaneous 500-dial
				// burst. Ramp connections while holding all streams open, so this
				// still verifies 500 simultaneous requests rather than the OS backlog.
				close(ready)
				ramp = time.NewTicker(time.Millisecond)
				defer ramp.Stop()
			}
			errorsC := make(chan error, count)
			var wg sync.WaitGroup
			start := time.Now()
			for i := 0; i < count; i++ {
				if ramp != nil {
					<-ramp.C
				}
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-ready
					c, rec := downstreamTestContext(11)
					account := accounts[i%len(accounts)]
					result, err := gateway.Forward(ctx, c, account, []byte(`{"model":"gpt-6-astra","stream":true,"input":"hi"}`))
					if err != nil {
						errorsC <- err
						return
					}
					if result.Usage.CacheReadInputTokens != 8 || !strings.Contains(rec.Body.String(), account.GetChatGPTAccountID()) || strings.Count(rec.Body.String(), `"type":"response.completed"`) != 1 {
						errorsC <- fmt.Errorf("incorrect account/usage/terminal for request %d", i)
					}
				}(i)
			}
			if ramp == nil {
				close(ready)
			}
			select {
			case <-allArrived:
			case err := <-errorsC:
				t.Errorf("request failed before full concurrency: %v", err)
				cancel()
			case <-ctx.Done():
				t.Error("did not reach full concurrency before the deadline")
			}
			close(release)
			wg.Wait()
			close(errorsC)
			for err := range errorsC {
				t.Error(err)
			}
			require.EqualValues(t, count, calls.Load())
			require.EqualValues(t, count, peak.Load())
			t.Logf("WS concurrency=%d completed=%d peak_active=%d elapsed=%s", count, calls.Load(), peak.Load(), time.Since(start))
		})
	}
}

func TestBPSUpstreamWSInheritsAccountProxy(t *testing.T) {
	gateway, account := bpsTestGateway(t)
	account.GroupIDs = []int64{11}
	var connections, upstreamCalls atomic.Int32
	server := bpsTestWSServer(t, func(r *http.Request, p map[string]any) string {
		upstreamCalls.Add(1)
		assert.Equal(t, "Bearer fixture-token", r.Header.Get("Authorization"))
		assert.Empty(t, r.Header.Get("Proxy-Authorization"))
		return strings.ReplaceAll(bpsTextFixture, `  hello\n`, "21")
	})
	target, err := url.Parse(server.URL)
	require.NoError(t, err)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connections.Add(1)
		assert.Equal(t, http.MethodConnect, r.Method)
		assert.Equal(t, target.Host, r.Host)
		assert.Equal(t, "Basic "+base64.StdEncoding.EncodeToString([]byte("fixture-user:fixture-password")), r.Header.Get("Proxy-Authorization"))
		upstream, err := net.DialTimeout("tcp", target.Host, time.Second)
		if !assert.NoError(t, err) {
			return
		}
		defer upstream.Close()
		client, buffer, err := w.(http.Hijacker).Hijack()
		if !assert.NoError(t, err) {
			return
		}
		defer client.Close()
		_, _ = buffer.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n")
		_ = buffer.Flush()
		done := make(chan struct{})
		go func() {
			_, _ = io.Copy(upstream, buffer)
			_ = upstream.Close()
			close(done)
		}()
		_, _ = io.Copy(client, upstream)
		_ = client.Close()
		<-done
	}))
	defer proxy.Close()
	proxyURL, err := url.Parse(proxy.URL)
	require.NoError(t, err)
	port, err := strconv.Atoi(proxyURL.Port())
	require.NoError(t, err)
	account.Proxy = &Proxy{ID: 99, Protocol: "http", Host: proxyURL.Hostname(), Port: port, Username: "fixture-user", Password: "fixture-password"}
	account.ProxyID = &account.Proxy.ID
	cfg := gateway.settingService.bpsSettings(context.Background()).settings
	cfg.UpstreamTransport, cfg.ResponsesURL = "auto", server.URL+"/responses"
	gateway.settingService.bpsSettingsCache.Store(newCachedOpenAIBPS(cfg, time.Hour))
	gateway.httpUpstream = &keeperHTTPStub{do: func(*http.Request, string, int64, int) (*http.Response, error) {
		return nil, fmt.Errorf("unexpected fallback instead of using account proxy")
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, rec := downstreamTestContext(11)
	result, err := gateway.Forward(ctx, c, account, []byte(`{"model":"gpt-6-astra","stream":true,"input":"hi"}`))
	require.NoError(t, err, rec.Body.String())
	require.Equal(t, 8, result.Usage.CacheReadInputTokens)
	require.EqualValues(t, 1, connections.Load())
	require.EqualValues(t, 1, upstreamCalls.Load())

	quality := &AccountQualityService{tests: &AccountTestService{
		accountRepo: &qualityAccountRepo{account: account}, httpUpstream: gateway.httpUpstream,
		bpsGateway: gateway, settingService: gateway.settingService,
	}}
	q := DefaultAccountQualitySettings()
	observer := &upstreamResponseModelObserver{}
	answer, _, err := quality.testAnswer(ctx, account.ID, q, q.Questions[0], observer)
	require.NoError(t, err)
	require.Equal(t, "21", answer)
	require.Equal(t, q.Model, observer.Model())
	require.EqualValues(t, 2, connections.Load(), "quality detection must use the authenticated CONNECT proxy too")
	require.EqualValues(t, 2, upstreamCalls.Load())
}
