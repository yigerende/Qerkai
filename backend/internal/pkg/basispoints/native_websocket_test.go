package basispoints

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func nativeWSRequest(stream bool) ExecutorRequest {
	r := nativeFixtureRequest([]byte(`{"model":"gpt-6-astra-basispoints","input":"hello","prompt_cache_key":"stable-session"}`), stream)
	r.AuthAttributes = map[string]string{"websockets": "true", "basispoints_proxy_url": "direct"}
	return r
}

func TestNativeWebSocketAndFallbackBoundary(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, status := range []int{101, 404, 503, 401, 403, 407, 429} {
			t.Run(fmt.Sprintf("stream=%t/status=%d", stream, status), func(t *testing.T) {
				var handshakes, generations, httpCalls atomic.Int32
				terminal := incrementalTerminal("native-ws-ok")
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					handshakes.Add(1)
					if r.Header.Get("Authorization") != "Bearer fixture-token" || r.Header.Get("ChatGPT-Account-ID") != "fixture-account" || r.Header.Get("User-Agent") != "oai-basispoints/0.2.2" {
						t.Error("WS lost the current OAuth identity or protocol version")
					}
					if status != 101 {
						http.Error(w, `{"error":{"message":"handshake rejected"}}`, status)
						return
					}
					conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
					if err != nil {
						t.Error(err)
						return
					}
					defer conn.Close()
					var payload map[string]any
					if err := conn.ReadJSON(&payload); err != nil {
						t.Error(err)
						return
					}
					generations.Add(1)
					if payload["type"] != "response.create" || payload["stream"] != nil || payload["background"] != nil || payload["prompt_cache_key"] != "stable-session" {
						t.Error("incorrect WS payload or lost cache key")
					}
					_ = newSSEDecoder().feed(syntheticStream(terminal), func(_ string, data string) error {
						if data == "[DONE]" {
							return nil
						}
						return conn.WriteMessage(websocket.TextMessage, []byte(data))
					})
				}))
				defer server.Close()
				cfg := DefaultConfig()
				cfg.UpstreamTransport, cfg.ResponsesURL = "auto", server.URL+"/responses"
				var engine Engine
				resp, err := engine.Open(context.Background(), cfg, nativeWSRequest(stream), func(r *http.Request) (*http.Response, error) {
					httpCalls.Add(1)
					if r.URL.String() != cfg.ResponsesURL || r.Header.Get("Authorization") != "Bearer fixture-token" {
						t.Error("fallback changed endpoint or credential")
					}
					return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(bytes.NewReader(jsonBytes(terminal)))}, nil
				})
				hardFailure := status == 401 || status == 403 || status == 407 || status == 429
				if hardFailure {
					var api *APIError
					if !errors.As(err, &api) || api.Status != status {
						t.Fatalf("wrong status: %v", err)
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					defer resp.Body.Close()
					body, err := io.ReadAll(resp.Body)
					if err != nil || !bytes.Contains(body, []byte("native-ws-ok")) {
						t.Fatalf("response: %s, %v", body, err)
					}
					transport := "http"
					if status == 101 {
						transport = "ws"
					}
					if resp.Header.Get("X-Qerkai-BPS-Transport") != transport || resp.Header.Get("X-Qerkai-BPS-Attempts") != "1" {
						t.Fatalf("incorrect native transport metadata: %v", resp.Header)
					}
				}
				wantHTTP, wantGenerations := int32(0), int32(0)
				if !hardFailure && status != 101 {
					wantHTTP = 1
				}
				if status == 101 {
					wantGenerations = 1
				}
				if handshakes.Load() != 1 || generations.Load() != wantGenerations || httpCalls.Load() != wantHTTP {
					t.Fatalf("handshakes=%d generations=%d HTTP=%d", handshakes.Load(), generations.Load(), httpCalls.Load())
				}
			})
		}
	}
}

func TestNativeWebSocketCancellationDuringHandshake(t *testing.T) {
	entered, closed := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		close(entered)
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, _ = conn.Read(make([]byte, 1))
		close(closed)
	}))
	defer server.Close()
	cfg := DefaultConfig()
	cfg.UpstreamTransport, cfg.ResponsesURL = "auto", server.URL+"/responses"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	var httpCalls atomic.Int32
	go func() {
		var engine Engine
		_, err := engine.Open(ctx, cfg, nativeWSRequest(true), func(*http.Request) (*http.Response, error) {
			httpCalls.Add(1)
			return nil, errors.New("canceled request must not fall back")
		})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("handshake not started")
	}
	cancel()
	select {
	case err := <-done:
		var api *APIError
		if !errors.As(err, &api) || api.Status != 499 {
			t.Fatalf("cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("client cancellation waited for handshake timeout")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("handshake socket was not closed")
	}
	if httpCalls.Load() != 0 {
		t.Fatal("cancellation started HTTP fallback")
	}
}

func TestNativeWebSocketNotificationsPreventToolRegeneration(t *testing.T) {
	var generations, httpCalls atomic.Int32
	var observed atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		if _, _, err := conn.ReadMessage(); err != nil {
			t.Error(err)
			return
		}
		generations.Add(1)
		_ = conn.WriteJSON(map[string]any{"type": "response.created", "response": map[string]any{"id": "resp_bad_tool", "model": "gpt-6-astra", "status": "in_progress", "output": []any{}}})
		bad := rawRelayNative("native-ws-bad", "exec", `{"cmd":`, []any{"exec"})
		_ = conn.WriteJSON(map[string]any{"type": "response.completed", "response": map[string]any{"id": "resp_bad_tool", "status": "completed", "output": []any{bad}}})
	}))
	defer server.Close()
	cfg := DefaultConfig()
	cfg.UpstreamTransport, cfg.ResponsesURL = "auto", server.URL+"/responses"
	request := nativeWSRequest(true)
	request.Payload = jsonBytes(map[string]any{"model": DefaultModelID, "input": "run", "tools": []any{map[string]any{"type": "function", "name": "exec", "parameters": map[string]any{"type": "object"}}}})
	var engine Engine
	resp, err := engine.Open(context.Background(), cfg, request, func(*http.Request) (*http.Response, error) {
		httpCalls.Add(1)
		return nil, errors.New("unexpected fallback")
	}, StreamOptions{ForwardNotifications: true, ObserveEvent: func(event, data string) {
		if event == "response.created" {
			observed.Add(1)
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(body, []byte(`"type":"response.created"`)) || !bytes.Contains(body, []byte("invalid_tool_call")) || bytes.Contains(body, []byte(`"type":"response.completed"`)) {
		t.Fatalf("incorrect delivery after network notification: %s", body)
	}
	if generations.Load() != 1 || httpCalls.Load() != 0 || observed.Load() != 1 {
		t.Fatal("notification was duplicated or committed request was replayed")
	}
}

func TestNativeWebSocketBodyCloseReleasesConnection(t *testing.T) {
	closed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		_ = conn.WriteJSON(map[string]any{"type": "response.created", "response": map[string]any{"id": "resp_cancel", "status": "in_progress", "output": []any{}}})
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, _, _ = conn.ReadMessage()
		close(closed)
	}))
	defer server.Close()
	cfg := DefaultConfig()
	cfg.UpstreamTransport, cfg.ResponsesURL = "auto", server.URL+"/responses"
	var engine Engine
	resp, err := engine.Open(context.Background(), cfg, nativeWSRequest(true), nil, StreamOptions{ForwardNotifications: true})
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Body.Close did not cancel upstream WS")
	}
}

func TestNativeHistoryReplayKeepsTenantIsolationWithoutCatalog(t *testing.T) {
	callID := "call_" + strings.ReplaceAll(t.Name(), "/", "_")
	for i := 0; i < 2; i++ {
		scope := "tenant-" + strconv.Itoa(i)
		native := namespaceTestNative("history-isolation", "exec", fmt.Sprintf(`{"tenant":%d}`, i))
		native["call_id"] = callID
		rememberNativeCall(native, scope)
	}
	for i := 0; i < 2; i++ {
		scope := "tenant-" + strconv.Itoa(i)
		history := []any{map[string]any{"type": "function_call", "name": "exec", "call_id": callID, "arguments": "{}"}, map[string]any{"type": "function_call_output", "call_id": callID, "output": "done"}}
		items := translateInputItems(history, scope)
		envelope, err := transportEnvelope(objectValue(items[0]))
		if err != nil || envelope["args"] != fmt.Sprintf(`{"tenant":%d}`, i) {
			t.Fatalf("cross-tenant history: %v %v", envelope, err)
		}
	}
}

func TestNativeWebSocketToolRoundTripWithoutCurrentCatalog(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(strconv.FormatBool(stream), func(t *testing.T) {
			patch := "*** Begin Patch\n*** Add File: hello.py\n+print(\"hello\")\n*** End Patch\n"
			callID := "native-ws-tool-" + strconv.FormatBool(stream)
			call := rawRelayNative(callID, "apply_patch", patch, []any{"apply_patch"})
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
				if err != nil {
					t.Error(err)
					return
				}
				defer conn.Close()
				var payload map[string]any
				if err := conn.ReadJSON(&payload); err != nil {
					t.Error(err)
					return
				}
				terminal := incrementalTerminal("tool-result-accepted")
				if calls.Add(1) == 1 {
					terminal["output"] = []any{call}
				} else {
					foundCall, foundOutput := false, false
					for _, raw := range payload["input"].([]any) {
						item := objectValue(raw)
						if item["call_id"] != callID {
							continue
						}
						if item["type"] == "function_call" {
							envelope, err := transportEnvelope(item)
							foundCall = err == nil && envelope["tool"] == "apply_patch" && envelope["args"] == patch
						}
						if item["type"] == "function_call_output" {
							foundOutput = item["output"] == "file created"
						}
					}
					if !foundCall || !foundOutput {
						t.Error("WS lost historical tool arguments or the tool result without a current catalog")
					}
				}
				_ = conn.WriteJSON(map[string]any{"type": "response.completed", "response": terminal})
			}))
			defer server.Close()
			cfg := DefaultConfig()
			cfg.UpstreamTransport, cfg.ResponsesURL = "auto", server.URL+"/responses"
			request := nativeWSRequest(stream)
			request.CacheScope = t.Name()
			source := map[string]any{"model": DefaultModelID, "prompt_cache_key": "stable-session", "input": "edit file", "tools": []any{map[string]any{"type": "custom", "name": "apply_patch"}}}
			var engine Engine
			for turn := 0; turn < 2; turn++ {
				request.Payload = jsonBytes(source)
				resp, err := engine.Open(context.Background(), cfg, request, func(*http.Request) (*http.Response, error) {
					return nil, errors.New("tool requests must not fall back after WS establishment")
				})
				if err != nil {
					t.Fatal(err)
				}
				raw, err := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				if err != nil {
					t.Fatal(err)
				}
				response, err := parseResponse(raw, resp.Header)
				if err != nil {
					t.Fatal(err)
				}
				if turn == 0 {
					restored := objectValue(response["output"].([]any)[0])
					if restored["type"] != "custom_tool_call" || restored["input"] != patch || !strings.HasPrefix(stringValue(restored["id"]), "ctc_") {
						t.Fatalf("WS changed the custom tool payload: %s", raw)
					}
					source["input"] = []any{messageItem("user", "edit file"), restored, map[string]any{"type": "custom_tool_call_output", "call_id": callID, "output": "file created"}}
					delete(source, "tools")
				} else if !bytes.Contains(raw, []byte("tool-result-accepted")) {
					t.Fatalf("missing final answer after tool result: %s", raw)
				}
			}
			if calls.Load() != 2 {
				t.Fatalf("expected one fresh WS per turn, got %d", calls.Load())
			}
		})
	}
}
