//go:build unit

package service

import (
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

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func downstreamTestGateway(t *testing.T, enabled bool, groups []int64) *OpenAIGatewayService {
	t.Helper()
	settings := OpenAIDownstreamModelAlignmentSettings{Enabled: enabled, GroupIDs: groups, Models: []string{"gpt-6-astra"}}
	raw, err := json.Marshal(settings)
	require.NoError(t, err)
	s := NewSettingService(&settingRepoStub{values: map[string]string{SettingKeyOpenAIDownstreamModelAlignment: string(raw)}}, &config.Config{})
	return &OpenAIGatewayService{settingService: s}
}

func downstreamTestContext(group int64) (*gin.Context, *httptest.ResponseRecorder) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Set("api_key", &APIKey{GroupID: &group})
	SetOpenAIClientTransport(c, OpenAIClientTransportHTTP)
	return c, rec
}

func TestDownstreamModelScopeAndPayloadIntegrity(t *testing.T) {
	for _, tc := range []struct {
		name           string
		enabled        bool
		groups         []int64
		group          int64
		sent, returned string
		wantRewrite    bool
	}{
		{"off", false, nil, 11, "gpt-6-astra", "gpt-5.6-luna", false},
		{"all", true, nil, 11, "gpt-6-astra", "gpt-5.6-luna", true},
		{"selected", true, []int64{11}, 11, "gpt-6-astra", "gpt-5.6-luna", true},
		{"other-group", true, []int64{11}, 22, "gpt-6-astra", "gpt-5.6-luna", false},
		{"empty-groups", true, []int64{}, 11, "gpt-6-astra", "gpt-5.6-luna", false},
		{"other-model", true, nil, 11, "gpt-5.6-terra", "gpt-5.6-luna", false},
		{"same-model", true, nil, 11, "gpt-6-astra", "gpt-6-astra", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gateway := downstreamTestGateway(t, tc.enabled, tc.groups)
			c, _ := downstreamTestContext(tc.group)
			account := &Account{Platform: PlatformOpenAI}
			w := gateway.newDownstreamModelWriter(c, account, tc.sent, tc.sent)
			body := []byte(fmt.Sprintf(`{"type":"response.completed","response":{"model":%q,"output":[{"text":"gpt-5.6-luna","model":"nested-tool-model"}],"usage":{"input_tokens":7}}}`, tc.returned))
			original := string(body)
			out := w.JSON(body, "response.completed")
			want := tc.returned
			if tc.wantRewrite {
				want = tc.sent
			} else {
				require.Equal(t, body, out)
			}
			require.Equal(t, original, string(body), "never mutate the upstream byte slice")
			require.Equal(t, want, gjson.GetBytes(out, "response.model").String())
			require.Equal(t, want, w.Model())
			require.Equal(t, "gpt-5.6-luna", gjson.GetBytes(out, "response.output.0.text").String())
			require.Equal(t, "nested-tool-model", gjson.GetBytes(out, "response.output.0.model").String())
			require.EqualValues(t, 7, gjson.GetBytes(out, "response.usage.input_tokens").Int())
			for _, frame := range []string{`{"type":"response.output_text.delta","delta":"gpt-5.6-luna"}`, "data: [DONE]\n\n", `{"model":"gpt-5.6-luna"`} {
				require.Equal(t, frame, string(w.Body([]byte(frame))))
			}
		})
	}
}

func TestDownstreamModelHTTPForward(t *testing.T) {
	t.Cleanup(setForceUpstreamWSForTest(false))
	defer setTTFTModeForTest(t, OpenAITTFTModeNetwork)()
	for _, passthrough := range []bool{false, true} {
		for _, stream := range []bool{false, true} {
			for _, enabled := range []bool{false, true} {
				t.Run(fmt.Sprintf("passthrough=%t/stream=%t/enabled=%t", passthrough, stream, enabled), func(t *testing.T) {
					gateway := downstreamTestGateway(t, enabled, []int64{11})
					gateway.cfg, gateway.cache, gateway.toolCorrector = &config.Config{}, &stubGatewayCache{}, NewCodexToolCorrector()
					gateway.openaiWSResolver = keeperWSResolver{}
					account := keeperTestAccount(1)
					account.Extra = map[string]any{"openai_passthrough": passthrough}
					var sentModel string
					gateway.httpUpstream = &keeperHTTPStub{do: func(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
						body, err := io.ReadAll(req.Body)
						require.NoError(t, err)
						sentModel = gjson.GetBytes(body, "model").String()
						return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(downstreamTestSSE()))}, nil
					}}
					c, rec := downstreamTestContext(11)
					result, err := gateway.Forward(context.Background(), c, account, []byte(fmt.Sprintf(`{"model":"gpt-6-astra","stream":%t,"input":"original question"}`, stream)))
					require.NoError(t, err)
					require.Equal(t, "gpt-6-astra", sentModel)
					require.Equal(t, "gpt-5.6-luna", result.UpstreamResponseModel)
					require.True(t, *upstreamModelMismatch(sentModel, result.UpstreamResponseModel))
					want := "gpt-5.6-luna"
					if enabled {
						want = "gpt-6-astra"
					}
					require.Equal(t, want, result.DownstreamModel)
					payload := rec.Body.Bytes()
					if bodyHasSSEFraming(payload) {
						count := 0
						forEachOpenAISSEFrame(string(payload), func(_ string, frame []byte) {
							if model := gjson.GetBytes(frame, "response.model"); model.Exists() {
								require.Equal(t, want, model.String())
								count++
							}
						})
						require.Equal(t, 2, count)
					} else {
						require.Equal(t, want, gjson.GetBytes(payload, "model").String())
					}
					require.Contains(t, string(payload), "original answer gpt-5.6-luna")
					require.Equal(t, 2, result.Usage.InputTokens)
					require.Equal(t, 3, result.Usage.OutputTokens)
				})
			}
		}
	}
}

func downstreamTestSSE() string {
	return "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_alignment\",\"model\":\"gpt-5.6-luna\"}}\n\n" +
		"data: {\"type\":\"response.output_text.delta\",\"delta\":\"original answer gpt-5.6-luna\"}\n\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_alignment\",\"model\":\"gpt-5.6-luna\",\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"original answer gpt-5.6-luna\"}]}],\"usage\":{\"input_tokens\":2,\"output_tokens\":3}}}\n\n"
}

func TestDownstreamModelConcurrentRequestsStayIsolated(t *testing.T) {
	gateway := downstreamTestGateway(t, true, []int64{11})
	gateway.settingService.openAIDownstreamModelAlignment(context.Background())
	account := &Account{Platform: PlatformOpenAI}
	var wg sync.WaitGroup
	errors := make(chan string, 200)
	for i := range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			group := int64(11 + i%2)
			c, _ := downstreamTestContext(group)
			w := gateway.newDownstreamModelWriter(c, account, "gpt-6-astra", "gpt-6-astra")
			want := "gpt-5.6-luna"
			if group == 11 {
				want = "gpt-6-astra"
			}
			for range 20 {
				out := w.JSON([]byte(`{"response":{"model":"gpt-5.6-luna"}}`), "response.created")
				if gjson.GetBytes(out, "response.model").String() != want {
					errors <- "group leaked"
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
}

func TestDownstreamModelSettingsCacheRefresh(t *testing.T) {
	gateway := downstreamTestGateway(t, true, []int64{11})
	s := gateway.settingService
	require.True(t, s.openAIDownstreamModelAlignment(context.Background()).matches(11, "gpt-6-astra"))
	s.downstreamModelAlignmentCache.Store(newCachedOpenAIDownstreamModelAlignment(defaultOpenAIDownstreamModelAlignment(), time.Minute))
	require.False(t, s.openAIDownstreamModelAlignment(context.Background()).matches(11, "gpt-6-astra"))
}

func TestDownstreamModelMappedRequests(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, returned := range []string{"gpt-6-astra", "gpt-5.6-luna"} {
			for _, path := range []string{"responses", "responses-passthrough", "responses-stream", "responses-passthrough-stream", "chat-json", "chat-stream", "messages-json", "messages-stream"} {
				t.Run(fmt.Sprintf("%s/enabled=%t/returned=%s", path, enabled, returned), func(t *testing.T) {
					gateway := downstreamTestGateway(t, enabled, nil)
					gateway.cfg = &config.Config{}
					account := &Account{ID: 7, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
					c, rec := downstreamTestContext(11)
					const requested, sent = "client-model-alias", "gpt-6-astra"
					payload := strings.ReplaceAll(downstreamTestSSE(), "gpt-5.6-luna", returned)
					contentType := "text/event-stream"
					if strings.HasPrefix(path, "responses") && !strings.HasSuffix(path, "-stream") {
						payload = fmt.Sprintf(`{"id":"resp_mapped","model":%q,"output":[],"usage":{"input_tokens":2,"output_tokens":3}}`, returned)
						contentType = "application/json"
					}
					resp := &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {contentType}}, Body: io.NopCloser(strings.NewReader(payload))}
					var err error
					switch path {
					case "responses":
						_, err = gateway.handleNonStreamingResponse(context.Background(), resp, c, account, requested, sent)
					case "responses-passthrough":
						_, err = gateway.handleNonStreamingResponsePassthrough(context.Background(), resp, c, account, requested, sent)
					case "responses-stream":
						_, err = gateway.handleStreamingResponse(context.Background(), resp, c, account, time.Now(), requested, sent)
					case "responses-passthrough-stream":
						_, err = gateway.handleStreamingResponsePassthrough(context.Background(), resp, c, account, time.Now(), requested, sent)
					case "chat-json":
						_, err = gateway.handleChatBufferedStreamingResponse(resp, c, account, requested, sent, sent, time.Now())
					case "chat-stream":
						_, err = gateway.handleChatStreamingResponse(resp, c, account, requested, sent, sent, time.Now(), 10)
					case "messages-json":
						_, err = gateway.handleAnthropicBufferedStreamingResponse(resp, c, account, requested, sent, sent, time.Now())
					case "messages-stream":
						_, err = gateway.handleAnthropicStreamingResponse(resp, c, account, requested, sent, sent, time.Now())
					}
					require.NoError(t, err)
					want := requested
					if returned != sent {
						if enabled {
							want = sent
						} else if strings.HasPrefix(path, "responses") {
							want = returned
						}
					}
					require.Equal(t, returned, observedUpstreamResponseModel(c))
					require.Equal(t, want, observedDownstreamModel(c))
					if strings.HasSuffix(path, "-stream") {
						models := 0
						forEachOpenAISSEFrame(rec.Body.String(), func(_ string, frame []byte) {
							if model := firstValidTrimmedGJSONString(frame, "model", "message.model", "response.model"); model != "" {
								require.Equal(t, want, model)
								models++
							}
						})
						require.Positive(t, models)
					} else {
						require.Equal(t, want, gjson.GetBytes(rec.Body.Bytes(), "model").String())
					}
				})
			}
		}
	}
}

func TestDownstreamModelForcedWSKeepsOptimizedConnection(t *testing.T) {
	t.Cleanup(setForceUpstreamWSForTest(true))
	setForceWSGroupsForTest(t, nil)
	defer setTTFTModeForTest(t, OpenAITTFTModeNetwork)()
	previousOptimization := openAIWSPoolOptimizationCache.Load()
	t.Cleanup(func() { openAIWSPoolOptimizationCache.Store(previousOptimization) })
	refreshOpenAIWSPoolOptimizationSettings(&SystemSettings{OpenAIWSPoolOptimizationEnabled: true})
	gateway := downstreamTestGateway(t, true, []int64{11})
	cfg := forceWSTestConfig()
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount, cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 2, 2
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds, cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 5, 5
	capture := &openAIWSCaptureConn{}
	dialer := &openAIWSCaptureDialer{conn: capture}
	pool := newOpenAIWSConnPool(cfg)
	t.Cleanup(pool.Close)
	pool.setClientDialerForTest(dialer)
	gateway.cfg, gateway.cache, gateway.openaiWSPool = cfg, &stubGatewayCache{}, pool
	gateway.toolCorrector, gateway.openaiWSResolver = NewCodexToolCorrector(), NewOpenAIWSProtocolResolver(cfg)
	account := keeperTestAccount(892021)
	account.Extra = map[string]any{"openai_oauth_responses_websockets_v2_enabled": true}
	account.Credentials["model_mapping"] = map[string]any{"client-model-alias": "gpt-6-astra", "gpt-6-astra": "gpt-6-astra"}
	for _, requested := range []string{"gpt-6-astra", "client-model-alias"} {
		for _, returned := range []string{"gpt-6-astra", "gpt-5.6-luna"} {
			for _, enabled := range []bool{true, false, true} {
				gateway.settingService.downstreamModelAlignmentCache.Store(newCachedOpenAIDownstreamModelAlignment(OpenAIDownstreamModelAlignmentSettings{
					Enabled: enabled, GroupIDs: []int64{11}, Models: []string{"gpt-6-astra"},
				}, time.Minute))
				capture.mu.Lock()
				upstreamSSE := strings.ReplaceAll(downstreamTestSSE(), "gpt-5.6-luna", returned)
				forEachOpenAISSEFrame(upstreamSSE, func(_ string, frame []byte) { capture.events = append(capture.events, append([]byte(nil), frame...)) })
				capture.mu.Unlock()
				c, rec := downstreamTestContext(11)
				result, err := gateway.Forward(context.Background(), c, account, []byte(fmt.Sprintf(`{"model":%q,"stream":true,"prompt_cache_key":"alignment-session","input":"original prompt"}`, requested)))
				require.NoError(t, err)
				require.True(t, result.OpenAIWSMode)
				require.Equal(t, "gpt-6-astra", capture.lastWrite["model"])
				require.Equal(t, returned, result.UpstreamResponseModel)
				want := returned
				if returned == "gpt-6-astra" {
					want = requested // Preserve legacy alias restoration without an upstream mismatch.
				} else if enabled {
					want = "gpt-6-astra"
				}
				require.Equal(t, want, result.DownstreamModel)
				var created bool
				forEachOpenAISSEFrame(rec.Body.String(), func(_ string, frame []byte) {
					if gjson.GetBytes(frame, "type").String() == "response.created" {
						created = true
					}
					if model := gjson.GetBytes(frame, "response.model"); model.Exists() {
						require.Equal(t, want, model.String())
					}
				})
				require.True(t, created, "CPA notification must still be forwarded")
				require.Contains(t, rec.Body.String(), "original answer "+returned)
				require.Equal(t, 1, dialer.DialCount(), "alignment must not change WS connection reuse")
			}
		}
	}
}

func TestDownstreamModelJSONAndChatCompletions(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, path := range []string{"responses", "responses-passthrough", "chat-json", "chat-stream"} {
			t.Run(fmt.Sprintf("%s/enabled=%t", path, enabled), func(t *testing.T) {
				gateway := downstreamTestGateway(t, enabled, nil)
				gateway.cfg = &config.Config{}
				account := &Account{ID: 7, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
				c, rec := downstreamTestContext(11)
				payload := `{"id":"response_alignment","model":"gpt-5.6-luna","output":[{"type":"message","content":[{"type":"output_text","text":"original gpt-5.6-luna answer"}]}],"usage":{"input_tokens":2,"output_tokens":3}}`
				if strings.HasPrefix(path, "chat") {
					payload = `{"id":"chat_alignment","object":"chat.completion","model":"gpt-5.6-luna","choices":[{"index":0,"message":{"role":"assistant","content":"original gpt-5.6-luna answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":3}}`
				}
				contentType := "application/json"
				if path == "chat-stream" {
					payload = "data: {\"id\":\"chat_alignment\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-5.6-luna\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"original gpt-5.6-luna answer\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":3}}\n\ndata: [DONE]\n\n"
					contentType = "text/event-stream"
				}
				resp := &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {contentType}}, Body: io.NopCloser(strings.NewReader(payload))}
				var usage *OpenAIUsage
				switch path {
				case "responses":
					result, err := gateway.handleNonStreamingResponse(context.Background(), resp, c, account, "gpt-6-astra", "gpt-6-astra")
					require.NoError(t, err)
					usage = result.usage
				case "responses-passthrough":
					result, err := gateway.handleNonStreamingResponsePassthrough(context.Background(), resp, c, account, "gpt-6-astra", "gpt-6-astra")
					require.NoError(t, err)
					usage = result.usage
				case "chat-json":
					result, err := gateway.bufferRawChatCompletions(c, resp, account, "gpt-6-astra", "gpt-6-astra", "gpt-6-astra", nil, nil, time.Now())
					require.NoError(t, err)
					usage = &result.Usage
				case "chat-stream":
					result, err := gateway.streamRawChatCompletions(c, resp, account, "gpt-6-astra", "gpt-6-astra", "gpt-6-astra", nil, nil, time.Now(), 10)
					require.NoError(t, err)
					usage = &result.Usage
				}
				want := "gpt-5.6-luna"
				if enabled {
					want = "gpt-6-astra"
				}
				require.Equal(t, want, observedDownstreamModel(c))
				require.Equal(t, "gpt-5.6-luna", observedUpstreamResponseModel(c))
				require.Contains(t, rec.Body.String(), "original gpt-5.6-luna answer")
				if path == "chat-stream" {
					forEachOpenAISSEFrame(rec.Body.String(), func(_ string, frame []byte) {
						if gjson.ValidBytes(frame) {
							require.Equal(t, want, gjson.GetBytes(frame, "model").String())
						}
					})
				} else {
					require.Equal(t, want, gjson.GetBytes(rec.Body.Bytes(), "model").String())
				}
				if !enabled {
					require.Equal(t, payload, rec.Body.String(), "disabled returns the original body byte for byte")
				}
				require.Equal(t, 2, usage.InputTokens)
				require.Equal(t, 3, usage.OutputTokens)
			})
		}
	}
}
