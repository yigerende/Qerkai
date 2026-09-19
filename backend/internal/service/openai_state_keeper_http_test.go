//go:build unit

package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestStateKeeperHTTPWithForceWSDisabled(t *testing.T) {
	defer setForceUpstreamWSForTest(false)()
	defer setTTFTModeForTest(t, OpenAITTFTModeNetwork)()
	for _, route := range []string{"responses", "passthrough", "chat"} {
		for _, stream := range []bool{true, false} {
			for _, mode := range []string{"normal", "degraded", "no-state", "injection-off", "keeper-off", "refresh-off", "other-model", "other-account", "other-group"} {
				t.Run(fmt.Sprintf("%s/stream=%t/%s", route, stream, mode), func(t *testing.T) {
					s, gateway, account := keeperTestService(t)
					q := s.config.Load().OpenAIStateKeeperSettings
					q.ResponseRefreshEnabled = mode != "refresh-off"
					q.InjectionEnabled = mode != "injection-off"
					q.Enabled = mode != "keeper-off"
					q.DegradedStateLengths = []int{356}
					require.NoError(t, s.Save(context.Background(), q))
					if mode == "no-state" {
						s.entryLocked(account.ID).value = ""
					}
					if mode == "other-account" {
						account = keeperTestAccount(2)
					}
					account.Extra = map[string]any{"responses_websockets_v2_enabled": true, "openai_passthrough": route == "passthrough"}
					businessProxy := &Proxy{ID: 9, Protocol: "http", Host: "business-proxy", Port: 8080}
					account.ProxyID, account.Proxy = &businessProxy.ID, businessProxy
					gateway.cfg, gateway.cache, gateway.toolCorrector = &config.Config{}, &stubGatewayCache{}, NewCodexToolCorrector()
					if route != "chat" {
						gateway.openaiWSResolver = keeperWSResolver{}
					}
					model := q.Model
					if mode == "other-model" {
						model = "unconfigured-model"
					}
					eligible := q.Enabled && q.InjectionEnabled && mode != "other-model" && mode != "other-account" && mode != "other-group"
					injected := eligible && mode != "no-state"
					length := 356
					if mode == "normal" {
						length = 332
					}
					returnedModel := model
					if mode == "degraded" {
						returnedModel = "different-upstream-model"
					}
					calls := 0
					gateway.httpUpstream = &keeperHTTPStub{do: func(req *http.Request, proxy string, id int64, _ int) (*http.Response, error) {
						calls++
						require.Equal(t, "https://chatgpt.com/backend-api/codex/responses", req.URL.String())
						require.Equal(t, http.MethodPost, req.Method)
						require.Empty(t, req.Header.Get("Upgrade"))
						require.Equal(t, businessProxy.URL(), proxy)
						require.Equal(t, account.ID, id)
						require.Equal(t, "Bearer "+account.GetCredential("access_token"), req.Header.Get("Authorization"))
						wantState := "client-native-state"
						if injected {
							wantState = "collected-secret"
						}
						require.Equal(t, wantState, req.Header.Get(openAICodexTurnStateHeader))
						body, err := io.ReadAll(req.Body)
						require.NoError(t, err)
						require.Equal(t, model, gjson.GetBytes(body, "model").String())
						require.Contains(t, string(body), "original question")
						payload := fmt.Sprintf("data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_http_state\",\"model\":%q}}\n\n"+
							"data: {\"type\":\"response.output_text.delta\",\"delta\":\"original answer\"}\n\n"+
							"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_http_state\",\"model\":%q,\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"original answer\"}]}],\"usage\":{\"input_tokens\":2,\"output_tokens\":3}}}\n\n", returnedModel, returnedModel)
						return &http.Response{StatusCode: http.StatusOK, Header: http.Header{
							"Content-Type": {"text/event-stream"}, "X-Codex-Turn-State": {strings.Repeat("s", length)},
						}, Body: io.NopCloser(strings.NewReader(payload))}, nil
					}}
					recorder := httptest.NewRecorder()
					c, _ := gin.CreateTestContext(recorder)
					path := "/v1/responses"
					body := []byte(fmt.Sprintf(`{"model":%q,"stream":%t,"input":"original question"}`, model, stream))
					if route == "chat" {
						path = "/v1/chat/completions"
						body = []byte(fmt.Sprintf(`{"model":%q,"stream":%t,"messages":[{"role":"user","content":"original question"}]}`, model, stream))
					}
					c.Request = httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(body)))
					c.Request.Header.Set(openAICodexTurnStateHeader, "client-native-state")
					SetOpenAIClientTransport(c, OpenAIClientTransportHTTP)
					group := int64(11)
					if mode == "other-group" {
						group = 12
					}
					c.Set("api_key", &APIKey{ID: 1, GroupID: &group})
					var result *OpenAIForwardResult
					var err error
					if route == "chat" {
						result, err = gateway.ForwardAsChatCompletions(context.Background(), c, account, body, "", "")
					} else {
						result, err = gateway.Forward(context.Background(), c, account, body)
					}
					require.NoError(t, err)
					require.NotNil(t, result)
					require.Equal(t, 1, calls, "the response signal must never resend the business request")
					require.Equal(t, injected, result.StateInjected)
					require.Equal(t, returnedModel, result.UpstreamResponseModel)
					require.NotNil(t, upstreamModelMismatch(model, result.UpstreamResponseModel))
					require.Equal(t, mode == "degraded", *upstreamModelMismatch(model, result.UpstreamResponseModel))
					require.Equal(t, 2, result.Usage.InputTokens)
					require.Equal(t, 3, result.Usage.OutputTokens)
					require.Contains(t, recorder.Body.String(), "original answer")
					keeperDrainObservations(s)
					if eligible && q.ResponseRefreshEnabled && length == 356 {
						require.Len(t, s.queue, 1)
						job := <-s.queue
						require.Equal(t, "response", job.source)
						require.Equal(t, account.ID, job.accountID)
						require.Equal(t, model, job.model)
					} else {
						require.Empty(t, s.queue)
					}
				})
			}
		}
	}
}

func TestStateKeeperHTTPCollectionPersistsWithoutForceWSOrInjection(t *testing.T) {
	defer setForceUpstreamWSForTest(false)()
	s, gateway, account := keeperTestService(t)
	s.files = keeperTestFileStore(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.InjectionEnabled = false
	q.AllowedStateLengths = []int{332}
	require.NoError(t, s.Save(context.Background(), q))
	gateway.openaiWSResolver = keeperWSResolver{}
	state := strings.Repeat("s", 332)
	calls := 0
	gateway.httpUpstream = &keeperHTTPStub{do: func(req *http.Request, proxy string, id int64, _ int) (*http.Response, error) {
		calls++
		require.Equal(t, "https://chatgpt.com/backend-api/codex/responses", req.URL.String())
		require.Equal(t, "http://127.0.0.1:8888", proxy)
		require.Equal(t, account.ID, id)
		require.Empty(t, req.Header.Get(openAICodexTurnStateHeader))
		body, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		require.Equal(t, q.Model, gjson.GetBytes(body, "model").String())
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"X-Codex-Turn-State": {state}}, Body: keeperHeaderOnlyBody{t: t}}, nil
	}}
	s.probe = s.collect
	require.NoError(t, s.Schedule([]int64{account.ID}))
	s.run(<-s.queue)
	require.Equal(t, 1, calls)
	file, err := s.FileDetail(account.ID, q.Model)
	require.NoError(t, err)
	require.Equal(t, state, file.Content.Value)
	require.Empty(t, s.observations, "collection must not observe its own response as a business signal")
	q.InjectionEnabled = true
	require.NoError(t, s.Save(context.Background(), q))
	req, err := gateway.buildUpstreamRequest(context.Background(), keeperTestContext(11), account, []byte(fmt.Sprintf(`{"model":%q}`, q.Model)), "token", true, "", false)
	require.NoError(t, err)
	require.Equal(t, state, req.Header.Get(openAICodexTurnStateHeader))
}
