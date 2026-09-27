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
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func bpsTestGateway(t *testing.T) (*OpenAIGatewayService, *Account) {
	t.Helper()
	cfg := DefaultOpenAIBPSSettings()
	cfg.Enabled = true
	cfg.GroupIDs = []int64{11}
	raw, _ := json.Marshal(cfg)
	settings := NewSettingService(&settingRepoStub{values: map[string]string{SettingKeyOpenAIBPS: string(raw)}}, &config.Config{})
	settings.bpsSettingsCache.Store(newCachedOpenAIBPS(cfg, time.Hour))
	account := keeperTestAccount(1)
	account.Credentials["chatgpt_account_id"] = "fixture-account"
	account.Credentials["access_token"] = "fixture-token"
	return &OpenAIGatewayService{settingService: settings, cfg: &config.Config{}, cache: &stubGatewayCache{}, toolCorrector: NewCodexToolCorrector()}, account
}

const bpsTextFixture = `{"id":"resp_bps","model":"gpt-6-astra","status":"completed","output":[{"id":"msg_bps","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"  hello\n"}]}],"usage":{"input_tokens":12,"output_tokens":4,"input_tokens_details":{"cached_tokens":8}}}`

func bpsFixtureHTTP(body string) *keeperHTTPStub {
	return &keeperHTTPStub{do: func(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
	}}
}

func TestBPSCompatibilityFormatsAndCacheKey(t *testing.T) {
	for _, format := range []string{"chat", "anthropic"} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%t", format, stream), func(t *testing.T) {
				gateway, account := bpsTestGateway(t)
				gateway.httpUpstream = bpsFixtureHTTP(bpsTextFixture)
				c, rec := downstreamTestContext(11)
				body := []byte(fmt.Sprintf(`{"model":"gpt-6-astra","messages":[{"role":"user","content":"hello"}],"max_tokens":100,"stream":%t,"prompt_cache_key":"client-session"}`, stream))
				converted, err := bpsConvertRequest(body, format, "gpt-6-astra", "fallback-session", "")
				require.NoError(t, err)
				require.Equal(t, "client-session", gjson.GetBytes(converted, "prompt_cache_key").String())
				result, err := gateway.forwardBPS(context.Background(), c, account, body, gateway.bpsRoute(context.Background(), c, account, body, ""), format, "", "")
				require.NoError(t, err, rec.Body.String())
				require.Equal(t, 12, result.Usage.InputTokens)
				require.Equal(t, 8, result.Usage.CacheReadInputTokens)
				require.Contains(t, rec.Body.String(), `  hello\n`)
				if stream && format == "chat" {
					require.Contains(t, rec.Body.String(), "[DONE]")
				}
				if stream && format == "anthropic" {
					require.Contains(t, rec.Body.String(), "message_stop")
				}
			})
		}
	}
}

func TestBPSCompatibilityFailureAfterTextNeverSendsSuccess(t *testing.T) {
	prefix := "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_failed\",\"model\":\"gpt-6-astra\",\"status\":\"in_progress\",\"output\":[]}}\n\n" +
		"data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"msg_failed\",\"role\":\"assistant\",\"status\":\"in_progress\",\"content\":[]}}\n\n" +
		"data: {\"type\":\"response.content_part.added\",\"output_index\":0,\"content_index\":0,\"item_id\":\"msg_failed\",\"part\":{\"type\":\"output_text\",\"text\":\"\",\"annotations\":[]}}\n\n" +
		"data: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_failed\",\"output_index\":0,\"content_index\":0,\"delta\":\"hello\"}\n\n" +
		"data: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"message\":\"upstream lost\"}}}\n\n"
	for _, format := range []string{"responses", "chat", "anthropic"} {
		t.Run(format, func(t *testing.T) {
			gateway, account := bpsTestGateway(t)
			calls := 0
			gateway.httpUpstream = &keeperHTTPStub{do: func(*http.Request, string, int64, int) (*http.Response, error) {
				calls++
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(prefix))}, nil
			}}
			body := []byte(`{"model":"gpt-6-astra","stream":true,"input":"hi","messages":[{"role":"user","content":"hi"}]}`)
			c, rec := downstreamTestContext(11)
			_, err := gateway.forwardBPS(context.Background(), c, account, body, gateway.bpsRoute(context.Background(), c, account, body, ""), format, "", "")
			require.Error(t, err)
			require.Equal(t, 1, calls)
			require.Contains(t, rec.Body.String(), "hello")
			require.Contains(t, rec.Body.String(), "error")
			require.NotContains(t, rec.Body.String(), "[DONE]")
			require.NotContains(t, rec.Body.String(), "message_stop")
			require.NotContains(t, rec.Body.String(), "response.completed")
		})
	}
}

func TestBPSCatalogKeepsAliasAndCanonical(t *testing.T) {
	gateway, account := bpsTestGateway(t)
	cfg := DefaultOpenAIBPSSettings()
	cfg.Enabled = true
	cfg.Models = []string{"astra-bps", "gpt-6-astra"}
	cfg.ModelMappings = map[string]string{"astra-bps": "gpt-6-astra", "gpt-6-astra": "gpt-6-astra"}
	gateway.settingService.bpsSettingsCache.Store(newCachedOpenAIBPS(cfg, time.Hour))
	gateway.accountRepo = codexModelsVisibilityAccountRepo{byGroup: map[int64][]Account{11: {*account}}}
	body, err := gateway.ApplyBPSModelCatalog(context.Background(), &Group{ID: 11, Platform: PlatformOpenAI}, []byte(`{"models":[]}`))
	require.NoError(t, err)
	require.Len(t, gjson.GetBytes(body, "models").Array(), 2)
	for _, model := range gjson.GetBytes(body, "models").Array() {
		require.False(t, model.Get("use_responses_lite").Bool())
		require.Equal(t, "null", model.Get("tool_mode").Raw)
		require.Contains(t, model.Get("input_modalities").Raw, "image")
	}
}

func TestBPSWebsocketUsesHTTPAndPreservesOriginalModelAcrossTurns(t *testing.T) {
	gateway, account := bpsTestGateway(t)
	gateway.httpUpstream = bpsFixtureHTTP(bpsTextFixture)
	account.Extra = map[string]any{"openai_ws_responses_v2_mode": "off"}
	require.True(t, gateway.isOpenAIAccountTransportCompatible(account, OpenAIUpstreamTransportBPSIngress, 11))
	for _, kind := range []string{AccountTypeAPIKey, AccountTypeSetupToken} {
		other := *account
		other.Type = kind
		require.False(t, gateway.isOpenAIAccountTransportCompatible(&other, OpenAIUpstreamTransportBPSIngress, 11))
	}
	turns := make(chan *OpenAIForwardResult, 2)
	done := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			done <- err
			return
		}
		c, _ := downstreamTestContext(11)
		c.Request = r
		hooks := &OpenAIWSIngressHooks{InitialRequestModel: "client-alias", MapRequestModel: func(turn int, model string) (string, error) {
			if model != "client-alias" {
				return "", fmt.Errorf("model mapped twice: %q", model)
			}
			return "gpt-6-astra", nil
		}, AfterTurn: func(_ int, result *OpenAIForwardResult, err error) {
			if err == nil {
				turns <- result
			}
		}}
		done <- gateway.ProxyResponsesWebSocketFromClient(r.Context(), c, conn, account, "fixture-token", []byte(`{"type":"response.create","model":"gpt-6-astra","input":"hi"}`), hooks)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	require.NoError(t, err)
	defer conn.CloseNow()
	for turn := 0; turn < 2; turn++ {
		if turn > 0 {
			require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","input":[{"role":"user","content":"hi"},{"role":"assistant","content":"hello"},{"role":"user","content":"again"}]}`)))
		}
		for {
			_, raw, err := conn.Read(ctx)
			require.NoError(t, err)
			if gjson.GetBytes(raw, "type").String() == "response.completed" {
				break
			}
		}
		select {
		case result := <-turns:
			require.Equal(t, "client-alias", result.Model)
			require.Equal(t, openAIBPSEndpoint, result.UpstreamEndpoint)
		case <-ctx.Done():
			t.Fatal("missing turn accounting")
		}
	}
	conn.CloseNow()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("BPS reader leaked after disconnect")
	}
}

func TestBPSRouteRequiresExactOAuthGroupAndModel(t *testing.T) {
	gateway, account := bpsTestGateway(t)
	body := []byte(`{"model":"gpt-6-astra","input":"hello"}`)
	c, _ := downstreamTestContext(11)
	require.NotNil(t, gateway.bpsRoute(context.Background(), c, account, body, ""))
	for _, kind := range []string{AccountTypeAPIKey, AccountTypeSetupToken} {
		copy := *account
		copy.Type = kind
		require.Nil(t, gateway.bpsRoute(context.Background(), c, &copy, body, ""))
	}
	copy := *account
	copy.Platform = PlatformAnthropic
	require.Nil(t, gateway.bpsRoute(context.Background(), c, &copy, body, ""))
	other, _ := downstreamTestContext(12)
	require.Nil(t, gateway.bpsRoute(context.Background(), other, account, body, ""))
	require.Nil(t, gateway.bpsRoute(context.Background(), c, account, []byte(`{"model":"gpt-5.6-sol"}`), ""))
	cfg := DefaultOpenAIBPSSettings()
	gateway.settingService.bpsSettingsCache.Store(newCachedOpenAIBPS(cfg, time.Hour))
	require.Nil(t, gateway.bpsRoute(context.Background(), c, account, body, ""))
}

func TestBPSForwardPreservesToolsUsageAndNoState(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			gateway, account := bpsTestGateway(t)
			calls := 0
			gateway.httpUpstream = &keeperHTTPStub{do: func(req *http.Request, _ string, id int64, _ int) (*http.Response, error) {
				calls++
				require.Equal(t, account.ID, id)
				require.Equal(t, "bps.openai.com", req.URL.Host)
				require.Empty(t, req.Header.Get("x-codex-turn-state"))
				raw, _ := io.ReadAll(req.Body)
				require.Equal(t, "gpt-6-astra", gjson.GetBytes(raw, "model").String())
				require.Equal(t, "explicit", gjson.GetBytes(raw, "model_selection").String())
				require.Contains(t, string(raw), "tools.exec_command")
				terminal := `{"id":"resp_bps","status":"completed","model":"gpt-6-astra","output":[{"type":"function_call","call_id":"call_bps","name":"run_officejs","arguments":"{\"references\":[\"tools.exec_command\"],\"code\":\"{\\\"cmd\\\":\\\"echo hello\\\"}\"}"}],"usage":{"input_tokens":12,"output_tokens":4,"input_tokens_details":{"cached_tokens":8}}}`
				contentType := "application/json"
				if stream {
					terminal = "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":" + terminal + "}\n\n"
					contentType = "text/event-stream"
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {contentType}}, Body: io.NopCloser(strings.NewReader(terminal))}, nil
			}}
			c, rec := downstreamTestContext(11)
			body := []byte(fmt.Sprintf(`{"model":"gpt-6-astra","stream":%t,"input":"hello","tools":[{"type":"namespace","name":"tools","tools":[{"type":"function","name":"exec_command","parameters":{"type":"object","properties":{"cmd":{"type":"string"}},"required":["cmd"]}}]}]}`, stream))
			result, err := gateway.Forward(context.Background(), c, account, body)
			require.NoError(t, err, rec.Body.String())
			require.Equal(t, 1, calls)
			require.Equal(t, 12, result.Usage.InputTokens)
			require.Equal(t, 8, result.Usage.CacheReadInputTokens)
			require.False(t, result.StateInjected)
			require.Zero(t, result.OpenAIUpstream5xxRetryCount)
			require.Equal(t, openAIBPSEndpoint, result.UpstreamEndpoint)
			require.Contains(t, rec.Body.String(), `"namespace":"tools"`)
			require.NotContains(t, rec.Body.String(), "run_officejs")
		})
	}
}

func TestBPSValidationNeverCallsUpstream(t *testing.T) {
	for _, body := range []string{`{"model":"gpt-6-astra","input":"hi","previous_response_id":"resp_old"}`, `{"model":"gpt-6-astra","input":"hi","service_tier":"priority"}`, `{"model":"gpt-6-astra","input":"hi","text":{"format":{"type":"json_schema"}}}`} {
		gateway, account := bpsTestGateway(t)
		gateway.httpUpstream = &keeperHTTPStub{do: func(*http.Request, string, int64, int) (*http.Response, error) {
			t.Fatal("validation sent upstream")
			return nil, nil
		}}
		c, rec := downstreamTestContext(11)
		_, err := gateway.Forward(context.Background(), c, account, []byte(body))
		require.Error(t, err)
		require.Equal(t, 400, rec.Code)
	}
}

func TestBPSEightConfiguredModelsAreSentByTheirOwnNames(t *testing.T) {
	models := []string{"gpt-5.5", "codex-auto-review", "gpt-5.6-luna", "gpt-5.6-sol", "gpt-5.6-terra", "gpt-6-astra", "gpt-6-sol", "gpt-6-luna"}
	gateway, account := bpsTestGateway(t)
	cfg := DefaultOpenAIBPSSettings()
	cfg.Enabled = true
	cfg.Models = models
	cfg.ModelMappings = nil
	cfg, err := NormalizeOpenAIBPSSettings(cfg)
	require.NoError(t, err)
	gateway.settingService.publishBPSUpdate(map[string]string{SettingKeyOpenAIBPS: func() string { raw, _ := json.Marshal(cfg); return string(raw) }()})
	for _, model := range models {
		t.Run(model, func(t *testing.T) {
			gateway.httpUpstream = &keeperHTTPStub{do: func(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
				body, err := io.ReadAll(req.Body)
				require.NoError(t, err)
				require.Equal(t, model, gjson.GetBytes(body, "model").String())
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(strings.ReplaceAll(bpsTextFixture, "gpt-6-astra", model)))}, nil
			}}
			c, _ := downstreamTestContext(11)
			result, err := gateway.Forward(context.Background(), c, account, []byte(fmt.Sprintf(`{"model":%q,"input":"hi"}`, model)))
			require.NoError(t, err)
			require.Equal(t, model, result.UpstreamModel)
			require.Equal(t, model, result.UpstreamResponseModel)
		})
	}
	cfg.Enabled = false
	raw, _ := json.Marshal(cfg)
	gateway.settingService.publishBPSUpdate(map[string]string{SettingKeyOpenAIBPS: string(raw)})
	c, _ := downstreamTestContext(11)
	require.Nil(t, gateway.bpsRoute(context.Background(), c, account, []byte(`{"model":"gpt-6-astra"}`), ""))
}

func TestBPSAccountProbeRequiresSwitchAndDoesNotRequireGroup(t *testing.T) {
	gateway, account := bpsTestGateway(t)
	probe := &AccountTestService{settingService: gateway.settingService, bpsGateway: gateway, httpUpstream: bpsFixtureHTTP(bpsTextFixture)}
	c, rec := downstreamTestContext(999)
	require.NoError(t, probe.testBPSAccount(c, account, "gpt-6-astra", "hi", "default", AccountTestOptions{}))
	require.Contains(t, rec.Body.String(), `"success":true`)
	require.Contains(t, rec.Body.String(), "/basispoints/api/responses")
	gateway.settingService.bpsSettingsCache.Store(newCachedOpenAIBPS(DefaultOpenAIBPSSettings(), time.Hour))
	probe.httpUpstream = &keeperHTTPStub{do: func(*http.Request, string, int64, int) (*http.Response, error) {
		t.Fatal("disabled BPS probe sent upstream")
		return nil, nil
	}}
	c, rec = downstreamTestContext(999)
	require.Error(t, probe.testBPSAccount(c, account, "gpt-6-astra", "hi", "default", AccountTestOptions{}))
	require.NotContains(t, rec.Body.String(), `"success":true`)
}
