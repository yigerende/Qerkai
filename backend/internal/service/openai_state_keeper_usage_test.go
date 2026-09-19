package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestStateKeeperUsageTracksActualHTTPDispatch(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		trace, response, want bool
	}{
		{"prepared_only", false, false, false},
		{"sent_then_read_failed", true, false, true},
		{"plugin_response_without_trace", false, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, gateway, account := keeperTestService(t)
			c := keeperTestContext(11)
			body := []byte(fmt.Sprintf(`{"model":%q}`, s.config.Load().Model))
			req := gateway.prepareCollectedStateHTTP(c, account, body, httptest.NewRequest(http.MethodPost, "/v1/responses", nil))
			var result OpenAIForwardResult
			SnapshotOpenAIStateUsage(c, &result)
			require.False(t, result.StateInjected)
			gateway.httpUpstream = &keeperHTTPStub{do: func(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
				require.Equal(t, "collected-secret", req.Header.Get(openAICodexTurnStateHeader))
				if tc.trace {
					httptrace.ContextClientTrace(req.Context()).WroteRequest(httptrace.WroteRequestInfo{})
				}
				if tc.response {
					return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}}, nil
				}
				return nil, errors.New("transport failure")
			}}
			_, _ = gateway.doOpenAIUpstream(req, "", account)
			SnapshotOpenAIStateUsage(c, &result)
			require.Equal(t, tc.want, result.StateInjected)
		})
	}
}

func TestStateKeeperUsageDisabledBeforeDispatchPreservesNativeHeader(t *testing.T) {
	s, gateway, account := keeperTestService(t)
	c := keeperTestContext(11)
	q := s.config.Load().OpenAIStateKeeperSettings
	body := []byte(fmt.Sprintf(`{"model":%q}`, q.Model))
	native := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	native.Header.Set(openAICodexTurnStateHeader, "client-native-state")
	req := gateway.prepareCollectedStateHTTP(c, account, body, native)
	q.InjectionEnabled = false
	require.NoError(t, s.Save(context.Background(), q))
	gateway.httpUpstream = &keeperHTTPStub{do: func(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
		require.Equal(t, "client-native-state", req.Header.Get(openAICodexTurnStateHeader))
		httptrace.ContextClientTrace(req.Context()).WroteRequest(httptrace.WroteRequestInfo{})
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}}, nil
	}}
	_, err := gateway.doOpenAIUpstream(req, "", account)
	require.NoError(t, err)
	var result OpenAIForwardResult
	SnapshotOpenAIStateUsage(c, &result)
	require.False(t, result.StateInjected)
}

func TestStateKeeperUsageDoesNotLeakAcrossAttempts(t *testing.T) {
	s, gateway, account := keeperTestService(t)
	c := keeperTestContext(11)
	q := s.config.Load().OpenAIStateKeeperSettings
	ticket := gateway.prepareCollectedStateWS(c, account, q.Model, http.Header{})
	var result OpenAIForwardResult
	SnapshotOpenAIStateUsage(c, &result)
	require.False(t, result.StateInjected, "a handshake alone is not a sent business request")
	ticket.noteSent()
	SnapshotOpenAIStateUsage(c, &result)
	require.True(t, result.StateInjected)
	for _, next := range []struct {
		account *Account
		model   string
	}{
		{keeperTestAccount(2), q.Model}, {account, "unconfigured"},
	} {
		gateway.prepareCollectedStateWS(c, next.account, next.model, http.Header{})
		SnapshotOpenAIStateUsage(c, &result)
		require.False(t, result.StateInjected)
	}
	gateway.prepareCollectedStateWS(c, account, q.Model, http.Header{}).noteSent()
	resetOpenAIStateUsage(c)
	SnapshotOpenAIStateUsage(c, &result)
	require.False(t, result.StateInjected, "a new forwarding attempt resets the previous send")
	gateway.prepareCollectedStateWS(c, account, q.Model, http.Header{}).noteSent()
	q.InjectionEnabled = false
	require.NoError(t, s.Save(context.Background(), q))
	gateway.prepareCollectedStateHTTP(c, account, []byte(fmt.Sprintf(`{"model":%q}`, q.Model)), httptest.NewRequest(http.MethodPost, "/v1/responses", nil))
	SnapshotOpenAIStateUsage(c, &result)
	require.False(t, result.StateInjected, "disabled HTTP fallback must not inherit WS injection")
}

func TestStateKeeperUsageHTTPForwardResults(t *testing.T) {
	for _, endpoint := range []string{"/v1/responses", "/v1/chat/completions", "ws-http-bridge"} {
		for _, enabled := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/injected=%t", endpoint, enabled), func(t *testing.T) {
				s, gateway, account := keeperTestService(t)
				q := s.config.Load().OpenAIStateKeeperSettings
				q.InjectionEnabled = enabled
				require.NoError(t, s.Save(context.Background(), q))
				gateway.cfg, gateway.cache, gateway.toolCorrector = &config.Config{}, &stubGatewayCache{}, NewCodexToolCorrector()
				gateway.httpUpstream = &keeperHTTPStub{do: func(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
					require.Equal(t, enabled, req.Header.Get(openAICodexTurnStateHeader) == "collected-secret")
					payload := `data: {"type":"response.completed","response":{"id":"resp_state_usage","status":"completed","output":[],"usage":{"input_tokens":2,"output_tokens":3}}}` + "\n\n"
					return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(payload))}, nil
				}}
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				if endpoint == "ws-http-bridge" {
					c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
				} else {
					c.Request = httptest.NewRequest(http.MethodPost, endpoint, nil)
				}
				group := int64(11)
				c.Set("api_key", &APIKey{GroupID: &group})
				var result *OpenAIForwardResult
				var err error
				if endpoint == "/v1/responses" {
					result, err = gateway.Forward(context.Background(), c, account, []byte(fmt.Sprintf(`{"model":%q,"stream":true,"input":"hello"}`, q.Model)))
				} else if endpoint == "ws-http-bridge" {
					payload := []byte(fmt.Sprintf(`{"type":"response.create","model":%q,"input":[]}`, q.Model))
					result, err = gateway.proxyOpenAIWSHTTPBridgeTurn(context.Background(), c, account, "test-token", payload, len(payload), q.Model, "", "", "", "", 1, func([]byte) error { return nil })
				} else {
					result, err = gateway.ForwardAsChatCompletions(context.Background(), c, account, []byte(fmt.Sprintf(`{"model":%q,"stream":true,"messages":[{"role":"user","content":"hello"}]}`, q.Model)), "", "")
				}
				require.NoError(t, err)
				require.NotNil(t, result)
				require.Equal(t, enabled, result.StateInjected)
				require.Equal(t, 2, result.Usage.InputTokens)
			})
		}
	}
}

func TestStateKeeperUsageRecordsInjectionSnapshot(t *testing.T) {
	for _, injected := range []bool{false, true} {
		usageRepo := &openAIRecordUsageLogRepoStub{inserted: true}
		svc := newOpenAIRecordUsageServiceForTest(usageRepo, &openAIRecordUsageUserRepoStub{}, &openAIRecordUsageSubRepoStub{}, nil)
		svc.RecordCyberPolicyUsageLog(context.Background(), CyberPolicyUsageInput{
			APIKey: &APIKey{ID: 2, User: &User{ID: 1}}, Account: &Account{ID: 3},
			RequestID: "state-usage", Model: "gpt-5.1", InputTokens: 10, StateInjected: injected,
		})
		require.Equal(t, 1, usageRepo.calls)
		require.NotNil(t, usageRepo.lastLog.StateInjected)
		require.Equal(t, injected, *usageRepo.lastLog.StateInjected)
	}
}
