package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func setForceWSGroupsForTest(t *testing.T, ids []int64) {
	t.Helper()
	previous := forceUpstreamWSGroupsCache.Load()
	refreshForceUpstreamWSGroupsCache(ids)
	t.Cleanup(func() { forceUpstreamWSGroupsCache.Store(previous) })
}

func TestForceWSGroupScopeSharedAccount(t *testing.T) {
	t.Cleanup(setForceUpstreamWSForTest(true))
	setForceWSGroupsForTest(t, []int64{11})
	cfg := forceWSTestConfig()
	resolver := NewOpenAIWSProtocolResolver(cfg)
	account := forceWSOAuthAccount(false)
	account.GroupIDs = []int64{11, 22}
	svc := &OpenAIGatewayService{cfg: cfg, openaiWSResolver: resolver}
	for _, id := range []int64{11, 22, 0} {
		selected := id == 11
		decision := resolveOpenAIWSProtocolForGroup(resolver, account, id)
		got := resolveOpenAIWSDecisionByClientTransport(decision, OpenAIClientTransportHTTP, id)
		require.Equal(t, selected, got.Transport == OpenAIUpstreamTransportResponsesWebsocketV2)
		require.Equal(t, selected, svc.isOpenAIAccountTransportCompatible(account, OpenAIUpstreamTransportResponsesWebsocketV2Ingress, id))
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest("POST", "/v1/responses", nil)
		c.Set("api_key", &APIKey{GroupID: &id})
		require.Equal(t, selected, svc.shouldRouteChatCompletionsViaWS(c, account))
	}
	// Native account WS capability remains available outside the forced scope.
	account.Extra["openai_oauth_responses_websockets_v2_enabled"] = true
	decision := resolveOpenAIWSProtocolForGroup(resolver, account, 22)
	require.Equal(t, OpenAIUpstreamTransportResponsesWebsocketV2, resolveOpenAIWSDecisionByClientTransport(decision, OpenAIClientTransportWS, 22).Transport)
	require.Equal(t, OpenAIUpstreamTransportHTTPSSE, resolveOpenAIWSDecisionByClientTransport(decision, OpenAIClientTransportHTTP, 22).Transport)
	account.Extra["openai_ws_force_http"] = true
	require.Equal(t, "account_force_http", resolveOpenAIWSProtocolForGroup(resolver, account, 11).Reason)
}

func TestForceWSGroupScopeDefaultsAndDisabled(t *testing.T) {
	t.Cleanup(setForceUpstreamWSForTest(true))
	setForceWSGroupsForTest(t, nil)
	require.True(t, ForceUpstreamWSEnabledForGroup(11))
	require.True(t, ForceUpstreamWSEnabledForGroup(0))
	refreshForceUpstreamWSGroupsCache([]int64{})
	require.False(t, ForceUpstreamWSEnabledForGroup(11))
	refreshForceUpstreamWSGroupsCache([]int64{11})
	require.True(t, ForceUpstreamWSEnabledForGroup(11))
	off := false
	forceUpstreamWSOverride.Store(&off)
	require.False(t, ForceUpstreamWSEnabledForGroup(11))
	for _, raw := range []string{"[]", "invalid", "[-1]", "[0]", "[1.5]"} {
		require.NotNil(t, parseForceUpstreamWSGroupIDs(raw))
		require.Empty(t, parseForceUpstreamWSGroupIDs(raw))
	}
	require.Nil(t, parseForceUpstreamWSGroupIDs(""))
	require.Nil(t, parseForceUpstreamWSGroupIDs("null"))
	require.Equal(t, []int64{11, 22}, parseForceUpstreamWSGroupIDs("[11,11,22]"))
}

func TestForceWSGroupScopeExcludesBusinessRetry(t *testing.T) {
	t.Cleanup(setForceUpstreamWSForTest(true))
	t.Cleanup(setOpenAIUpstream5xxRetryForTest(OpenAIUpstream5xxRetryConfig{Enabled: true, Total: 5}))
	setForceWSGroupsForTest(t, []int64{11})
	svc := &OpenAIGatewayService{}
	for _, id := range []int64{11, 22} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest("POST", "/v1/responses", nil)
		c.Set("api_key", &APIKey{GroupID: &id})
		state := svc.openAIWSBusinessRetryState(c, forceWSOAuthAccount(false), true, time.Now())
		require.Equal(t, id == 11, state != nil)
	}
}

func TestForceWSGroupScopeHTTPForward(t *testing.T) {
	t.Cleanup(setForceUpstreamWSForTest(true))
	setForceWSGroupsForTest(t, []int64{11})
	groupID := int64(22)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/responses", nil)
	c.Set("api_key", &APIKey{GroupID: &groupID})
	SetOpenAIClientTransport(c, OpenAIClientTransportHTTP)
	upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"id":"resp_http","usage":{"input_tokens":1,"output_tokens":2}}`))}}
	cfg := forceWSTestConfig()
	svc := &OpenAIGatewayService{cfg: cfg, httpUpstream: upstream, openaiWSResolver: NewOpenAIWSProtocolResolver(cfg)}
	account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{"api_key": "local-test"}, Extra: map[string]any{"responses_websockets_v2_enabled": true}}
	result, err := svc.Forward(context.Background(), c, account, []byte(`{"model":"gpt-5.1","stream":false,"input":"hello"}`))
	require.NoError(t, err)
	require.False(t, result.OpenAIWSMode)
	require.NotNil(t, upstream.lastReq)
	require.Equal(t, string(OpenAIUpstreamTransportHTTPSSE), GetOpenAIUpstreamTransport(c))
}
