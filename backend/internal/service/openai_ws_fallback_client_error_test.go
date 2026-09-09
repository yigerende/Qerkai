package service

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// 生产实测形态：22.4% 的客户端可见错误是 "Upstream request failed"，
// 真实原因（read_event / acquire_timeout 等）被整体丢弃。
func TestFallbackClientErrorCoversLostReasons(t *testing.T) {
	cases := []struct {
		reason     string
		cause      string
		wantStatus int
		wantType   string
	}{
		{"read_event", `received close frame: reason = "keepalive ping timeout"`, http.StatusBadGateway, "upstream_error"},
		{openAIWSStaleConnReason, "keepalive ping timeout", http.StatusBadGateway, "upstream_error"},
		{"acquire_timeout", "context deadline exceeded", http.StatusServiceUnavailable, "server_error"},
		{"acquire_conn", "no connection available", http.StatusServiceUnavailable, "server_error"},
		{"conn_queue_full", "queue full", http.StatusServiceUnavailable, "server_error"},
		{"preferred_conn_unavailable", "gone", http.StatusServiceUnavailable, "server_error"},
		{"write_request", "context deadline exceeded", http.StatusBadGateway, "upstream_error"},
		{"invalid_event_json", "malformed", http.StatusBadGateway, "upstream_error"},
		{"missing_final_response", "no terminal response payload", http.StatusBadGateway, "upstream_error"},
		{"dial_failed", "dial tcp: i/o timeout", http.StatusBadGateway, "upstream_error"},
		{"streaming_not_supported", "streaming not supported", http.StatusInternalServerError, "server_error"},
	}
	for _, tc := range cases {
		err := wrapOpenAIWSFallback(tc.reason, errors.New(tc.cause))

		// 前提：上游写出器确实认不出这些 reason（返回 ok=false）。
		_, _, _, _, upstreamOK := resolveOpenAIWSFallbackErrorResponse(err)
		require.False(t, upstreamOK, "前提失效 reason=%s：上游已能处理，无需本补丁", tc.reason)

		msg, errType, status, reason := openAIWSFallbackClientErrorMessage(err)
		require.NotEmpty(t, msg, "reason=%s 必须产出客户端可见文案", tc.reason)
		require.Equal(t, tc.wantStatus, status, "reason=%s", tc.reason)
		require.Equal(t, tc.wantType, errType, "reason=%s", tc.reason)
		require.Equal(t, tc.reason, reason)
		require.Contains(t, msg, tc.reason, "文案必须带 reason token 便于定位")
		require.Contains(t, msg, tc.cause, "文案必须带底层真实原因")
		require.NotEqual(t, "Upstream request failed", msg)
	}
}

// prewarm_ 前缀只表示失败发生在预热连接上，客户端语义与常规路径一致。
func TestFallbackClientErrorHandlesPrewarmPrefix(t *testing.T) {
	err := wrapOpenAIWSFallback("prewarm_read_event", errors.New("boom"))
	msg, errType, status, reason := openAIWSFallbackClientErrorMessage(err)

	require.NotEmpty(t, msg)
	require.Equal(t, http.StatusBadGateway, status)
	require.Equal(t, "upstream_error", errType)
	require.Equal(t, "prewarm_read_event", reason, "reason token 保留原始前缀")
}

// 上游已能正确处理的 reason 不得被本补丁接管——否则会覆盖上游语义。
func TestFallbackClientErrorSkipsUpstreamHandledReasons(t *testing.T) {
	for _, reason := range []string{
		"invalid_encrypted_content", "previous_response_not_found",
		"upgrade_required", "ws_unsupported", "auth_failed", "upstream_rate_limited",
	} {
		err := wrapOpenAIWSFallback(reason, errors.New("boom"))
		_, _, _, _, upstreamOK := resolveOpenAIWSFallbackErrorResponse(err)
		require.True(t, upstreamOK, "前提：上游应能处理 reason=%s", reason)

		msg, _, _, _ := openAIWSFallbackClientErrorMessage(err)
		require.Empty(t, msg, "reason=%s 已由上游处理，不应被接管", reason)
	}
}

// 客户端已断开时没有下游可写，包装无意义。
func TestFallbackClientErrorSkipsCanceledBackoff(t *testing.T) {
	err := wrapOpenAIWSFallback("retry_backoff_canceled", errors.New("context canceled"))
	msg, _, _, _ := openAIWSFallbackClientErrorMessage(err)
	require.Empty(t, msg)
}

func TestFallbackClientErrorIgnoresNonFallbackErrors(t *testing.T) {
	for _, err := range []error{nil, errors.New("plain")} {
		msg, _, _, _ := openAIWSFallbackClientErrorMessage(err)
		require.Empty(t, msg)
	}
}

// 包装后必须能被 #3 建好的 handler 通路取出。
func TestFallbackClientErrorFlowsThroughClientVisibleChannel(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)

	svc := &OpenAIGatewayService{}
	base := wrapOpenAIWSFallback("read_event", errors.New("keepalive ping timeout"))
	wrapped := svc.wrapOpenAIWSFallbackClientError(c, &Account{ID: 1, Platform: PlatformOpenAI}, base)

	msg, errType, status, ok := OpenAIWSClientVisibleMessage(wrapped)
	require.True(t, ok, "必须能被 handler 的通路取出")
	require.Contains(t, msg, "keepalive ping timeout")
	require.Equal(t, "upstream_error", errType)
	require.Equal(t, http.StatusBadGateway, status)

	// 错误链必须保留，否则 shouldFallbackToHTTPAfterWSFailure 等判定会失效。
	require.ErrorIs(t, wrapped, base)
	require.True(t, isOpenAIWSStaleConnError(
		svc.wrapOpenAIWSFallbackClientError(c, nil, wrapOpenAIWSStaleConnFallback(errors.New("x"))),
	), "陈旧连接判定必须穿透包装")
}

// 不需要包装时原样返回，不得凭空产生客户端可见错误。
func TestFallbackClientErrorPassthroughWhenNotApplicable(t *testing.T) {
	svc := &OpenAIGatewayService{}
	base := wrapOpenAIWSFallback("auth_failed", errors.New("401"))
	require.Equal(t, base, svc.wrapOpenAIWSFallbackClientError(nil, nil, base))

	plain := errors.New("plain")
	require.Equal(t, plain, svc.wrapOpenAIWSFallbackClientError(nil, nil, plain))
}
