package service

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func newSSEGateTestContext() (*gin.Context, *httptest.ResponseRecorder) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	return c, rec
}

// 核心回归：设了 SSE 头却没写字节时，后续 JSON 错误体会带着
// text/event-stream 发给客户端（gin 覆盖不掉已有的 Content-Type），
// 下游按 SSE 解析裸 JSON 取不到 data: 行，报 unexpected end of JSON input。
func TestSSEHeaderGateDefersUntilFirstWrite(t *testing.T) {
	c, rec := newSSEGateTestContext()
	gate := newOpenAIWSSSEHeaderGate(c, nil, true)

	// 缓冲窗口内失败：从未调用 ensure。
	require.False(t, gate.Sent())
	c.JSON(http.StatusBadGateway, gin.H{
		"error": gin.H{"type": "upstream_error", "message": "Our servers are currently overloaded."},
	})

	require.Equal(t, "application/json; charset=utf-8", rec.Header().Get("Content-Type"),
		"未写出任何 SSE 字节时，错误响应必须是 JSON 形态")
	require.Empty(t, rec.Header().Get("X-Accel-Buffering"))
}

// 真正写出 SSE 时，头必须完整——漏写会让客户端把 SSE 当普通响应体。
func TestSSEHeaderGateSetsHeadersOnWrite(t *testing.T) {
	c, rec := newSSEGateTestContext()
	gate := newOpenAIWSSSEHeaderGate(c, nil, true)

	gate.ensure()
	_, err := c.Writer.Write([]byte("data: {}\n\n"))
	require.NoError(t, err)

	require.True(t, gate.Sent())
	require.Equal(t, "text/event-stream", rec.Header().Get("Content-Type"))
	require.Equal(t, "no-cache", rec.Header().Get("Cache-Control"))
	require.Equal(t, "keep-alive", rec.Header().Get("Connection"))
	require.Equal(t, "no", rec.Header().Get("X-Accel-Buffering"))
}

// CC 非流式客户端走的是 reqStream=true 分支（上游恒流式），收尾用
// c.Data(application/json)。延迟提交后该响应不应再带 SSE 头。
func TestSSEHeaderGateAllowsJSONBodyOnBufferedResponse(t *testing.T) {
	c, rec := newSSEGateTestContext()
	gate := newOpenAIWSSSEHeaderGate(c, nil, true)

	require.False(t, gate.Sent())
	c.Data(http.StatusOK, "application/json", []byte(`{"object":"chat.completion"}`))

	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
}

// ensure 幂等：多次写出不得重复追加头。
func TestSSEHeaderGateEnsureIdempotent(t *testing.T) {
	c, rec := newSSEGateTestContext()
	gate := newOpenAIWSSSEHeaderGate(c, nil, true)

	gate.ensure()
	gate.ensure()
	gate.ensure()

	require.Len(t, rec.Header().Values("Content-Type"), 1)
	require.Len(t, rec.Header().Values("Cache-Control"), 1)
}

// 非流式请求整体 no-op，调用方无需自己判断形态。
func TestSSEHeaderGateNoopWhenNotStreaming(t *testing.T) {
	c, rec := newSSEGateTestContext()
	gate := newOpenAIWSSSEHeaderGate(c, nil, false)

	gate.ensure()
	require.False(t, gate.Sent())
	require.Empty(t, rec.Header().Get("Content-Type"))

	c.JSON(http.StatusOK, gin.H{"ok": true})
	require.Equal(t, "application/json; charset=utf-8", rec.Header().Get("Content-Type"))
}

func TestSSEHeaderGateNilSafe(t *testing.T) {
	var gate *openAIWSSSEHeaderGate
	require.NotPanics(t, func() { gate.ensure() })
	require.False(t, gate.Sent())
}
