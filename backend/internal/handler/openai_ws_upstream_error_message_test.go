package handler

// WS 上游错误原文保真的端到端验证。
// 新增文件，上游不存在，不产生合并冲突。

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

func TestE2EWSUpstreamMessageReachesClient(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const upstream = "Our servers are currently overloaded. Please try again later."

	run := func(t *testing.T, name string, err error, streamStarted bool) string {
		t.Helper()
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
		h := &OpenAIGatewayHandler{}
		wrote := h.ensureForwardErrorResponseWithErr(c, streamStarted, err)
		body := rec.Body.String()
		t.Logf("[%s] wrote=%v\n%s", name, wrote, body)
		return body
	}

	wsErr := func() error {
		return service.WrapOpenAIWSClientVisibleErrorForTest(
			upstream, "server_is_overloaded", http.StatusBadGateway,
			fmt.Errorf("openai ws error event: %s", upstream))
	}

	// --- 非流式 JSON ---
	body := run(t, "WS降载/非流式", wsErr(), false)
	if !strings.Contains(body, upstream) {
		t.Fatalf("客户端未收到上游原文: %s", body)
	}
	if strings.Contains(body, "Upstream request failed") {
		t.Fatalf("兜底文案仍出现: %s", body)
	}
	if strings.Contains(body, "server_is_overloaded") {
		t.Fatalf("降载码未改写: %s", body)
	}

	// --- 流式 SSE：这是用户实际场景，走 writeResponsesFailedSSE 合成 response.failed ---
	body = run(t, "WS降载/流式SSE", wsErr(), true)
	if !strings.Contains(body, "event: response.failed") {
		t.Fatalf("流式必须发 response.failed 终止事件: %s", body)
	}
	if !strings.Contains(body, upstream) {
		t.Fatalf("流式下客户端未收到上游原文: %s", body)
	}
	if strings.Contains(body, "Upstream request failed") {
		t.Fatalf("流式下兜底文案仍出现: %s", body)
	}

	// --- 回归：非 WS 错误必须保持原行为 ---
	body = run(t, "普通错误/流式", errors.New("some internal failure"), true)
	if !strings.Contains(body, "Upstream request failed") {
		t.Fatalf("普通错误应保持兜底文案: %s", body)
	}
	body = run(t, "nil错误", nil, false)
	if !strings.Contains(body, "Upstream request failed") {
		t.Fatalf("nil 应保持兜底文案: %s", body)
	}
}

// TestE2EWSFallbackReasonReachesClient 覆盖 #3 当初漏掉的出口。
//
// #3 只包装了 error 事件那一条路径，dial 失败 / 读失败 / fallback 出口仍在丢
// 原因：生产实测 22.4% 的客户端可见错误是 "Upstream request failed"，
// stream=true、attempts=0、usage 全 0，真实原因（read_event、acquire_timeout
// 等）整体丢失。
func TestE2EWSFallbackReasonReachesClient(t *testing.T) {
	gin.SetMode(gin.TestMode)

	cases := []struct {
		name    string
		message string
		errType string
		status  int
		want    []string
	}{
		{
			name:    "读失败/连接被上游keepalive关闭",
			message: `Upstream websocket closed before sending a response (read_event): received close frame: reason = "keepalive ping timeout"`,
			errType: "upstream_error",
			status:  http.StatusBadGateway,
			want:    []string{"read_event", "keepalive ping timeout"},
		},
		{
			name:    "连接池获取超时",
			message: "Upstream websocket connection could not be acquired in time (acquire_timeout): context deadline exceeded",
			errType: "server_error",
			status:  http.StatusServiceUnavailable,
			want:    []string{"acquire_timeout", "context deadline exceeded"},
		},
	}

	for _, tc := range cases {
		for _, streamStarted := range []bool{false, true} {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			h := &OpenAIGatewayHandler{}

			err := service.WrapOpenAIWSClientVisibleErrorForTest(
				tc.message, "", tc.status, errors.New("openai ws fallback"))
			// errType 由 service 侧分类表决定；这里断言最终落到客户端的文案。
			_ = tc.errType

			h.ensureForwardErrorResponseWithErr(c, streamStarted, err)
			body := rec.Body.String()

			for _, want := range tc.want {
				if !strings.Contains(body, want) {
					t.Fatalf("[%s stream=%v] 客户端未收到 %q:\n%s", tc.name, streamStarted, want, body)
				}
			}
			if strings.Contains(body, "Upstream request failed") {
				t.Fatalf("[%s stream=%v] 兜底文案仍出现:\n%s", tc.name, streamStarted, body)
			}
			if streamStarted && !strings.Contains(body, "event: response.failed") {
				t.Fatalf("[%s] 流式必须发 response.failed 终止事件:\n%s", tc.name, body)
			}
		}
	}
}
