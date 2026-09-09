package service

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
)

// WS 上游错误原文保真测试。
// 新增文件，上游不存在，不产生合并冲突。

// TestWSClientVisibleErrorPreservesUpstreamMessage 核心用例：
// 上游降载原文必须能被取出，且错误码映射为 Codex 可重试的 server_error。
func TestWSClientVisibleErrorPreservesUpstreamMessage(t *testing.T) {
	const upstream = "Our servers are currently overloaded. Please try again later."
	base := fmt.Errorf("openai ws error event: %s", upstream)
	wrapped := wrapOpenAIWSClientVisibleError(upstream, "server_is_overloaded", http.StatusBadGateway, base)

	msg, errType, status, ok := OpenAIWSClientVisibleMessage(wrapped)
	if !ok {
		t.Fatal("must expose upstream message")
	}
	if msg != upstream {
		t.Fatalf("message = %q, want upstream original %q", msg, upstream)
	}
	// server_is_overloaded 对 Codex CLI 判致命；必须映射成 server_error 才会走退避重试。
	if errType != "server_error" {
		t.Fatalf("errType = %q, want server_error", errType)
	}
	if status != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", status)
	}
}

// TestWSClientVisibleErrorKeepsErrorChain 保证不破坏既有的 errors.As/Is 判定 ——
// 重试、failover、fallback 分类全部依赖类型链，这是本改动最重要的安全边界。
func TestWSClientVisibleErrorKeepsErrorChain(t *testing.T) {
	sentinel := errors.New("sentinel")
	fallback := wrapOpenAIWSFallback("event_error", sentinel)
	wrapped := wrapOpenAIWSClientVisibleError("boom", "server_error", http.StatusBadGateway, fallback)

	var fb *openAIWSFallbackError
	if !errors.As(wrapped, &fb) {
		t.Fatal("errors.As must still resolve openAIWSFallbackError through the wrapper")
	}
	if !errors.Is(wrapped, sentinel) {
		t.Fatal("errors.Is must still reach the root cause")
	}
	// 分类逻辑必须与包装前一致，否则会改变重试行为。
	if _, retryable := classifyOpenAIWSReconnectReason(wrapped); !retryable {
		t.Fatal("wrapping must not change reconnect classification")
	}
	if wrapped.Error() != fallback.Error() {
		t.Fatalf("Error() text changed: %q vs %q", wrapped.Error(), fallback.Error())
	}
}

// TestWSClientVisibleErrorNoopCases 空原文与 nil 不构造包装，保持上游默认行为。
func TestWSClientVisibleErrorNoopCases(t *testing.T) {
	base := errors.New("x")
	if got := wrapOpenAIWSClientVisibleError("", "code", 502, base); got != base {
		t.Fatal("empty message must return the original error unchanged")
	}
	if got := wrapOpenAIWSClientVisibleError("  ", "code", 502, base); got != base {
		t.Fatal("blank message must return the original error unchanged")
	}
	if got := wrapOpenAIWSClientVisibleError("msg", "code", 502, nil); got != nil {
		t.Fatal("nil error must stay nil")
	}
	if _, _, _, ok := OpenAIWSClientVisibleMessage(nil); ok {
		t.Fatal("nil error exposes no message")
	}
	if _, _, _, ok := OpenAIWSClientVisibleMessage(errors.New("plain")); ok {
		t.Fatal("plain error exposes no message")
	}
}

// TestWSClientErrorTypeMapping 错误码到客户端类型的映射。
func TestWSClientErrorTypeMapping(t *testing.T) {
	cases := []struct {
		code   string
		status int
		want   string
	}{
		{"server_is_overloaded", http.StatusBadGateway, "server_error"},
		{"SERVER_IS_OVERLOADED", http.StatusBadGateway, "server_error"},
		{"slow_down", http.StatusBadGateway, "server_error"},
		{"rate_limit_exceeded", http.StatusTooManyRequests, "rate_limit_error"},
		{"", http.StatusBadGateway, "upstream_error"},
		{"unknown_code", http.StatusBadGateway, "upstream_error"},
	}
	for _, tc := range cases {
		if got := openAIWSClientErrorType(tc.code, tc.status); got != tc.want {
			t.Fatalf("openAIWSClientErrorType(%q, %d) = %q, want %q", tc.code, tc.status, got, tc.want)
		}
	}
}
