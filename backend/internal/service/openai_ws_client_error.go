package service

import (
	"errors"
	"net/http"
	"strings"
)

// OpenAI WS 上游错误信息保真。
//
// 二次开发新增文件，上游不存在，不产生合并冲突。
//
// 背景：WS v2 转发器收到上游 error 事件时，内部 errMsg 一直带着上游原文
// （例如 "Our servers are currently overloaded. Please try again later."），
// 但出口只返回 fmt.Errorf("openai ws error event: %s", errMsg)——一个裸 error。
// handler 侧 ensureForwardErrorResponse 的签名不接收 err，只能硬编码兜底文案
// "Upstream request failed"，上游原文在这一步丢失。
//
// HTTP SSE 链路没有这个问题：它构造 *UpstreamFailoverError 并填好 ClientMessage，
// handler 的 IsOpenAICapacityShed() 分支会原样透传原文。本文件补齐 WS 侧。
//
// 刻意不改动 error 的类型链语义：openAIWSClientVisibleError 实现 Unwrap，
// 现有 errors.As/errors.Is 判定（重试、failover、fallback 分类）完全不受影响。
// 本改动只影响「兜底文案」这一步，不改变任何重试或调度行为。

// openAIWSClientVisibleError 携带一份可直接展示给客户端的上游错误描述。
type openAIWSClientVisibleError struct {
	// Message 上游原文，已 TrimSpace；为空时本类型不会被构造。
	Message string
	// ErrType 客户端错误类型（如 upstream_error / server_error / rate_limit_error）。
	ErrType string
	// StatusCode 网关回给客户端的 HTTP 状态码。
	StatusCode int
	// Err 原始错误，保持类型链不变。
	Err error
}

func (e *openAIWSClientVisibleError) Error() string {
	if e == nil || e.Err == nil {
		return ""
	}
	return e.Err.Error()
}

func (e *openAIWSClientVisibleError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// wrapOpenAIWSClientVisibleError 把上游原文附到 err 上。
// message 为空时原样返回 err——没有原文可保真，不必包一层。
func wrapOpenAIWSClientVisibleError(message, code string, statusCode int, err error) error {
	message = strings.TrimSpace(message)
	if message == "" || err == nil {
		return err
	}
	if statusCode <= 0 {
		statusCode = http.StatusBadGateway
	}
	return &openAIWSClientVisibleError{
		Message:    message,
		ErrType:    openAIWSClientErrorType(code, statusCode),
		StatusCode: statusCode,
		Err:        err,
	}
}

// openAIWSClientErrorType 决定回给客户端的错误类型。
//
// 容量降载码（server_is_overloaded / slow_down）必须映射成 server_error：
// Codex CLI 对这两个码判致命并直接终止会话，对 server_error 则走内置退避重试。
// 上游在 HTTP SSE / ingress / http_bridge 三条链路都做了这个改写
// （sanitizeOpenAICapacityShedErrorCodeForClient），唯独 WS v2 没有，这里补齐。
// 与上游那个函数语义一致：只改类型/码，不动 message。
func openAIWSClientErrorType(code string, statusCode int) string {
	switch strings.ToLower(strings.TrimSpace(code)) {
	case "server_is_overloaded", "slow_down":
		return "server_error"
	}
	if statusCode == http.StatusTooManyRequests {
		return "rate_limit_error"
	}
	return "upstream_error"
}

// OpenAIWSClientVisibleMessage 从 err 链中取出可展示给客户端的上游错误。
// handler 包通过这个导出函数消费；ok=false 时调用方保持原有兜底行为。
func OpenAIWSClientVisibleMessage(err error) (message string, errType string, statusCode int, ok bool) {
	if err == nil {
		return "", "", 0, false
	}
	var visible *openAIWSClientVisibleError
	if !errors.As(err, &visible) || visible == nil {
		return "", "", 0, false
	}
	message = strings.TrimSpace(visible.Message)
	if message == "" {
		return "", "", 0, false
	}
	errType = visible.ErrType
	if errType == "" {
		errType = "upstream_error"
	}
	statusCode = visible.StatusCode
	if statusCode <= 0 {
		statusCode = http.StatusBadGateway
	}
	return message, errType, statusCode, true
}

// WrapOpenAIWSClientVisibleErrorForTest 仅供跨包测试构造该错误类型。
func WrapOpenAIWSClientVisibleErrorForTest(message, code string, statusCode int, err error) error {
	return wrapOpenAIWSClientVisibleError(message, code, statusCode, err)
}
