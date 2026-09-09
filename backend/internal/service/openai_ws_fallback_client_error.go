package service

import (
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// WS fallback 错误的客户端可见化。
//
// 二次开发新增文件，上游不存在，不产生合并冲突。
//
// ## 问题
//
// 生产实测（1 小时窗口，客户端可见错误 500 条样本）：
//
//	104 条  502 | Upstream request failed        ← 22.4%
//	  6 条  503 | Upstream request failed
//	  2 条  400 | Upstream request failed
//
// 全部为 stream=true（97 条 /v1/responses、13 条 /v1/chat/completions），
// ops 明细里 attempts=0、ttft=null、usage 全 0，耗时 3.9s~47.6s，
// 散布在 20 多个不同账号——不是账号问题，是错误原因被整体丢弃。
//
// ## 根因
//
// resolveOpenAIWSFallbackErrorResponse 的 switch 只覆盖六个 reason
// （invalid_encrypted_content / previous_response_not_found /
// upgrade_required / ws_unsupported / auth_failed / upstream_rate_limited），
// 其余全部落到 default；default 分支在拿不到 dialErr 状态码时返回 ok=false：
//
//	default:
//	    if statusCode == 0 {
//	        return 0, "", "", "", false
//	    }
//
// 于是 writeOpenAIWSFallbackErrorResponse 直接返回 false，既不写响应也不记
// ops（这解释了 attempts=0），错误裸着传回 handler，由
// ensureForwardErrorResponseWithErr 的硬编码兜底写出：
//
//	status, errType, message := http.StatusBadGateway, "upstream_error", "Upstream request failed"
//
// 落到 default 且没有 dialErr 状态码的 reason 包括：read_event、
// acquire_timeout、acquire_conn、conn_queue_full、preferred_conn_unavailable、
// write_request、missing_final_response、invalid_event_json、
// streaming_not_supported、retry_backoff_canceled，以及本项目新增的
// read_event_stale_pooled_conn。这些恰恰是 WS 链路最常见的失败形态。
//
// 二次开发的 #3（openai_ws_client_error.go）只把 error 事件那一条路径的原文
// 保住了，dial 失败、读失败、fallback 这些出口仍在丢——本文件补齐剩余出口。
//
// ## 修法
//
// 不扩写上游那个 switch（它是上游文件，且其 default 的「无状态码即放弃」
// 语义在 HTTP 侧另有用途），而是在 WS 失败出口处：上游写出器放弃后，把
// wsErr 包成 openAIWSClientVisibleError，交给 handler 已有的
// OpenAIWSClientVisibleMessage 通路展示——这条通路是 #3 建好的，直接复用。
//
// 消息内容坚持两条：
//   - 不编造上游文案。底层 err 文本才是真实原因（keepalive ping timeout、
//     context deadline exceeded 等），沿用并 sanitize，不替换成想象的措辞。
//   - 保留 reason token。ops 与客户端都能据此定位到具体失败形态，
//     这正是当前 "Upstream request failed" 拿不到的东西。

// openAIWSFallbackClientErrorClass 描述一类 fallback reason 的客户端语义。
type openAIWSFallbackClientErrorClass struct {
	statusCode int
	errType    string
	// summary 是给客户端的简短说明，回答「网关这边发生了什么」。
	// 底层 err 文本作为原因附在其后，两者组合才是完整信息。
	summary string
}

// openAIWSFallbackClientErrorClasses 把 reason 映射到客户端语义。
//
// 状态码的选取原则是「让客户端做出正确反应」：
//   - 503 用于网关侧容量问题（连接池排队/耗尽）。客户端应退避重试，
//     而不是把它当成请求本身有问题。
//   - 502 用于上游连接层故障（连着但读不到/写不出）。请求可安全重试。
//   - 500 用于网关自身能力缺失（下游不支持流式），重试无用。
//
// retry_backoff_canceled 刻意不在表内：它意味着客户端自己断开了连接，
// 此时没有下游可写，包装一层客户端可见错误没有意义。
var openAIWSFallbackClientErrorClasses = map[string]openAIWSFallbackClientErrorClass{
	"acquire_timeout": {
		statusCode: http.StatusServiceUnavailable,
		errType:    "server_error",
		summary:    "Upstream websocket connection could not be acquired in time",
	},
	"acquire_conn": {
		statusCode: http.StatusServiceUnavailable,
		errType:    "server_error",
		summary:    "Upstream websocket connection could not be acquired",
	},
	"conn_queue_full": {
		statusCode: http.StatusServiceUnavailable,
		errType:    "server_error",
		summary:    "Upstream websocket connection queue is full",
	},
	"preferred_conn_unavailable": {
		statusCode: http.StatusServiceUnavailable,
		errType:    "server_error",
		summary:    "Upstream websocket session connection is no longer available",
	},
	"dial_failed": {
		statusCode: http.StatusBadGateway,
		errType:    "upstream_error",
		summary:    "Upstream websocket handshake failed",
	},
	"read_event": {
		statusCode: http.StatusBadGateway,
		errType:    "upstream_error",
		summary:    "Upstream websocket closed before sending a response",
	},
	openAIWSStaleConnReason: {
		statusCode: http.StatusBadGateway,
		errType:    "upstream_error",
		summary:    "Upstream websocket connection expired before sending a response",
	},
	"write_request": {
		statusCode: http.StatusBadGateway,
		errType:    "upstream_error",
		summary:    "Upstream websocket request could not be sent",
	},
	"write": {
		statusCode: http.StatusBadGateway,
		errType:    "upstream_error",
		summary:    "Upstream websocket request could not be sent",
	},
	"invalid_event_json": {
		statusCode: http.StatusBadGateway,
		errType:    "upstream_error",
		summary:    "Upstream websocket returned a malformed response event",
	},
	"missing_final_response": {
		statusCode: http.StatusBadGateway,
		errType:    "upstream_error",
		summary:    "Upstream websocket finished without a final response",
	},
	"streaming_not_supported": {
		statusCode: http.StatusInternalServerError,
		errType:    "server_error",
		summary:    "Streaming is not supported on this connection",
	},
}

// openAIWSFallbackClientErrorMessage 组装客户端可见文案。
//
// 返回空串表示该 reason 不做包装，调用方保持原有行为。
func openAIWSFallbackClientErrorMessage(err error) (message string, errType string, statusCode int, reason string) {
	if err == nil {
		return "", "", 0, ""
	}
	var fallbackErr *openAIWSFallbackError
	if !errors.As(err, &fallbackErr) || fallbackErr == nil {
		return "", "", 0, ""
	}
	rawReason := strings.TrimSpace(fallbackErr.Reason)
	// prewarm_ 前缀只表示失败发生在预热连接上，客户端语义与常规路径一致。
	lookupReason := strings.TrimPrefix(rawReason, "prewarm_")
	class, ok := openAIWSFallbackClientErrorClasses[lookupReason]
	if !ok {
		return "", "", 0, ""
	}
	// 底层原因带上，且带上 reason token：这两样正是兜底文案丢掉的信息。
	detail := ""
	if fallbackErr.Err != nil {
		detail = sanitizeUpstreamErrorMessage(strings.TrimSpace(fallbackErr.Err.Error()))
	}
	message = class.summary + " (" + rawReason + ")"
	if detail != "" {
		message += ": " + detail
	}
	return message, class.errType, class.statusCode, rawReason
}

// wrapOpenAIWSFallbackClientError 让 fallback 错误携带客户端可见原因，
// 并补记 ops 归属（上游写出器放弃时连 ops 也不记，明细里 attempts 恒为 0）。
//
// 返回原 err 表示不需要包装。
func (s *OpenAIGatewayService) wrapOpenAIWSFallbackClientError(
	c *gin.Context,
	account *Account,
	wsErr error,
) error {
	message, errType, statusCode, reason := openAIWSFallbackClientErrorMessage(wsErr)
	if message == "" {
		return wsErr
	}
	if c != nil {
		setOpsUpstreamError(c, statusCode, message, "")
		if account != nil {
			proxyID, proxyName := opsUpstreamWSProxyAttribution(account)
			appendOpsUpstreamError(c, OpsUpstreamErrorEvent{
				ProxyID:            proxyID,
				ProxyName:          proxyName,
				Platform:           account.Platform,
				AccountID:          account.ID,
				AccountName:        account.Name,
				UpstreamStatusCode: statusCode,
				Kind:               "ws_error",
				Reason:             reason,
				Message:            message,
			})
		}
	}
	// 复用 #3 的通路：handler 的 ensureForwardErrorResponseWithErr 会通过
	// OpenAIWSClientVisibleMessage 取出该消息，替换硬编码兜底文案。
	// errType 已由本文件的分类表决定，这里传空 code 避免被重新推断。
	return &openAIWSClientVisibleError{
		Message:    message,
		ErrType:    errType,
		StatusCode: statusCode,
		Err:        wsErr,
	}
}
