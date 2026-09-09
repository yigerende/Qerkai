package service

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// WS error 事件的 failover 判定。
//
// 二次开发新增文件，上游不存在，不产生合并冲突。
//
// 问题：连接建立后上游推来的 error 事件（如 code=server_is_overloaded、
// type=service_unavailable_error 的容量降载），在 WS v2 里被
// classifyOpenAIWSErrorEventFromRaw 判为 reason="event_error"、canFallback=false
// ——既不同账号重试也不换账号，错误直接甩给客户端。
//
// 实测同一 payload 两条链路结论相反：
//   WS  classifyOpenAIWSErrorEventFromRaw      -> canFallback=false
//   HTTP openAIStreamErrorEventShouldFailover  -> true
//
// 本文件把 HTTP SSE 那套判定移植到 WS。三个关键函数都是纯函数（只吃 payload
// 与 message，不依赖 *http.Response），且 openai_ws_http_bridge.go 已有先例
// ——上游自己在 bridge 路径做过这个移植，唯独 v2 转发器漏了。
//
// 处置顺序与 CPA 一致（sdk/cliproxy/auth/same_credential_retry.go 的注释：
// "retries on the same credential before the caller falls back to credential
// switching"）：先同账号退避重试保住 prompt 缓存亲和性，再换账号。
// sub2api 侧这两级都由 newOpenAIStreamFailoverErrorWithModel 构造的
// UpstreamFailoverError 承载——降载会被识别为 requestScopedCapacity，
// 从而同时置上 RetryableOnSameAccount 与 ShouldRetryNextAccount。

// shouldFailoverOpenAIWSErrorEvent 判断 WS error 事件是否应触发 failover。
//
// wroteDownstream 为真时一律返回 false：已经给客户端写过内容再重试会产生
// 重复输出与重复计费，这条守卫优先于任何重试判定。
func shouldFailoverOpenAIWSErrorEvent(payload []byte, message string, wroteDownstream bool) bool {
	if wroteDownstream || len(payload) == 0 {
		return false
	}
	// 限流类 error 事件不接管：上游 WS 路径对它已有既定语义——
	// persistOpenAIWSRateLimitSignal 把 resets_at 等信号落库，并直接把 429
	// 返回客户端（见 openai_ws_ratelimit_signal_test.go 的断言）。
	// 改成 failover 会破坏该契约，且限流本就该让客户端看到以便自行退避。
	if isOpenAIWSErrorEventRateLimited(payload) {
		return false
	}
	// 二次开发：账号模型无权限要换号。上游那套判定对它返回 false
	// （invalid_request 不在任何可 failover 分支里），导致请求在第一个账号上
	// 就死、账号也不被记住，客户端重试等于重新抽签。
	// 详见 openai_model_access_denied.go。
	if isOpenAIModelAccessDeniedError(payload, message) {
		return true
	}
	// 其余直接复用 HTTP SSE 的判定，不另写一套：cyber_policy、上下文超限、
	// 访问态错误、403 账号故障、瞬态处理错误等例外都在里面，复制必然漂移。
	return openAIStreamErrorEventShouldFailover(payload, message)
}

// isOpenAIWSErrorEventRateLimited 判断 error 事件是否为限流。
//
// 复用上游既有的 isOpenAIWSRateLimitError，与 v2 里
// persistOpenAIWSRateLimitSignal 走的是同一判据，不会出现「一边落库限流
// 信号、一边又 failover」的分裂。
func isOpenAIWSErrorEventRateLimited(payload []byte) bool {
	code, errType, msg := parseOpenAIWSErrorEventFields(payload)
	return isOpenAIWSRateLimitError(code, errType, msg)
}

// newOpenAIWSErrorEventFailover 为 WS error 事件构造 failover 错误。
//
// 复用 newOpenAIStreamFailoverErrorWithModel 而非自行构造 UpstreamFailoverError：
// 降载识别（isOpenAIRequestScopedCapacityShed）、ClientMessage 填充、
// RetryableOnSameAccount 置位、ops 错误记录都在那里，且与 HTTP 链路共用同一
// 实现，语义天然一致。
//
// 构造完成后过一遍 applyOpenAICapacityShedBudget：重试的次数预算在慢失败下
// 会退化成分钟级等待（生产实测 max 544.5s），需要墙钟封顶。详见
// openai_ws_capacity_budget.go。
func (s *OpenAIGatewayService) newOpenAIWSErrorEventFailover(
	c *gin.Context,
	account *Account,
	requestID string,
	payload []byte,
	message string,
	mappedModel string,
	handshakeHeaders http.Header,
) error {
	if s == nil {
		return nil
	}
	failoverErr := s.newOpenAIStreamFailoverErrorWithModel(
		c, account, true, requestID, payload, message, mappedModel, handshakeHeaders,
	)
	// 二次开发：模型无权限记入 (账号, 模型) 黑名单，并放宽换号预算。
	// 详见 openai_model_access_denied.go。
	s.applyOpenAIModelAccessDenied(account, mappedModel, payload, message, failoverErr)
	return applyOpenAICapacityShedBudget(c, failoverErr)
}
