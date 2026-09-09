package service

import (
	"context"
	"errors"
	"net/http"
	"strings"
)

// WS 握手账号级错误（401/402/403/429/529）的账号处理与 failover。
//
// 二次开发新增文件，上游不存在，不产生合并冲突。
//
// 问题：WS 握手返回 401（token 失效）时，上游把它与 403 一起归入 auth_failed
// （openai_ws_forwarder_support.go 的 classifyOpenAIWSAcquireFailure），该分类
// 在重连表里不可重试，于是请求直接失败返回客户端；账号既不被标记，也不会
// failover。同一账号会被反复调度、每次都撞 401。
//
// 而 HTTP 链路对 401 有完整处理（ratelimit_service.go 的 HandleUpstreamError
// case 401）：区分 token_invalidated/token_revoked（永久禁用）、
// {"detail":"Unauthorized"}（永久禁用）、其余走临时不可调度并在重复 401 时
// 升级为 error。dial 路径本已调用 handleOpenAIWSDialTransientFailure，但它被
// shouldCooldownOpenAITransientUpstreamError 拦住——那个判定只放行 5xx 与部分
// 400，401 命中 default 分支返回 false。
//
// 本文件让 WS 握手 401 走与 HTTP 完全相同的账号处理链路，并把错误包装成
// UpstreamFailoverError 使请求切换到其他账号。
//
// 覆盖 401（凭证失效）、402（余额/计费）与上游真实的 403。
//
// 刻意不接管 429/529：上游握手路径对这两者已有既定语义——
// persistOpenAIWSRateLimitSignal 落库 x-codex-* 限流信号，并直接把 429
// 返回给客户端（见 openai_ws_ratelimit_signal_test.go 的断言）。
// 改成 failover 会破坏该契约，且限流信号本就该让客户端看到。
//
// 403 需要特别说明：它有两种来源。
//   - Cloudflare 边缘的握手速率限制（dial_resp_server=cloudflare 且无
//     x-request-id）：账号本身健康，由 openai_ws_dial_limiter.go 退避重试处理，
//     绝不能在此标记账号，否则健康账号会因限速被误伤。
//   - 上游真实的 403（携带 x-request-id）：属于账号级问题，与 HTTP 侧一致处理。
// 判据见 isOpenAIWSDialEdgeRateLimited。

// isOpenAIWSDialUnauthorized 判断握手错误是否为 401。
func isOpenAIWSDialUnauthorized(err error) (*openAIWSDialError, bool) {
	dialErr, ok := openAIWSDialErrorWithStatus(err, http.StatusUnauthorized)
	return dialErr, ok
}

// openAIWSDialErrorWithStatus 提取指定状态码的握手错误。
func openAIWSDialErrorWithStatus(err error, status int) (*openAIWSDialError, bool) {
	if err == nil {
		return nil, false
	}
	var dialErr *openAIWSDialError
	if !errors.As(err, &dialErr) || dialErr == nil {
		return nil, false
	}
	if dialErr.StatusCode != status {
		return nil, false
	}
	return dialErr, true
}

// isOpenAIWSDialEdgeRateLimited 判断 403 是否来自 Cloudflare 边缘的握手限速。
//
// 实测特征：dial_resp_server=cloudflare 且响应不含 x-request-id——说明请求
// 被边缘拦下、从未到达 OpenAI，账号凭证根本没被校验。这类 403 必须交给
// dial limiter 退避重试，不能标记账号。
func isOpenAIWSDialEdgeRateLimited(dialErr *openAIWSDialError) bool {
	if dialErr == nil || dialErr.StatusCode != http.StatusForbidden {
		return false
	}
	if dialErr.ResponseHeaders == nil {
		return false
	}
	if strings.TrimSpace(dialErr.ResponseHeaders.Get("x-request-id")) != "" {
		// 上游真实响应：属于账号级 403。
		return false
	}
	server := strings.ToLower(strings.TrimSpace(dialErr.ResponseHeaders.Get("server")))
	return strings.Contains(server, "cloudflare")
}

// isOpenAIWSDialEdgeRateLimitedError 从 error 链中识别边缘限速的 403。
func isOpenAIWSDialEdgeRateLimitedError(err error) (*openAIWSDialError, bool) {
	dialErr, ok := openAIWSDialErrorWithStatus(err, http.StatusForbidden)
	if !ok {
		return nil, false
	}
	if !isOpenAIWSDialEdgeRateLimited(dialErr) {
		return nil, false
	}
	return dialErr, true
}

// openAIWSDialAccountErrorStatus 返回该握手错误应交给账号处理链路的状态码。
//
// 第二个返回值为 false 表示不做账号处理（非账号级错误，或边缘限速的 403）。
func openAIWSDialAccountErrorStatus(err error) (*openAIWSDialError, int, bool) {
	if err == nil {
		return nil, 0, false
	}
	var dialErr *openAIWSDialError
	if !errors.As(err, &dialErr) || dialErr == nil {
		return nil, 0, false
	}
	switch dialErr.StatusCode {
	case http.StatusUnauthorized, http.StatusPaymentRequired:
		return dialErr, dialErr.StatusCode, true
	case http.StatusForbidden:
		if isOpenAIWSDialEdgeRateLimited(dialErr) {
			return nil, 0, false
		}
		return dialErr, dialErr.StatusCode, true
	}
	return nil, 0, false
}

// wrapOpenAIWSEdgeRateLimitedFailover 让边缘限速的 403 换账号，但不标记原账号。
//
// 边缘限速（Cloudflare 拦下、请求未达 OpenAI）说明该账号短时间内握手过多，
// 账号本身是健康的：实测同一 IP 下账号 A 被限时账号 B 握手照常成功。所以
//   - 不能调 HandleUpstreamError 标记账号（会误伤健康账号）
//   - 但应该 failover 到其他账号（否则请求白白失败）
//
// dial limiter 已在此之前做过退避重试（默认 3 次尝试、900ms + 1800ms 退避）；
// 走到这里说明该账号的限速窗口还没过去，换账号是唯一有意义的动作。
//
// 已知代价：每个账号都要付一遍退避成本（约 2.7s），最坏情况下
// gateway.max_account_switches（默认 10）个账号轮流试会把请求拖到 20s 以上，
// 客户端可能先超时。彻底解决需要给被限账号打短期 WS 黑名单，让后续请求直接
// 跳过它，那是独立的一件事。
func wrapOpenAIWSEdgeRateLimitedFailover(dialErr *openAIWSDialError) error {
	if dialErr == nil {
		return nil
	}
	return newOpenAIUpstreamFailoverError(
		http.StatusForbidden,
		dialErr.ResponseHeaders,
		dialErr.ResponseBody,
		"upstream websocket handshake rate limited at edge",
		// retryableOnSameAccount=false：同账号刚被限速，立刻重试没有意义，
		// 直接换账号。
		false,
	)
}

// handleOpenAIWSDialAccountError 对 WS 握手的账号级错误执行与 HTTP 一致的处理。
//
// 直接复用 RateLimitService.HandleUpstreamError，而不是另写一套判定：
// 永久禁用条件（token_invalidated / token_revoked / detail=Unauthorized）、
// 影子账号凭据归属、重复 401 升级为 error 等语义都在那里，复制一份必然漂移。
//
// 返回 true 表示账号已被标记为不可用（永久禁用）。
func (s *OpenAIGatewayService) handleOpenAIWSDialAccountError(
	ctx context.Context,
	account *Account,
	canonicalModel string,
	status int,
	dialErr *openAIWSDialError,
) bool {
	if s == nil || s.rateLimitService == nil || account == nil || dialErr == nil || status <= 0 {
		return false
	}
	return s.rateLimitService.HandleUpstreamError(
		ctx,
		account,
		status,
		dialErr.ResponseHeaders,
		dialErr.ResponseBody,
		canonicalModel,
	)
}

// wrapOpenAIWSDialAccountErrorFailover 把握手账号级错误包装成可 failover 的错误。
//
// 构造 UpstreamFailoverError 是让 handler 切换账号的唯一途径：
// openai_gateway_handler.go 的重试循环以 errors.As(err, &failoverErr) 为门票，
// 拿不到这个类型就不会换账号。
//
// ClientMessage 用通用描述：这些状态码的上游原文多为内部鉴权/计费细节，
// 不适合直接透给客户端；failover 耗尽时由 handler 的兜底文案接管。
func wrapOpenAIWSDialAccountErrorFailover(status int, dialErr *openAIWSDialError) error {
	if dialErr == nil || status <= 0 {
		return nil
	}
	msg := "upstream websocket handshake rejected"
	switch status {
	case http.StatusUnauthorized:
		msg = "upstream websocket authentication failed"
	case http.StatusPaymentRequired:
		msg = "upstream websocket payment required"
	}
	return newOpenAIUpstreamFailoverError(
		status,
		dialErr.ResponseHeaders,
		dialErr.ResponseBody,
		msg,
		false,
	)
}
