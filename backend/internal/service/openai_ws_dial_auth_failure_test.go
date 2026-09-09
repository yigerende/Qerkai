package service

import (
	"errors"
	"net/http"
	"testing"
)

// WS 握手账号级错误处理测试。
// 新增文件，上游不存在，不产生合并冲突。

func dialErrWith(status int, hdr http.Header) *openAIWSDialError {
	return &openAIWSDialError{
		StatusCode:      status,
		ResponseHeaders: hdr,
		ResponseBody:    []byte(`{"error":{"message":"boom"}}`),
	}
}

// TestDialAccountErrorStatusCoverage 覆盖范围必须与 HTTP 侧
// HandleUpstreamError 的 case 分支一致：401/402/403/429/529。
func TestDialAccountErrorStatusCoverage(t *testing.T) {
	upstreamHdr := http.Header{"X-Request-Id": []string{"rid-1"}}
	for _, status := range []int{401, 402, 403} {
		_, got, ok := openAIWSDialAccountErrorStatus(dialErrWith(status, upstreamHdr))
		if !ok || got != status {
			t.Fatalf("status %d must be handled as an account error (ok=%v got=%d)", status, ok, got)
		}
	}
	// 不接管：429/529 上游握手路径已有既定语义（落库限流信号并直接返回客户端），
	// 其余非账号级错误交给既有的重连/回退逻辑。
	for _, status := range []int{400, 404, 426, 429, 500, 502, 529} {
		if _, _, ok := openAIWSDialAccountErrorStatus(dialErrWith(status, upstreamHdr)); ok {
			t.Fatalf("status %d must not be treated as an account error", status)
		}
	}
	if _, _, ok := openAIWSDialAccountErrorStatus(nil); ok {
		t.Fatal("nil must not be an account error")
	}
	if _, _, ok := openAIWSDialAccountErrorStatus(errors.New("plain")); ok {
		t.Fatal("plain error must not be an account error")
	}
}

// TestEdgeRateLimited403IsNotAccountError 这是本文件最关键的一条边界：
// Cloudflare 边缘限速的 403（server=cloudflare 且无 x-request-id）说明请求
// 从未到达 OpenAI、凭证未被校验，必须交给 dial limiter 退避重试。
// 若在此标记账号，健康账号会因握手限速被误伤下线。
func TestEdgeRateLimited403IsNotAccountError(t *testing.T) {
	edge := dialErrWith(403, http.Header{"Server": []string{"cloudflare"}})
	if !isOpenAIWSDialEdgeRateLimited(edge) {
		t.Fatal("cloudflare 403 without x-request-id must be classified as edge rate limiting")
	}
	if _, _, ok := openAIWSDialAccountErrorStatus(edge); ok {
		t.Fatal("edge rate limited 403 must not trigger account handling")
	}

	// 携带 x-request-id 说明是上游真实 403 —— 属于账号级问题。
	real403 := dialErrWith(403, http.Header{
		"Server":       []string{"cloudflare"},
		"X-Request-Id": []string{"rid-real"},
	})
	if isOpenAIWSDialEdgeRateLimited(real403) {
		t.Fatal("403 carrying x-request-id is an upstream response, not edge rate limiting")
	}
	if _, got, ok := openAIWSDialAccountErrorStatus(real403); !ok || got != 403 {
		t.Fatal("real upstream 403 must be handled as an account error")
	}
}

// TestDialAccountErrorProducesFailover 必须产出 *UpstreamFailoverError：
// handler 的重试循环以 errors.As(err, &failoverErr) 为门票，拿不到这个类型
// 请求就不会换账号，而是直接把错误甩给客户端。
func TestDialAccountErrorProducesFailover(t *testing.T) {
	for _, status := range []int{401, 402, 403} {
		err := wrapOpenAIWSDialAccountErrorFailover(status, dialErrWith(status, nil))
		if err == nil {
			t.Fatalf("status %d: expected an error", status)
		}
		var failoverErr *UpstreamFailoverError
		if !errors.As(err, &failoverErr) {
			t.Fatalf("status %d: must be *UpstreamFailoverError, got %T", status, err)
		}
		if failoverErr.StatusCode != status {
			t.Fatalf("StatusCode = %d, want %d", failoverErr.StatusCode, status)
		}
		if !failoverErr.ShouldRetryNextAccount() {
			t.Fatalf("status %d must allow switching to another account", status)
		}
	}
}

// TestDialUnauthorizedHelperStillWorks 401 专用判定保持可用。
func TestDialUnauthorizedHelperStillWorks(t *testing.T) {
	if _, ok := isOpenAIWSDialUnauthorized(dialErrWith(401, nil)); !ok {
		t.Fatal("401 must be recognized")
	}
	if _, ok := isOpenAIWSDialUnauthorized(dialErrWith(403, nil)); ok {
		t.Fatal("403 must not be recognized as unauthorized")
	}
}

// TestDialAccountErrorNilSafe 依赖缺失时退化为不处理，不 panic。
func TestDialAccountErrorNilSafe(t *testing.T) {
	if err := wrapOpenAIWSDialAccountErrorFailover(401, nil); err != nil {
		t.Fatal("nil dial error must yield nil")
	}
	if err := wrapOpenAIWSDialAccountErrorFailover(0, dialErrWith(401, nil)); err != nil {
		t.Fatal("zero status must yield nil")
	}
	svc := &OpenAIGatewayService{}
	if svc.handleOpenAIWSDialAccountError(t.Context(), &Account{ID: 1}, "m", 401, dialErrWith(401, nil)) {
		t.Fatal("missing rateLimitService must not report a disable")
	}
	if svc.handleOpenAIWSDialAccountError(t.Context(), nil, "m", 401, dialErrWith(401, nil)) {
		t.Fatal("nil account must be a no-op")
	}
}

// TestEdgeRateLimited403Failover 边缘限速 403 必须换账号但不走账号处理。
//
// 两件事都要成立：
//   - 产出 *UpstreamFailoverError（否则 handler 不换账号，请求白白失败）
//   - 不被 openAIWSDialAccountErrorStatus 认领（否则健康账号会被误标记下线）
func TestEdgeRateLimited403Failover(t *testing.T) {
	edge := dialErrWith(403, http.Header{"Server": []string{"cloudflare"}})

	got, ok := isOpenAIWSDialEdgeRateLimitedError(edge)
	if !ok || got == nil {
		t.Fatal("edge rate limited 403 must be recognized from the error chain")
	}

	err := wrapOpenAIWSEdgeRateLimitedFailover(edge)
	if err == nil {
		t.Fatal("expected a failover error")
	}
	var failoverErr *UpstreamFailoverError
	if !errors.As(err, &failoverErr) {
		t.Fatalf("must be *UpstreamFailoverError, got %T", err)
	}
	if !failoverErr.ShouldRetryNextAccount() {
		t.Fatal("edge rate limiting must allow switching to another account")
	}
	// 同账号刚被限速，立刻重试没有意义。
	if failoverErr.RetryableOnSameAccount {
		t.Fatal("must not retry on the same account right after being rate limited")
	}
	// 关键边界：不能被账号处理链路认领。
	if _, _, claimed := openAIWSDialAccountErrorStatus(edge); claimed {
		t.Fatal("edge rate limited 403 must not be handled as an account error")
	}
}

// TestEdgeRateLimitedHelperRejectsOthers 非边缘限速的错误不得命中该分支。
func TestEdgeRateLimitedHelperRejectsOthers(t *testing.T) {
	// 上游真实 403（带 x-request-id）：属于账号级，不走边缘分支。
	real403 := dialErrWith(403, http.Header{
		"Server":       []string{"cloudflare"},
		"X-Request-Id": []string{"rid"},
	})
	if _, ok := isOpenAIWSDialEdgeRateLimitedError(real403); ok {
		t.Fatal("upstream 403 with x-request-id must not be edge rate limiting")
	}
	for _, status := range []int{401, 402, 429, 500} {
		if _, ok := isOpenAIWSDialEdgeRateLimitedError(dialErrWith(status, http.Header{"Server": []string{"cloudflare"}})); ok {
			t.Fatalf("status %d must not be edge rate limiting", status)
		}
	}
	if _, ok := isOpenAIWSDialEdgeRateLimitedError(nil); ok {
		t.Fatal("nil must not be edge rate limiting")
	}
	if err := wrapOpenAIWSEdgeRateLimitedFailover(nil); err != nil {
		t.Fatal("nil dial error must yield nil")
	}
}
