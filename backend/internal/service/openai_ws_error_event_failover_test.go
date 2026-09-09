package service

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// WS error 事件 failover 测试。
// 新增文件，上游不存在，不产生合并冲突。

const overloadEventPayload = `{"error":{"code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later.","param":null,"type":"service_unavailable_error"},"sequence_number":2,"type":"error"}`

const overloadEventMessage = "Our servers are currently overloaded. Please try again later."

// TestOverloadEventTriggersFailover 核心用例：容量降载必须触发 failover。
// 这正是生产上反复出现、此前完全不重试的那个 payload。
func TestOverloadEventTriggersFailover(t *testing.T) {
	if !shouldFailoverOpenAIWSErrorEvent([]byte(overloadEventPayload), overloadEventMessage, false) {
		t.Fatal("capacity shed error event must trigger failover")
	}
}

// TestWroteDownstreamBlocksFailover 最重要的安全边界：
// 已向客户端写过内容后重试会产生重复输出与重复计费，必须一律拒绝，
// 且这条守卫要优先于任何重试判定。
func TestWroteDownstreamBlocksFailover(t *testing.T) {
	if shouldFailoverOpenAIWSErrorEvent([]byte(overloadEventPayload), overloadEventMessage, true) {
		t.Fatal("must never retry after downstream output has started")
	}
}

// TestEmptyPayloadNoFailover 空载荷不触发。
func TestEmptyPayloadNoFailover(t *testing.T) {
	if shouldFailoverOpenAIWSErrorEvent(nil, "", false) {
		t.Fatal("empty payload must not trigger failover")
	}
}

// TestNonRetryableEventsNoFailover 不可重试的错误不得触发 failover ——
// 上下文超限重试多少次都一样，只会浪费配额与延迟。
func TestNonRetryableEventsNoFailover(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		message string
	}{
		{
			"上下文超限",
			`{"type":"error","error":{"code":"context_length_exceeded","type":"invalid_request_error","message":"maximum context length exceeded"}}`,
			"maximum context length exceeded",
		},
		{
			"普通请求错误",
			`{"type":"error","error":{"code":"invalid_request","type":"invalid_request_error","message":"bad field"}}`,
			"bad field",
		},
	}
	for _, tc := range cases {
		if shouldFailoverOpenAIWSErrorEvent([]byte(tc.payload), tc.message, false) {
			t.Fatalf("%s: must not trigger failover", tc.name)
		}
	}
}

// TestFailoverErrorCarriesBothRetryLevels 验证构造出的错误同时具备两级重试：
// 先同账号退避重试（保住 prompt 缓存亲和性），再换账号 —— 与 CPA 的
// same-credential-then-rotate 顺序一致。
func TestFailoverErrorCarriesBothRetryLevels(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)

	svc := &OpenAIGatewayService{}
	err := svc.newOpenAIWSErrorEventFailover(
		c,
		&Account{ID: 3, Platform: PlatformOpenAI, Type: AccountTypeOAuth},
		"rid-1",
		[]byte(overloadEventPayload),
		overloadEventMessage,
		"gpt-5.5",
		http.Header{},
	)
	if err == nil {
		t.Fatal("expected a failover error")
	}
	failoverErr, ok := err.(*UpstreamFailoverError)
	if !ok {
		t.Fatalf("must be *UpstreamFailoverError, got %T", err)
	}
	if !failoverErr.RetryableOnSameAccount {
		t.Fatal("capacity shed must first retry on the same account")
	}
	if !failoverErr.ShouldRetryNextAccount() {
		t.Fatal("capacity shed must also allow switching accounts")
	}
	// 上游原文必须保留，客户端才能看到真实原因。
	if failoverErr.ClientMessage != overloadEventMessage {
		t.Fatalf("ClientMessage = %q, want upstream original", failoverErr.ClientMessage)
	}
	if !failoverErr.IsOpenAICapacityShed() {
		t.Fatal("must be recognized as a capacity shed")
	}
}

// TestNewFailoverNilServiceSafe nil service 不 panic。
func TestNewFailoverNilServiceSafe(t *testing.T) {
	var svc *OpenAIGatewayService
	if err := svc.newOpenAIWSErrorEventFailover(nil, nil, "", nil, "", "", nil); err != nil {
		t.Fatalf("nil service must yield nil, got %v", err)
	}
}

// TestRateLimitEventNotFailover 限流类 error 事件不得被接管。
//
// 上游 WS 路径对它有既定语义：persistOpenAIWSRateLimitSignal 落库 resets_at
// 等信号并直接返回 429 给客户端。最初的实现把它一并认领，破坏了上游测试
// TestOpenAIGatewayService_Forward_WSv2ErrorEventUsageLimitPersistsRateLimit
// （期望 429，实际 200），本用例守住这条边界。
func TestRateLimitEventNotFailover(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		message string
	}{
		{
			"usage_limit_reached",
			`{"type":"error","error":{"code":"rate_limit_exceeded","type":"usage_limit_reached","message":"The usage limit has been reached","resets_at":1788800000}}`,
			"The usage limit has been reached",
		},
		{
			"rate_limit_exceeded",
			`{"type":"error","error":{"code":"rate_limit_exceeded","type":"rate_limit_error","message":"rate limited"}}`,
			"rate limited",
		},
		{
			"insufficient_quota",
			`{"type":"error","error":{"code":"insufficient_quota","type":"insufficient_quota","message":"quota exhausted"}}`,
			"quota exhausted",
		},
	}
	for _, tc := range cases {
		if !isOpenAIWSErrorEventRateLimited([]byte(tc.payload)) {
			t.Fatalf("%s: must be recognized as rate limited", tc.name)
		}
		if shouldFailoverOpenAIWSErrorEvent([]byte(tc.payload), tc.message, false) {
			t.Fatalf("%s: rate limit events must keep the upstream 429 semantics", tc.name)
		}
	}
	// 降载不是限流，必须仍然 failover。
	if isOpenAIWSErrorEventRateLimited([]byte(overloadEventPayload)) {
		t.Fatal("capacity shed must not be classified as rate limiting")
	}
	if !shouldFailoverOpenAIWSErrorEvent([]byte(overloadEventPayload), overloadEventMessage, false) {
		t.Fatal("capacity shed must still trigger failover")
	}
}
