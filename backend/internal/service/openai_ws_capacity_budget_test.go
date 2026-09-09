package service

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// 容量降载 payload：与生产实测的 error 事件同形。
func capacityShedPayload() []byte {
	return []byte(`{"type":"error","error":{"code":"server_is_overloaded","type":"service_unavailable_error","message":"Our servers are currently overloaded. Please try again later."}}`)
}

func newCapacityShedFailoverErr() *UpstreamFailoverError {
	return &UpstreamFailoverError{
		StatusCode:             http.StatusServiceUnavailable,
		ResponseBody:           capacityShedPayload(),
		RetryableOnSameAccount: true,
		RequestScopedTransient: true,
		ClientStatusCode:       http.StatusServiceUnavailable,
		ClientMessage:          "Our servers are currently overloaded. Please try again later.",
	}
}

func newBudgetTestContext() *gin.Context {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	return c
}

// 首次降载必须保留完整重试能力：立刻封顶会丢掉生产实测的 73.8% 救回量。
func TestCapacityShedBudgetFirstHitKeepsRetry(t *testing.T) {
	c := newBudgetTestContext()
	err := applyOpenAICapacityShedBudget(c, newCapacityShedFailoverErr())

	require.True(t, err.RetryableOnSameAccount)
	require.True(t, err.ShouldRetryNextAccount())
	_, seen := openAICapacityShedElapsed(c)
	require.True(t, seen, "首次降载应打下墙钟起点")
}

// 预算内的后续降载同样保留两级恢复能力。
func TestCapacityShedBudgetWithinBudgetKeepsRetry(t *testing.T) {
	c := newBudgetTestContext()
	c.Set(openAICapacityShedStartKey, time.Now().Add(-5*time.Second))

	err := applyOpenAICapacityShedBudget(c, newCapacityShedFailoverErr())
	require.True(t, err.RetryableOnSameAccount)
	require.True(t, err.ShouldRetryNextAccount())
}

// 预算耗尽后两级恢复能力必须一起收掉：只收一级会留下产生 544s 的那个乘法。
func TestCapacityShedBudgetExhaustedStopsBothLevels(t *testing.T) {
	c := newBudgetTestContext()
	c.Set(openAICapacityShedStartKey, time.Now().Add(-120*time.Second))

	err := applyOpenAICapacityShedBudget(c, newCapacityShedFailoverErr())
	require.False(t, err.RetryableOnSameAccount, "同账号退避必须停")
	require.False(t, err.ShouldRetryNextAccount(), "换账号必须停")
}

// 封顶不得改写客户端可见语义：客户端需要 503 + 上游原文才能自行退避。
func TestCapacityShedBudgetPreservesClientMessage(t *testing.T) {
	c := newBudgetTestContext()
	c.Set(openAICapacityShedStartKey, time.Now().Add(-120*time.Second))

	err := applyOpenAICapacityShedBudget(c, newCapacityShedFailoverErr())
	require.Equal(t, http.StatusServiceUnavailable, err.ClientStatusCode)
	require.Contains(t, err.ClientMessage, "overloaded")
}

// 非降载错误完全不受影响：限流、账号级 4xx 有各自的既定语义。
func TestCapacityShedBudgetIgnoresNonCapacityErrors(t *testing.T) {
	c := newBudgetTestContext()
	c.Set(openAICapacityShedStartKey, time.Now().Add(-120*time.Second))

	rateLimited := &UpstreamFailoverError{
		StatusCode:             http.StatusTooManyRequests,
		ResponseBody:           []byte(`{"type":"error","error":{"type":"rate_limit_error","message":"usage limit reached"}}`),
		RetryableOnSameAccount: true,
	}
	err := applyOpenAICapacityShedBudget(c, rateLimited)
	require.True(t, err.RetryableOnSameAccount, "限流不该被降载预算收掉")
	require.True(t, err.ShouldRetryNextAccount())
}

// 预算设为 0 表示禁用封顶，回到纯次数预算的上游行为。
func TestCapacityShedBudgetDisabledByZero(t *testing.T) {
	t.Setenv(openAICapacityShedBudgetEnv, "0")
	c := newBudgetTestContext()
	c.Set(openAICapacityShedStartKey, time.Now().Add(-600*time.Second))

	err := applyOpenAICapacityShedBudget(c, newCapacityShedFailoverErr())
	require.True(t, err.RetryableOnSameAccount)
	require.True(t, err.ShouldRetryNextAccount())
}

// 非法配置回落默认值，而不是让重试行为比没有该功能时更差。
func TestCapacityShedBudgetInvalidEnvFallsBack(t *testing.T) {
	for _, raw := range []string{"abc", "-5", ""} {
		t.Setenv(openAICapacityShedBudgetEnv, raw)
		require.Equal(t, openAICapacityShedBudgetDefault, openAICapacityShedBudget(), "raw=%q", raw)
	}
}

func TestCapacityShedBudgetEnvOverride(t *testing.T) {
	t.Setenv(openAICapacityShedBudgetEnv, "30")
	require.Equal(t, 30*time.Second, openAICapacityShedBudget())

	c := newBudgetTestContext()
	c.Set(openAICapacityShedStartKey, time.Now().Add(-45*time.Second))
	err := applyOpenAICapacityShedBudget(c, newCapacityShedFailoverErr())
	require.False(t, err.ShouldRetryNextAccount(), "45s 已超过 30s 预算")
}

// nil 安全：降级路径上的旁路保护不应引入新的崩溃点。
func TestCapacityShedBudgetNilSafe(t *testing.T) {
	require.Nil(t, applyOpenAICapacityShedBudget(newBudgetTestContext(), nil))
	require.NotNil(t, applyOpenAICapacityShedBudget(nil, newCapacityShedFailoverErr()))
}
