package service

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// 生产实测的真实 payload。
func modelAccessDeniedPayload() []byte {
	return []byte(`{"type":"error","error":{"code":"model_not_found","type":"invalid_request_err",` +
		`"message":"The model ` + "`gpt-5.5`" + ` does not exist or you do not have access to it."}}`)
}

const modelAccessDeniedMessage = "The model `gpt-5.5` does not exist or you do not have access to it."

// ---------------------------------------------------------------------------
// 判据
// ---------------------------------------------------------------------------

func TestModelAccessDeniedDetectsProductionPayload(t *testing.T) {
	require.True(t, isOpenAIModelAccessDeniedError(modelAccessDeniedPayload(), modelAccessDeniedMessage))
	// 仅 message、无 payload 的调用方同样要能识别（payload 里已含同一文案）。
	require.True(t, isOpenAIModelAccessDeniedError(modelAccessDeniedPayload(), ""))
}

// 只匹配文案不看错误码会误伤：其它 invalid_request 里恰好出现 "does not exist"
// 的场景换号毫无意义。
func TestModelAccessDeniedRequiresBothCodeAndText(t *testing.T) {
	// 有码无文案特征
	require.False(t, isOpenAIModelAccessDeniedError(
		[]byte(`{"error":{"code":"invalid_request_error","message":"temperature must be <= 2"}}`), ""))
	// 有文案特征但错误码不相关（如容量降载）
	require.False(t, isOpenAIModelAccessDeniedError(
		[]byte(`{"error":{"code":"server_is_overloaded","type":"service_unavailable_err","message":"the model does not exist"}}`), ""))
	// 空输入
	require.False(t, isOpenAIModelAccessDeniedError(nil, ""))
	require.False(t, isOpenAIModelAccessDeniedError([]byte(`{}`), ""))
}

// 无权限的另一种措辞。
func TestModelAccessDeniedDetectsAccessWording(t *testing.T) {
	require.True(t, isOpenAIModelAccessDeniedError(
		[]byte(`{"error":{"code":"model_not_found","message":"You do not have access to this model."}}`), ""))
}

// 容量降载与限流不得被卷入——它们各有既定处置。
func TestModelAccessDeniedIgnoresOtherErrorClasses(t *testing.T) {
	require.False(t, isOpenAIModelAccessDeniedError(capacityShedPayload(), ""))
	require.False(t, isOpenAIModelAccessDeniedError(
		[]byte(`{"error":{"type":"rate_limit_error","message":"usage limit reached"}}`), ""))
}

// ---------------------------------------------------------------------------
// failover 放行（A）
// ---------------------------------------------------------------------------

// 核心回归：修复前该 payload 在整条判定链上全为 false，请求在第一个账号上就死。
func TestModelAccessDeniedTriggersFailover(t *testing.T) {
	payload, msg := modelAccessDeniedPayload(), modelAccessDeniedMessage

	// 前提：上游那套判定确实不放行——证明本补丁必要。
	require.False(t, openAIStreamErrorEventShouldFailover(payload, msg),
		"前提失效：上游已能 failover，无需本补丁")

	require.True(t, shouldFailoverOpenAIWSErrorEvent(payload, msg, false),
		"必须换号")
}

// 已写出下游内容时不得重试：会产生重复输出与重复计费。
func TestModelAccessDeniedRespectsWroteDownstreamGuard(t *testing.T) {
	require.False(t, shouldFailoverOpenAIWSErrorEvent(
		modelAccessDeniedPayload(), modelAccessDeniedMessage, true))
}

// ---------------------------------------------------------------------------
// 黑名单（B）
// ---------------------------------------------------------------------------

func TestModelAccessDenyListBlocksAfterThreshold(t *testing.T) {
	l := newOpenAIModelAccessDenyList(16)
	now := time.Now()

	// 前两次只累计，不冷却——单次失败可能是瞬时抖动，直接冷却 1 小时会误伤。
	streak, until := l.recordFailure(7, "gpt-5.5", now)
	require.Equal(t, 1, streak)
	require.True(t, until.IsZero())
	require.False(t, l.isBlocked(7, "gpt-5.5", now))

	streak, until = l.recordFailure(7, "gpt-5.5", now.Add(time.Second))
	require.Equal(t, 2, streak)
	require.True(t, until.IsZero())
	require.False(t, l.isBlocked(7, "gpt-5.5", now.Add(time.Second)))

	// 第三次进入冷却。
	streak, until = l.recordFailure(7, "gpt-5.5", now.Add(2*time.Second))
	require.Equal(t, openAIModelAccessDeniedStreakThreshold, streak)
	require.False(t, until.IsZero())
	require.True(t, l.isBlocked(7, "gpt-5.5", now.Add(2*time.Second)))

	// 冷却时长为 1 小时。
	require.True(t, l.isBlocked(7, "gpt-5.5", now.Add(59*time.Minute)))
	require.False(t, l.isBlocked(7, "gpt-5.5", now.Add(61*time.Minute)))
}

// 黑名单必须是 (账号, 模型) 维度：不能因为一个模型无权限就把账号整体摘掉。
func TestModelAccessDenyListIsPerAccountPerModel(t *testing.T) {
	l := newOpenAIModelAccessDenyList(16)
	now := time.Now()
	for i := 0; i < 3; i++ {
		l.recordFailure(7, "gpt-5.5", now)
	}
	require.True(t, l.isBlocked(7, "gpt-5.5", now))
	require.False(t, l.isBlocked(7, "gpt-5.6-sol", now), "同账号的其它模型不受影响")
	require.False(t, l.isBlocked(8, "gpt-5.5", now), "其它账号不受影响")
}

// 成功一次即解除，账号升级订阅后不必等冷却自然到期。
func TestModelAccessDenyListClearedBySuccess(t *testing.T) {
	l := newOpenAIModelAccessDenyList(16)
	now := time.Now()
	for i := 0; i < 3; i++ {
		l.recordFailure(7, "gpt-5.5", now)
	}
	require.True(t, l.isBlocked(7, "gpt-5.5", now))

	l.recordSuccess(7, "gpt-5.5")
	require.False(t, l.isBlocked(7, "gpt-5.5", now))

	// 解除后重新计数，需再攒满 3 次。
	streak, _ := l.recordFailure(7, "gpt-5.5", now)
	require.Equal(t, 1, streak)
}

// 冷却自然到期后保留连败计数：再失败一次应立刻重新冷却，
// 而不是又要攒满 3 次（否则等于每小时放 3 个请求进坑）。
func TestModelAccessDenyListReblocksImmediatelyAfterExpiry(t *testing.T) {
	l := newOpenAIModelAccessDenyList(16)
	now := time.Now()
	for i := 0; i < 3; i++ {
		l.recordFailure(7, "gpt-5.5", now)
	}
	after := now.Add(openAIModelAccessDeniedCooldown + time.Minute)
	require.False(t, l.isBlocked(7, "gpt-5.5", after))

	streak, until := l.recordFailure(7, "gpt-5.5", after)
	require.Equal(t, 4, streak, "计数不应被冷却到期清零")
	require.False(t, until.IsZero())
	require.True(t, l.isBlocked(7, "gpt-5.5", after))
}

// TTL 必须显著大于冷却，否则条目会在冷却期内过期。
func TestModelAccessDenyListStreakTTLExceedsCooldown(t *testing.T) {
	require.Greater(t, openAIModelAccessDeniedStreakTTL, openAIModelAccessDeniedCooldown)

	l := newOpenAIModelAccessDenyList(16)
	now := time.Now()
	l.recordFailure(7, "gpt-5.5", now)
	streak, _ := l.recordFailure(7, "gpt-5.5", now.Add(openAIModelAccessDeniedStreakTTL+time.Minute))
	require.Equal(t, 1, streak, "超过 TTL 后计数重置")
}

// 容量上限：不得无界增长。
func TestModelAccessDenyListEvictsWhenFull(t *testing.T) {
	l := newOpenAIModelAccessDenyList(4)
	base := time.Now()
	for i := 1; i <= 10; i++ {
		l.recordFailure(int64(i), "gpt-5.5", base.Add(time.Duration(i)*time.Second))
	}
	require.LessOrEqual(t, l.size(), 4)
}

func TestModelAccessDenyListRejectsInvalidKeys(t *testing.T) {
	l := newOpenAIModelAccessDenyList(16)
	now := time.Now()
	streak, _ := l.recordFailure(0, "gpt-5.5", now)
	require.Equal(t, 0, streak)
	streak, _ = l.recordFailure(7, "", now)
	require.Equal(t, 0, streak)
	require.False(t, l.isBlocked(0, "gpt-5.5", now))
	require.False(t, l.isBlocked(7, "", now))
}

func TestModelAccessDenyListNilSafe(t *testing.T) {
	var l *openAIModelAccessDenyList
	require.NotPanics(t, func() {
		l.recordFailure(1, "m", time.Now())
		l.recordSuccess(1, "m")
		require.False(t, l.isBlocked(1, "m", time.Now()))
		require.Equal(t, 0, l.size())
	})
}

// ---------------------------------------------------------------------------
// 端到端：记录 + 标记 + 选号跳过
// ---------------------------------------------------------------------------

func TestModelAccessDeniedAppliesToFailoverError(t *testing.T) {
	resetOpenAIModelAccessDenyListForTest()
	t.Cleanup(resetOpenAIModelAccessDenyListForTest)

	svc := &OpenAIGatewayService{}
	account := &Account{ID: 4242, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	failoverErr := &UpstreamFailoverError{
		StatusCode:             http.StatusBadGateway,
		RetryableOnSameAccount: true,
		RequestScopedTransient: true,
	}

	svc.applyOpenAIModelAccessDenied(account, "gpt-5.5",
		modelAccessDeniedPayload(), modelAccessDeniedMessage, failoverErr)

	require.Equal(t, openAIModelAccessDeniedReason, failoverErr.Reason)
	require.Equal(t, GatewayFailureScopeAccount, failoverErr.Scope)
	require.True(t, failoverErr.ShouldRetryNextAccount(), "必须换号")
	require.False(t, failoverErr.RetryableOnSameAccount, "无权限不是瞬时故障，同账号重试无意义")
	require.False(t, failoverErr.RequestScopedTransient)

	// 换号预算放宽到 20。
	require.Equal(t, openAIModelAccessDeniedMaxSwitches,
		OpenAIModelAccessDeniedMaxSwitches(10, failoverErr))
}

// 达阈值后选号阶段必须跳过该 (账号, 模型)。
func TestModelAccessDeniedBlocksSelectionAfterThreshold(t *testing.T) {
	resetOpenAIModelAccessDenyListForTest()
	t.Cleanup(resetOpenAIModelAccessDenyListForTest)

	svc := &OpenAIGatewayService{}
	account := &Account{ID: 4343, Platform: PlatformOpenAI, Type: AccountTypeOAuth}

	require.False(t, svc.isOpenAIModelAccessDeniedBlocked(account, "gpt-5.5"))
	for i := 0; i < openAIModelAccessDeniedStreakThreshold; i++ {
		svc.applyOpenAIModelAccessDenied(account, "gpt-5.5",
			modelAccessDeniedPayload(), modelAccessDeniedMessage,
			&UpstreamFailoverError{StatusCode: http.StatusBadGateway})
	}

	require.True(t, svc.isOpenAIModelAccessDeniedBlocked(account, "gpt-5.5"),
		"达阈值后选号必须跳过")
	require.True(t, svc.isOpenAIAccountRequestRuntimeBlocked(account, "gpt-5.5"),
		"必须并入选号阶段的统一判定")
	require.False(t, svc.isOpenAIAccountRequestRuntimeBlocked(account, "gpt-5.6-sol"),
		"其它模型不得被牵连")

	// 成功一次即解除。
	svc.ReportOpenAIAccountScheduleResult(account, "gpt-5.5", true, nil)
	require.False(t, svc.isOpenAIModelAccessDeniedBlocked(account, "gpt-5.5"))
}

// 非该类错误不得被标记，也不得放宽换号预算。
func TestModelAccessDeniedLeavesOtherErrorsUntouched(t *testing.T) {
	resetOpenAIModelAccessDenyListForTest()
	t.Cleanup(resetOpenAIModelAccessDenyListForTest)

	svc := &OpenAIGatewayService{}
	account := &Account{ID: 4444, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	failoverErr := &UpstreamFailoverError{
		StatusCode:             http.StatusServiceUnavailable,
		RetryableOnSameAccount: true,
		RequestScopedTransient: true,
	}

	svc.applyOpenAIModelAccessDenied(account, "gpt-5.5", capacityShedPayload(), "overloaded", failoverErr)

	require.NotEqual(t, openAIModelAccessDeniedReason, failoverErr.Reason)
	require.True(t, failoverErr.RetryableOnSameAccount, "降载的同账号重试必须保留")
	require.False(t, svc.isOpenAIModelAccessDeniedBlocked(account, "gpt-5.5"))
	require.Equal(t, 10, OpenAIModelAccessDeniedMaxSwitches(10, failoverErr))
}

// 换号预算只放宽、不收窄。
func TestModelAccessDeniedMaxSwitchesNeverShrinks(t *testing.T) {
	denied := &UpstreamFailoverError{Reason: openAIModelAccessDeniedReason}
	require.Equal(t, openAIModelAccessDeniedMaxSwitches, OpenAIModelAccessDeniedMaxSwitches(3, denied))
	require.Equal(t, 50, OpenAIModelAccessDeniedMaxSwitches(50, denied), "已高于预算时保持原值")
	require.Equal(t, 10, OpenAIModelAccessDeniedMaxSwitches(10, nil))
}
