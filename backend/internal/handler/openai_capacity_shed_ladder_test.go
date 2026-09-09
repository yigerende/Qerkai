package handler

// 容量降载重试阶梯的端到端推演。
// 新增文件，上游不存在，不产生合并冲突。
//
// 背景：只给整体加墙钟预算后，生产实测 21 条多次尝试请求【跨账号 0/21】——
// 同账号阶梯（1 + pool_mode_retry_count(3) 次 × 约 31s ≈ 97s）自己就把 90s
// 预算吃光，换号那一级永远走不到。本测试用真实的 sameAccountRetryAllowed
// 判定推演完整阶梯，确保两档尝试耗时下换号都真的发生。

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// ladderStep 是推演出的一步动作。
type ladderStep string

const (
	stepSameAccount ladderStep = "same"
	stepSwitch      ladderStep = "switch"
	stepStop        ladderStep = "stop"
)

// simulateShedLadder 按给定的单次尝试耗时推演重试阶梯。
//
// 每一轮代表「一次上游尝试跑完并返回降载」，随后走与生产完全相同的判定：
// 先 applyOpenAICapacityShedBudget，再 sameAccountRetryAllowed，
// 最后 ShouldRetryNextAccount。虚拟时钟通过显式传入首次降载时刻实现，
// 不依赖 sleep。
func simulateShedLadder(t *testing.T, attemptDur time.Duration, poolRetryLimit int) []ladderStep {
	t.Helper()
	gin.SetMode(gin.TestMode)

	const maxSteps = 20
	elapsed := time.Duration(0)
	accountID := int64(1)
	sameAccountRetryCount := map[int64]int{}
	steps := make([]ladderStep, 0, maxSteps)

	for i := 0; i < maxSteps; i++ {
		// 一次上游尝试跑完并返回降载。
		elapsed += attemptDur

		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		failoverErr := service.NewOpenAICapacityShedFailoverErrorForTest()
		// 虚拟时钟：把首次降载时刻回拨 elapsed，使 time.Since(startedAt) == elapsed。
		failoverErr = service.ApplyOpenAICapacityShedBudgetForTest(
			c, failoverErr, time.Now().Add(-elapsed),
		)

		if sameAccountRetryAllowed(failoverErr, sameAccountRetryCount[accountID], poolRetryLimit) {
			sameAccountRetryCount[accountID]++
			steps = append(steps, stepSameAccount)
			continue
		}
		if !failoverErr.ShouldRetryNextAccount() {
			steps = append(steps, stepStop)
			break
		}
		steps = append(steps, stepSwitch)
		accountID++
	}
	return steps
}

func countSteps(steps []ladderStep, want ladderStep) int {
	n := 0
	for _, s := range steps {
		if s == want {
			n++
		}
	}
	return n
}

// 慢档：生产实测的 31s/次。修复前这一档同账号会占满 97s，换号零次。
func TestCapacityShedLadderSlowAttemptsStillSwitches(t *testing.T) {
	steps := simulateShedLadder(t, 31*time.Second, 3)
	t.Logf("慢档(31s/次) 阶梯: %v", steps)

	require.Greater(t, countSteps(steps, stepSwitch), 0,
		"换号必须真的发生——这正是修复前 0/21 的那个缺陷")
	require.Greater(t, countSteps(steps, stepSameAccount), 0,
		"仍须保留「先同账号退避」")
	require.LessOrEqual(t, countSteps(steps, stepSameAccount), 2,
		"单账号同号重试不得超过 2 次")
	require.Equal(t, stepStop, steps[len(steps)-1], "最终必须由预算收口")
}

// 快档：早前实测的 11s/次。时间闸未越线时由次数闸兜底。
func TestCapacityShedLadderFastAttemptsCapsSameAccount(t *testing.T) {
	steps := simulateShedLadder(t, 11*time.Second, 3)
	t.Logf("快档(11s/次) 阶梯: %v", steps)

	require.Greater(t, countSteps(steps, stepSwitch), 0, "换号必须发生")
	require.Equal(t, stepStop, steps[len(steps)-1])

	// 首个账号连续同号重试次数必须 <= 2（次数闸生效）。
	consecutive := 0
	for _, s := range steps {
		if s != stepSameAccount {
			break
		}
		consecutive++
	}
	require.LessOrEqual(t, consecutive, 2,
		"首账号同号重试超过 2 次说明次数闸失效")
}

// 两档都必须先同账号、后换号——顺序是用户明确要求的。
func TestCapacityShedLadderKeepsSameAccountBeforeSwitch(t *testing.T) {
	for _, dur := range []time.Duration{11 * time.Second, 20 * time.Second, 31 * time.Second} {
		steps := simulateShedLadder(t, dur, 3)
		require.NotEmpty(t, steps)
		require.Equal(t, stepSameAccount, steps[0],
			"attemptDur=%s：第一次失败后应先同账号重试，实际 %v", dur, steps)
		idxSwitch := -1
		for i, s := range steps {
			if s == stepSwitch {
				idxSwitch = i
				break
			}
		}
		require.Greater(t, idxSwitch, 0, "attemptDur=%s：必须出现换号，实际 %v", dur, steps)
	}
}

// 账号显式配置了更小的 pool_mode_retry_count 时仍以账号为准（只收紧不放宽）。
func TestCapacityShedLadderRespectsSmallerAccountLimit(t *testing.T) {
	steps := simulateShedLadder(t, 11*time.Second, 1)
	t.Logf("账号限 1 次: %v", steps)

	consecutive := 0
	for _, s := range steps {
		if s != stepSameAccount {
			break
		}
		consecutive++
	}
	require.Equal(t, 1, consecutive, "账号配置更小值时必须以账号为准")
	require.Greater(t, countSteps(steps, stepSwitch), 0)
}

// pool_mode_retry_count=0 表示禁用同账号重试，应直接换号。
func TestCapacityShedLadderZeroAccountLimitSwitchesImmediately(t *testing.T) {
	steps := simulateShedLadder(t, 31*time.Second, 0)
	t.Logf("账号禁用同号重试: %v", steps)

	require.Equal(t, 0, countSteps(steps, stepSameAccount))
	require.Greater(t, countSteps(steps, stepSwitch), 0)
}

// 总时长仍受墙钟预算约束：阶梯步数不得无界增长。
func TestCapacityShedLadderStaysWithinWallClockBudget(t *testing.T) {
	for _, dur := range []time.Duration{11 * time.Second, 31 * time.Second} {
		steps := simulateShedLadder(t, dur, 3)
		require.Equal(t, stepStop, steps[len(steps)-1],
			"attemptDur=%s 必须以 stop 收口，实际 %v", dur, steps)
		// 首次降载 + 预算 90s，单次 dur：最多约 90/dur + 2 步。
		maxSteps := int(90*time.Second/dur) + 2
		require.LessOrEqual(t, len(steps), maxSteps,
			"attemptDur=%s 步数 %d 超出墙钟预算可容纳的上限 %d", dur, len(steps), maxSteps)
	}
}
