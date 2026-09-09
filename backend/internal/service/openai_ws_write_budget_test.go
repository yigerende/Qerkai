package service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// 生产实测的那个 payload：2.4MB 写满 120s 超时，把 5s 重连预算彻底吃光
// （reconnect_budget_exhausted elapsed_ms=120004 budget_ms=5000）。
func TestWriteBudgetTightensLargePayload(t *testing.T) {
	const configured = 120 * time.Second
	got := openAIWSWriteBudgetCap(2431550, configured)

	require.Less(t, got, configured, "大 payload 的写超时必须被收紧")
	require.GreaterOrEqual(t, got, openAIWSWriteBudgetFloor, "不得低于下限")
	// 2431550B / 512KB/s ≈ 4.64s，×3 ≈ 13.9s
	require.InDelta(t, 13.9, got.Seconds(), 1.0)
}

// 小 payload 不得因收紧而误判：跨洲链路的正常抖动要留出余量。
func TestWriteBudgetKeepsFloorForSmallPayload(t *testing.T) {
	require.Equal(t, openAIWSWriteBudgetFloor, openAIWSWriteBudgetCap(1024, 120*time.Second))
	require.Equal(t, openAIWSWriteBudgetFloor, openAIWSWriteBudgetCap(200*1024, 120*time.Second))
}

// 只收紧不放宽：估算值超过配置值时以配置值为准。
func TestWriteBudgetNeverExceedsConfigured(t *testing.T) {
	require.Equal(t, 5*time.Second, openAIWSWriteBudgetCap(1024, 5*time.Second),
		"配置值低于下限时也不得被放宽到下限")
	require.Equal(t, 8*time.Second, openAIWSWriteBudgetCap(64*1024*1024, 8*time.Second))
}

// 未知大小时不做任何假设。
func TestWriteBudgetPassthroughOnUnknownSize(t *testing.T) {
	require.Equal(t, 120*time.Second, openAIWSWriteBudgetCap(-1, 120*time.Second))
	require.Equal(t, 120*time.Second, openAIWSWriteBudgetCap(0, 120*time.Second))
}

// 配置为 0（无超时）时保持原样，不引入新的超时。
func TestWriteBudgetRespectsDisabledTimeout(t *testing.T) {
	require.Equal(t, time.Duration(0), openAIWSWriteBudgetCap(2431550, 0))
}

func TestWriteBudgetEnvOverride(t *testing.T) {
	t.Setenv(openAIWSWriteBudgetEnv, "20")
	require.Equal(t, 20*time.Second, openAIWSWriteBudgetCap(2431550, 120*time.Second))
	// 覆盖值同样不得放宽配置上限。
	require.Equal(t, 10*time.Second, openAIWSWriteBudgetCap(2431550, 10*time.Second))
}

// 覆盖值 0 表示禁用按字节收紧，回到配置值。
func TestWriteBudgetEnvZeroDisables(t *testing.T) {
	t.Setenv(openAIWSWriteBudgetEnv, "0")
	require.Equal(t, 120*time.Second, openAIWSWriteBudgetCap(2431550, 120*time.Second))
}

// 非法配置回落默认行为，而不是让结果比没有该功能时更差。
func TestWriteBudgetInvalidEnvFallsBack(t *testing.T) {
	for _, raw := range []string{"abc", "-5"} {
		t.Setenv(openAIWSWriteBudgetEnv, raw)
		_, ok := openAIWSWriteBudgetOverride()
		require.False(t, ok, "raw=%q 应视为未设置", raw)
		require.Less(t, openAIWSWriteBudgetCap(2431550, 120*time.Second), 120*time.Second)
	}
}
