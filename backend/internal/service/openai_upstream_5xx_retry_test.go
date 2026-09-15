package service

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// 上游 502/503 重试的单元测试（二次开发功能，非上游代码）
//
// 覆盖重点按「出错代价」排序：
//  1. 开关关闭时必须与上游原始逻辑完全一致（最高优先级 —— 这是本功能的安全底线）；
//  2. 覆盖范围判定（状态码 / 账号类型 / 上游已有归因）不得越界；
//  3. 重试预算（同账号 N、请求累计 Total）按配置生效，不被上游默认值静默截断；
//  4. 观测日志的环形缓冲与请求级追踪。

func testOpenAIUpstream5xxOAuthAccount() *Account {
	return &Account{
		ID:       7,
		Name:     "codex-oauth",
		Platform: PlatformOpenAI,
		Type:     AccountTypeOAuth,
	}
}

func testOpenAIUpstream5xxRetryConfig() OpenAIUpstream5xxRetryConfig {
	return OpenAIUpstream5xxRetryConfig{
		Enabled:     true,
		SameAccount: 2,
		Total:       5,
		Delay:       500 * time.Millisecond,
	}
}

// TestMarkOpenAIUpstream5xxRetryableDisabledIsNoOp 是本功能最重要的一条测试：
// 开关关闭时不得触碰 failoverErr 的任何字段，否则会改变上游的 failover 语义。
func TestMarkOpenAIUpstream5xxRetryableDisabledIsNoOp(t *testing.T) {
	restore := setOpenAIUpstream5xxRetryForTest(OpenAIUpstream5xxRetryConfig{Enabled: false})
	defer restore()

	failoverErr := &UpstreamFailoverError{StatusCode: http.StatusBadGateway}
	markOpenAIUpstream5xxRetryable(failoverErr, testOpenAIUpstream5xxOAuthAccount())

	if failoverErr.RetryableOnSameAccount {
		t.Fatal("RetryableOnSameAccount must stay false when the feature is disabled")
	}
	if failoverErr.RequestScopedTransient {
		t.Fatal("RequestScopedTransient must stay false when the feature is disabled")
	}
	if failoverErr.SameAccountRetryMax != 0 || failoverErr.SameAccountRetryDelay != 0 {
		t.Fatalf("retry budget must stay zero when disabled, got max=%d delay=%s",
			failoverErr.SameAccountRetryMax, failoverErr.SameAccountRetryDelay)
	}
}

func TestMarkOpenAIUpstream5xxRetryableMarks502And503(t *testing.T) {
	config := testOpenAIUpstream5xxRetryConfig()
	restore := setOpenAIUpstream5xxRetryForTest(config)
	defer restore()

	for _, statusCode := range []int{http.StatusBadGateway, http.StatusServiceUnavailable} {
		failoverErr := &UpstreamFailoverError{StatusCode: statusCode}
		markOpenAIUpstream5xxRetryable(failoverErr, testOpenAIUpstream5xxOAuthAccount())

		if !failoverErr.RetryableOnSameAccount {
			t.Fatalf("status %d: expected RetryableOnSameAccount", statusCode)
		}
		// RequestScopedTransient 让上游 TempUnscheduleRetryableError 早退，
		// 避免重试耗尽时把账号临时封禁 —— 少了这一项，重试机制会反过来伤害调度。
		if !failoverErr.RequestScopedTransient {
			t.Fatalf("status %d: expected RequestScopedTransient", statusCode)
		}
		if failoverErr.SameAccountRetryMax != config.SameAccount {
			t.Fatalf("status %d: SameAccountRetryMax = %d, want %d",
				statusCode, failoverErr.SameAccountRetryMax, config.SameAccount)
		}
		if failoverErr.SameAccountRetryDelay != config.Delay {
			t.Fatalf("status %d: SameAccountRetryDelay = %s, want %s",
				statusCode, failoverErr.SameAccountRetryDelay, config.Delay)
		}
	}
}

// TestMarkOpenAIUpstream5xxRetryableSkipsOutOfScope 固定住覆盖边界。
// 这些分支放宽任何一条都会带来重复计费或重复副作用的风险。
func TestMarkOpenAIUpstream5xxRetryableSkipsOutOfScope(t *testing.T) {
	restore := setOpenAIUpstream5xxRetryForTest(testOpenAIUpstream5xxRetryConfig())
	defer restore()

	shadowParent := int64(1)
	cases := []struct {
		name        string
		failoverErr *UpstreamFailoverError
		account     *Account
	}{
		{
			// 500 可能来自模型侧已开始处理之后，重试有重复副作用风险。
			name:        "500 not covered",
			failoverErr: &UpstreamFailoverError{StatusCode: http.StatusInternalServerError},
			account:     testOpenAIUpstream5xxOAuthAccount(),
		},
		{
			// 504 意味着上游已在等待模型响应，重试会造成重复计费。
			name:        "504 not covered",
			failoverErr: &UpstreamFailoverError{StatusCode: http.StatusGatewayTimeout},
			account:     testOpenAIUpstream5xxOAuthAccount(),
		},
		{
			name:        "429 keeps upstream oauth semantics",
			failoverErr: &UpstreamFailoverError{StatusCode: http.StatusTooManyRequests},
			account:     testOpenAIUpstream5xxOAuthAccount(),
		},
		{
			// API Key 账号有上游自己的账号+模型级降温策略，不应被绕过。
			name:        "api key account not covered",
			failoverErr: &UpstreamFailoverError{StatusCode: http.StatusBadGateway},
			account:     &Account{ID: 8, Platform: PlatformOpenAI, Type: AccountTypeAPIKey},
		},
		{
			name:        "non-openai account not covered",
			failoverErr: &UpstreamFailoverError{StatusCode: http.StatusBadGateway},
			account:     &Account{ID: 9, Platform: PlatformAnthropic, Type: AccountTypeOAuth},
		},
		{
			// Spark 影子账号的调度与降温有独立语义。
			name:        "shadow account not covered",
			failoverErr: &UpstreamFailoverError{StatusCode: http.StatusBadGateway},
			account: &Account{
				ID: 10, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
				ParentAccountID: &shadowParent,
			},
		},
		{
			name:        "nil account not covered",
			failoverErr: &UpstreamFailoverError{StatusCode: http.StatusBadGateway},
			account:     nil,
		},
		{
			// 凭证类失败重试同一账号必然再次失败。
			name: "account auth stage not covered",
			failoverErr: &UpstreamFailoverError{
				StatusCode: http.StatusBadGateway,
				Stage:      GatewayFailureStageAccountAuth,
			},
			account: testOpenAIUpstream5xxOAuthAccount(),
		},
		{
			// 上游已给出更具体归因时不接管。
			name: "existing reason not overridden",
			failoverErr: &UpstreamFailoverError{
				StatusCode: http.StatusBadGateway,
				Reason:     GatewayFailureReason("some_upstream_reason"),
			},
			account: testOpenAIUpstream5xxOAuthAccount(),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := *tc.failoverErr
			markOpenAIUpstream5xxRetryable(tc.failoverErr, tc.account)
			if tc.failoverErr.RetryableOnSameAccount != before.RetryableOnSameAccount ||
				tc.failoverErr.RequestScopedTransient != before.RequestScopedTransient ||
				tc.failoverErr.SameAccountRetryMax != before.SameAccountRetryMax ||
				tc.failoverErr.SameAccountRetryDelay != before.SameAccountRetryDelay {
				t.Fatalf("out-of-scope failure must not be modified: %+v", tc.failoverErr)
			}
		})
	}
}

// TestMarkOpenAIUpstream5xxRetryablePreservesCapacityShed 确认容量降载
// （上游已填好重试预算与客户端文案）不被本功能覆盖。
func TestMarkOpenAIUpstream5xxRetryablePreservesCapacityShed(t *testing.T) {
	restore := setOpenAIUpstream5xxRetryForTest(testOpenAIUpstream5xxRetryConfig())
	defer restore()

	failoverErr := &UpstreamFailoverError{
		StatusCode:             http.StatusServiceUnavailable,
		RetryableOnSameAccount: true,
		RequestScopedTransient: true,
		SameAccountRetryMax:    9,
		SameAccountRetryDelay:  time.Second,
	}
	markOpenAIUpstream5xxRetryable(failoverErr, testOpenAIUpstream5xxOAuthAccount())

	if failoverErr.SameAccountRetryMax != 9 || failoverErr.SameAccountRetryDelay != time.Second {
		t.Fatalf("capacity shed budget must be preserved, got max=%d delay=%s",
			failoverErr.SameAccountRetryMax, failoverErr.SameAccountRetryDelay)
	}
}

// TestOpenAIUpstream5xxRetryEnabledRequiresPositiveSameAccount 确认
// SameAccount=0 时视为未启用 —— 否则会打上可重试标记却没有任何预算可用。
func TestOpenAIUpstream5xxRetryEnabledRequiresPositiveSameAccount(t *testing.T) {
	restore := setOpenAIUpstream5xxRetryForTest(OpenAIUpstream5xxRetryConfig{
		Enabled:     true,
		SameAccount: 0,
		Total:       5,
	})
	defer restore()

	if OpenAIUpstream5xxRetryEnabled() {
		t.Fatal("SameAccount=0 must be treated as disabled")
	}
}

// TestOpenAIUpstream5xxSameAccountRetryLimitOverridesPoolDefault 覆盖本功能
// 存在的核心理由之一：上游 effectiveSameAccountRetryLimit 默认取
// GetPoolModeRetryCount()（非池模式恒为 3），会把管理员配置的 N>3 静默截断。
func TestOpenAIUpstream5xxSameAccountRetryLimitOverridesPoolDefault(t *testing.T) {
	config := OpenAIUpstream5xxRetryConfig{
		Enabled:     true,
		SameAccount: 6,
		Total:       12,
		Delay:       200 * time.Millisecond,
	}
	restore := setOpenAIUpstream5xxRetryForTest(config)
	defer restore()

	account := testOpenAIUpstream5xxOAuthAccount()
	failoverErr := &UpstreamFailoverError{StatusCode: http.StatusBadGateway}
	markOpenAIUpstream5xxRetryable(failoverErr, account)

	if limit := OpenAIUpstream5xxSameAccountRetryLimit(failoverErr, account); limit != 6 {
		t.Fatalf("retry limit = %d, want 6 (must not be capped to the pool-mode default)", limit)
	}
}

func TestOpenAIUpstream5xxSameAccountRetryLimitZeroWhenNotOwned(t *testing.T) {
	restore := setOpenAIUpstream5xxRetryForTest(testOpenAIUpstream5xxRetryConfig())
	defer restore()

	account := testOpenAIUpstream5xxOAuthAccount()
	// 未经 markOpenAIUpstream5xxRetryable 标记（SameAccountRetryMax 与配置不符）：
	// 说明该错误的重试预算由上游其他机制设定，本功能不接管。
	failoverErr := &UpstreamFailoverError{
		StatusCode:          http.StatusBadGateway,
		SameAccountRetryMax: 9,
	}
	if limit := OpenAIUpstream5xxSameAccountRetryLimit(failoverErr, account); limit != 0 {
		t.Fatalf("retry limit = %d, want 0 for an error this feature did not mark", limit)
	}
}

func TestOpenAIUpstream5xxTotalRetryBudgetExhausted(t *testing.T) {
	restore := setOpenAIUpstream5xxRetryForTest(OpenAIUpstream5xxRetryConfig{
		Enabled:     true,
		SameAccount: 2,
		Total:       5,
	})
	defer restore()

	account := testOpenAIUpstream5xxOAuthAccount()
	failoverErr := &UpstreamFailoverError{StatusCode: http.StatusBadGateway}

	cases := []struct {
		name        string
		retryCounts map[int64]int
		switchCount int
		want        bool
	}{
		{"fresh request", map[int64]int{}, 0, false},
		{"under budget", map[int64]int{7: 2}, 1, false},
		{"at budget", map[int64]int{7: 2, 8: 2}, 1, true},
		{"over budget", map[int64]int{7: 2, 8: 2, 9: 2}, 3, true},
		{"switches alone reach budget", map[int64]int{}, 5, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := OpenAIUpstream5xxTotalRetryBudgetExhausted(failoverErr, account, tc.retryCounts, tc.switchCount)
			if got != tc.want {
				t.Fatalf("exhausted = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestOpenAIUpstream5xxTotalRetryBudgetIgnoredWhenDisabled 确认关闭开关后
// 调用方完全沿用上游原有的换号预算判定。
func TestOpenAIUpstream5xxTotalRetryBudgetIgnoredWhenDisabled(t *testing.T) {
	restore := setOpenAIUpstream5xxRetryForTest(OpenAIUpstream5xxRetryConfig{Enabled: false})
	defer restore()

	exhausted := OpenAIUpstream5xxTotalRetryBudgetExhausted(
		&UpstreamFailoverError{StatusCode: http.StatusBadGateway},
		testOpenAIUpstream5xxOAuthAccount(),
		map[int64]int{7: 99},
		99,
	)
	if exhausted {
		t.Fatal("disabled feature must never report its own budget as exhausted")
	}
}

// TestIsOpenAIUpstream5xxRetryCandidateStatusScope 固定「只接管 502/503」这条边界。
//
// 这条判据是全功能的唯一入口（mark / 预算 / 日志三处共用），因此单独锁定：
// 500 可能来自模型侧已开始处理后的内部错误，504 意味着上游已在等待模型响应，
// 两者原地重试都可能造成重复计费或重复副作用。
func TestIsOpenAIUpstream5xxRetryCandidateStatusScope(t *testing.T) {
	restore := setOpenAIUpstream5xxRetryForTest(testOpenAIUpstream5xxRetryConfig())
	defer restore()

	account := testOpenAIUpstream5xxOAuthAccount()
	for _, status := range []int{http.StatusBadGateway, http.StatusServiceUnavailable} {
		if !IsOpenAIUpstream5xxRetryCandidate(status, account) {
			t.Fatalf("status %d should be in scope", status)
		}
	}
	for _, status := range []int{
		http.StatusInternalServerError,
		http.StatusGatewayTimeout,
		http.StatusTooManyRequests,
		520,
	} {
		if IsOpenAIUpstream5xxRetryCandidate(status, account) {
			t.Fatalf("status %d must keep upstream semantics", status)
		}
	}
}

func TestOpenAIUpstream5xxRetryClamps(t *testing.T) {
	if got := clampOpenAIUpstream5xxRetrySameAccount(99); got != MaxOpenAIUpstream5xxRetrySameAccount {
		t.Fatalf("same-account clamp = %d, want %d", got, MaxOpenAIUpstream5xxRetrySameAccount)
	}
	if got := clampOpenAIUpstream5xxRetrySameAccount(-3); got != MinOpenAIUpstream5xxRetrySameAccount {
		t.Fatalf("same-account clamp = %d, want %d", got, MinOpenAIUpstream5xxRetrySameAccount)
	}
	if got := clampOpenAIUpstream5xxRetryTotal(0); got != MinOpenAIUpstream5xxRetryTotal {
		t.Fatalf("total clamp = %d, want %d", got, MinOpenAIUpstream5xxRetryTotal)
	}
	if got := clampOpenAIUpstream5xxRetryDelayMS(99999); got != MaxOpenAIUpstream5xxRetryDelayMS {
		t.Fatalf("delay clamp = %d, want %d", got, MaxOpenAIUpstream5xxRetryDelayMS)
	}
}

// --- 观测日志 ---

func newOpenAIUpstream5xxRetryTestContext(t *testing.T) *gin.Context {
	t.Helper()
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	return c
}

func TestOpenAIUpstream5xxRetryTrackerSucceeded(t *testing.T) {
	restore := setOpenAIUpstream5xxRetryForTest(testOpenAIUpstream5xxRetryConfig())
	defer restore()
	ClearOpenAIUpstream5xxRetryLog()
	defer ClearOpenAIUpstream5xxRetryLog()

	c := newOpenAIUpstream5xxRetryTestContext(t)
	account := testOpenAIUpstream5xxOAuthAccount()
	failoverErr := &UpstreamFailoverError{
		StatusCode:          http.StatusBadGateway,
		ResponseBody:        []byte(`{"error":{"message":"upstream overloaded"}}`),
		SameAccountRetryMax: 2,
	}

	NoteOpenAIUpstream5xxRetryIntercept(c, account, failoverErr, "gpt-5.3-codex", 1, 0, 500*time.Millisecond)
	FlushOpenAIUpstream5xxRetryTracker(c, http.StatusOK)

	page := GetOpenAIUpstream5xxRetryLog(OpenAIUpstream5xxRetryLogFilter{})
	if page.Total != 2 {
		t.Fatalf("expected 2 entries (intercepted + succeeded), got %d", page.Total)
	}
	// snapshot 按时间倒序，最新的终态事件在前。
	if page.Entries[0].Event != OpenAIUpstream5xxRetryEventSucceeded {
		t.Fatalf("newest event = %q, want %q", page.Entries[0].Event, OpenAIUpstream5xxRetryEventSucceeded)
	}
	if page.Entries[0].UpstreamStatus != http.StatusBadGateway {
		t.Fatalf("terminal entry must keep the intercepted upstream status, got %d", page.Entries[0].UpstreamStatus)
	}
	if page.Entries[1].Event != OpenAIUpstream5xxRetryEventIntercepted {
		t.Fatalf("oldest event = %q, want %q", page.Entries[1].Event, OpenAIUpstream5xxRetryEventIntercepted)
	}
	if page.Entries[1].UpstreamMessage != "upstream overloaded" {
		t.Fatalf("upstream message = %q", page.Entries[1].UpstreamMessage)
	}
	if page.Stats.Succeeded != 1 || page.Stats.Intercepted != 1 {
		t.Fatalf("stats = %+v", page.Stats)
	}
}

func TestOpenAIUpstream5xxRetryTrackerExhausted(t *testing.T) {
	restore := setOpenAIUpstream5xxRetryForTest(testOpenAIUpstream5xxRetryConfig())
	defer restore()
	ClearOpenAIUpstream5xxRetryLog()
	defer ClearOpenAIUpstream5xxRetryLog()

	c := newOpenAIUpstream5xxRetryTestContext(t)
	account := testOpenAIUpstream5xxOAuthAccount()
	failoverErr := &UpstreamFailoverError{StatusCode: http.StatusServiceUnavailable, SameAccountRetryMax: 2}

	NoteOpenAIUpstream5xxRetryIntercept(c, account, failoverErr, "gpt-5.3-codex", 1, 0, 500*time.Millisecond)
	NoteOpenAIUpstream5xxRetryIntercept(c, account, failoverErr, "gpt-5.3-codex", 2, 1, 500*time.Millisecond)
	FlushOpenAIUpstream5xxRetryTracker(c, http.StatusServiceUnavailable)

	page := GetOpenAIUpstream5xxRetryLog(OpenAIUpstream5xxRetryLogFilter{})
	if page.Stats.Exhausted != 1 {
		t.Fatalf("expected 1 exhausted event, stats = %+v", page.Stats)
	}
	if page.Entries[0].Attempt != 2 {
		t.Fatalf("terminal attempt = %d, want 2", page.Entries[0].Attempt)
	}
}

// TestFlushOpenAIUpstream5xxRetryTrackerIsIdempotent 确认重复 flush 不会
// 把一次请求记成两条终态事件（中间件与兜底路径都可能调用）。
func TestFlushOpenAIUpstream5xxRetryTrackerIsIdempotent(t *testing.T) {
	restore := setOpenAIUpstream5xxRetryForTest(testOpenAIUpstream5xxRetryConfig())
	defer restore()
	ClearOpenAIUpstream5xxRetryLog()
	defer ClearOpenAIUpstream5xxRetryLog()

	c := newOpenAIUpstream5xxRetryTestContext(t)
	failoverErr := &UpstreamFailoverError{StatusCode: http.StatusBadGateway, SameAccountRetryMax: 2}
	NoteOpenAIUpstream5xxRetryIntercept(c, testOpenAIUpstream5xxOAuthAccount(), failoverErr, "m", 1, 0, time.Millisecond)

	FlushOpenAIUpstream5xxRetryTracker(c, http.StatusOK)
	FlushOpenAIUpstream5xxRetryTracker(c, http.StatusOK)

	page := GetOpenAIUpstream5xxRetryLog(OpenAIUpstream5xxRetryLogFilter{})
	if page.Stats.Succeeded != 1 {
		t.Fatalf("expected exactly 1 terminal event, stats = %+v", page.Stats)
	}
}

// TestFlushOpenAIUpstream5xxRetryTrackerWithoutInterceptIsNoOp 是访问日志
// 中间件的安全前提：绝大多数请求从未被拦截，flush 必须零成本、零写入。
func TestFlushOpenAIUpstream5xxRetryTrackerWithoutInterceptIsNoOp(t *testing.T) {
	ClearOpenAIUpstream5xxRetryLog()
	defer ClearOpenAIUpstream5xxRetryLog()

	FlushOpenAIUpstream5xxRetryTracker(newOpenAIUpstream5xxRetryTestContext(t), http.StatusOK)

	if page := GetOpenAIUpstream5xxRetryLog(OpenAIUpstream5xxRetryLogFilter{}); page.Total != 0 {
		t.Fatalf("expected no entries, got %d", page.Total)
	}
}

func TestNoteOpenAIUpstream5xxRetryInterceptDisabledIsNoOp(t *testing.T) {
	restore := setOpenAIUpstream5xxRetryForTest(OpenAIUpstream5xxRetryConfig{Enabled: false})
	defer restore()
	ClearOpenAIUpstream5xxRetryLog()
	defer ClearOpenAIUpstream5xxRetryLog()

	c := newOpenAIUpstream5xxRetryTestContext(t)
	NoteOpenAIUpstream5xxRetryIntercept(c, testOpenAIUpstream5xxOAuthAccount(),
		&UpstreamFailoverError{StatusCode: http.StatusBadGateway}, "m", 1, 0, time.Millisecond)

	if page := GetOpenAIUpstream5xxRetryLog(OpenAIUpstream5xxRetryLogFilter{}); page.Total != 0 {
		t.Fatalf("disabled feature must not write log entries, got %d", page.Total)
	}
}

func TestOpenAIUpstream5xxRetryLogFilters(t *testing.T) {
	restore := setOpenAIUpstream5xxRetryForTest(testOpenAIUpstream5xxRetryConfig())
	defer restore()
	ClearOpenAIUpstream5xxRetryLog()
	defer ClearOpenAIUpstream5xxRetryLog()

	recordOpenAIUpstream5xxRetryEvent(OpenAIUpstream5xxRetryLogEntry{
		Event: OpenAIUpstream5xxRetryEventIntercepted, StatusCode: http.StatusBadGateway, AccountID: 1,
	})
	recordOpenAIUpstream5xxRetryEvent(OpenAIUpstream5xxRetryLogEntry{
		Event: OpenAIUpstream5xxRetryEventSucceeded, StatusCode: http.StatusOK, AccountID: 2, ExtraLatencyMS: 600,
	})
	recordOpenAIUpstream5xxRetryEvent(OpenAIUpstream5xxRetryLogEntry{
		Event: OpenAIUpstream5xxRetryEventExhausted, StatusCode: http.StatusServiceUnavailable, AccountID: 2, ExtraLatencyMS: 1400,
	})

	byEvent := GetOpenAIUpstream5xxRetryLog(OpenAIUpstream5xxRetryLogFilter{Event: OpenAIUpstream5xxRetryEventSucceeded})
	if byEvent.Total != 1 || byEvent.Entries[0].AccountID != 2 {
		t.Fatalf("event filter returned %+v", byEvent.Entries)
	}

	byAccount := GetOpenAIUpstream5xxRetryLog(OpenAIUpstream5xxRetryLogFilter{AccountID: 2})
	if byAccount.Total != 2 {
		t.Fatalf("account filter total = %d, want 2", byAccount.Total)
	}
	// 平均额外耗时只统计终态事件：(600+1400)/2 = 1000。
	if byAccount.Stats.AvgExtraLatencyMS != 1000 {
		t.Fatalf("avg extra latency = %d, want 1000", byAccount.Stats.AvgExtraLatencyMS)
	}
	if byAccount.Stats.MaxExtraLatencyMS != 1400 {
		t.Fatalf("max extra latency = %d, want 1400", byAccount.Stats.MaxExtraLatencyMS)
	}

	byStatus := GetOpenAIUpstream5xxRetryLog(OpenAIUpstream5xxRetryLogFilter{StatusCode: http.StatusBadGateway})
	if byStatus.Total != 1 {
		t.Fatalf("status filter total = %d, want 1", byStatus.Total)
	}
}

func TestOpenAIUpstream5xxRetryLogPagination(t *testing.T) {
	ClearOpenAIUpstream5xxRetryLog()
	defer ClearOpenAIUpstream5xxRetryLog()

	for i := 0; i < 10; i++ {
		recordOpenAIUpstream5xxRetryEvent(OpenAIUpstream5xxRetryLogEntry{
			Event: OpenAIUpstream5xxRetryEventIntercepted, AccountID: int64(i),
		})
	}

	first := GetOpenAIUpstream5xxRetryLog(OpenAIUpstream5xxRetryLogFilter{Limit: 4})
	if len(first.Entries) != 4 || first.Total != 10 {
		t.Fatalf("first page: entries=%d total=%d", len(first.Entries), first.Total)
	}
	// 倒序：最后写入的 AccountID=9 在最前。
	if first.Entries[0].AccountID != 9 {
		t.Fatalf("newest entry AccountID = %d, want 9", first.Entries[0].AccountID)
	}

	second := GetOpenAIUpstream5xxRetryLog(OpenAIUpstream5xxRetryLogFilter{Limit: 4, Offset: 4})
	if len(second.Entries) != 4 || second.Entries[0].AccountID != 5 {
		t.Fatalf("second page: entries=%d first=%d", len(second.Entries), second.Entries[0].AccountID)
	}

	beyond := GetOpenAIUpstream5xxRetryLog(OpenAIUpstream5xxRetryLogFilter{Limit: 4, Offset: 100})
	if len(beyond.Entries) != 0 {
		t.Fatalf("offset beyond end should be empty, got %d", len(beyond.Entries))
	}
}

// TestOpenAIUpstream5xxRetryLogRingOverwrite 确认环形缓冲写满后覆盖最旧记录
// 且不越界 —— 这是「不落库、定长内存」选型的正确性前提。
func TestOpenAIUpstream5xxRetryLogRingOverwrite(t *testing.T) {
	ClearOpenAIUpstream5xxRetryLog()
	defer ClearOpenAIUpstream5xxRetryLog()

	total := openAIUpstream5xxRetryLogCapacity + 25
	for i := 0; i < total; i++ {
		recordOpenAIUpstream5xxRetryEvent(OpenAIUpstream5xxRetryLogEntry{
			Event: OpenAIUpstream5xxRetryEventIntercepted, Attempt: i,
		})
	}

	page := GetOpenAIUpstream5xxRetryLog(OpenAIUpstream5xxRetryLogFilter{Limit: openAIUpstream5xxRetryLogCapacity})
	if page.Total != openAIUpstream5xxRetryLogCapacity {
		t.Fatalf("total = %d, want %d", page.Total, openAIUpstream5xxRetryLogCapacity)
	}
	if page.Entries[0].Attempt != total-1 {
		t.Fatalf("newest Attempt = %d, want %d", page.Entries[0].Attempt, total-1)
	}
	// 最旧留存记录应是第 25 条（0..24 已被覆盖）。
	oldest := page.Entries[len(page.Entries)-1]
	if oldest.Attempt != total-openAIUpstream5xxRetryLogCapacity {
		t.Fatalf("oldest Attempt = %d, want %d", oldest.Attempt, total-openAIUpstream5xxRetryLogCapacity)
	}
}

func TestTruncateOpenAIUpstream5xxRetryMessageKeepsValidUTF8(t *testing.T) {
	long := ""
	for i := 0; i < openAIUpstream5xxRetryLogMessageMax; i++ {
		long += "上"
	}
	got := truncateOpenAIUpstream5xxRetryMessage(long)
	if runes := []rune(got); len(runes) != openAIUpstream5xxRetryLogMessageMax {
		t.Fatalf("truncated rune count = %d, want %d", len(runes), openAIUpstream5xxRetryLogMessageMax)
	}
	// 按 rune 截断，不得产生非法 UTF-8（否则前端显示乱码）。
	for _, r := range got {
		if r == '�' {
			t.Fatal("truncation produced invalid UTF-8")
		}
	}
}
