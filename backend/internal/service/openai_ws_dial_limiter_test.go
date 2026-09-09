package service

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// WS 建连限速与 403 退避重试测试。
// 新增文件，上游不存在，不产生合并冲突。

// TestDialLimiterCapsConcurrency 核心用例：同账号在途握手数不超过闸门容量。
func TestDialLimiterCapsConcurrency(t *testing.T) {
	limiter := newOpenAIWSAccountDialLimiter()
	var inFlight, peak atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := limiter.acquire(context.Background())
			if err != nil {
				t.Errorf("acquire failed: %v", err)
				return
			}
			cur := inFlight.Add(1)
			for {
				old := peak.Load()
				if cur <= old || peak.CompareAndSwap(old, cur) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			inFlight.Add(-1)
			release()
		}()
	}
	wg.Wait()
	if got := peak.Load(); got > openAIWSDialConcurrencyPerAccount {
		t.Fatalf("peak in-flight dials = %d, want <= %d", got, openAIWSDialConcurrencyPerAccount)
	}
}

// TestDialLimiterEnforcesMinInterval 验证同账号连续握手被拉开最小间隔 ——
// 这是实测中真正起作用的那一条：并发数没打满时，「串行但极快」同样会被限流。
func TestDialLimiterEnforcesMinInterval(t *testing.T) {
	limiter := newOpenAIWSAccountDialLimiter()
	start := time.Now()
	const n = 4
	for i := 0; i < n; i++ {
		release, err := limiter.acquire(context.Background())
		if err != nil {
			t.Fatalf("acquire %d failed: %v", i, err)
		}
		release()
	}
	// n 次握手至少跨越 (n-1) 个最小间隔。留 10% 容差应对计时抖动。
	want := time.Duration(float64(n-1) * float64(openAIWSDialMinInterval) * 0.9)
	if elapsed := time.Since(start); elapsed < want {
		t.Fatalf("elapsed %v, want >= %v (min interval not enforced)", elapsed, want)
	}
}

// TestDialLimiterRespectsDeadline 超时保护：剩余时间不足时放行而非阻塞到失败。
//
// acquire 的总预算只有 dial_timeout+2s，限速等待若吃满预算，会把「本可成功
// 的握手」变成必然超时，比不限速更糟。
func TestDialLimiterRespectsDeadline(t *testing.T) {
	limiter := newOpenAIWSAccountDialLimiter()
	// 先占用一次，让后续 acquire 面临最小间隔等待。
	release, err := limiter.acquire(context.Background())
	if err != nil {
		t.Fatalf("warmup acquire: %v", err)
	}
	release()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	release2, err := limiter.acquire(ctx)
	if err != nil {
		t.Fatalf("acquire must not fail when deadline is tight: %v", err)
	}
	release2()
	// 剩余时间远小于 openAIWSDialWaitReserve，应立即放行而不是等满间隔。
	if elapsed := time.Since(start); elapsed >= openAIWSDialMinInterval {
		t.Fatalf("elapsed %v: tight deadline must skip the interval wait", elapsed)
	}
}

// TestDialLimiterCanceledContext 已取消的 ctx 必须立刻返回错误。
func TestDialLimiterCanceledContext(t *testing.T) {
	limiter := newOpenAIWSAccountDialLimiter()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := limiter.acquire(ctx); err == nil {
		t.Fatal("canceled context must fail acquire")
	}
}

// TestIsOpenAIWSDialRateLimited 只认 403：401 是真鉴权失败，重试有害。
func TestIsOpenAIWSDialRateLimited(t *testing.T) {
	if !isOpenAIWSDialRateLimited(&openAIWSDialError{StatusCode: 403}) {
		t.Fatal("403 must be treated as rate limited")
	}
	for _, code := range []int{401, 429, 500, 0} {
		if isOpenAIWSDialRateLimited(&openAIWSDialError{StatusCode: code}) {
			t.Fatalf("status %d must not be treated as rate limited", code)
		}
	}
	if isOpenAIWSDialRateLimited(nil) || isOpenAIWSDialRateLimited(errors.New("plain")) {
		t.Fatal("non dial errors must not be treated as rate limited")
	}
}

// TestDialWithRateLimitRetriesOn403 403 后退避重试，最终成功。
func TestDialWithRateLimitRetriesOn403(t *testing.T) {
	var calls atomic.Int32
	got, err := dialOpenAIWSWithRateLimit(context.Background(), 4242, func(context.Context) (string, error) {
		if calls.Add(1) == 1 {
			return "", &openAIWSDialError{StatusCode: 403}
		}
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("expected retry to succeed, got %v", err)
	}
	if got != "ok" || calls.Load() != 2 {
		t.Fatalf("got=%q calls=%d, want ok/2", got, calls.Load())
	}
}

// TestDialWithRateLimitNoRetryOnOtherErrors 非 403 立即返回，不浪费预算。
func TestDialWithRateLimitNoRetryOnOtherErrors(t *testing.T) {
	var calls atomic.Int32
	_, err := dialOpenAIWSWithRateLimit(context.Background(), 4243, func(context.Context) (string, error) {
		calls.Add(1)
		return "", &openAIWSDialError{StatusCode: 401}
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if calls.Load() != 1 {
		t.Fatalf("calls=%d, want 1 (401 must not be retried)", calls.Load())
	}
}

// TestDialWithRateLimitGivesUpAfterMax 持续 403 时按上限收敛，不无限重试。
func TestDialWithRateLimitGivesUpAfterMax(t *testing.T) {
	var calls atomic.Int32
	_, err := dialOpenAIWSWithRateLimit(context.Background(), 4244, func(context.Context) (string, error) {
		calls.Add(1)
		return "", &openAIWSDialError{StatusCode: 403}
	})
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if want := int32(openAIWSDialRetryOn403Max + 1); calls.Load() != want {
		t.Fatalf("calls=%d, want %d", calls.Load(), want)
	}
}

// TestDialLimitersArePerAccount 账号之间互不影响 ——
// 实测确认 403 作用域是账号：同一 IP 下账号 A 被限时账号 B 握手照常成功，
// 因此闸门必须按账号隔离，否则会无谓拖慢其他账号。
func TestDialLimitersArePerAccount(t *testing.T) {
	a := globalOpenAIWSDialLimiters.get(9001)
	b := globalOpenAIWSDialLimiters.get(9002)
	if a == nil || b == nil {
		t.Fatal("limiters must be created")
	}
	if a == b {
		t.Fatal("different accounts must not share a limiter")
	}
	if again := globalOpenAIWSDialLimiters.get(9001); again != a {
		t.Fatal("same account must reuse its limiter")
	}
	// 占满账号 A 的闸门，账号 B 不应被阻塞。
	releases := make([]func(), 0, openAIWSDialConcurrencyPerAccount)
	for i := 0; i < openAIWSDialConcurrencyPerAccount; i++ {
		r, err := a.acquire(context.Background())
		if err != nil {
			t.Fatalf("acquire A: %v", err)
		}
		releases = append(releases, r)
	}
	done := make(chan struct{})
	go func() {
		r, err := b.acquire(context.Background())
		if err == nil {
			r()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("account B blocked by account A's saturated limiter")
	}
	for _, r := range releases {
		r()
	}
}

// TestAcquireDialSlotInvalidAccount 无效 accountID 退化为 no-op，
// 保证异常输入不改变上游行为。
func TestAcquireDialSlotInvalidAccount(t *testing.T) {
	release, err := acquireOpenAIWSDialSlot(context.Background(), 0)
	if err != nil || release == nil {
		t.Fatalf("invalid account must yield no-op release, err=%v", err)
	}
	release()
}
