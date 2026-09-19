package service

import (
	"context"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// All models share the account's start rate, collection budgets and 429 backoff.
// Snapshots are stored with model runtimes; restore takes the newest snapshot.
type openAIStateCollectionLimit struct {
	UpdatedAt       time.Time               `json:"updated_at"`
	NextStartAt     time.Time               `json:"next_start_at"`
	CooldownUntil   time.Time               `json:"cooldown_until"`
	CooldownReason  string                  `json:"cooldown_reason"`
	RateLimitStreak int                     `json:"rate_limit_streak"`
	WindowStartedAt time.Time               `json:"window_started_at"`
	WindowRequests  int                     `json:"window_requests"`
	FiveMinute      openAIStateBudgetWindow `json:"five_minute"`
	TenMinute       openAIStateBudgetWindow `json:"ten_minute"`
}

type openAIStateBudgetWindow struct {
	StartedAt time.Time `json:"started_at"`
	Requests  int       `json:"requests"`
}

func (w openAIStateBudgetWindow) count(now time.Time, duration time.Duration) int {
	if w.StartedAt.Add(duration).After(now) {
		return w.Requests
	}
	return 0
}

func (w *openAIStateBudgetWindow) reserve(now time.Time, duration time.Duration) {
	if !w.StartedAt.Add(duration).After(now) {
		w.StartedAt, w.Requests = now, 0
	}
	w.Requests++
}

func (s *OpenAIStateKeeperService) collectionLimitLocked(id int64) *openAIStateCollectionLimit {
	limit := s.collectionLimits[id]
	if limit == nil {
		limit = &openAIStateCollectionLimit{}
		s.collectionLimits[id] = limit
	}
	return limit
}

func keeperJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	bound := int64(d / 5)
	if bound < 1 {
		bound = 1
	}
	return d + time.Duration(rand.Int64N(bound))
}

func keeperBackoff(q OpenAIStateKeeperSettings, failures int) time.Duration {
	seconds := max(1, q.CooldownSeconds)
	ceiling := max(seconds, q.MaxCooldownSeconds)
	for i := 1; i < failures && seconds < ceiling; i++ {
		seconds = min(ceiling, seconds*2)
	}
	return min(time.Duration(ceiling)*time.Second, keeperJitter(time.Duration(seconds)*time.Second))
}

func keeperRetryAfter(header string, now time.Time) time.Duration {
	value := strings.TrimSpace(header)
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil && seconds >= 0 {
		// A malformed huge value must not overflow into an immediate retry.
		return time.Duration(min(seconds, int64((365*24*time.Hour)/time.Second))) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil && at.After(now) {
		return at.Sub(now)
	}
	return 0
}

func (s *OpenAIStateKeeperService) accountCollectionWaitLocked(id int64, cfg *openAIStateKeeperConfig, now time.Time) (time.Time, string) {
	limit := s.collectionLimitLocked(id)
	return limit.wait(cfg.OpenAIStateKeeperSettings, now)
}

func (limit *openAIStateCollectionLimit) wait(cfg OpenAIStateKeeperSettings, now time.Time) (time.Time, string) {
	until, reason := limit.CooldownUntil, limit.CooldownReason
	for _, budget := range []struct {
		window   openAIStateBudgetWindow
		maximum  int
		duration time.Duration
		reason   string
	}{
		{limit.FiveMinute, cfg.AccountFiveMinuteLimit, 5 * time.Minute, "已达单账号每 5 分钟采集上限，等待自动恢复"},
		{limit.TenMinute, cfg.AccountTenMinuteLimit, 10 * time.Minute, "已达单账号每 10 分钟采集上限，等待自动恢复"},
		{openAIStateBudgetWindow{limit.WindowStartedAt, limit.WindowRequests}, cfg.AccountHourlyLimit, time.Hour, "已达单账号每小时采集上限，等待自动恢复"},
	} {
		if budget.maximum > 0 && budget.window.Requests >= budget.maximum {
			if end := budget.window.StartedAt.Add(budget.duration); end.After(until) {
				until, reason = end, budget.reason
			}
		}
	}
	if until.After(now) {
		return until, reason
	}
	return time.Time{}, ""
}

func (s *OpenAIStateKeeperService) noteCollectionRateLimitLocked(id int64, q OpenAIStateKeeperSettings, result openAIStateProbeResult, now time.Time) {
	if result.status != http.StatusTooManyRequests || result.permanentFailure || result.accountUnavailable {
		return
	}
	limit := s.collectionLimitLocked(id)
	// Concurrent 429s from the same burst count as one backoff step.
	if !limit.CooldownUntil.After(now) {
		limit.RateLimitStreak = min(20, limit.RateLimitStreak+1)
	}
	delay := keeperBackoff(q, limit.RateLimitStreak)
	if result.retryAfter > delay {
		delay = result.retryAfter + keeperJitter(time.Second)
	}
	until := now.Add(delay)
	if until.After(limit.CooldownUntil) {
		limit.CooldownUntil = until
	}
	limit.CooldownReason = "上游 429，账号全部模型冷却后自动重试"
	limit.UpdatedAt = now
	for key := range s.rows {
		if key.accountID == id {
			s.dirtyRuntime[key] = true
		}
	}
}

func (s *OpenAIStateKeeperService) decorateCollectionLimitLocked(row *OpenAIStateKeeperRow, now time.Time) {
	cfg := s.config.Load()
	row.EffectiveConcurrency = cfg.AccountConcurrency
	if limit := s.collectionLimits[row.AccountID]; limit != nil {
		if limit.RateLimitStreak > 0 {
			row.EffectiveConcurrency = 1
		}
		if limit.WindowStartedAt.Add(time.Hour).After(now) {
			row.HourlyRequests = limit.WindowRequests
		}
		row.FiveMinuteRequests = limit.FiveMinute.count(now, 5*time.Minute)
		row.TenMinuteRequests = limit.TenMinute.count(now, 10*time.Minute)
		until, reason := limit.wait(cfg.OpenAIStateKeeperSettings, now)
		if until.After(now) && !row.Paused && !row.AccountUnavailable {
			row.CooldownUntil = &until
			if row.AutoRetryPending && (row.NextRetryAt == nil || until.After(*row.NextRetryAt)) {
				row.NextRetryAt, row.NextAttemptAt, row.RetryReason = &until, &until, reason
			}
		}
	}
}

func (s *OpenAIStateKeeperService) acquireStateProbe(ctx context.Context, cfg *openAIStateKeeperConfig, job openAIStateKeeperJob, budget int) (int, bool) {
	for {
		s.mu.Lock()
		e := s.entryLocked(job.accountID, job.model)
		if ctx.Err() != nil || s.config.Load() != cfg || e == nil || e.row.AccountUnavailable || e.row.Paused || e.row.RoundAttempts >= budget {
			s.mu.Unlock()
			return 0, false
		}
		now := time.Now().UTC()
		if until, _ := s.accountCollectionWaitLocked(job.accountID, cfg, now); !until.IsZero() {
			s.mu.Unlock()
			return 0, false
		}
		limit := s.collectionLimitLocked(job.accountID)
		parallel := cfg.AccountConcurrency
		if limit.RateLimitStreak > 0 {
			parallel = 1
		}
		if s.activeProbes >= cfg.Concurrency || s.accountProbes[job.accountID] >= parallel {
			s.workerSlots.Wait()
			s.mu.Unlock()
			continue
		}
		if delay := limit.NextStartAt.Sub(now); delay > 0 {
			s.mu.Unlock()
			if !keeperWait(ctx, delay) {
				return 0, false
			}
			continue
		}
		if !limit.WindowStartedAt.Add(time.Hour).After(now) {
			limit.WindowStartedAt, limit.WindowRequests = now, 0
		}
		limit.WindowRequests++
		limit.FiveMinute.reserve(now, 5*time.Minute)
		limit.TenMinute.reserve(now, 10*time.Minute)
		limit.UpdatedAt = now
		limit.NextStartAt = now.Add(keeperJitter(time.Duration(cfg.RequestIntervalSeconds) * time.Second))
		s.activeProbes++
		s.accountProbes[job.accountID]++
		e.row.RoundAttempts++
		e.row.Attempts++
		e.row.LastAttemptAt = &now
		number := e.row.RoundAttempts
		s.mu.Unlock()
		return number, true
	}
}

func keeperWait(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}

func (s *OpenAIStateKeeperService) deferCollectionLocked(e *openAIKeptState, until time.Time, reason string) {
	e.row.Paused, e.row.PauseReason = false, ""
	e.row.AutoRetryPending, e.row.NextRetryAt, e.row.NextAttemptAt, e.row.RetryReason = true, &until, &until, reason
}

func keeperLegacyTransientPause(reason string) bool {
	switch reason {
	case "已用完采集和重试次数，等待人工重试", "所有采集代理均已用完采集和重试次数，等待人工重试", "采集被中断，等待人工重试", "上次采集未完成，等待人工重试":
		return true
	}
	return false
}
