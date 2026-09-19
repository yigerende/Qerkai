package service

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStateKeeperScheduleCoolingSelectsIdleEligibleModels(t *testing.T) {
	s, _, _ := keeperTestService(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.AccountIDs, q.Models = []int64{1, 2, 3}, []string{"a", "b", "c", "d"}
	q.Revision = "cooling-models"
	keeperAddTestAccounts(s, q.AccountIDs)
	s.install(q)
	s.accounts.(*keeperAccountsStub).accounts[3].Status = StatusError
	s.entryLocked(1, "a").row.AutoRetryPending = true
	s.entryLocked(1, "a").value = "saved-state"
	s.entryLocked(1, "c").row.Paused = true
	s.entryLocked(1, "d").row.Collecting = true
	s.entryLocked(2, "c").row.Queued = true
	s.activeCancels[openAIStateKey{2, "d"}] = func() {}
	now := time.Now().UTC()
	limit := s.collectionLimitLocked(2)
	limit.CooldownUntil = now.Add(time.Hour)
	limit.WindowStartedAt, limit.WindowRequests = now, q.AccountHourlyLimit
	limit.RateLimitStreak, limit.NextStartAt = 2, now.Add(time.Second)
	blockedLimit := s.collectionLimitLocked(3)
	blockedLimit.CooldownUntil = now.Add(time.Hour)
	count, err := s.ScheduleCooling()
	require.NoError(t, err)
	require.Equal(t, 3, count)
	require.True(t, limit.CooldownUntil.IsZero())
	require.Zero(t, limit.WindowRequests)
	require.Equal(t, 2, limit.RateLimitStreak)
	require.Equal(t, now.Add(time.Second), limit.NextStartAt)
	require.Equal(t, now.Add(time.Hour), blockedLimit.CooldownUntil)
	require.Equal(t, "saved-state", s.entryLocked(1, "a").value)
	require.False(t, s.entryLocked(1, "b").row.Queued)
	require.True(t, s.entryLocked(1, "c").row.Paused)
	count, err = s.ScheduleCooling()
	require.NoError(t, err)
	require.Zero(t, count, "repeated clicks do not enqueue duplicates")
	for _, key := range []openAIStateKey{{1, "a"}, {2, "a"}, {2, "b"}} {
		job := <-s.queue
		require.Equal(t, key, openAIStateKey{job.accountID, job.model})
		require.Equal(t, "manual", job.source)
		require.False(t, s.rows[key].row.AutoRetryPending)
		require.True(t, s.dirtyRuntime[key])
	}
	require.Empty(t, s.queue)
}

func TestStateKeeperScheduleCoolingDoesNotChangeLimitsWithoutQueueing(t *testing.T) {
	for _, mode := range []string{"disabled", "full", "scope_loading", "active"} {
		t.Run(mode, func(t *testing.T) {
			s, _, _ := keeperTestService(t)
			q := s.config.Load().OpenAIStateKeeperSettings
			if mode == "disabled" {
				q.Enabled = false
				q.Revision = "cooling-disabled"
				s.install(q)
			}
			if mode == "full" {
				s.queue = make(chan openAIStateKeeperJob, 1)
				s.queue <- openAIStateKeeperJob{}
			}
			if mode == "scope_loading" {
				s.entryLocked(1).scopeLoading = true
			}
			if mode == "active" {
				s.entryLocked(1).row.Collecting = true
			}
			limit := s.collectionLimitLocked(1)
			limit.CooldownUntil = time.Now().Add(time.Hour)
			before := *limit
			count, err := s.ScheduleCooling()
			if mode == "disabled" || mode == "full" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Zero(t, count)
			require.Equal(t, before, *limit)
		})
	}
}

func TestStateKeeperScheduleCoolingRunsAndStillHonorsNew429(t *testing.T) {
	s, _, a := keeperTestService(t)
	s.files = keeperTestFileStore(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	limit := s.collectionLimitLocked(a.ID)
	limit.WindowStartedAt, limit.WindowRequests = time.Now(), q.AccountHourlyLimit
	limit.CooldownUntil = time.Now().Add(time.Hour)
	calls := 0
	s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
		calls++
		return openAIStateProbeResult{status: http.StatusTooManyRequests, result: "upstream_error", retryAfter: 2 * time.Minute}
	}
	// The old single-model manual action still cannot bypass the local limit.
	require.NoError(t, s.Schedule([]int64{a.ID}))
	s.run(<-s.queue)
	require.Zero(t, calls)
	count, err := s.ScheduleCooling()
	require.NoError(t, err)
	require.Equal(t, 1, count)
	s.run(<-s.queue)
	require.Equal(t, 1, calls)
	require.Equal(t, 1, limit.WindowRequests)
	require.True(t, limit.CooldownUntil.After(time.Now().Add(time.Minute)))
	require.True(t, s.entryLocked(1).row.AutoRetryPending)
	require.Equal(t, "collected-secret", s.entryLocked(1).value)
	restarted := newOpenAIStateKeeper(s.settings, s.accounts, s.proxies, s.gateway)
	t.Cleanup(restarted.Stop)
	restarted.files = s.files
	restarted.install(q)
	restarted.restoreRuntime()
	require.Equal(t, 1, restarted.collectionLimitLocked(1).WindowRequests)
	require.WithinDuration(t, limit.CooldownUntil, restarted.collectionLimitLocked(1).CooldownUntil, time.Millisecond)
}
