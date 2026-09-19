package service

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStateKeeperShortBudgetsShareConcurrentModelsAndSurviveRestart(t *testing.T) {
	for _, minutes := range []int{5, 10} {
		t.Run(fmt.Sprint(minutes), func(t *testing.T) {
			s, _, _ := keeperTestService(t)
			s.files = keeperTestFileStore(t)
			q := s.config.Load().OpenAIStateKeeperSettings
			q.Models, q.MaxAttempts, q.AccountConcurrency = []string{"a", "b", "c"}, 10, 3
			if minutes == 5 {
				q.AccountFiveMinuteLimit, q.AccountTenMinuteLimit = 5, 9
			} else {
				q.AccountTenMinuteLimit = 5
			}
			require.NoError(t, s.Save(context.Background(), q))
			var calls atomic.Int32
			s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
				calls.Add(1)
				return openAIStateProbeResult{status: 200, result: "not_observed"}
			}
			var jobs sync.WaitGroup
			for _, model := range q.Models {
				jobs.Add(1)
				go func() {
					defer jobs.Done()
					s.run(openAIStateKeeperJob{accountID: 1, model: model, revision: s.config.Load().Revision})
				}()
			}
			jobs.Wait()
			require.EqualValues(t, 4, calls.Load(), "fixture's initial request consumes one shared slot")
			for _, row := range s.Snapshot().Rows[0].Models {
				require.Equal(t, 5, row.FiveMinuteRequests)
				require.Equal(t, 5, row.TenMinuteRequests)
				require.True(t, row.AutoRetryPending)
				require.False(t, row.Paused)
				require.Contains(t, row.RetryReason, fmt.Sprintf("%d 分钟", minutes))
			}
			restarted := newOpenAIStateKeeper(s.settings, s.accounts, s.proxies, s.gateway)
			t.Cleanup(restarted.Stop)
			restarted.files, restarted.probe = s.files, s.probe
			restarted.reload(context.Background())
			q.Models = []string{"new-model"}
			require.NoError(t, restarted.Save(context.Background(), q))
			restarted.run(openAIStateKeeperJob{accountID: 1, model: "new-model", revision: restarted.config.Load().Revision})
			require.EqualValues(t, 4, calls.Load(), "changing models and restarting cannot reset budgets")
		})
	}
}

func TestStateKeeperBudgetWindowsAndExplicitReset(t *testing.T) {
	now := time.Now().UTC()
	q := DefaultOpenAIStateKeeperSettings()
	q.AccountFiveMinuteLimit, q.AccountTenMinuteLimit = 2, 3
	limit := openAIStateCollectionLimit{
		FiveMinute: openAIStateBudgetWindow{now, 2}, TenMinute: openAIStateBudgetWindow{now, 3},
		WindowStartedAt: now, WindowRequests: q.AccountHourlyLimit,
	}
	until, reason := limit.wait(q, now)
	require.Equal(t, now.Add(time.Hour), until)
	require.Contains(t, reason, "每小时")
	limit.WindowRequests = 0
	until, reason = limit.wait(q, now)
	require.Equal(t, now.Add(10*time.Minute), until)
	require.Contains(t, reason, "10 分钟")
	limit.TenMinute.Requests = 0
	until, _ = limit.wait(q, now)
	require.Equal(t, now.Add(5*time.Minute), until)
	until, _ = limit.wait(q, now.Add(5*time.Minute))
	require.True(t, until.IsZero(), "window expires exactly at its end")
	limit.FiveMinute.reserve(now.Add(5*time.Minute), 5*time.Minute)
	require.Equal(t, 1, limit.FiveMinute.Requests)
	limit.CooldownUntil = now.Add(20 * time.Minute)
	until, _ = limit.wait(q, now)
	require.Equal(t, limit.CooldownUntil, until)
	q.AccountFiveMinuteLimit, q.AccountTenMinuteLimit = 0, 0
	limit.CooldownUntil = time.Time{}
	limit.FiveMinute.Requests, limit.TenMinute.Requests = 999, 999
	until, _ = limit.wait(q, now)
	require.True(t, until.IsZero(), "zero disables the added limits")

	s, _, _ := keeperTestService(t)
	q = s.config.Load().OpenAIStateKeeperSettings
	q.AccountFiveMinuteLimit, q.AccountTenMinuteLimit = 2, 3
	require.NoError(t, s.Save(context.Background(), q))
	limit.FiveMinute.Requests, limit.TenMinute.Requests = 2, 3
	s.collectionLimits[1] = &limit
	count, err := s.ScheduleCooling()
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Zero(t, limit.FiveMinute.Requests)
	require.Zero(t, limit.TenMinute.Requests)
}

func TestStateKeeperProxySuccessTotalsPersistIndependentlyOfAccounts(t *testing.T) {
	s, _, _ := keeperTestService(t)
	s.files = keeperTestFileStore(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.Models, q.ProxyIDs = []string{"a", "b"}, []int64{1, 2}
	q.AccountIDs = nil
	for id := int64(1); id <= 10; id++ {
		q.AccountIDs = append(q.AccountIDs, id)
	}
	keeperAddTestAccounts(s, q.AccountIDs)
	require.NoError(t, s.Save(context.Background(), q))
	var jobs sync.WaitGroup
	for _, id := range q.AccountIDs {
		for _, model := range q.Models {
			jobs.Add(1)
			go func() {
				defer jobs.Done()
				s.run(openAIStateKeeperJob{accountID: id, model: model, revision: s.config.Load().Revision})
			}()
		}
	}
	jobs.Wait()
	require.EqualValues(t, 20, s.Snapshot().ProxySuccesses[1])
	q.ProxyIDs = []int64{2, 1}
	require.NoError(t, s.Save(context.Background(), q))
	s.run(openAIStateKeeperJob{accountID: 1, model: "a", revision: s.config.Load().Revision})
	require.EqualValues(t, 1, s.Snapshot().ProxySuccesses[2])
	s.accounts.(*keeperAccountsStub).accounts = map[int64]*Account{}
	require.NoError(t, s.SyncSelection(context.Background()))
	require.Empty(t, s.Snapshot().Rows)
	restarted := newOpenAIStateKeeper(s.settings, s.accounts, s.proxies, s.gateway)
	t.Cleanup(restarted.Stop)
	restarted.files = s.files
	restarted.reload(context.Background())
	require.Equal(t, map[int64]int64{1: 20, 2: 1}, restarted.Snapshot().ProxySuccesses)
	require.Empty(t, restarted.Snapshot().ProxyStatsError)
}

func TestStateKeeperProxySuccessCountsOnlySavedStates(t *testing.T) {
	s, _, _ := keeperTestService(t)
	s.files = keeperTestFileStore(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.AllowedStateLengths = []int{3}
	require.NoError(t, s.Save(context.Background(), q))
	for _, result := range []openAIStateProbeResult{
		{status: 200, result: "collected", value: "wrong-length"},
		{status: 503, result: "upstream_error"},
	} {
		s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult { return result }
		s.run(openAIStateKeeperJob{accountID: 1, revision: s.config.Load().Revision})
	}
	require.Empty(t, s.Snapshot().ProxySuccesses)
	s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
		return openAIStateProbeResult{status: 200, result: "collected", value: "yes"}
	}
	s.files.cipher = nil
	s.run(openAIStateKeeperJob{accountID: 1, revision: s.config.Load().Revision})
	require.Empty(t, s.Snapshot().ProxySuccesses, "failed State writes are not successes")
}

func TestStateKeeperProxyStatsReadFailureDoesNotResetHistory(t *testing.T) {
	s, _, _ := keeperTestService(t)
	s.files = keeperTestFileStore(t)
	require.NoError(t, os.MkdirAll(s.files.dir, 0700))
	require.NoError(t, os.WriteFile(s.files.proxyStatsPath(), []byte("broken"), 0600))
	s.run(openAIStateKeeperJob{accountID: 1, revision: s.config.Load().Revision})
	require.EqualValues(t, 1, s.Snapshot().ProxySuccesses[1])
	require.NotEmpty(t, s.Snapshot().ProxyStatsError)
	body, err := os.ReadFile(s.files.proxyStatsPath())
	require.NoError(t, err)
	require.Equal(t, "broken", string(body))
	require.NoError(t, os.WriteFile(s.files.proxyStatsPath(), []byte(`{"1":10}`), 0600))
	s.saveMu.Lock()
	s.flushProxySuccessesLocked()
	s.saveMu.Unlock()
	require.EqualValues(t, 11, s.Snapshot().ProxySuccesses[1])
	require.Empty(t, s.Snapshot().ProxyStatsError)
	restarted := newOpenAIStateKeeper(s.settings, s.accounts, s.proxies, s.gateway)
	t.Cleanup(restarted.Stop)
	restarted.files = s.files
	restarted.restoreRuntime()
	require.EqualValues(t, 11, restarted.Snapshot().ProxySuccesses[1])
}

func TestStateKeeperProxyStatsWriteFailureRetriesWithoutLosingState(t *testing.T) {
	s, _, _ := keeperTestService(t)
	s.files = keeperTestFileStore(t)
	require.NoError(t, s.Save(context.Background(), s.config.Load().OpenAIStateKeeperSettings))
	require.NoError(t, os.MkdirAll(s.files.proxyStatsPath(), 0700))
	s.run(openAIStateKeeperJob{accountID: 1, revision: s.config.Load().Revision})
	require.True(t, s.Snapshot().Rows[0].StateFileSaved)
	require.EqualValues(t, 1, s.Snapshot().ProxySuccesses[1])
	require.True(t, s.proxyStatsDirty)
	require.NotEmpty(t, s.Snapshot().ProxyStatsError)
	require.NoError(t, os.Remove(s.files.proxyStatsPath()))
	s.saveMu.Lock()
	s.flushProxySuccessesLocked()
	s.saveMu.Unlock()
	require.False(t, s.proxyStatsDirty)
	restarted := newOpenAIStateKeeper(s.settings, s.accounts, s.proxies, s.gateway)
	t.Cleanup(restarted.Stop)
	restarted.files = s.files
	restarted.restoreRuntime()
	require.EqualValues(t, 1, restarted.Snapshot().ProxySuccesses[1])
}
