package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStateKeeperRetryAfterAndPermanentClassification(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	for _, tc := range []struct {
		value string
		want  time.Duration
	}{{"120", 120 * time.Second}, {now.Add(3 * time.Minute).Format(http.TimeFormat), 3 * time.Minute}, {"garbage", 0}, {"-1", 0}, {now.Add(-time.Minute).Format(http.TimeFormat), 0}} {
		require.Equal(t, tc.want, keeperRetryAfter(tc.value, now))
	}
	for _, tc := range []struct {
		status                 int
		code                   string
		permanent, unavailable bool
	}{
		{429, "rate_limit_exceeded", false, false}, {503, "server_error", false, false},
		{403, "", false, false}, {401, "invalid_token", false, true},
		{404, "model_not_found", true, false}, {429, "insufficient_quota", true, false},
	} {
		r := parseOpenAIStateProbeResponse(&http.Response{StatusCode: tc.status, Header: http.Header{"Retry-After": {"120"}}, Body: io.NopCloser(strings.NewReader(fmt.Sprintf(`{"error":{"code":%q}}`, tc.code)))})
		require.Equal(t, tc.permanent, r.permanentFailure, tc.code)
		require.Equal(t, tc.unavailable, r.accountUnavailable, tc.code)
		if tc.status == 429 {
			require.Equal(t, 120*time.Second, r.retryAfter)
		}
	}
}

func TestStateKeeperShortBatchesRotateBeforeBudgetExhaustion(t *testing.T) {
	s, _, _ := keeperTestService(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.ProxyIDs, q.MaxAttempts, q.ProxyFailureThreshold = []int64{3, 2, 1}, 6, 2
	q.AllowedStateLengths = []int{332}
	require.NoError(t, s.Save(context.Background(), q))
	var used []int64
	s.probe = func(_ context.Context, cfg OpenAIStateKeeperSettings, _ int64) openAIStateProbeResult {
		used = append(used, cfg.ProxyID)
		length := 356
		if cfg.ProxyID == 1 {
			length = 332
		}
		return openAIStateProbeResult{status: 200, result: "collected", value: strings.Repeat("s", length)}
	}
	s.run(openAIStateKeeperJob{accountID: 1, revision: s.config.Load().Revision})
	require.Equal(t, []int64{3, 3, 2, 2, 1}, used)
	require.Equal(t, "ready", s.Snapshot().Rows[0].Status)
	require.False(t, s.Snapshot().Rows[0].AutoRetryPending)

	used = nil
	s.probe = func(_ context.Context, cfg OpenAIStateKeeperSettings, _ int64) openAIStateProbeResult {
		used = append(used, cfg.ProxyID)
		if cfg.ProxyID == 3 {
			return openAIStateProbeResult{result: "failed", proxyFailure: true}
		}
		return openAIStateProbeResult{status: 200, result: "collected", value: strings.Repeat("s", 332)}
	}
	s.run(openAIStateKeeperJob{accountID: 1, revision: s.config.Load().Revision})
	require.Equal(t, []int64{3, 2}, used, "connection failures move to the next configured proxy immediately")
}

func TestStateKeeperRejectedLengthRetriesUntilAcceptedAcrossRoundsAndRestart(t *testing.T) {
	s, _, account := keeperTestService(t)
	s.files = keeperTestFileStore(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.AllowedStateLengths, q.DegradedStateLengths = []int{332}, []int{356}
	q.MaxAttempts, q.RetryCount, q.AccountConcurrency = 2, 0, 1
	q.AutoRefresh, q.DegradationScanEnabled, q.InjectionEnabled = false, false, false
	require.NoError(t, s.Save(context.Background(), q))
	calls := 0
	probe := func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
		calls++
		length := 356
		if calls > 6 {
			length = 332
		}
		return openAIStateProbeResult{status: http.StatusOK, result: "collected", value: strings.Repeat("s", length), credentialStamp: stateKeeperCredentialStamp(account)}
	}
	s.probe = probe
	s.run(openAIStateKeeperJob{accountID: account.ID, revision: s.config.Load().Revision, source: "manual"})
	for cycle := 0; cycle < 3; cycle++ {
		row := s.Snapshot().Rows[0]
		require.Equal(t, (cycle+1)*q.MaxAttempts, calls)
		require.Equal(t, "filtered", row.Status)
		require.Equal(t, 356, row.TurnStateLength)
		require.False(t, row.Paused)
		require.Empty(t, row.PauseReason)
		require.False(t, row.StateFileSaved)
		require.True(t, row.AutoRetryPending)
		require.NotNil(t, row.NextRetryAt)
		if cycle == 0 {
			s.Stop()
			restarted := newOpenAIStateKeeper(s.settings, s.accounts, s.proxies, s.gateway)
			t.Cleanup(restarted.Stop)
			restarted.files, restarted.probe = s.files, probe
			restarted.reload(context.Background())
			s = restarted
			restored := s.Snapshot().Rows[0]
			require.False(t, restored.Paused)
			require.True(t, restored.AutoRetryPending)
			require.Equal(t, row.NextRetryAt.UnixNano(), restored.NextRetryAt.UnixNano())
		}
		s.scheduleDue(row.NextRetryAt.Add(-time.Nanosecond))
		require.Empty(t, s.queue)
		s.scheduleDue(*row.NextRetryAt)
		s.scheduleDue(*row.NextRetryAt)
		require.Len(t, s.queue, 1, "only one automatic continuation may be queued")
		job := <-s.queue
		require.Equal(t, "automatic_retry", job.source)
		s.run(job)
	}
	row := s.Snapshot().Rows[0]
	require.Equal(t, 7, calls)
	require.Equal(t, "ready", row.Status)
	require.True(t, row.StateFileSaved)
	require.Equal(t, 332, row.SavedStateLength)
	require.False(t, row.Paused)
	require.False(t, row.AutoRetryPending)
	s.scheduleDue(time.Now().Add(24 * time.Hour))
	require.Empty(t, s.queue, "successful collection stops the automatic recovery loop")
}

func TestStateKeeper429IsAccountWidePersistentAndRespectsRetryAfter(t *testing.T) {
	s, _, _ := keeperTestService(t)
	s.files = keeperTestFileStore(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.Models, q.ProxyIDs, q.MaxAttempts = []string{"a", "b"}, []int64{1, 2}, 10
	q.AccountConcurrency = 3
	require.NoError(t, s.Save(context.Background(), q))
	var mu sync.Mutex
	calls := 0
	s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
		mu.Lock()
		calls++
		mu.Unlock()
		return openAIStateProbeResult{status: 429, result: "upstream_error", retryAfter: 2 * time.Minute}
	}
	start := time.Now()
	s.run(openAIStateKeeperJob{accountID: 1, model: "a", revision: s.config.Load().Revision})
	before := calls
	require.LessOrEqual(t, before, 3)
	row := s.Snapshot().Rows[0].Models[0]
	require.False(t, row.Paused)
	require.True(t, row.AutoRetryPending)
	require.True(t, row.NextRetryAt.After(start.Add(2*time.Minute)))
	require.Equal(t, 1, row.EffectiveConcurrency)
	s.run(openAIStateKeeperJob{accountID: 1, model: "b", revision: s.config.Load().Revision})
	require.Equal(t, before, calls, "manual collection of another model cannot bypass the 429")
	s.scheduleDue(time.Now())
	require.Empty(t, s.queue)

	// Changing the model list cannot discard the account's saved throttle.
	q.Models = []string{"new-model"}
	require.NoError(t, s.Save(context.Background(), q))
	restarted := newOpenAIStateKeeper(s.settings, s.accounts, s.proxies, s.gateway)
	t.Cleanup(restarted.Stop)
	restarted.files, restarted.probe = s.files, s.probe
	restarted.reload(context.Background())
	restarted.run(openAIStateKeeperJob{accountID: 1, model: "new-model", revision: restarted.config.Load().Revision})
	require.Equal(t, before, calls)
	require.NotNil(t, restarted.Snapshot().Rows[0].Models[0].CooldownUntil)
}

func TestStateKeeperHourlyBudgetSharedAcrossModelsAndRestart(t *testing.T) {
	s, _, _ := keeperTestService(t)
	s.files = keeperTestFileStore(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.Models, q.MaxAttempts, q.AccountHourlyLimit, q.AccountConcurrency = []string{"a", "b"}, 10, 5, 3
	require.NoError(t, s.Save(context.Background(), q))
	var mu sync.Mutex
	calls := 0
	s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
		mu.Lock()
		calls++
		mu.Unlock()
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
	require.Equal(t, 4, calls, "the fixture's first collection also belongs to the hourly budget")
	for _, row := range s.Snapshot().Rows[0].Models {
		require.True(t, row.AutoRetryPending)
		require.False(t, row.Paused)
		require.Equal(t, 5, row.HourlyRequests)
		require.Contains(t, row.RetryReason, "每小时")
	}
	restarted := newOpenAIStateKeeper(s.settings, s.accounts, s.proxies, s.gateway)
	t.Cleanup(restarted.Stop)
	restarted.files, restarted.probe = s.files, s.probe
	restarted.reload(context.Background())
	restarted.run(openAIStateKeeperJob{accountID: 1, model: "b", revision: restarted.config.Load().Revision})
	require.Equal(t, 4, calls)
}

func TestStateKeeperRequestPacingSharedByModels(t *testing.T) {
	s, _, _ := keeperTestService(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.Models, q.AccountConcurrency, q.RequestIntervalSeconds = []string{"a", "b", "c"}, 3, 1
	require.NoError(t, s.Save(context.Background(), q))
	var mu sync.Mutex
	var starts []time.Time
	s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
		mu.Lock()
		starts = append(starts, time.Now())
		mu.Unlock()
		return openAIStateProbeResult{status: 200, result: "collected", value: "ok"}
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
	sort.Slice(starts, func(i, j int) bool { return starts[i].Before(starts[j]) })
	require.Len(t, starts, 3)
	for i := 1; i < len(starts); i++ {
		require.GreaterOrEqual(t, starts[i].Sub(starts[i-1]), time.Second)
	}
}

func TestStateKeeperPermanentFailurePausesAndKeepsOldState(t *testing.T) {
	s, _, _ := keeperTestService(t)
	old := s.entryLocked(1).value
	s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
		return openAIStateProbeResult{status: 404, result: "upstream_error", permanentFailure: true, message: "model_not_found"}
	}
	s.run(openAIStateKeeperJob{accountID: 1, revision: s.config.Load().Revision})
	row := s.Snapshot().Rows[0]
	require.True(t, row.Paused)
	require.False(t, row.AutoRetryPending)
	require.Contains(t, row.PauseReason, "model_not_found")
	require.Equal(t, old, s.entryLocked(1).value)
	s.scheduleDue(time.Now().Add(24 * time.Hour))
	require.Empty(t, s.queue)
}

func TestStateKeeperResponseRecoveryStopsWhenInjectionDisabled(t *testing.T) {
	s, _, _ := keeperTestService(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.ResponseRefreshEnabled = true
	require.NoError(t, s.Save(context.Background(), q))
	s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
		return openAIStateProbeResult{status: 503, result: "upstream_error"}
	}
	s.run(openAIStateKeeperJob{accountID: 1, revision: s.config.Load().Revision, source: "response"})
	require.True(t, s.Snapshot().Rows[0].AutoRetryPending)
	q.InjectionEnabled = false
	require.NoError(t, s.Save(context.Background(), q))
	s.scheduleDue(time.Now().Add(time.Hour))
	require.Empty(t, s.queue)
	require.False(t, s.Snapshot().Rows[0].AutoRetryPending)
	s.run(openAIStateKeeperJob{accountID: 1, revision: s.config.Load().Revision, source: "manual"})
	require.True(t, s.Snapshot().Rows[0].AutoRetryPending, "manual collection recovery remains independent of injection")
}

func TestStateKeeperRecoverySettingsDefaultsAndValidation(t *testing.T) {
	s, _, _ := keeperTestService(t)
	require.NoError(t, s.settings.Set(context.Background(), openAIStateKeeperSettingKey, `{"revision":"legacy"}`))
	s.reload(context.Background())
	q := s.config.Load().OpenAIStateKeeperSettings
	require.Equal(t, 1, q.RequestIntervalSeconds)
	require.Equal(t, 2, q.ProxyFailureThreshold)
	require.Equal(t, 30, q.CooldownSeconds)
	require.Equal(t, 900, q.MaxCooldownSeconds)
	require.Equal(t, 120, q.AccountHourlyLimit)
	for _, edit := range []func(*OpenAIStateKeeperSettings){
		func(q *OpenAIStateKeeperSettings) { q.RequestIntervalSeconds = -1 },
		func(q *OpenAIStateKeeperSettings) { q.RequestIntervalSeconds = 301 },
		func(q *OpenAIStateKeeperSettings) { q.ProxyFailureThreshold = 0 },
		func(q *OpenAIStateKeeperSettings) { q.CooldownSeconds = 0 },
		func(q *OpenAIStateKeeperSettings) { q.MaxCooldownSeconds = 29 },
		func(q *OpenAIStateKeeperSettings) { q.AccountHourlyLimit = 0 },
	} {
		invalid := q
		edit(&invalid)
		require.Error(t, invalid.Validate())
	}
}

func TestStateKeeperUnavailableAccountClearsAutomaticRecovery(t *testing.T) {
	s, _, account := keeperTestService(t)
	s.mu.Lock()
	s.deferCollectionLocked(s.entryLocked(1), time.Now().Add(time.Minute), "retry")
	s.mu.Unlock()
	account.Status = StatusError
	s.syncAccountAvailability([]*Account{account})
	row := s.Snapshot().Rows[0]
	require.True(t, row.AccountUnavailable)
	require.False(t, row.AutoRetryPending)
	require.Nil(t, row.NextRetryAt)
	require.Empty(t, row.RetryReason)
	s.scheduleDue(time.Now().Add(time.Hour))
	require.Empty(t, s.queue)
}

func TestStateKeeperAutomaticRecoveryLoad(t *testing.T) {
	for _, persist := range []bool{false, true} {
		t.Run(fmt.Sprintf("persist=%t", persist), func(t *testing.T) {
			s, _, _ := keeperTestService(t)
			if persist {
				s.files = keeperTestFileStore(t)
			}
			q := s.config.Load().OpenAIStateKeeperSettings
			q.AccountIDs, q.Models = nil, []string{"a", "b", "c", "d"}
			for id := int64(1); id <= 40; id++ {
				q.AccountIDs = append(q.AccountIDs, id)
			}
			keeperAddTestAccounts(s, q.AccountIDs)
			q.ProxyIDs, q.ProxyFailureThreshold, q.MaxAttempts = []int64{1, 2, 3}, 1, 3
			q.Concurrency, q.AccountConcurrency = 30, 2
			q.CooldownSeconds, q.MaxCooldownSeconds, q.Revision = 1, 2, "recovery-load"
			s.install(q)
			var mu sync.Mutex
			calls, accountCalls, accountActive := map[openAIStateKey]int{}, map[int64]int{}, map[int64]int{}
			active, peak, total := 0, 0, 0
			violated := false
			s.probe = func(ctx context.Context, cfg OpenAIStateKeeperSettings, id int64) openAIStateProbeResult {
				mu.Lock()
				key := openAIStateKey{id, cfg.Model}
				calls[key]++
				accountCalls[id]++
				accountActive[id]++
				active++
				total++
				n, first := calls[key], accountCalls[id] == 1
				peak = max(peak, active)
				violated = violated || active > q.Concurrency || accountActive[id] > q.AccountConcurrency
				mu.Unlock()
				keeperWait(ctx, 3*time.Millisecond)
				mu.Lock()
				active--
				accountActive[id]--
				mu.Unlock()
				if first {
					return openAIStateProbeResult{status: 429, result: "upstream_error"}
				}
				if n < 3 {
					return openAIStateProbeResult{status: 200, result: "not_observed"}
				}
				return openAIStateProbeResult{status: 200, result: "collected", value: fmt.Sprintf("state-%d-%s", id, cfg.Model)}
			}
			s.startWorkers()
			require.NoError(t, s.Schedule(nil))
			require.Eventually(t, func() bool {
				s.scheduleDue(time.Now())
				for _, account := range s.Snapshot().Rows {
					for _, row := range account.Models {
						if row.Status != "ready" || row.Collecting || row.Queued || row.AutoRetryPending || row.Paused {
							return false
						}
					}
				}
				return true
			}, 30*time.Second, 10*time.Millisecond)
			s.Stop()
			mu.Lock()
			defer mu.Unlock()
			require.False(t, violated)
			require.Len(t, calls, 160)
			for key := range calls {
				require.Equal(t, fmt.Sprintf("state-%d-%s", key.accountID, key.model), s.rows[key].value)
			}
			t.Logf("40 accounts x 4 models recovered automatically: %d probes, global peak %d/30, account limit 2; 429 + rejected headers + proxy rotation", total, peak)
		})
	}
}
