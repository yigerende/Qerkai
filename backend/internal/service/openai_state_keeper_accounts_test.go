package service

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStateKeeperUnavailableAccountsRemainConfiguredAndSkipEveryTrigger(t *testing.T) {
	s, _, a := keeperTestService(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.AutoRefresh, q.AutoCollectIntervalSeconds, q.DegradationScanEnabled = true, 1, true
	for _, status := range []string{StatusError, StatusDisabled} {
		a.Status = status
		require.NoError(t, s.Save(context.Background(), q))
		require.Equal(t, []int64{a.ID}, s.Snapshot().Settings.AccountIDs)
		require.True(t, s.Snapshot().Rows[0].AccountUnavailable)
		require.NotEmpty(t, s.entryLocked(a.ID).value, "do not discard collected State")
		require.NoError(t, s.Schedule(nil))
		s.entryLocked(a.ID).row.Paused = true
		n, err := s.SchedulePaused()
		require.NoError(t, err)
		require.Zero(t, n)
		s.scheduleDue(time.Now().Add(time.Hour))
		s.scheduleDegradationScan(time.Now().Add(time.Hour))
		s.mu.Lock()
		require.NoError(t, s.enqueueSourceLocked(s.config.Load(), []int64{a.ID}, "response"))
		s.mu.Unlock()
		require.Empty(t, s.queue)
	}
	a.Status = StatusActive
	require.NoError(t, s.SyncSelection(context.Background()))
	require.False(t, s.Snapshot().Rows[0].AccountUnavailable)
	require.NoError(t, s.Schedule(nil))
	require.Len(t, s.queue, 1)
}

func TestStateKeeperQueuedAccountBecomingInvalidDoesNotProbe(t *testing.T) {
	s, _, a := keeperTestService(t)
	require.NoError(t, s.Schedule(nil))
	a.Status = StatusError
	s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
		t.Fatal("invalid queued account reached upstream")
		return openAIStateProbeResult{}
	}
	s.run(<-s.queue)
	require.True(t, s.Snapshot().Rows[0].AccountUnavailable)
	require.False(t, s.Snapshot().Rows[0].Queued)
}

func TestStateKeeper401StopsParallelCollectionAndSurvivesSaveAndRestart(t *testing.T) {
	s, gateway, a := keeperTestService(t)
	s.files = keeperTestFileStore(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.Models, q.AccountConcurrency, q.MaxAttempts = []string{q.Model, "other-model"}, 4, 20
	q.RetryCount, q.RetryIntervalSeconds = 5, 1
	require.NoError(t, s.Save(context.Background(), q))
	var calls atomic.Int32
	started := make(chan struct{}, 4)
	s.probe = func(ctx context.Context, _ OpenAIStateKeeperSettings, _ int64) openAIStateProbeResult {
		n := calls.Add(1)
		started <- struct{}{}
		if n == 1 {
			for range 4 {
				<-started
			}
			return openAIStateProbeResult{status: 401, result: "upstream_error", credentialStamp: stateKeeperCredentialStamp(a)}
		}
		<-ctx.Done()
		return openAIStateProbeResult{result: "failed"}
	}
	require.NoError(t, s.Schedule(nil))
	for len(s.queue) > 0 {
		s.run(<-s.queue)
	}
	require.EqualValues(t, 4, calls.Load(), "only already-started siblings are allowed; no retries or next model")
	for _, row := range s.Snapshot().Rows[0].Models {
		require.True(t, row.AccountUnavailable)
		require.True(t, row.Paused)
	}
	q.Concurrency++
	q.Models = append(q.Models, "new-model-after-401")
	require.NoError(t, s.Save(context.Background(), q))
	require.NoError(t, s.Schedule(nil))
	require.Empty(t, s.queue)
	restarted := newOpenAIStateKeeper(s.settings, s.accounts, s.proxies, gateway)
	t.Cleanup(restarted.Stop)
	restarted.files = s.files
	restarted.reload(context.Background())
	require.NoError(t, restarted.SyncSelection(context.Background()))
	require.NoError(t, restarted.Schedule(nil))
	require.Empty(t, restarted.queue)
	for _, row := range restarted.Snapshot().Rows[0].Models {
		require.True(t, row.AccountUnavailable)
	}
	a.Credentials["access_token"] = "repaired-token"
	require.NoError(t, restarted.Schedule(nil))
	require.Len(t, restarted.queue, 3)
	require.False(t, restarted.Snapshot().Rows[0].AccountUnavailable)
}

func TestStateKeeperClassifiesOnlyCredentialFailuresAsUnavailable(t *testing.T) {
	for _, tc := range []struct {
		status  int
		body    string
		blocked bool
	}{
		{401, `{}`, true},
		{403, `{"error":{"code":"account_deactivated"}}`, true},
		{403, `{"error":{"code":"unsupported_country_region_territory"}}`, false},
		{429, `{"error":{"code":"rate_limit_exceeded"}}`, false},
		{502, `{}`, false},
		{503, `{}`, false},
	} {
		response := &http.Response{StatusCode: tc.status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(tc.body))}
		r := parseOpenAIStateProbeResponse(response)
		require.Equal(t, tc.blocked, r.accountUnavailable, "status=%d body=%s", tc.status, tc.body)
	}
}

func keeperAccountRecoveryFixture(t *testing.T) (*OpenAIStateKeeperService, *Account) {
	t.Helper()
	s, _, a := keeperTestService(t)
	s.files = keeperTestFileStore(t)
	q := s.Snapshot().Settings
	q.Models = defaultStateKeeperModels()
	require.NoError(t, s.Save(context.Background(), q))
	s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
		return openAIStateProbeResult{status: 401, result: "upstream_error", credentialStamp: stateKeeperCredentialStamp(a)}
	}
	s.run(openAIStateKeeperJob{accountID: a.ID, model: q.Models[0], revision: s.config.Load().Revision, source: "manual"})
	return s, a
}

func TestStateKeeperAccountRecoveryAutomaticallyResumesAllModels(t *testing.T) {
	for _, repair := range []string{"token", "status", "legacy-cleared-block"} {
		t.Run(repair, func(t *testing.T) {
			s, a := keeperAccountRecoveryFixture(t)
			switch repair {
			case "token":
				a.Credentials["access_token"] = "external-api-updated-token"
			case "status":
				a.Status = StatusError
				require.NoError(t, s.SyncSelection(context.Background()))
				a.Status = StatusActive
			case "legacy-cleared-block":
				for _, e := range s.rows {
					e.blockedCredentialStamp = ""
					e.row.AccountUnavailable, e.row.AccountUnavailableReason = false, ""
				}
			}
			require.NoError(t, s.SyncSelection(context.Background()))
			for _, row := range s.Snapshot().Rows[0].Models {
				require.False(t, row.AccountUnavailable)
				require.False(t, row.Paused, "a repaired account must leave manual pause")
				require.Empty(t, row.PauseReason)
				require.True(t, row.AutoRetryPending)
				require.NotNil(t, row.NextRetryAt)
			}
			next := *s.Snapshot().Rows[0].Models[0].NextRetryAt
			require.NoError(t, s.SyncSelection(context.Background()))
			require.Equal(t, next, *s.Snapshot().Rows[0].Models[0].NextRetryAt, "polling must not restart recovery")
			s.scheduleDue(time.Now().Add(time.Second))
			require.Len(t, s.queue, 4, "recovery continues the interrupted collection without manual action")
			s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
				return openAIStateProbeResult{status: 200, result: "collected", value: "recovered-state", credentialStamp: stateKeeperCredentialStamp(a)}
			}
			for len(s.queue) > 0 {
				s.run(<-s.queue)
			}
			for _, row := range s.Snapshot().Rows[0].Models {
				require.True(t, row.StateFileSaved)
				require.Equal(t, "ready", row.Status)
				require.False(t, row.AutoRetryPending)
			}
		})
	}
}

func TestStateKeeperAccountRecoveryKeepsUnrepairedAccountsStopped(t *testing.T) {
	for _, change := range []string{"unchanged", "unrelated-edit", "empty-token", "still-disabled"} {
		t.Run(change, func(t *testing.T) {
			s, a := keeperAccountRecoveryFixture(t)
			switch change {
			case "unrelated-edit":
				a.Name, a.UpdatedAt = "renamed", time.Now().Add(time.Minute)
			case "empty-token":
				a.Credentials["access_token"] = ""
			case "still-disabled":
				a.Credentials["access_token"], a.Status = "new-token", StatusDisabled
			}
			require.NoError(t, s.SyncSelection(context.Background()))
			s.scheduleDue(time.Now().Add(time.Hour))
			require.Empty(t, s.queue)
			for _, row := range s.Snapshot().Rows[0].Models {
				require.True(t, row.AccountUnavailable)
				require.True(t, row.Paused)
			}
		})
	}
}

func TestStateKeeperAccountRecoveryPreservesLimitsAndDisabledTriggers(t *testing.T) {
	for _, mode := range []string{"429", "hourly", "disabled", "response-injection-off", "response-trigger-off"} {
		t.Run(mode, func(t *testing.T) {
			s, a := keeperAccountRecoveryFixture(t)
			q := s.Snapshot().Settings
			now := time.Now().UTC()
			expected := now.Add(time.Hour)
			switch mode {
			case "429":
				s.collectionLimitLocked(a.ID).CooldownUntil = expected
			case "hourly":
				limit := s.collectionLimitLocked(a.ID)
				limit.WindowStartedAt, limit.WindowRequests = now, q.AccountHourlyLimit
			case "disabled":
				q.Enabled = false
			case "response-injection-off", "response-trigger-off":
				a.Status = StatusError
				for _, e := range s.rows {
					e.row.RoundSource = "response"
				}
				q.InjectionEnabled = mode != "response-injection-off"
				q.ResponseRefreshEnabled = mode != "response-trigger-off"
			}
			require.NoError(t, s.Save(context.Background(), q))
			if strings.HasPrefix(mode, "response-") {
				a.Status = StatusActive
			} else {
				a.Credentials["access_token"] = "repaired-token"
			}
			require.NoError(t, s.SyncSelection(context.Background()))
			for _, row := range s.Snapshot().Rows[0].Models {
				require.False(t, row.Paused)
				require.False(t, row.AccountUnavailable)
				if mode == "429" || mode == "hourly" {
					require.Equal(t, expected, *row.NextRetryAt)
				}
				if strings.HasPrefix(mode, "response-") {
					require.False(t, row.AutoRetryPending)
				}
			}
			s.scheduleDue(now.Add(time.Second))
			require.Empty(t, s.queue, "recovery cannot bypass existing limits or disabled triggers")
		})
	}
}

func TestStateKeeperAccountRecoveryPersistsAndPreservesOtherManualPauses(t *testing.T) {
	s, a := keeperAccountRecoveryFixture(t)
	q := s.Snapshot().Settings
	other := s.entryLocked(a.ID, q.Models[1])
	other.row.PauseReason = "采集轮次文件无法读取，等待人工重试"
	s.blockAccountLocked(a.ID, stateKeeperCredentialStamp(a), s.entryLocked(a.ID, q.Models[0]).row.PauseReason)
	q.Concurrency++
	require.NoError(t, s.Save(context.Background(), q))
	for _, model := range q.Models {
		require.NoError(t, s.persistRuntime(a.ID, model))
	}
	restart := func() *OpenAIStateKeeperService {
		r := newOpenAIStateKeeper(s.settings, s.accounts, s.proxies, s.gateway)
		t.Cleanup(r.Stop)
		r.files = s.files
		r.reload(context.Background())
		require.NoError(t, r.SyncSelection(context.Background()))
		return r
	}
	restarted := restart()
	a.Credentials["access_token"] = "repaired-while-service-was-running"
	require.NoError(t, restarted.SyncSelection(context.Background()))
	for i, model := range q.Models {
		e := restarted.entryLocked(a.ID, model)
		require.False(t, e.row.AccountUnavailable)
		require.Equal(t, i == 1, e.row.Paused, "only credential-related pauses may be resumed")
		require.Equal(t, i != 1, e.row.AutoRetryPending)
		require.NoError(t, restarted.persistRuntime(a.ID, model))
	}
	again := restart()
	again.scheduleDue(time.Now().Add(time.Second))
	require.Len(t, again.queue, 3, "recovery must survive restart without resuming the file error")
}
