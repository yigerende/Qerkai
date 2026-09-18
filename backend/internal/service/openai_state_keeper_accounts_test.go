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
