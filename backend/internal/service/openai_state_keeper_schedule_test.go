package service

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStateKeeperFixedIntervalControlsSuccessAndFailure(t *testing.T) {
	for _, result := range []openAIStateProbeResult{
		{status: 292, result: "collected", value: "new-state"},
		{status: 200, result: "not_observed"},
		{status: 503, result: "upstream_error"},
	} {
		t.Run(result.result, func(t *testing.T) {
			s, _, _ := keeperTestService(t)
			q := s.config.Load().OpenAIStateKeeperSettings
			q.AutoRefresh = true
			q.AutoCollectIntervalSeconds = 90
			require.NoError(t, s.Save(context.Background(), q))
			s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult { return result }
			s.run(openAIStateKeeperJob{1, s.config.Load().Revision})
			row := s.Snapshot().Rows[0]
			require.Equal(t, s.rows[1].lastFinishedAt.Add(90*time.Second), *row.NextAttemptAt)
			s.scheduleDue(row.NextAttemptAt.Add(-time.Nanosecond))
			require.Empty(t, s.queue)
			s.scheduleDue(*row.NextAttemptAt)
			s.scheduleDue(row.NextAttemptAt.Add(time.Second))
			require.Len(t, s.queue, 1, "an already queued account must not be scheduled twice")

			q.AutoRefresh = false
			require.NoError(t, s.Save(context.Background(), q))
			s.scheduleDue(row.NextAttemptAt.Add(time.Hour))
			require.Empty(t, s.queue, "turning off automatic collection discards queued work")
			require.NoError(t, s.Schedule([]int64{1}))
			require.Len(t, s.queue, 1, "manual collection remains available")
		})
	}
}

func TestStateKeeperIntervalSaveReloadAndRestore(t *testing.T) {
	s, _, _ := keeperTestService(t)
	s.files = keeperTestFileStore(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.AutoCollectIntervalSeconds = 120
	require.NoError(t, s.Save(context.Background(), q))
	previous := s.Snapshot().Rows[0]
	require.Equal(t, s.rows[1].lastFinishedAt.Add(120*time.Second), *previous.NextAttemptAt)
	s.run(openAIStateKeeperJob{1, s.config.Load().Revision})

	restarted := newOpenAIStateKeeper(s.settings, s.accounts, s.proxies, s.gateway)
	t.Cleanup(restarted.Stop)
	restarted.files = s.files
	restarted.reload(context.Background())
	restarted.restoreStateFiles(context.Background())
	row := restarted.Snapshot().Rows[0]
	require.Equal(t, 120, restarted.Snapshot().Settings.AutoCollectIntervalSeconds)
	require.Equal(t, "ready", row.Status)
	require.Equal(t, row.CollectedAt.Add(120*time.Second), *row.NextAttemptAt)

	for _, invalid := range []int{-1, 86401} {
		q.AutoCollectIntervalSeconds = invalid
		require.Error(t, s.Save(context.Background(), q))
	}
	require.Equal(t, 120, s.Snapshot().Settings.AutoCollectIntervalSeconds)
}

func TestStateKeeperParallelCollectionBoundedAndAccountIsolated(t *testing.T) {
	s, _, _ := keeperTestService(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.AccountIDs = nil
	for id := int64(1); id <= 60; id++ {
		q.AccountIDs = append(q.AccountIDs, id)
	}
	q.Revision = "parallel"
	s.install(q)
	started := make(chan int64, 60)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	s.probe = func(ctx context.Context, _ OpenAIStateKeeperSettings, id int64) openAIStateProbeResult {
		started <- id
		select {
		case <-release:
		case <-ctx.Done():
		}
		return openAIStateProbeResult{status: 292, result: "collected", value: fmt.Sprintf("state-%d", id), credentialStamp: stateKeeperCredentialStamp(keeperTestAccount(id))}
	}
	s.startWorkers()
	require.NoError(t, s.Schedule(nil))
	seen := make(map[int64]bool)
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for range openAIStateKeeperWorkers {
		select {
		case id := <-started:
			require.False(t, seen[id], "each active probe must use a different account")
			seen[id] = true
		case <-deadline.C:
			t.Fatal("collectors did not start concurrently")
		}
	}
	collecting, queued := 0, 0
	for _, row := range s.Snapshot().Rows {
		if row.Collecting {
			collecting++
		}
		if row.Queued {
			queued++
		}
	}
	require.Equal(t, 50, collecting)
	require.Equal(t, 10, queued)
	require.NoError(t, s.Schedule(nil))
	releaseOnce.Do(func() { close(release) })
	require.Eventually(t, func() bool {
		for _, row := range s.Snapshot().Rows {
			if row.Queued || row.Collecting || row.Status != "ready" {
				return false
			}
		}
		return true
	}, 5*time.Second, 10*time.Millisecond)
	s.Stop()
	for id, entry := range s.rows {
		require.Equal(t, fmt.Sprintf("state-%d", id), entry.value)
		wantAttempts := int64(1)
		if id == 1 {
			wantAttempts++ // The shared fixture already collected account 1 once.
		}
		require.Equal(t, wantAttempts, entry.row.Attempts)
	}
}

func TestStateKeeperConfigChangeCancelsAllAndPreventsOverlappingAccount(t *testing.T) {
	s, _, _ := keeperTestService(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.AccountIDs = []int64{1, 2, 3}
	q.Revision = "cancellable"
	s.install(q)
	started := make(chan int64, 3)
	cancelled := make(chan int64, 3)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	s.probe = func(ctx context.Context, _ OpenAIStateKeeperSettings, id int64) openAIStateProbeResult {
		started <- id
		<-ctx.Done()
		cancelled <- id
		<-release
		return openAIStateProbeResult{status: 292, result: "collected", value: "stale-state"}
	}
	s.startWorkers()
	require.NoError(t, s.Schedule(nil))
	for range 3 {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("probe did not start")
		}
	}
	q.Enabled = false
	q.Revision = "disabled"
	s.install(q)
	for range 3 {
		select {
		case <-cancelled:
		case <-time.After(5 * time.Second):
			t.Fatal("configuration change failed to cancel every account")
		}
	}
	q.Enabled = true
	q.Revision = "reenabled"
	s.install(q)
	require.NoError(t, s.Schedule(nil))
	require.Empty(t, s.queue, "a cancelling account is still reserved until its old request exits")
	releaseOnce.Do(func() { close(release) })
	require.Eventually(t, func() bool {
		for _, row := range s.Snapshot().Rows {
			if row.Collecting || row.Queued {
				return false
			}
		}
		return true
	}, 5*time.Second, 10*time.Millisecond)
	s.Stop()
	for _, entry := range s.rows {
		require.Empty(t, entry.value, "cancelled results must not enter the new configuration")
	}
}
