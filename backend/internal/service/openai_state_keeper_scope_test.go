package service

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStateKeeperCollectionGroupsFollowMembershipWithoutPinningAccounts(t *testing.T) {
	s, gateway, a := keeperTestService(t)
	repo := s.accounts.(*keeperAccountsStub)
	for _, id := range []int64{2, 3, 4, 5, 6} {
		repo.accounts[id] = keeperTestAccount(id)
	}
	repo.accounts[2].GroupIDs = []int64{21, 22}
	repo.accounts[3].GroupIDs, repo.accounts[3].Status = []int64{21}, StatusError
	repo.accounts[4].GroupIDs, repo.accounts[4].Type = []int64{21}, AccountTypeAPIKey
	repo.accounts[5].GroupIDs, repo.accounts[5].Platform = []int64{21}, PlatformAnthropic
	q := s.config.Load().OpenAIStateKeeperSettings
	q.CollectionGroupIDs = []int64{21, 22}
	q.AutoRefresh, q.AutoCollectIntervalSeconds = true, 60
	require.NoError(t, s.Save(context.Background(), q))
	require.Equal(t, []int64{1, 2, 3}, s.collectionAccountIDs())
	require.True(t, s.entryLocked(3).row.AccountUnavailable)
	require.Equal(t, repo.accounts[2].Name, s.entryLocked(2).row.AccountName)
	require.NoError(t, s.Schedule(nil))
	require.Len(t, s.queue, 2, "unavailable members stay visible without being collected")
	for len(s.queue) > 0 {
		s.run(<-s.queue)
	}
	before, entry := s.config.Load(), s.entryLocked(1)
	ticket := gateway.prepareCollectedStateWS(keeperTestContext(11), a, q.Model, http.Header{})
	require.NotNil(t, ticket)
	current := ticket.poolCurrentCheck()
	unchangedCancelled, removedCancelled := false, false
	s.activeCancels[s.key(1)] = func() { unchangedCancelled = true }
	s.activeCancels[s.key(2)] = func() { removedCancelled = true }
	repo.accounts[6].GroupIDs = []int64{22}
	repo.accounts[2].GroupIDs = nil
	require.NoError(t, s.SyncSelection(context.Background()))
	require.Equal(t, []int64{1, 3, 6}, s.collectionAccountIDs())
	require.Same(t, before, s.config.Load(), "membership changes must not invalidate unrelated requests or rounds")
	require.Same(t, entry, s.entryLocked(1))
	require.True(t, current(), "existing WS State remains reusable")
	require.False(t, unchangedCancelled)
	require.True(t, removedCancelled)
	require.Nil(t, gateway.prepareCollectedStateWS(keeperTestContext(11), repo.accounts[2], q.Model, http.Header{}))
	require.NotNil(t, gateway.prepareCollectedStateWS(keeperTestContext(11), repo.accounts[6], q.Model, http.Header{}))
	require.Equal(t, []int64{1}, s.Snapshot().Settings.AccountIDs, "group membership must not become a manual selection")
	s.scheduleDue(time.Now())
	require.Len(t, s.queue, 1)
	job := <-s.queue
	require.Equal(t, int64(6), job.accountID)
	require.Equal(t, "timer", job.source)
	restarted := newOpenAIStateKeeper(s.settings, repo, s.proxies, gateway)
	t.Cleanup(restarted.Stop)
	restarted.reload(context.Background())
	require.Equal(t, []int64{1, 3, 6}, restarted.collectionAccountIDs())
	require.Equal(t, []int64{1}, restarted.Snapshot().Settings.AccountIDs)
}

func TestStateKeeperEmptyCollectionGroupStaysEnabledAndFindsLaterMembers(t *testing.T) {
	s, _, a := keeperTestService(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.AccountIDs, q.CollectionGroupIDs = nil, []int64{21}
	require.NoError(t, s.Save(context.Background(), q))
	require.Empty(t, s.Snapshot().Rows)
	require.True(t, s.Snapshot().Settings.Enabled)
	require.NoError(t, s.SyncSelection(context.Background()))
	require.True(t, s.Snapshot().Settings.Enabled)
	a.GroupIDs = []int64{21}
	require.NoError(t, s.Schedule(nil), "manual collection resolves members added after the previous sync")
	require.Equal(t, []int64{a.ID}, s.collectionAccountIDs())
	require.Len(t, s.queue, 1)
	a.GroupIDs = nil
	calls := 0
	s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
		calls++
		return openAIStateProbeResult{}
	}
	s.run(<-s.queue)
	require.Zero(t, calls, "a queued account leaving the group is skipped before its next sync")
	require.NoError(t, s.SyncSelection(context.Background()))
	require.Empty(t, s.Snapshot().Rows)
	require.True(t, s.Snapshot().Settings.Enabled)
}

func TestStateKeeperCollectionGroupRejoinRejectsJobsFromPreviousMembership(t *testing.T) {
	s, _, a := keeperTestService(t)
	a.GroupIDs = []int64{21}
	q := s.config.Load().OpenAIStateKeeperSettings
	q.AccountIDs, q.CollectionGroupIDs = nil, []int64{21}
	require.NoError(t, s.Save(context.Background(), q))
	require.NoError(t, s.Schedule(nil))
	stale := <-s.queue
	a.GroupIDs = nil
	require.NoError(t, s.SyncSelection(context.Background()))
	a.GroupIDs = []int64{21}
	require.NoError(t, s.SyncSelection(context.Background()))
	require.NoError(t, s.Schedule(nil))
	fresh := <-s.queue
	s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
		t.Fatal("stale job must not collect after rejoining")
		return openAIStateProbeResult{}
	}
	s.run(stale)
	require.True(t, s.entryLocked(a.ID).row.Queued, "stale job must not clear the newly queued job")
	require.NotEqual(t, stale.scopeID, fresh.scopeID)
}

type keeperGroupLookupFailure struct{ *keeperAccountsStub }

func (*keeperGroupLookupFailure) ListAllWithFilters(context.Context, string, string, string, string, int64, string) ([]Account, error) {
	return nil, context.DeadlineExceeded
}

func TestStateKeeperCollectionGroupLookupFailurePreservesScope(t *testing.T) {
	s, _, a := keeperTestService(t)
	a.GroupIDs = []int64{21}
	q := s.config.Load().OpenAIStateKeeperSettings
	q.AccountIDs, q.CollectionGroupIDs = nil, []int64{21}
	require.NoError(t, s.Save(context.Background(), q))
	before := s.config.Load()
	s.accounts = &keeperGroupLookupFailure{s.accounts.(*keeperAccountsStub)}
	require.ErrorIs(t, s.SyncSelection(context.Background()), context.DeadlineExceeded)
	require.Same(t, before, s.config.Load())
	require.Equal(t, []int64{a.ID}, s.collectionAccountIDs())
	s.reload(context.Background())
	require.Same(t, before, s.config.Load())
	require.Equal(t, []int64{a.ID}, s.collectionAccountIDs())
}

func TestStateKeeperCollectionGroupRejoinRestoresStateAndPausedBudget(t *testing.T) {
	s, _, a := keeperTestService(t)
	s.files = keeperTestFileStore(t)
	a.GroupIDs = []int64{21}
	q := s.config.Load().OpenAIStateKeeperSettings
	q.AccountIDs, q.CollectionGroupIDs = nil, []int64{21}
	require.NoError(t, s.Save(context.Background(), q))
	require.NoError(t, s.Schedule(nil))
	s.run(<-s.queue)
	version := s.entryLocked(a.ID).version
	s.entryLocked(a.ID).row.Paused = true
	s.entryLocked(a.ID).row.PauseReason = "budget exhausted"
	require.NoError(t, s.persistRuntime(a.ID))
	a.GroupIDs = nil
	require.NoError(t, s.SyncSelection(context.Background()))
	require.Empty(t, s.Snapshot().Rows)
	a.GroupIDs = []int64{21}
	require.NoError(t, s.SyncSelection(context.Background()))
	require.Equal(t, "collected-secret", s.entryLocked(a.ID).value)
	require.Equal(t, version, s.entryLocked(a.ID).version)
	require.True(t, s.entryLocked(a.ID).row.Paused)
	require.False(t, s.entryLocked(a.ID).scopeLoading)
	require.Equal(t, "collected-secret", s.valueFor(keeperTestContext(11), a, q.Model))
}

func TestStateKeeperCollectionGroupsAreNotLimitedToFirst500Members(t *testing.T) {
	s, _, _ := keeperTestService(t)
	repo := s.accounts.(*keeperAccountsStub)
	for id := int64(1); id <= 510; id++ {
		a := keeperTestAccount(id)
		a.GroupIDs = []int64{21}
		repo.accounts[id] = a
	}
	q := s.config.Load().OpenAIStateKeeperSettings
	q.AccountIDs, q.CollectionGroupIDs = nil, []int64{21}
	require.NoError(t, s.Save(context.Background(), q))
	require.Len(t, s.Snapshot().Rows, 510)
	require.NoError(t, s.Schedule(nil))
	require.Len(t, s.queue, 510)
	require.Empty(t, s.Snapshot().Settings.AccountIDs)
}
