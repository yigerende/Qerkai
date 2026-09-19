package repository

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/migrations"
	"github.com/stretchr/testify/require"
)

func pauseAt(t *testing.T, db *sql.DB, id int64) *time.Time {
	t.Helper()
	var at *time.Time
	require.NoError(t, db.QueryRow(`SELECT scheduling_paused_at FROM accounts WHERE id=$1`, id).Scan(&at))
	return at
}

func TestAccountSchedulingPauseTracksContinuousSwitchClosure(t *testing.T) {
	db, repo := stateSchedulingRepository(t)
	ctx := context.Background()
	require.Nil(t, pauseAt(t, db, 1))
	require.NotNil(t, pauseAt(t, db, 2), "existing pauses start at migration time")
	require.NoError(t, repo.SetSchedulable(ctx, 1, false))
	first := pauseAt(t, db, 1)
	require.NotNil(t, first)
	require.NoError(t, repo.SetSchedulable(ctx, 1, false))
	require.Equal(t, first, pauseAt(t, db, 1))
	_, err := db.Exec(`UPDATE accounts SET scheduling_paused_at=NOW()-INTERVAL '1 day',name='changed' WHERE id=1`)
	require.NoError(t, err)
	require.Equal(t, first, pauseAt(t, db, 1), "other writes cannot reset the continuous pause")
	require.NoError(t, repo.SetSchedulable(ctx, 1, true))
	require.Nil(t, pauseAt(t, db, 1))
	require.NoError(t, repo.SetSchedulable(ctx, 1, false))
	require.True(t, pauseAt(t, db, 1).After(*first))
	second := pauseAt(t, db, 1)
	migration, err := migrations.FS.ReadFile("240_account_scheduling_paused_at.sql")
	require.NoError(t, err)
	_, err = db.Exec(string(migration))
	require.NoError(t, err)
	require.Equal(t, second, pauseAt(t, db, 1), "migration replay must preserve the timer")
}

func TestAccountSchedulingPauseCoversCreateQualityReauthBulk(t *testing.T) {
	db, repo := stateSchedulingRepository(t)
	ctx := context.Background()
	created := &service.Account{Name: "new", Platform: "openai", Type: "oauth", Status: "active", Schedulable: true, Credentials: map[string]any{"access_token": "new"}}
	require.NoError(t, repo.Create(ctx, created))
	require.NotNil(t, pauseAt(t, db, created.ID), "State gate pauses on insert")
	_, err := db.Exec(`UPDATE account_quality_states SET payload='{"scheduling":{"paused":true,"quality_paused":true}}' WHERE account_id=1`)
	require.NoError(t, err)
	require.NoError(t, repo.SyncQualityScheduling(ctx))
	require.NotNil(t, pauseAt(t, db, 1))
	_, err = db.Exec(`UPDATE account_quality_states SET payload='{"scheduling":{"paused":false}}' WHERE account_id=1`)
	require.NoError(t, err)
	require.NoError(t, repo.SyncQualityScheduling(ctx))
	require.Nil(t, pauseAt(t, db, 1), "quality recovery clears the pause")
	require.NoError(t, repo.UpdateCredentials(ctx, 1, map[string]any{"access_token": "new"}))
	require.NotNil(t, pauseAt(t, db, 1), "reauthorization closes the same switch")
	no := false
	_, err = repo.BulkUpdate(ctx, []int64{3}, service.AccountBulkUpdate{Schedulable: &no})
	require.NoError(t, err)
	require.NotNil(t, pauseAt(t, db, 3))
}

func TestAccountSchedulingPausesPaginationAndFiltering(t *testing.T) {
	db, repo := stateSchedulingRepository(t)
	_, err := db.Exec(`UPDATE accounts SET schedulable=false,credentials='{"email":"owner@example.com","access_token":"secret"}';
 ALTER TABLE accounts DISABLE TRIGGER accounts_scheduling_pause;
 UPDATE accounts SET scheduling_paused_at=clock_timestamp()-INTERVAL '61 seconds';
 ALTER TABLE accounts ENABLE TRIGGER accounts_scheduling_pause;
 UPDATE accounts SET deleted_at=NOW() WHERE id=3;`)
	require.NoError(t, err)
	page, err := repo.ListAccountSchedulingPauses(context.Background(), service.AccountSchedulingPausesInput{Limit: 1})
	require.NoError(t, err)
	require.Equal(t, 1, page.Version)
	require.True(t, page.HasMore)
	require.Equal(t, int64(1), page.NextAfterID)
	require.Len(t, page.Accounts, 1)
	require.Equal(t, "owner@example.com", page.Accounts[0].Email)
	require.GreaterOrEqual(t, page.Accounts[0].PausedSeconds, int64(61))
	next, err := repo.ListAccountSchedulingPauses(context.Background(), service.AccountSchedulingPausesInput{AfterID: page.NextAfterID, Limit: 1})
	require.NoError(t, err)
	require.False(t, next.HasMore)
	require.Equal(t, int64(2), next.Accounts[0].AccountID)
	filtered, err := repo.ListAccountSchedulingPauses(context.Background(), service.AccountSchedulingPausesInput{AccountIDs: []int64{1, 3}, Limit: 100})
	require.NoError(t, err)
	require.Len(t, filtered.Accounts, 1)
	_, err = db.Exec(`UPDATE accounts SET schedulable=true WHERE id=1`)
	require.NoError(t, err)
	filtered, err = repo.ListAccountSchedulingPauses(context.Background(), service.AccountSchedulingPausesInput{AccountIDs: []int64{1, 3}, Limit: 100})
	require.NoError(t, err)
	require.Empty(t, filtered.Accounts)
	require.NotNil(t, filtered.Accounts)
}
