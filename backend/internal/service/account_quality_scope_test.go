//go:build unit

package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAccountQualityGroupScopeDefaultsAndValidation(t *testing.T) {
	q := DefaultAccountQualitySettings()
	require.NoError(t, json.Unmarshal([]byte(`{"enabled":true}`), &q))
	require.True(t, q.AllGroups, "old settings keep the original all-account scope")
	require.NoError(t, q.Validate())
	q.AllGroups = false
	require.ErrorContains(t, q.Validate(), "至少一个")
	for _, ids := range [][]int64{{0}, {-1}, {1, 1}, make([]int64, 1001)} {
		q.GroupIDs = ids
		require.Error(t, q.Validate())
	}
	q.GroupIDs = []int64{1, 2}
	require.NoError(t, q.Validate())
}

func TestAccountQualityGroupScopeScheduledManualAndMembership(t *testing.T) {
	db := qualitySchedulingDB(t)
	u := &qualitySchedulingUpstream{calls: map[int64]int{}}
	svc, q := qualitySchedulingService(t, db, u)
	ctx := context.Background()
	_, err := db.Exec(`INSERT INTO groups(id) VALUES(10),(20),(30),(40);
 UPDATE groups SET deleted_at=NOW() WHERE id=40;
 INSERT INTO account_groups(account_id,group_id) VALUES(1,10),(1,20),(2,20),(3,30),(4,40),(5,10),(6,10);
 UPDATE accounts SET platform='claude' WHERE id=5;
 UPDATE accounts SET deleted_at=NOW() WHERE id=6`)
	require.NoError(t, err)
	q.AllGroups, q.GroupIDs = false, []int64{10, 20, 40}
	q, err = svc.SaveSettings(ctx, q)
	require.NoError(t, err)
	summary, err := svc.Summary(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 2, summary["total"])
	p, err := svc.Progress(ctx, q)
	require.NoError(t, err)
	require.EqualValues(t, 2, p.Question.Total)
	require.EqualValues(t, 2, p.Model.Pending)
	require.NoError(t, svc.runDue(ctx, 0))
	require.NoError(t, svc.runDue(ctx, 1))
	require.Equal(t, map[int64]int{1: 1, 2: 1}, u.calls, "multi-group accounts run only once")
	var stateCount int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM account_quality_states`).Scan(&stateCount))
	require.Equal(t, 2, stateCount)
	snapshot, err := svc.Results(ctx, []int64{1, 3, 4, 7})
	require.NoError(t, err)
	for _, result := range snapshot.Accounts {
		if result.AccountID != 1 {
			require.Equal(t, "excluded", result.QuestionExecution)
			require.Equal(t, "excluded", result.ModelExecution)
		}
	}
	// Explicit selection outside the scheduled groups is a one-shot request.
	_, err = svc.ScheduleSelected(ctx, []int64{3}, "")
	require.NoError(t, err)
	snapshot, err = svc.Results(ctx, []int64{3})
	require.NoError(t, err)
	require.Equal(t, "queued", snapshot.Accounts[0].QuestionExecution)
	require.NoError(t, svc.runDue(ctx, 0))
	snapshot, err = svc.Results(ctx, []int64{3})
	require.NoError(t, err)
	require.Equal(t, "excluded", snapshot.Accounts[0].QuestionExecution)
	require.Equal(t, "queued", snapshot.Accounts[0].ModelExecution, "question completion cannot consume the model request")
	require.NoError(t, svc.runDue(ctx, 1))
	require.Equal(t, 1, u.calls[3])
	var requested bool
	require.NoError(t, db.QueryRow(`SELECT question_requested_at IS NOT NULL OR model_requested_at IS NOT NULL FROM account_quality_states WHERE account_id=3`).Scan(&requested))
	require.False(t, requested)
	// Force every existing row due: excluded accounts must not get periodic repeats.
	_, err = db.Exec(`UPDATE account_quality_states SET question_next_at='epoch',model_next_at='epoch'`)
	require.NoError(t, err)
	require.NoError(t, svc.runDue(ctx, 0))
	require.NoError(t, svc.runDue(ctx, 1))
	require.Equal(t, 1, u.calls[3])
	// Settings-page "run now" only advances deadlines inside the saved group scope.
	_, err = db.Exec(`UPDATE account_quality_states SET question_next_at=NOW()+INTERVAL '1 day',model_next_at=NOW()+INTERVAL '1 day'`)
	require.NoError(t, err)
	require.NoError(t, svc.Schedule(ctx, nil))
	var due int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM account_quality_states WHERE question_next_at<=NOW()`).Scan(&due))
	require.Equal(t, 2, due)
	// A removed membership is checked again immediately before probing.
	_, err = db.Exec(`DELETE FROM account_groups WHERE account_id=2`)
	require.NoError(t, err)
	require.NoError(t, svc.probe(ctx, q, AccountQualityResult{AccountID: 2, Revision: q.Revision}, 0))
	require.Equal(t, 2, u.calls[2])
	_, err = db.Exec(`INSERT INTO account_groups(account_id,group_id) VALUES(7,10)`)
	require.NoError(t, err)
	require.NoError(t, svc.runDue(ctx, 0))
	require.Equal(t, 1, u.calls[7])
	summary, err = svc.Summary(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 2, summary["total"])
	// Changing saved scope invalidates old manual exceptions as well as old results.
	_, err = svc.ScheduleSelected(ctx, []int64{3}, "")
	require.NoError(t, err)
	q.GroupIDs = []int64{20}
	q, err = svc.SaveSettings(ctx, q)
	require.NoError(t, err)
	require.NoError(t, svc.runDue(ctx, 0))
	require.Equal(t, 1, u.calls[3])
	snapshot, err = svc.Results(ctx, []int64{3})
	require.NoError(t, err)
	require.Equal(t, "excluded", snapshot.Accounts[0].QuestionExecution)
	// Default all-groups mode includes ungrouped OpenAI accounts, preserving old behavior.
	q.AllGroups = true
	q, err = svc.SaveSettings(ctx, q)
	require.NoError(t, err)
	summary, err = svc.Summary(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 60, summary["total"])
	p, err = svc.Progress(ctx, q)
	require.NoError(t, err)
	require.EqualValues(t, 60, p.Question.Total)
}

func TestAccountQualityGroupScopeRunningManualDoesNotReduceScheduledPending(t *testing.T) {
	db := qualitySchedulingDB(t)
	u := &qualitySchedulingUpstream{gate: make(chan struct{}), started: make(chan int64, 128), calls: map[int64]int{}}
	svc, q := qualitySchedulingService(t, db, u)
	ctx := context.Background()
	_, err := db.Exec(`INSERT INTO groups(id) VALUES(10); INSERT INTO account_groups(account_id,group_id) VALUES(1,10)`)
	require.NoError(t, err)
	q.AllGroups, q.GroupIDs, q.Concurrency = false, []int64{10}, 1
	q, err = svc.SaveSettings(ctx, q)
	require.NoError(t, err)
	_, err = svc.ScheduleSelected(ctx, []int64{3}, "")
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- svc.runDue(ctx, 0) }()
	select {
	case id := <-u.started:
		require.EqualValues(t, 3, id)
	case <-time.After(10 * time.Second):
		t.Fatal("manual probe not started")
	}
	p, err := svc.Progress(ctx, q)
	require.NoError(t, err)
	require.EqualValues(t, 1, p.Question.Total)
	require.EqualValues(t, 1, p.Question.Pending, "outside-scope running requests do not subtract scoped pending accounts")
	close(u.gate)
	require.NoError(t, <-done)
}
