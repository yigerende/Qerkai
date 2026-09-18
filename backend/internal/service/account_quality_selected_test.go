//go:build unit

package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAccountQualitySelectedRejectsInvalidIDs(t *testing.T) {
	svc := &AccountQualityService{}
	for _, ids := range [][]int64{nil, {}, {0}, {-1}, {1, 1}, make([]int64, 101)} {
		_, err := svc.ScheduleSelected(context.Background(), ids, "")
		require.Error(t, err)
	}
}

func TestAccountQualitySelectedScopeBaselinesAndDisabledDetector(t *testing.T) {
	db := qualitySchedulingDB(t)
	u := &qualitySchedulingUpstream{calls: map[int64]int{}}
	svc, q := qualitySchedulingService(t, db, u)
	ctx := context.Background()
	q.ModelAuditEnabled = false
	q.DegradationConditions = []string{"question"}
	q, err := svc.SaveSettings(ctx, q)
	require.NoError(t, err)
	old := time.Now().Add(-time.Hour).UTC().Truncate(time.Microsecond)
	payload, err := json.Marshal(AccountQualityResult{Revision: q.Revision, Question: QualityQuestionResult{QualityVerdict: QualityVerdict{CheckedAt: &old}}})
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO account_quality_states(account_id,revision,version,payload,question_next_at,model_next_at) SELECT id,$1,'old',jsonb_set($2::jsonb,'{account_id}',to_jsonb(id)),NOW()+INTERVAL '1 day',NOW()+INTERVAL '2 days' FROM accounts WHERE id<>4`, q.Revision, string(payload))
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE accounts SET platform='claude' WHERE id=2; UPDATE accounts SET deleted_at=NOW() WHERE id=3`)
	require.NoError(t, err)
	var modelBefore, modelAfter time.Time
	require.NoError(t, db.QueryRow(`SELECT model_next_at FROM account_quality_states WHERE account_id=1`).Scan(&modelBefore))
	out, err := svc.ScheduleSelected(ctx, []int64{999, 4, 3, 2, 1}, "")
	require.NoError(t, err)
	require.Equal(t, q.Revision, out.Revision)
	require.True(t, out.QuestionEnabled)
	require.False(t, out.ModelEnabled)
	require.Len(t, out.Accounts, 5)
	require.True(t, old.Equal(*out.Accounts[0].QuestionChecked))
	require.Contains(t, out.Accounts[1].SkipReason, "OpenAI")
	require.Contains(t, out.Accounts[2].SkipReason, "删除")
	require.Nil(t, out.Accounts[3].QuestionChecked)
	require.Contains(t, out.Accounts[4].SkipReason, "不存在")
	require.NoError(t, db.QueryRow(`SELECT model_next_at FROM account_quality_states WHERE account_id=1`).Scan(&modelAfter))
	require.Equal(t, modelBefore, modelAfter)
	var due int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM account_quality_states WHERE question_next_at<=NOW()`).Scan(&due))
	require.Equal(t, 2, due)
	// The shared scheduler launches exactly the two requested accounts here.
	require.NoError(t, svc.runDue(ctx, 0))
	require.Equal(t, map[int64]int{1: 1, 4: 1}, u.calls)
	snapshot, err := svc.Results(ctx, []int64{1, 2, 3, 4, 999})
	require.NoError(t, err)
	byID := map[int64]AccountQualityResult{}
	for _, result := range snapshot.Accounts {
		byID[result.AccountID] = result
	}
	require.True(t, byID[1].Question.CheckedAt.After(old))
	require.NotNil(t, byID[4].Question.CheckedAt)
	for _, id := range []int64{2, 3, 999} {
		require.Equal(t, "unavailable", byID[id].QuestionExecution)
	}
	_, err = svc.ScheduleSelected(ctx, []int64{1}, "obsolete-revision")
	require.ErrorContains(t, err, "配置已变更")
	q.Enabled = false
	_, err = svc.SaveSettings(ctx, q)
	require.NoError(t, err)
	_, err = svc.ScheduleSelected(ctx, []int64{1}, "")
	require.ErrorContains(t, err, "启用")
}

func TestAccountQualitySelectedRunningProbeSatisfiesRequest(t *testing.T) {
	db := qualitySchedulingDB(t)
	u := &qualitySchedulingUpstream{gate: make(chan struct{}), started: make(chan int64, 128), calls: map[int64]int{}}
	svc, _ := qualitySchedulingService(t, db, u)
	ctx := context.Background()
	done := make(chan error, 1)
	go func() { done <- svc.runDue(ctx, 0) }()
	var id int64
	select {
	case id = <-u.started:
	case <-time.After(10 * time.Second):
		t.Fatal("probe did not start")
	}
	out, err := svc.ScheduleSelected(ctx, []int64{id}, "")
	require.NoError(t, err)
	require.Nil(t, out.Accounts[0].QuestionChecked)
	close(u.gate)
	require.NoError(t, <-done)
	snapshot, err := svc.Results(ctx, []int64{id})
	require.NoError(t, err)
	require.NotNil(t, snapshot.Accounts[0].Question.CheckedAt)
	require.Equal(t, "idle", snapshot.Accounts[0].QuestionExecution)
	require.NoError(t, svc.runDue(ctx, 0))
	require.Equal(t, 1, u.calls[id])
}
