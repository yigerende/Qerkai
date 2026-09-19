//go:build unit

package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAccountQualityPostgresStateRefreshPreservesQuestionAndRejectsStaleWorkers(t *testing.T) {
	db := qualitySchedulingDB(t)
	_, keeper, q, original := qualityCollectedStateFixture(t)
	svc := &AccountQualityService{db: db, settings: &SettingService{settingRepo: &qualityPGSettings{db: db}}}
	ctx := context.Background()
	var err error
	q, err = svc.SaveSettings(ctx, q)
	require.NoError(t, err)
	original.Revision = q.Revision
	require.NoError(t, svc.saveResult(ctx, q, original, 0))
	require.NoError(t, svc.saveResult(ctx, q, original, 1))
	before, err := svc.Summary(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, before["model_degraded"])
	keeper.quality = svc
	svc.stateKeeper.Store(keeper)
	preview, err := svc.Results(ctx, []int64{1})
	require.NoError(t, err)
	require.Equal(t, "state_pending", preview.Accounts[0].Model.Status)
	require.True(t, preview.Accounts[0].stateRefreshPending)
	keeper.syncQuality(ctx)
	stored := func() AccountQualityResult {
		t.Helper()
		var raw []byte
		require.NoError(t, db.QueryRow("SELECT payload FROM account_quality_states WHERE account_id=1").Scan(&raw))
		var v AccountQualityResult
		require.NoError(t, json.Unmarshal(raw, &v))
		return v
	}
	pending := stored()
	require.Equal(t, "state_pending", pending.Model.Status)
	require.Equal(t, original.Question.Answer, pending.Question.Answer)
	require.False(t, pending.Model.Degraded)
	require.Equal(t, "pending", pending.Overall.Status)
	summary, err := svc.Summary(ctx)
	require.NoError(t, err)
	require.Zero(t, summary["degraded"])
	require.Zero(t, summary["model_degraded"])
	history, err := svc.History(ctx, 1, 100)
	require.NoError(t, err)
	require.Equal(t, "state_refresh", history[0].DetectionKind)
	historyCount := len(history)
	keeper.syncQuality(ctx)
	history, err = svc.History(ctx, 1, 100)
	require.NoError(t, err)
	require.Len(t, history, historyCount, "polling cannot create repeated refresh events")
	// A model scan that started before collection must not reinstate its result.
	require.NoError(t, svc.saveResult(ctx, q, original, 1))
	require.Equal(t, pending.Model, stored().Model)
	// Another detector may finish concurrently and must preserve the reset.
	original.Question.Answer = "new-answer"
	require.NoError(t, svc.saveResult(ctx, q, original, 0))
	require.Equal(t, pending.Model, stored().Model)
	require.Equal(t, "new-answer", stored().Question.Answer)
	replica := &AccountQualityService{db: db, settings: svc.settings}
	snapshot, err := replica.Results(ctx, []int64{1})
	require.NoError(t, err)
	require.Equal(t, "state_pending", snapshot.Accounts[0].Model.Status, "pending survives without the original process memory")
	verified := stored()
	start := verified.Model.StateCollectedAt.Add(time.Second)
	no := false
	logs := []ModelAuditLog{
		{ID: 20, CreatedAt: start.Add(time.Second), RequestStartedAt: &start, SentModel: q.ModelAuditModel, ResponseModel: q.ModelAuditModel, Mismatch: &no},
		{ID: 21, CreatedAt: start.Add(2 * time.Second), RequestStartedAt: &start, SentModel: q.ModelAuditModel, ResponseModel: q.ModelAuditModel, Mismatch: &no},
	}
	applyQualityModelLogs(&verified.Model, q, logs, start.Add(3*time.Second))
	require.NoError(t, svc.saveResult(ctx, q, verified, 1))
	require.Equal(t, "normal", stored().Overall.Status)
	summary, err = svc.Summary(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, summary["model_normal"])
	// A second successful collection also fences the previous State's workers.
	keeper.run(openAIStateKeeperJob{accountID: 1, revision: keeper.config.Load().Revision})
	require.NoError(t, svc.saveResult(ctx, q, verified, 1))
	require.Equal(t, "state_pending", stored().Model.Status)
	require.NotEqual(t, verified.Model.StateVersion, stored().Model.StateVersion)
}
