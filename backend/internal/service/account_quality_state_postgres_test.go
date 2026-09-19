//go:build unit

package service

import (
	"context"
	"encoding/json"
	"net/http"
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
	require.EqualValues(t, 1, summary["model_state_pending"])
	require.EqualValues(t, 61, summary["no_samples"])
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
	require.Zero(t, summary["model_state_pending"])
	require.EqualValues(t, 61, summary["no_samples"])
	// A second successful collection also fences the previous State's workers.
	keeper.run(openAIStateKeeperJob{accountID: 1, revision: keeper.config.Load().Revision})
	require.NoError(t, svc.saveResult(ctx, q, verified, 1))
	require.Equal(t, "state_pending", stored().Model.Status)
	require.NotEqual(t, verified.Model.StateVersion, stored().Model.StateVersion)
}

func TestAccountQualityPostgresStateValidationRespectsIntervalsAndManualRun(t *testing.T) {
	defer setForceUpstreamWSForTest(false)()
	db := qualitySchedulingDB(t)
	_, keeper, q, original := qualityCollectedStateFixture(t)
	ctx := context.Background()
	_, err := db.Exec(`DELETE FROM accounts WHERE id<>1`)
	require.NoError(t, err)
	account := keeperTestAccount(1)
	account.GroupIDs = []int64{11}
	u := &queuedHTTPUpstream{responses: []*http.Response{
		newJSONResponse(200, "data: {\"type\":\"response.completed\",\"response\":{\"model\":\"gpt-6-astra\"}}\n\n"),
		newJSONResponse(200, "data: {\"type\":\"response.completed\",\"response\":{\"model\":\"gpt-6-astra\"}}\n\n"),
	}}
	repo := &qualityModelAuditStub{}
	s := &AccountQualityService{db: db, settings: &SettingService{settingRepo: &qualityPGSettings{db: db}},
		usage: &UsageService{usageRepo: repo}, tests: &AccountTestService{accountRepo: &qualityAccountRepo{account: account}, httpUpstream: u}}
	q.RecoveryLimit, q.ModelAuditIntervalSeconds = 3, 3600
	q, err = s.SaveSettings(ctx, q)
	require.NoError(t, err)
	original.Revision = q.Revision
	require.NoError(t, s.saveResult(ctx, q, original, 0))
	require.NoError(t, s.saveResult(ctx, q, original, 1))
	s.stateKeeper.Store(keeper)
	keeper.quality = s
	keeper.syncQuality(ctx)
	pending := readQualityRecovery(t, s)
	require.True(t, pending.Model.StateValidationPending)
	// The next background pass must notice the newly collected State immediately.
	var due bool
	require.NoError(t, db.QueryRow(`SELECT model_next_at<=NOW() FROM account_quality_states WHERE account_id=1`).Scan(&due))
	require.True(t, due)
	repo.logs = qualityModelTestLogs(1, q.ModelAuditModel, 1, time.Now().UTC())
	repo.logs[0].ID = original.Model.LatestID + 1
	repo.logs[0].ResponseModel = q.ModelAuditModel
	*repo.logs[0].Mismatch = false
	require.NoError(t, s.runDue(ctx, 1))
	v := readQualityRecovery(t, s)
	require.Equal(t, 1, v.Model.Successes)
	require.Empty(t, u.requests)
	keeper.syncQuality(ctx)
	require.NoError(t, s.runDue(ctx, 1))
	require.Empty(t, u.requests, "background polling must respect the configured model interval")
	// Simulate the next configured deadline without sleeping in the test.
	_, err = db.Exec(`UPDATE account_quality_states SET model_next_at=NOW() WHERE account_id=1`)
	require.NoError(t, err)
	require.NoError(t, s.runDue(ctx, 1))
	v = readQualityRecovery(t, s)
	require.Len(t, u.requests, 1)
	require.Equal(t, 2, v.Model.Successes)
	require.True(t, v.Model.StateValidationPending)
	require.NoError(t, s.runDue(ctx, 1))
	require.Len(t, u.requests, 1)
	require.NoError(t, s.Schedule(ctx, nil))
	require.NoError(t, s.runDue(ctx, 1))
	v = readQualityRecovery(t, s)
	require.Len(t, u.requests, 2, "manual detection also continues validation with consumed logs")
	require.Equal(t, 3, v.Model.Successes)
	require.False(t, v.Model.StateValidationPending)
	require.Equal(t, "normal", v.Model.Status)
}
