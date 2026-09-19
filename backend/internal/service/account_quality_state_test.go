package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func qualityCollectedStateFixture(t *testing.T) (*AccountQualityService, *OpenAIStateKeeperService, AccountQualitySettings, AccountQualityResult) {
	t.Helper()
	keeper, _, account := keeperTestService(t)
	account.GroupIDs = []int64{11}
	keeper.files = keeperTestFileStore(t)
	keeper.run(openAIStateKeeperJob{accountID: 1, revision: keeper.config.Load().Revision})
	svc := &AccountQualityService{}
	svc.stateKeeper.Store(keeper)
	q := DefaultAccountQualitySettings()
	q.Enabled, q.ModelAuditEnabled = true, true
	q.Revision, q.DegradationConditions = "quality-state", []string{"question", "model"}
	q.UpdatedAt = time.Now().Add(-time.Hour)
	before := keeper.entryLocked(1).row.CollectedAt.Add(-time.Minute)
	v := AccountQualityResult{AccountID: 1, Revision: q.Revision,
		Question: QualityQuestionResult{Answer: "21", QualityVerdict: QualityVerdict{Status: "normal", CheckedAt: &before}},
		Model:    QualityModelResult{LatestID: 10, SentModel: q.ModelAuditModel, ResponseModel: "wrong-model", QualityVerdict: QualityVerdict{Status: "degraded", Degraded: true, Failures: 2, CheckedAt: &before, EvidenceAt: &before}},
	}
	return svc, keeper, q, v
}

func TestAccountQualityNewStateInvalidatesOnlyMatchingEnabledSavedState(t *testing.T) {
	for _, mode := range []string{"matching", "other-account", "other-model", "injection-off", "collection-off", "not-saved", "unavailable", "other-group", "audit-off"} {
		t.Run(mode, func(t *testing.T) {
			svc, keeper, q, v := qualityCollectedStateFixture(t)
			cfg := keeper.config.Load().OpenAIStateKeeperSettings
			cfg.Revision = mode
			switch mode {
			case "other-account":
				v.AccountID = 2
			case "other-model":
				q.ModelAuditModel = "other-model"
			case "injection-off":
				cfg.InjectionEnabled = false
				keeper.install(cfg)
			case "collection-off":
				cfg.Enabled = false
				keeper.install(cfg)
			case "not-saved":
				keeper.entryLocked(1).row.StateFileSaved = false
			case "unavailable":
				keeper.entryLocked(1).row.AccountUnavailable = true
			case "other-group":
				cfg.AllGroups, cfg.GroupIDs = false, []int64{999}
				keeper.install(cfg)
			case "audit-off":
				q.ModelAuditEnabled = false
			}
			old := v
			changed := svc.reconcileCollectedModelState(q, &v)
			require.Equal(t, mode == "matching", changed)
			if !changed {
				require.Equal(t, old, v)
				return
			}
			require.Equal(t, old.Question, v.Question)
			require.Equal(t, "state_pending", v.Model.Status)
			require.False(t, v.Model.Degraded)
			require.Zero(t, v.Model.Failures)
			require.Zero(t, v.Model.Successes)
			require.Empty(t, v.Model.ResponseModel)
			require.Equal(t, "pending", evaluateQualityOverall(q, v).Status)
			require.False(t, svc.reconcileCollectedModelState(q, &v), "same State cannot reset evidence again")
			raw, err := json.Marshal(v)
			require.NoError(t, err)
			var restored AccountQualityResult
			require.NoError(t, json.Unmarshal(raw, &restored))
			require.False(t, svc.reconcileCollectedModelState(q, &restored), "persisted State marker survives restart")
		})
	}
}

func TestAccountQualityNewStateIgnoresOldSamplesAndRequiresFreshStreak(t *testing.T) {
	for _, matches := range []bool{false, true} {
		t.Run(map[bool]string{true: "recovery", false: "still-degraded"}[matches], func(t *testing.T) {
			svc, _, q, v := qualityCollectedStateFixture(t)
			require.True(t, svc.reconcileCollectedModelState(q, &v))
			collected := *v.Model.StateCollectedAt
			require.Equal(t, collected, qualityModelSampleSince(q, v.Model))
			before, after := collected.Add(-time.Minute), collected.Add(time.Second)
			yes := true
			applyQualityModelLogs(&v.Model, q, []ModelAuditLog{
				{ID: 11, CreatedAt: after, RequestStartedAt: &before, SentModel: q.ModelAuditModel, ResponseModel: "old-model", Mismatch: &yes},
				{ID: 12, CreatedAt: after, SentModel: q.ModelAuditModel, ResponseModel: "unknown-start", Mismatch: &yes},
			}, after)
			require.Equal(t, "state_pending", v.Model.Status)
			require.Zero(t, v.Model.Failures)
			require.True(t, v.Model.NoNewSamples)
			v.Model.Status, v.Model.Error = "error", "temporary log query failure"
			applyQualityModelLogs(&v.Model, q, nil, after)
			require.Equal(t, "state_pending", v.Model.Status)
			require.Empty(t, v.Model.Error)
			mismatch, response := !matches, "wrong-model"
			if matches {
				response = q.ModelAuditModel
			}
			first := ModelAuditLog{ID: 13, CreatedAt: after.Add(time.Second), RequestStartedAt: &after, SentModel: q.ModelAuditModel, ResponseModel: response, Mismatch: &mismatch}
			applyQualityModelLogs(&v.Model, q, []ModelAuditLog{first}, after.Add(2*time.Second))
			require.True(t, v.Model.StateValidationPending)
			require.Equal(t, "pending", evaluateQualityOverall(q, v).Status)
			applyQualityModelLogs(&v.Model, q, []ModelAuditLog{first}, after.Add(3*time.Second))
			require.Equal(t, 1, v.Model.Successes+v.Model.Failures, "a sample cannot count twice")
			second := first
			second.ID, second.CreatedAt = 14, after.Add(4*time.Second)
			applyQualityModelLogs(&v.Model, q, []ModelAuditLog{second}, after.Add(5*time.Second))
			require.False(t, v.Model.StateValidationPending)
			require.Equal(t, !matches, v.Model.Degraded)
			require.Equal(t, map[bool]string{true: "normal", false: "degraded"}[matches], evaluateQualityOverall(q, v).Status)
		})
	}
}

func TestAccountQualityNewerStateRejectsStaleDetectionCompletion(t *testing.T) {
	svc, keeper, q, v := qualityCollectedStateFixture(t)
	require.True(t, svc.reconcileCollectedModelState(q, &v))
	old := v.Model
	keeper.run(openAIStateKeeperJob{accountID: 1, revision: keeper.config.Load().Revision})
	require.True(t, svc.reconcileCollectedModelState(q, &v))
	require.True(t, qualityModelResultPredatesState(old, v.Model))
	require.True(t, qualityModelResultPredatesState(QualityModelResult{}, v.Model))
	require.False(t, qualityModelResultPredatesState(v.Model, v.Model))
	keeper.Stop()
	restarted := newOpenAIStateKeeper(keeper.settings, keeper.accounts, keeper.proxies, keeper.gateway)
	t.Cleanup(restarted.Stop)
	restarted.files = keeper.files
	restarted.install(keeper.config.Load().OpenAIStateKeeperSettings)
	restarted.restoreStateFiles(context.Background())
	svc.stateKeeper.Store(restarted)
	require.False(t, svc.reconcileCollectedModelState(q, &v))
}
