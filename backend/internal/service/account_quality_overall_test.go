package service

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAccountQualityOverallTruthTable(t *testing.T) {
	states := []string{"degraded", "normal", "pending"}
	for _, mode := range []string{"any", "all"} {
		for _, question := range states {
			for _, model := range states {
				t.Run(mode+"/"+question+"/"+model, func(t *testing.T) {
					q := AccountQualitySettings{Enabled: true, Revision: "current", QuestionEnabled: true, ModelAuditEnabled: true, DegradationMode: mode, DegradationConditions: []string{"question", "model"}}
					verdict := func(status string) QualityVerdict {
						now := time.Now()
						return QualityVerdict{CheckedAt: &now, Status: status, Degraded: status == "degraded"}
					}
					v := AccountQualityResult{Revision: q.Revision, Question: QualityQuestionResult{QualityVerdict: verdict(question)}, Model: QualityModelResult{QualityVerdict: verdict(model)}}
					want := "pending"
					if mode == "any" {
						if question == "degraded" || model == "degraded" {
							want = "degraded"
						} else if question == "normal" && model == "normal" {
							want = "normal"
						}
					} else {
						if question == "degraded" && model == "degraded" {
							want = "degraded"
						} else if question == "normal" || model == "normal" {
							want = "normal"
						}
					}
					require.Equal(t, want, evaluateQualityOverall(q, v).Status)
					v.Revision = "stale"
					require.Equal(t, "pending", evaluateQualityOverall(q, v).Status)
				})
			}
		}
	}
}

func TestAccountQualityOverallUnknownAndRecoveryThreshold(t *testing.T) {
	now := time.Now()
	for _, v := range []QualityVerdict{{Status: "normal"}, {Status: "error", Degraded: true, CheckedAt: &now}, {Status: "no_samples", CheckedAt: &now}, {Status: "normal", Error: "network error", CheckedAt: &now}, {Status: "suspect", CheckedAt: &now}} {
		require.Equal(t, "pending", qualityConditionStatus(v))
	}
	require.Equal(t, "degraded", qualityConditionStatus(QualityVerdict{Status: "normal", Degraded: true, CheckedAt: &now}), "one passing answer must not bypass the recovery streak")
	q := DefaultAccountQualitySettings()
	q.Enabled, q.QuestionEnabled, q.ModelAuditEnabled = true, true, true
	q.DegradationMode, q.DegradationConditions = "all", []string{"question", "model"}
	require.NoError(t, validateQualityOverallPolicy(q))
	q.DegradationConditions = nil
	require.NoError(t, validateQualityOverallPolicy(q), "legacy default selects the enabled detectors")
	q.DegradationConditions = []string{}
	require.Error(t, validateQualityOverallPolicy(q))
}
