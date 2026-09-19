package service

import "time"

const accountQualityStateRefresh = 2

// Collection metadata is read from memory; business forwarding never waits for
// quality persistence, and the keeper's existing background sync saves changes.
func (s *AccountQualityService) reconcileCollectedModelState(q AccountQualitySettings, v *AccountQualityResult) bool {
	keeper := s.stateKeeper.Load()
	if keeper == nil || !q.Enabled || !q.ModelAuditEnabled {
		return false
	}
	keeper.mu.RLock()
	defer keeper.mu.RUnlock()
	cfg := keeper.config.Load()
	if cfg == nil || !cfg.Enabled || !cfg.InjectionEnabled {
		return false
	}
	e := keeper.entryLocked(v.AccountID, q.ModelAuditModel)
	if e == nil || e.scopeLoading || e.row.AccountUnavailable || !e.row.StateFileSaved || e.value == "" || e.version == "" || e.row.CollectedAt == nil {
		return false
	}
	allowed := cfg.AllGroups
	for _, id := range e.row.AccountGroupIDs {
		allowed = allowed || cfg.groups[id]
	}
	if !allowed || v.Model.StateVersion == e.version || (v.Model.StateCollectedAt != nil && !e.row.CollectedAt.After(*v.Model.StateCollectedAt)) {
		return false
	}
	now, collected := time.Now().UTC(), *e.row.CollectedAt
	v.Model = QualityModelResult{
		QualityVerdict: QualityVerdict{Status: "state_pending", CheckedAt: &now, NextAt: &now},
		LatestID:       v.Model.LatestID, NoNewSamples: true,
		StateVersion: e.version, StateCollectedAt: &collected, StateValidationPending: true,
	}
	return true
}

func qualityModelSampleSince(q AccountQualitySettings, v QualityModelResult) time.Time {
	since := q.UpdatedAt
	if v.StateCollectedAt != nil && v.StateCollectedAt.After(since) {
		since = *v.StateCollectedAt
	}
	return since
}

func qualityModelResultPredatesState(candidate, current QualityModelResult) bool {
	return current.StateCollectedAt != nil && (candidate.StateCollectedAt == nil || candidate.StateCollectedAt.Before(*current.StateCollectedAt) || (candidate.StateCollectedAt.Equal(*current.StateCollectedAt) && candidate.StateVersion != current.StateVersion))
}
