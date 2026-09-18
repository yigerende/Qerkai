package service

import (
	"context"
	"time"
)

func (s *OpenAIStateKeeperService) qualityEligibleLocked(e *openAIKeptState) bool {
	return e != nil && s.qualityPolicy.Enabled && e.qualityRevision == s.qualityPolicy.Revision && e.row.QualityStatus == "degraded"
}

func (s *OpenAIStateKeeperService) qualityPolicyChanged(q AccountQualitySettings) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.installQualityPolicyLocked(q)
}

func (s *OpenAIStateKeeperService) installQualityPolicyLocked(q AccountQualitySettings) bool {
	if s.qualityPolicy.UpdatedAt.After(q.UpdatedAt) {
		return false
	}
	changed := s.qualityPolicy.Revision != q.Revision || s.qualityPolicy.Enabled != q.Enabled
	s.qualityPolicy = q
	if changed {
		for id, e := range s.rows {
			e.row.QualityStatus, e.row.QualityReason = "pending", "等待当前配置的检测结论"
			e.qualityRevision, e.qualityAt = q.Revision, time.Time{}
			if cancel := s.activeCancels[id]; cancel != nil && e.row.RoundSource == "degradation_scan" {
				cancel()
			}
		}
	}
	return true
}

func (s *OpenAIStateKeeperService) observeQualityResult(q AccountQualitySettings, v AccountQualityResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.installQualityPolicyLocked(q) {
		return
	}
	for _, model := range s.config.Load().modelNames() {
		e := s.entryLocked(v.AccountID, model)
		if e == nil {
			continue
		}
		at := time.Time{}
		for _, checked := range []*time.Time{v.Question.CheckedAt, v.Model.CheckedAt} {
			if checked != nil && checked.After(at) {
				at = *checked
			}
		}
		if e.qualityAt.After(at) {
			continue
		}
		overall := evaluateQualityOverall(q, v)
		e.qualityRevision, e.qualityAt = q.Revision, at
		e.row.QualityStatus, e.row.QualityReason = overall.Status, overall.Reason
		e.row.NextAttemptAt = stateKeeperNextAttempt(s.config.Load().OpenAIStateKeeperSettings, e)
		if overall.Status != "degraded" {
			if cancel := s.activeCancels[s.key(v.AccountID, model)]; cancel != nil && e.row.RoundSource == "degradation_scan" {
				cancel()
			}
		}
	}
}

func (s *OpenAIStateKeeperService) scheduleDegradationScan(now time.Time) {
	s.mu.Lock()
	cfg := s.config.Load()
	if s.ctx.Err() != nil || !cfg.Enabled || !cfg.DegradationScanEnabled || cfg.DegradationScanIntervalSeconds <= 0 || s.nextDegradationScan.After(now) {
		s.mu.Unlock()
		return
	}
	s.nextDegradationScan = now.Add(time.Duration(cfg.DegradationScanIntervalSeconds) * time.Second)
	s.mu.Unlock()
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	s.syncQuality(ctx)
	cancel()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.config.Load() == cfg {
		_ = s.enqueueSourceLocked(cfg, cfg.AccountIDs, "degradation_scan")
	}
}

func (s *OpenAIStateKeeperService) syncQuality(ctx context.Context) {
	if s.quality == nil {
		return
	}
	cfg := s.config.Load()
	for start := 0; start < len(cfg.AccountIDs); start += 100 {
		ids := cfg.AccountIDs[start:min(start+100, len(cfg.AccountIDs))]
		snapshot, err := s.quality.Results(ctx, ids)
		if err != nil {
			s.mu.Lock()
			for _, id := range ids {
				for _, model := range cfg.modelNames() {
					if e := s.entryLocked(id, model); e != nil {
						e.row.QualityStatus, e.row.QualityReason = "pending", "检测结果暂不可用"
					}
				}
			}
			s.mu.Unlock()
			continue
		}
		p := snapshot.Settings
		q := AccountQualitySettings{Enabled: p.Enabled, Revision: p.Revision, UpdatedAt: p.UpdatedAt, QuestionEnabled: p.QuestionEnabled, ModelAuditEnabled: p.ModelAuditEnabled, DegradationMode: p.DegradationMode, DegradationConditions: p.DegradationConditions}
		for _, v := range snapshot.Accounts {
			s.observeQualityResult(q, v)
		}
	}
}
