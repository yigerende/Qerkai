package service

import (
	"context"
	"errors"
	"strings"
	"time"
)

func (s *AccountQualityService) checkQualityModel(ctx context.Context, q AccountQualitySettings, v *AccountQualityResult) {
	route, err := s.qualityBPSRoute(ctx, v.AccountID, q.ModelAuditModel)
	if err != nil {
		applyQualityModelError(&v.Model, q, err, time.Now().UTC())
		return
	}
	requestedModel := q.ModelAuditModel
	endpoint := ""
	if route != nil {
		endpoint = openAIBPSEndpoint
		q.ModelAuditModel = route.models[requestedModel]
	}
	if v.Model.UpstreamEndpoint != endpoint {
		// Consecutive evidence from different endpoints must not combine.
		v.Model = QualityModelResult{UpstreamEndpoint: endpoint, StateVersion: v.Model.StateVersion,
			StateCollectedAt: v.Model.StateCollectedAt, StateValidationPending: v.Model.StateValidationPending}
	}
	results, err := s.usage.LatestModelAudit(ctx, ModelAuditInput{ExcludeBPS: route == nil, OnlyBPS: route != nil, Model: q.ModelAuditModel, Accounts: []ModelAuditAccount{{AccountID: v.AccountID, Since: qualityModelSampleSince(q, v.Model)}}})
	if err != nil {
		applyQualityModelError(&v.Model, q, err, time.Now().UTC())
		return
	}
	var logs []ModelAuditLog
	for _, result := range results {
		if result.AccountID != v.AccountID {
			continue
		}
		for _, item := range result.Logs {
			if item.AccountID == v.AccountID && qualityModelLogUsable(v.Model, q, item) {
				logs = append(logs, item)
			}
		}
	}
	if len(logs) > 0 {
		applyQualityModelLogs(&v.Model, q, logs, time.Now().UTC())
		if !v.Model.StateValidationPending || !v.Model.NoNewSamples {
			return
		}
	}
	// Pending State validation needs fresh evidence even when old logs exist.
	// A direct sample is not a usage log: do not invent IDs or change LatestID.
	modelPolicy := q
	modelPolicy.Model = requestedModel
	observer := &upstreamResponseModelObserver{}
	started := time.Now().UTC()
	_, _, err = s.testQualityRequest(ctx, v.AccountID, modelPolicy, "hi", observer)
	now := time.Now().UTC()
	if err == nil && observer.Model() == "" {
		err = errors.New("模型检测未取得有效的上游模型名")
	}
	if err != nil {
		applyQualityModelError(&v.Model, q, err, now)
		return
	}
	next := now.Add(time.Duration(q.ModelAuditIntervalSeconds) * time.Second)
	v.Model.CheckedAt, v.Model.NextAt = &now, &next
	v.Model.NoNewSamples = false
	v.Model.SentModel, v.Model.ResponseModel = q.ModelAuditModel, observer.Model()
	sent, response := strings.ToLower(strings.TrimSpace(q.ModelAuditModel)), strings.ToLower(observer.Model())
	variant := sent != response && qualityModelSuffix.ReplaceAllString(sent, "") == qualityModelSuffix.ReplaceAllString(response, "")
	passed := (sent == response || variant) && !observer.Conflict()
	applyQualityVerdict(&v.Model.QualityVerdict, passed, q, started)
	if variant && passed {
		v.Model.Status = "variant"
	}
	if v.Model.StateValidationPending {
		if v.Model.Failures >= q.FailureLimit || v.Model.Successes >= q.RecoveryLimit {
			v.Model.StateValidationPending = false
		} else if passed {
			v.Model.Status = "state_pending"
		}
	}
}

func (s *AccountQualityService) qualityBPSRoute(ctx context.Context, accountID int64, model string) (*cachedOpenAIBPS, error) {
	if s.tests == nil {
		return nil, nil
	}
	cfg := s.tests.settingService.bpsSettings(ctx)
	if cfg == nil || !cfg.settings.Enabled {
		return nil, nil
	}
	if _, ok := cfg.models[model]; !ok {
		return nil, nil
	}
	a, err := s.tests.accountRepo.GetByID(ctx, accountID)
	if err != nil {
		return nil, err
	}
	if !cfg.matchesAccount(a, model) {
		return nil, nil
	}
	return cfg, nil
}

func applyQualityModelError(v *QualityModelResult, q AccountQualitySettings, err error, now time.Time) {
	v.Status, v.Error, v.NoNewSamples = "error", err.Error(), true
	v.CheckedAt = &now
	next := now.Add(time.Duration(q.RetrySeconds) * time.Second)
	v.NextAt = &next
}
