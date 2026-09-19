package service

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
)

type openAIStateAttempt struct {
	result  openAIStateProbeResult
	number  int
	proxyID int64
	next    chan bool
}

// A round reserves its account/model until every sibling exits. Request permits
// are shared by all rounds, including requests still draining after cancellation.
func (s *OpenAIStateKeeperService) run(job openAIStateKeeperJob) {
	accountCtx, accountCancel := context.WithTimeout(s.ctx, 5*time.Second)
	account, err := s.accounts.GetByID(accountCtx, job.accountID)
	accountCancel()
	if err != nil || account == nil {
		s.mu.Lock()
		if entry := s.entryLocked(job.accountID, job.model); entry != nil {
			entry.row.Queued = false
			if !entry.row.Paused {
				s.deferCollectionLocked(entry, time.Now().UTC().Add(keeperBackoff(s.config.Load().OpenAIStateKeeperSettings, 1)), "账号信息暂时读取失败，稍后自动重试")
				s.dirtyRuntime[s.key(job.accountID, job.model)] = true
			}
		}
		s.mu.Unlock()
		return
	}
	s.syncAccountAvailability([]*Account{account})
	s.mu.Lock()
	cfg := s.config.Load()
	if job.model == "" {
		job.model = cfg.modelNames()[0]
	}
	key := s.key(job.accountID, job.model)
	entry := s.rows[key]
	if job.source == "" {
		job.source = "manual"
	}
	for s.ctx.Err() == nil && cfg.Enabled && cfg.Revision == job.revision && entry != nil && s.activeCancels[key] == nil {
		if len(s.activeCancels) < cfg.Concurrency && s.activeProbes < cfg.Concurrency {
			break
		}
		s.workerSlots.Wait()
		cfg, entry = s.config.Load(), s.rows[key]
	}
	if s.ctx.Err() != nil || !cfg.Enabled || cfg.Revision != job.revision || entry == nil || s.activeCancels[key] != nil || (job.scopeID != "" && job.scopeID != entry.scopeID) {
		s.mu.Unlock()
		return
	}
	entry.row.Queued = false
	if job.source == "automatic_retry" && entry.row.RoundSource == "response" && (!cfg.InjectionEnabled || !cfg.ResponseRefreshEnabled) {
		entry.row.AutoRetryPending, entry.row.NextRetryAt, entry.row.RetryReason = false, nil, ""
		entry.refreshVersion = ""
		entry.row.NextAttemptAt = stateKeeperNextAttempt(cfg.OpenAIStateKeeperSettings, entry)
		s.dirtyRuntime[key] = true
		s.mu.Unlock()
		return
	}
	if entry.scopeLoading || !cfg.includesCollectionAccount(account) || entry.row.AccountUnavailable || (job.source != "manual" && entry.row.Paused) || (job.source == "degradation_scan" && !s.qualityEligibleLocked(entry)) || (job.source == "response" && (!cfg.InjectionEnabled || !cfg.ResponseRefreshEnabled)) {
		s.mu.Unlock()
		return
	}
	if until, reason := s.accountCollectionWaitLocked(job.accountID, cfg, time.Now()); !until.IsZero() {
		if job.source != "automatic_retry" {
			entry.row.RoundSource = job.source
		}
		s.deferCollectionLocked(entry, until, reason)
		s.dirtyRuntime[key] = true
		s.mu.Unlock()
		_ = s.persistRuntime(job.accountID, job.model)
		return
	}
	ctx, cancel := context.WithCancel(s.ctx)
	s.activeCancels[key] = cancel
	entry.row.Collecting = true
	entry.row.Paused, entry.row.PauseReason = false, ""
	if job.source != "automatic_retry" {
		entry.row.RoundSource = job.source
	}
	entry.row.RoundID, entry.row.RoundAttempts = uuid.NewString(), 0
	entry.row.AutoRetryPending, entry.row.RetryReason = false, ""
	entry.row.RetryAttempt, entry.row.NextRetryAt = 0, nil
	entry.row.RetryLimit = cfg.RetryCount
	entry.row.CollectionProxyID, entry.row.ProxyAttempt, entry.row.ProxyCount = cfg.proxyIDs()[0], 1, len(cfg.proxyIDs())
	roundID := entry.row.RoundID
	proxyIndex := 0
	for i, id := range cfg.proxyIDs() {
		if id == entry.nextProxyID {
			proxyIndex = i
			break
		}
	}
	s.mu.Unlock()
	defer cancel()
	stopWake := context.AfterFunc(ctx, func() { s.mu.Lock(); s.workerSlots.Broadcast(); s.mu.Unlock() })
	defer stopWake()

	// Persist intent before sending; request reservations also persist the
	// account budget so restarting cannot bypass its rate limit.
	if err := s.persistRuntime(job.accountID, job.model); err != nil {
		s.mu.Lock()
		if e := s.rows[key]; e != nil {
			e.row.Paused = true
			e.row.Status, e.row.Message = "failed", "采集轮次文件保存失败，未发送采集请求"
			if e.value != "" {
				e.row.Status = "refresh_failed"
			}
		}
		s.mu.Unlock()
		s.finishRound(job, roundID, false, "无法保存采集轮次，已暂停；请检查文件权限后手动重试")
		return
	}
	won := false
	reason := "本轮未取得合格 State，冷却后自动继续"
roundsLoop:
	for retry := 0; retry <= cfg.RetryCount && ctx.Err() == nil && s.config.Load() == cfg; retry++ {
		if retry > 0 {
			next := time.Now().UTC().Add(keeperJitter(time.Duration(cfg.RetryIntervalSeconds) * time.Second))
			s.mu.Lock()
			if e := s.rows[key]; e != nil {
				e.row.NextRetryAt, e.row.RetryReason = &next, "等待下一短轮采集"
			}
			s.mu.Unlock()
			if !keeperWait(ctx, time.Until(next)) || s.config.Load() != cfg {
				break
			}
		}
		budget := (retry + 1) * cfg.MaxAttempts
		for ctx.Err() == nil && s.config.Load() == cfg {
			proxyID := cfg.proxyIDs()[proxyIndex]
			s.mu.Lock()
			e := s.rows[key]
			if e == nil || e.row.Paused || e.row.AccountUnavailable {
				s.mu.Unlock()
				break roundsLoop
			}
			if until, _ := s.accountCollectionWaitLocked(job.accountID, cfg, time.Now()); !until.IsZero() {
				s.mu.Unlock()
				break roundsLoop
			}
			if e.row.RoundAttempts >= budget {
				s.mu.Unlock()
				break
			}
			batchBudget := budget
			if len(cfg.proxyIDs()) > 1 {
				batchBudget = min(budget, e.row.RoundAttempts+cfg.ProxyFailureThreshold)
			}
			e.row.NextRetryAt, e.row.RetryReason = nil, ""
			e.row.RetryAttempt = retry
			e.row.CollectionProxyID, e.row.ProxyAttempt = proxyID, proxyIndex+1
			s.mu.Unlock()
			won = s.runStateBatch(ctx, cfg, job, roundID, batchBudget, proxyID)
			if won {
				break roundsLoop
			}
			proxyIndex = (proxyIndex + 1) % len(cfg.proxyIDs())
			s.mu.Lock()
			if e := s.rows[key]; e != nil {
				e.nextProxyID = cfg.proxyIDs()[proxyIndex]
			}
			s.mu.Unlock()
		}
	}
	if !won && ctx.Err() != nil {
		reason = "采集被中断，稍后自动继续"
	}
	s.finishRound(job, roundID, won, reason)
}

func (s *OpenAIStateKeeperService) runStateBatch(parent context.Context, cfg *openAIStateKeeperConfig, job openAIStateKeeperJob, roundID string, budget int, proxyID int64) bool {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	stopWake := context.AfterFunc(ctx, func() { s.mu.Lock(); s.workerSlots.Broadcast(); s.mu.Unlock() })
	defer stopWake()
	parallel := min(cfg.AccountConcurrency, cfg.Concurrency, cfg.MaxAttempts)
	results := make(chan openAIStateAttempt, parallel)
	var siblings sync.WaitGroup
	for range parallel {
		siblings.Add(1)
		go func() {
			defer siblings.Done()
			for {
				number, ok := s.acquireStateProbe(ctx, cfg, job, budget)
				if !ok {
					return
				}
				var result openAIStateProbeResult
				if err := s.persistCollectionLimit(job.accountID); err != nil {
					result = openAIStateProbeResult{result: "failed", permanentFailure: true, message: "采集进度保存失败，未发送请求；请检查文件权限"}
				} else if ctx.Err() == nil {
					s.mu.Lock()
					cooling := s.collectionLimitLocked(job.accountID).CooldownUntil.After(time.Now())
					s.mu.Unlock()
					if !cooling {
						probeCtx, probeCancel := context.WithTimeout(ctx, 45*time.Second)
						probeSettings := cfg.forModel(job.model)
						probeSettings.ProxyID = proxyID
						result = s.probe(probeCtx, probeSettings, job.accountID)
						probeCancel()
					} else {
						result = openAIStateProbeResult{result: "cancelled", message: "账号进入冷却，本次预留请求未发送"}
					}
				}
				s.mu.Lock()
				s.noteCollectionRateLimitLocked(job.accountID, cfg.OpenAIStateKeeperSettings, result, time.Now().UTC())
				s.activeProbes--
				s.accountProbes[job.accountID]--
				s.workerSlots.Broadcast()
				s.mu.Unlock()
				next := make(chan bool, 1)
				results <- openAIStateAttempt{result: result, number: number, proxyID: proxyID, next: next}
				select {
				case again := <-next:
					if !again {
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	go func() { siblings.Wait(); close(results) }()
	won := false
	for attempt := range results {
		if !won && ctx.Err() == nil && s.config.Load() == cfg {
			won = s.publishAttempt(cfg, job, roundID, attempt)
			if attempt.result.accountUnavailable || attempt.result.status == http.StatusUnauthorized {
				for _, model := range cfg.modelNames() {
					if err := s.persistRuntime(job.accountID, model); err != nil {
						s.mu.Lock()
						s.dirtyRuntime[openAIStateKey{job.accountID, model}] = true
						s.mu.Unlock()
					}
				}
			}
			if won {
				cancel()
			}
			if attempt.result.status == http.StatusTooManyRequests || attempt.result.permanentFailure || attempt.result.proxyFailure {
				cancel()
			}
		} else {
			s.recordCancelledAttempt(job, roundID, attempt.number)
		}
		attempt.next <- !won && ctx.Err() == nil
	}
	return won
}

func (s *OpenAIStateKeeperService) publishAttempt(cfg *openAIStateKeeperConfig, job openAIStateKeeperJob, roundID string, attempt openAIStateAttempt) bool {
	r := attempt.result
	valid := r.result == "collected" && r.status == http.StatusOK && validCollectedState(r.value)
	if valid {
		r.hasCodexTurnState, r.turnStateLength = true, len(r.value)
		if !cfg.allowsStateLength(r.turnStateLength) {
			r.result, r.message = "filtered", fmt.Sprintf("响应头长度 %d 不符合允许保存的规则", r.turnStateLength)
		}
	}
	s.saveMu.Lock()
	defer s.saveMu.Unlock()
	s.mu.RLock()
	current := s.entryLocked(job.accountID, job.model)
	stillCurrent := current != nil && current.row.RoundID == roundID
	s.mu.RUnlock()
	if s.config.Load() != cfg || !stillCurrent {
		return false
	}
	now := time.Now().UTC()
	saved := false
	if valid && r.result == "collected" {
		if s.files != nil {
			err := s.files.save(openAIStateFileRecord{AccountID: job.accountID, Model: job.model, ProxyID: attempt.proxyID, Endpoint: openAIStateKeeperCollectionURL, HeaderName: openAICodexTurnStateHeader, CredentialStamp: r.credentialStamp, Value: r.value, CollectedAt: now, Version: roundID})
			if err != nil {
				r.result, r.message = "failed", "响应头已取得，但 State 文件保存失败"
				r.permanentFailure = true
			} else {
				saved = true
			}
		}
	}
	won := valid && r.result == "collected"
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := s.entryLocked(job.accountID, job.model)
	if entry == nil || s.config.Load() != cfg {
		return false
	}
	entry.row.HTTPStatus, entry.row.TurnStateLength, entry.row.HasCodexTurnState = r.status, r.turnStateLength, r.hasCodexTurnState
	entry.row.Status, entry.row.Message = r.result, r.message
	if r.accountUnavailable || r.status == http.StatusUnauthorized {
		s.blockAccountLocked(job.accountID, r.credentialStamp, "采集收到 401 或账号失效错误，停止采集；请先修复账号凭据")
	}
	if r.permanentFailure {
		entry.row.Paused, entry.row.PauseReason = true, r.message+"；等待人工处理"
	}
	if won {
		entry.value, entry.credentialStamp, entry.version = r.value, r.credentialStamp, roundID
		entry.proxyID = attempt.proxyID
		entry.refreshVersion = ""
		entry.row.Status, entry.row.Message = "ready", "已保存该账号的 State"
		entry.row.CollectedAt, entry.row.StateFileSaved = &now, saved
		sum := sha256.Sum256([]byte(r.value))
		entry.row.Fingerprint = fmt.Sprintf("%x", sum[:])
		entry.row.Successes++
		entry.nextProxyID = 0
		limit := s.collectionLimitLocked(job.accountID)
		if !limit.CooldownUntil.After(now) {
			limit.RateLimitStreak, limit.UpdatedAt = 0, time.Now().UTC()
		}
	} else if r.result != "filtered" && entry.value != "" {
		entry.row.Status = "refresh_failed"
	}
	if valid {
		entry.detail = &OpenAIStateKeeperDetail{AccountID: job.accountID, Model: job.model, HTTPStatus: r.status, HeaderName: openAICodexTurnStateHeader, HeaderValue: r.value, TurnStateLength: r.turnStateLength, CollectedAt: now, SaveAllowed: cfg.allowsStateLength(r.turnStateLength), StateFileSaved: saved}
		entry.row.HasDetails = true
	}
	s.appendEventLocked(entry, OpenAIStateKeeperEvent{At: now, AccountID: job.accountID, Model: job.model, ProxyID: attempt.proxyID, Kind: "collection", Source: job.source, RoundID: roundID, Attempt: attempt.number, HTTPStatus: r.status, TurnStateLength: r.turnStateLength, Result: r.result, Message: entry.row.Message})
	return won
}

func (s *OpenAIStateKeeperService) recordCancelledAttempt(job openAIStateKeeperJob, roundID string, number int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry := s.entryLocked(job.accountID, job.model); entry != nil && entry.row.RoundID == roundID {
		s.appendEventLocked(entry, OpenAIStateKeeperEvent{At: time.Now().UTC(), AccountID: job.accountID, Model: entry.row.Model, Kind: "collection", Source: job.source, RoundID: roundID, Attempt: number, Result: "cancelled", Message: "本轮已结束，此并发请求的结果不再写入"})
	}
}

func (s *OpenAIStateKeeperService) finishRound(job openAIStateKeeperJob, roundID string, won bool, reason string) {
	s.mu.Lock()
	entry := s.entryLocked(job.accountID, job.model)
	if entry != nil && entry.row.RoundID == roundID {
		entry.row.Collecting = false
		entry.row.NextRetryAt = nil
		entry.row.Paused = entry.row.Paused || entry.row.AccountUnavailable
		if entry.row.AccountUnavailable {
			entry.row.PauseReason = entry.row.AccountUnavailableReason
		} else if entry.row.Paused && entry.row.PauseReason == "" {
			entry.row.PauseReason = reason
		}
		entry.row.AutoRetryPending, entry.row.RetryReason = false, ""
		if won {
			entry.row.FailureCycles = 0
		} else if !entry.row.Paused && !(entry.row.RoundSource == "response" && (!s.config.Load().InjectionEnabled || !s.config.Load().ResponseRefreshEnabled)) {
			entry.row.FailureCycles = min(20, entry.row.FailureCycles+1)
			now := time.Now().UTC()
			next := now.Add(keeperBackoff(s.config.Load().OpenAIStateKeeperSettings, entry.row.FailureCycles))
			if until, waitReason := s.accountCollectionWaitLocked(job.accountID, s.config.Load(), now); !until.IsZero() {
				if until.After(next) {
					next = until
				}
				reason = waitReason
			}
			s.deferCollectionLocked(entry, next, reason)
		}
		entry.lastFinishedAt = time.Now().UTC()
		finished := entry.lastFinishedAt
		entry.row.LastCollectionAt = &finished
		entry.row.NextAttemptAt = stateKeeperNextAttempt(s.config.Load().OpenAIStateKeeperSettings, entry)
	}
	s.mu.Unlock()
	err := s.persistRuntime(job.accountID, job.model)
	s.mu.Lock()
	if current := s.entryLocked(job.accountID, job.model); err != nil && current != nil && current.row.RoundID == roundID {
		current.row.Paused, current.row.PauseReason, current.row.NextAttemptAt = true, "保存轮次结果失败，等待人工重试", nil
		current.row.AutoRetryPending, current.row.NextRetryAt = false, nil
	}
	delete(s.activeCancels, s.key(job.accountID, job.model))
	s.workerSlots.Broadcast()
	s.mu.Unlock()
}
