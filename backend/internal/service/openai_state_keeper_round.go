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
	if entry.scopeLoading || !cfg.includesCollectionAccount(account) || entry.row.AccountUnavailable || (job.source != "manual" && entry.row.Paused) || (job.source == "degradation_scan" && !s.qualityEligibleLocked(entry)) || (job.source == "response" && (!cfg.InjectionEnabled || !cfg.ResponseRefreshEnabled)) {
		s.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(s.ctx)
	s.activeCancels[key] = cancel
	entry.row.Collecting = true
	entry.row.Paused, entry.row.PauseReason = false, ""
	entry.row.RoundID, entry.row.RoundSource, entry.row.RoundAttempts = uuid.NewString(), job.source, 0
	entry.row.RetryAttempt, entry.row.NextRetryAt = 0, nil
	entry.row.RetryLimit = cfg.RetryCount
	entry.row.CollectionProxyID, entry.row.ProxyAttempt, entry.row.ProxyCount = cfg.proxyIDs()[0], 1, len(cfg.proxyIDs())
	roundID := entry.row.RoundID
	s.mu.Unlock()
	defer cancel()
	stopWake := context.AfterFunc(ctx, func() { s.mu.Lock(); s.workerSlots.Broadcast(); s.mu.Unlock() })
	defer stopWake()

	// Persist intent before sending anything. A crash cannot silently start a
	// fresh budget for this account on the next process startup.
	if err := s.persistRuntime(job.accountID, job.model); err != nil {
		s.mu.Lock()
		if e := s.rows[key]; e != nil {
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
	reason := "已用完采集和重试次数，等待人工重试"
	if len(cfg.proxyIDs()) > 1 {
		reason = "所有采集代理均已用完采集和重试次数，等待人工重试"
	}
proxiesLoop:
	for proxyIndex, proxyID := range cfg.proxyIDs() {
		for retry := 0; retry <= cfg.RetryCount && ctx.Err() == nil && s.config.Load() == cfg; retry++ {
			if retry > 0 || proxyIndex > 0 {
				next := time.Now().UTC().Add(time.Duration(cfg.RetryIntervalSeconds) * time.Second)
				s.mu.Lock()
				if e := s.rows[key]; e != nil {
					e.row.NextRetryAt = &next
				}
				s.mu.Unlock()
				timer := time.NewTimer(time.Until(next))
				select {
				case <-ctx.Done():
					timer.Stop()
				case <-timer.C:
				}
				if ctx.Err() != nil || s.config.Load() != cfg {
					break proxiesLoop
				}
				s.mu.Lock()
				if e := s.rows[key]; e != nil {
					e.row.NextRetryAt = nil
					e.row.RetryAttempt = retry
					e.row.CollectionProxyID, e.row.ProxyAttempt = proxyID, proxyIndex+1
				}
				s.mu.Unlock()
				if err := s.persistRuntime(job.accountID, job.model); err != nil {
					reason = "保存重试进度失败，等待人工重试"
					break proxiesLoop
				}
			}
			budget := (proxyIndex*(cfg.RetryCount+1) + retry + 1) * cfg.MaxAttempts
			won = s.runStateBatch(ctx, cfg, job, roundID, budget, proxyID)
			if won {
				break proxiesLoop
			}
		}
	}
	if !won && ctx.Err() != nil {
		reason = "采集被中断，等待人工重试"
	}
	s.finishRound(job, roundID, won, reason)
}

func (s *OpenAIStateKeeperService) runStateBatch(parent context.Context, cfg *openAIStateKeeperConfig, job openAIStateKeeperJob, roundID string, budget int, proxyID int64) bool {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	stopWake := context.AfterFunc(ctx, func() { s.mu.Lock(); s.workerSlots.Broadcast(); s.mu.Unlock() })
	defer stopWake()
	key := s.key(job.accountID, job.model)
	parallel := min(cfg.AccountConcurrency, cfg.Concurrency, cfg.MaxAttempts)
	results := make(chan openAIStateAttempt, parallel)
	var siblings sync.WaitGroup
	for range parallel {
		siblings.Add(1)
		go func() {
			defer siblings.Done()
			for {
				s.mu.Lock()
				for ctx.Err() == nil && s.config.Load() == cfg && (s.activeProbes >= cfg.Concurrency || s.accountProbes[job.accountID] >= cfg.AccountConcurrency) {
					s.workerSlots.Wait()
				}
				current := s.rows[key]
				if ctx.Err() != nil || s.config.Load() != cfg || current == nil || current.row.AccountUnavailable || current.row.RoundAttempts >= budget {
					s.mu.Unlock()
					return
				}
				s.activeProbes++
				s.accountProbes[job.accountID]++
				current.row.RoundAttempts++
				current.row.Attempts++
				number := current.row.RoundAttempts
				now := time.Now().UTC()
				current.row.LastAttemptAt = &now
				s.mu.Unlock()
				probeCtx, probeCancel := context.WithTimeout(ctx, 45*time.Second)
				probeSettings := cfg.forModel(job.model)
				probeSettings.ProxyID = proxyID
				result := s.probe(probeCtx, probeSettings, job.accountID)
				probeCancel()
				s.mu.Lock()
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
	if won {
		entry.value, entry.credentialStamp, entry.version = r.value, r.credentialStamp, roundID
		entry.proxyID = attempt.proxyID
		entry.refreshVersion = ""
		entry.row.Status, entry.row.Message = "ready", "已保存该账号的 State"
		entry.row.CollectedAt, entry.row.StateFileSaved = &now, saved
		sum := sha256.Sum256([]byte(r.value))
		entry.row.Fingerprint = fmt.Sprintf("%x", sum[:])
		entry.row.Successes++
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
		entry.row.Paused = !won || entry.row.AccountUnavailable
		if entry.row.AccountUnavailable {
			entry.row.PauseReason = entry.row.AccountUnavailableReason
		} else if !won {
			entry.row.PauseReason = reason
		}
		entry.lastFinishedAt = time.Now().UTC()
		finished := entry.lastFinishedAt
		entry.row.LastCollectionAt = &finished
		entry.row.NextAttemptAt = stateKeeperNextAttempt(s.config.Load().OpenAIStateKeeperSettings, entry)
	}
	s.mu.Unlock()
	err := s.persistRuntime(job.accountID, job.model)
	s.mu.Lock()
	if err != nil && entry != nil {
		entry.row.Paused, entry.row.PauseReason, entry.row.NextAttemptAt = true, "保存轮次结果失败，等待人工重试", nil
	}
	delete(s.activeCancels, s.key(job.accountID, job.model))
	s.workerSlots.Broadcast()
	s.mu.Unlock()
}
