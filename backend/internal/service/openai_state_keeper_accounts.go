package service

import (
	"context"
	"errors"
	"time"
)

const (
	keeperCredentialPause      = "采集收到 401 或账号失效错误，停止采集；请先修复账号凭据"
	keeperAccountErrorPause    = "账号已失效或认证异常，停止采集；请先修复账号"
	keeperAccountDisabledPause = "账号未启用或不再是 OpenAI OAuth 账号，停止采集"
	keeperAccountExpiredPause  = "账号已过期，停止采集"
)

// Exact legacy reasons also recover pauses written before account recovery was automatic.
func keeperAccountPause(reason string) bool {
	switch reason {
	case keeperCredentialPause, keeperAccountErrorPause, keeperAccountDisabledPause, keeperAccountExpiredPause:
		return true
	}
	return false
}

func pauseStateCollectionForAccount(e *openAIKeptState, reason string) {
	if !e.row.Paused || e.row.PauseReason == "" || keeperAccountPause(e.row.PauseReason) {
		e.row.Paused, e.row.PauseReason = true, reason
	}
}

func (s *OpenAIStateKeeperService) refreshAccountAvailability() error {
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()
	if len(s.config.Load().CollectionGroupIDs) > 0 {
		if err := s.SyncSelection(ctx); err != nil {
			return err
		}
	}
	accounts, err := s.accounts.GetByIDs(ctx, s.collectionAccountIDs())
	if err != nil {
		return errors.New("无法读取采集账号状态，请稍后重试")
	}
	s.syncAccountAvailability(accounts)
	return nil
}

func (s *OpenAIStateKeeperService) syncAccountAvailability(accounts []*Account) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, account := range accounts {
		s.syncAccountAvailabilityLocked(account)
	}
}

func (s *OpenAIStateKeeperService) syncAccountAvailabilityLocked(a *Account) {
	if a == nil {
		return
	}
	stamp := stateKeeperCredentialStamp(a)
	cfg := s.config.Load()
	for _, model := range cfg.modelNames() {
		key := openAIStateKey{a.ID, model}
		e := s.rows[key]
		if e == nil {
			continue
		}
		if a.GetOpenAIAccessToken() != "" {
			previous := e.observedCredentialStamp
			if previous == "" {
				previous = e.credentialStamp
				if previous == "" {
					previous = e.blockedCredentialStamp
				}
			}
			if previous != "" && previous != stamp {
				e.credentialRefreshPending = true
				if cancel := s.activeCancels[key]; cancel != nil {
					cancel()
				}
			}
			if e.observedCredentialStamp != stamp {
				e.observedCredentialStamp = stamp
				s.dirtyRuntime[key] = true
			}
		}
		previousStamp, previousStatus := e.blockedCredentialStamp, e.row.AccountStatus
		accountPaused := e.row.Paused && keeperAccountPause(e.row.PauseReason)
		if e.blockedCredentialStamp != "" && a.GetOpenAIAccessToken() != "" && (e.blockedCredentialStamp != stamp || (e.row.AccountStatus != "" && e.row.AccountStatus != StatusActive && a.Status == StatusActive)) {
			e.blockedCredentialStamp = ""
		}
		reason := ""
		switch {
		case a.Status == StatusError:
			reason = keeperAccountErrorPause
		case !stateKeeperAccountEligible(a):
			reason = keeperAccountDisabledPause
		case a.AutoPauseOnExpired && a.ExpiresAt != nil && !a.ExpiresAt.After(time.Now()):
			reason = keeperAccountExpiredPause
		case e.blockedCredentialStamp != "":
			reason = keeperCredentialPause
		}
		e.row.AccountStatus = a.Status
		e.row.AccountName = a.Name
		e.row.AccountGroupIDs = append([]int64{}, a.GroupIDs...)
		s.setAccountUnavailableLocked(key, e, reason)
		if reason == "" && accountPaused && a.GetOpenAIAccessToken() != "" {
			e.row.Paused, e.row.PauseReason, e.refreshVersion = false, "", ""
			e.row.Message = "账号凭据或状态已恢复，已解除账号停采"
			e.row.NextAttemptAt = stateKeeperNextAttempt(cfg.OpenAIStateKeeperSettings, e)
			// Continue the interrupted collection, but never revive a disabled response trigger.
			if e.row.RoundSource != "response" || (cfg.InjectionEnabled && cfg.ResponseRefreshEnabled) {
				next, message := time.Now().UTC(), "账号凭据或状态已恢复，自动继续采集"
				if until, waitReason := s.accountCollectionWaitLocked(a.ID, cfg, next); !until.IsZero() {
					next, message = until, waitReason
				}
				s.deferCollectionLocked(e, next, message)
			}
			s.dirtyRuntime[key] = true
		}
		if previousStamp != e.blockedCredentialStamp || previousStatus != e.row.AccountStatus {
			s.dirtyRuntime[key] = true
		}
		s.scheduleCredentialRefreshLocked(cfg, key, e)
	}
}

// Preserve the intent while an old round drains or the account is unavailable.
// It is consumed only when a round starts with the currently observed token.
func (s *OpenAIStateKeeperService) scheduleCredentialRefreshLocked(cfg *openAIStateKeeperConfig, key openAIStateKey, e *openAIKeptState) {
	if !e.credentialRefreshPending || !cfg.Enabled || e.scopeLoading || e.row.AccountUnavailable || e.row.Paused || s.activeCancels[key] != nil {
		return
	}
	if e.row.AutoRetryPending && e.row.RoundSource == "credentials_updated" {
		return
	}
	e.row.RoundSource, e.refreshVersion = "credentials_updated", ""
	next, message := time.Now().UTC(), "账号凭据已更新，等待后台重新采集"
	if until, reason := s.accountCollectionWaitLocked(key.accountID, cfg, next); !until.IsZero() {
		next, message = until, reason
	}
	s.deferCollectionLocked(e, next, message)
	e.row.Message = message
	s.dirtyRuntime[key] = true
}

func (s *OpenAIStateKeeperService) setAccountUnavailableLocked(key openAIStateKey, e *openAIKeptState, reason string) {
	e.row.AccountUnavailable, e.row.AccountUnavailableReason = reason != "", reason
	if reason != "" {
		e.row.Queued = false
		e.row.AutoRetryPending, e.row.RetryReason = false, ""
		e.row.NextAttemptAt, e.row.NextRetryAt = nil, nil
		if cancel := s.activeCancels[key]; cancel != nil {
			cancel()
		}
	} else {
		e.row.NextAttemptAt = stateKeeperNextAttempt(s.config.Load().OpenAIStateKeeperSettings, e)
	}
}

// A credential failure applies to all models of this account, including queued
// rounds and other in-flight workers. Keep the block across config edits/restarts.
func (s *OpenAIStateKeeperService) blockAccountLocked(id int64, stamp, reason string) {
	for _, model := range s.config.Load().modelNames() {
		key := openAIStateKey{id, model}
		e := s.rows[key]
		if e == nil {
			continue
		}
		e.blockedCredentialStamp = stamp
		pauseStateCollectionForAccount(e, reason)
		s.setAccountUnavailableLocked(key, e, reason)
		s.dirtyRuntime[key] = true
	}
	s.workerSlots.Broadcast()
}
