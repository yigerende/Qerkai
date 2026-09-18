package service

import (
	"context"
	"errors"
	"time"
)

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
	for _, model := range s.config.Load().modelNames() {
		key := openAIStateKey{a.ID, model}
		e := s.rows[key]
		if e == nil {
			continue
		}
		previousStamp, previousStatus := e.blockedCredentialStamp, e.row.AccountStatus
		if e.blockedCredentialStamp != "" && (e.blockedCredentialStamp != stamp || (e.row.AccountStatus != "" && e.row.AccountStatus != StatusActive && a.Status == StatusActive)) {
			e.blockedCredentialStamp = ""
		}
		reason := ""
		switch {
		case a.Status == StatusError:
			reason = "账号已失效或认证异常，停止采集；请先修复账号"
		case !stateKeeperAccountEligible(a):
			reason = "账号未启用或不再是 OpenAI OAuth 账号，停止采集"
		case a.AutoPauseOnExpired && a.ExpiresAt != nil && !a.ExpiresAt.After(time.Now()):
			reason = "账号已过期，停止采集"
		case e.blockedCredentialStamp != "":
			reason = "采集收到 401 或账号失效错误，停止采集；请先修复账号凭据"
		}
		e.row.AccountStatus = a.Status
		e.row.AccountName = a.Name
		e.row.AccountGroupIDs = append([]int64{}, a.GroupIDs...)
		s.setAccountUnavailableLocked(key, e, reason)
		if previousStamp != e.blockedCredentialStamp || previousStatus != e.row.AccountStatus {
			s.dirtyRuntime[key] = true
		}
	}
}

func (s *OpenAIStateKeeperService) setAccountUnavailableLocked(key openAIStateKey, e *openAIKeptState, reason string) {
	e.row.AccountUnavailable, e.row.AccountUnavailableReason = reason != "", reason
	if reason != "" {
		e.row.Queued = false
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
		e.row.Paused, e.row.PauseReason = true, reason
		s.setAccountUnavailableLocked(key, e, reason)
		s.dirtyRuntime[key] = true
	}
	s.workerSlots.Broadcast()
}
