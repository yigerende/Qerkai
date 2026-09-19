package service

import (
	"context"
	"errors"
	"time"
)

type StateSchedulingValidity struct {
	Model       string     `json:"model"`
	Status      string     `json:"status"`
	Reason      string     `json:"reason"`
	CollectedAt *time.Time `json:"collected_at,omitempty"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty"`
	CheckedAt   time.Time  `json:"checked_at"`
	Version     string     `json:"-"`
}

func validateStateSchedulingQuality(q AccountQualitySettings) error {
	if !q.Enabled || !q.ModelAuditEnabled || !q.AllGroups {
		return errors.New("调度有效 State 要求启用降智检测、模型一致性检测，并将定时检测范围设置为全部分组")
	}
	return nil
}

func (s *OpenAIStateKeeperService) stateSchedulingEnabled() bool {
	return s != nil && s.config.Load() != nil && s.config.Load().RequireValidState
}

func (s *AccountQualityService) schedulingEnabled(q AccountQualitySettings) bool {
	return q.PauseOnDegradation || s.stateKeeper.Load().stateSchedulingEnabled()
}

// Used only by background checks and account views, never by request routing.
func (s *OpenAIStateKeeperService) schedulingValidity(a *Account, now time.Time) *StateSchedulingValidity {
	if !s.stateSchedulingEnabled() || a == nil || a.Platform != PlatformOpenAI || a.Type != AccountTypeOAuth {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	cfg := s.config.Load()
	return s.schedulingValidityLocked(cfg, a, now)
}

func (s *OpenAIStateKeeperService) schedulingValidityLocked(cfg *openAIStateKeeperConfig, a *Account, now time.Time) *StateSchedulingValidity {
	v := &StateSchedulingValidity{Model: cfg.SchedulingStateModel, Status: "missing", Reason: "指定模型尚未成功保存 State", CheckedAt: now}
	e := s.entryLocked(a.ID, cfg.SchedulingStateModel)
	if e == nil || !e.row.StateFileSaved || e.value == "" || e.row.CollectedAt == nil {
		return v
	}
	collected := *e.row.CollectedAt
	expires := collected.Add(time.Duration(cfg.SchedulingStateMinutes) * time.Minute)
	v.CollectedAt, v.ExpiresAt, v.Version = &collected, &expires, e.version
	allowed := cfg.AllGroups
	for _, id := range a.GroupIDs {
		allowed = allowed || cfg.groups[id]
	}
	if !cfg.Enabled || !cfg.InjectionEnabled || !allowed || !cfg.includesCollectionAccount(a) || e.scopeLoading || e.row.AccountUnavailable || !stateKeeperAccountEligible(a) || !cfg.allowsStateLength(len(e.value)) || !stateKeeperStateMatchesAccount(cfg.OpenAIStateKeeperSettings, e.credentialStamp, e.identityStamp, a) {
		v.Status, v.Reason = "unavailable", "指定模型 State 不符合当前账号或注入配置"
	} else if now.Before(collected) || !now.Before(expires) {
		v.Status, v.Reason = "expired", "指定模型 State 已超过调度有效期"
	} else {
		v.Status, v.Reason = "valid", "指定模型 State 在调度有效期内"
	}
	return v
}

func (s *AccountQualityService) schedulingState(ctx context.Context, id int64) (*StateSchedulingValidity, error) {
	keeper := s.stateKeeper.Load()
	if !keeper.stateSchedulingEnabled() {
		return nil, nil
	}
	a, err := keeper.accounts.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	return keeper.schedulingValidity(a, time.Now().UTC()), nil
}

// Repository account writes use the same identity policy as State injection.
func StateSchedulingCredentialsChanged(q OpenAIStateKeeperSettings, old, current *Account) bool {
	return !stateKeeperStateMatchesAccount(q, stateKeeperCredentialStamp(old), stateKeeperIdentityStamp(old), current)
}
