package service

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type openAIStateRuntime struct {
	Version                  int                        `json:"version"`
	AutoRetryPending         bool                       `json:"auto_retry_pending"`
	NextRetryAt              *time.Time                 `json:"next_retry_at,omitempty"`
	RetryReason              string                     `json:"retry_reason"`
	FailureCycles            int                        `json:"failure_cycles"`
	NextProxyID              int64                      `json:"next_proxy_id"`
	CollectionLimit          openAIStateCollectionLimit `json:"collection_limit"`
	AccountID                int64                      `json:"account_id"`
	AccountStatus            string                     `json:"account_status,omitempty"`
	BlockedCredentialStamp   string                     `json:"blocked_credential_stamp,omitempty"`
	ObservedCredentialStamp  string                     `json:"observed_credential_stamp,omitempty"`
	CredentialRefreshPending bool                       `json:"credential_refresh_pending,omitempty"`
	Model                    string                     `json:"model"`
	RetryAttempt             int                        `json:"retry_attempt"`
	RetryLimit               int                        `json:"retry_limit"`
	CollectionProxyID        int64                      `json:"collection_proxy_id"`
	ProxyAttempt             int                        `json:"proxy_attempt"`
	ProxyCount               int                        `json:"proxy_count"`
	Paused                   bool                       `json:"paused"`
	PauseReason              string                     `json:"pause_reason"`
	InProgress               bool                       `json:"in_progress"`
	RoundID                  string                     `json:"round_id"`
	RoundAttempts            int                        `json:"round_attempts"`
	RoundSource              string                     `json:"round_source"`
	Attempts                 int64                      `json:"attempts"`
	Successes                int64                      `json:"successes"`
	Injections               int64                      `json:"injections"`
	LastFinishedAt           time.Time                  `json:"last_finished_at"`
	RefreshVersion           string                     `json:"refresh_version"`
	Collections              []OpenAIStateKeeperEvent   `json:"collections"`
	InjectionEvents          []OpenAIStateKeeperEvent   `json:"injection_events"`
}

func (s *openAIStateFileStore) runtimePath(id int64, models ...string) string {
	return filepath.Join(s.dir, stateKeeperFileStem(id, models...)+".runtime.json")
}

func (s *openAIStateFileStore) collectionLimitPath(id int64) string {
	return filepath.Join(s.dir, stateKeeperFileStem(id)+".collection.json")
}

func (s *OpenAIStateKeeperService) persistRuntime(id int64, model ...string) error {
	s.saveMu.Lock()
	defer s.saveMu.Unlock()
	return s.persistRuntimeLocked(id, model...)
}

func (s *OpenAIStateKeeperService) persistCollectionLimit(id int64) error {
	if s.files == nil {
		return nil
	}
	lock := &s.collectionSaves[uint64(id)%uint64(len(s.collectionSaves))]
	lock.Lock()
	defer lock.Unlock()
	s.mu.RLock()
	var snapshot openAIStateCollectionLimit
	if limit := s.collectionLimits[id]; limit != nil {
		snapshot = *limit
	}
	s.mu.RUnlock()
	return writeKeeperRuntime(s.files.collectionLimitPath(id), snapshot)
}

func (s *OpenAIStateKeeperService) persistRuntimeLocked(id int64, models ...string) error {
	if s.files == nil {
		return nil
	}
	s.mu.RLock()
	key := s.key(id, models...)
	e := s.rows[key]
	if e == nil {
		s.mu.RUnlock()
		return nil
	}
	r := openAIStateRuntime{AccountID: id, Model: key.model, RetryAttempt: e.row.RetryAttempt, RetryLimit: e.row.RetryLimit, Paused: e.row.Paused, PauseReason: e.row.PauseReason, InProgress: e.row.Collecting || e.row.Queued,
		Version: 2, AutoRetryPending: e.row.AutoRetryPending, NextRetryAt: e.row.NextRetryAt, RetryReason: e.row.RetryReason, FailureCycles: e.row.FailureCycles, NextProxyID: e.nextProxyID,
		AccountStatus: e.row.AccountStatus, BlockedCredentialStamp: e.blockedCredentialStamp,
		ObservedCredentialStamp: e.observedCredentialStamp, CredentialRefreshPending: e.credentialRefreshPending,
		CollectionProxyID: e.row.CollectionProxyID, ProxyAttempt: e.row.ProxyAttempt, ProxyCount: e.row.ProxyCount,
		RoundID: e.row.RoundID, RoundAttempts: e.row.RoundAttempts, RoundSource: e.row.RoundSource, Attempts: e.row.Attempts, Successes: e.row.Successes, Injections: e.row.Injections,
		LastFinishedAt: e.lastFinishedAt, RefreshVersion: e.refreshVersion, Collections: append([]OpenAIStateKeeperEvent{}, e.collections...), InjectionEvents: append([]OpenAIStateKeeperEvent{}, e.injections...)}
	if limit := s.collectionLimits[id]; limit != nil {
		r.CollectionLimit = *limit
	}
	s.mu.RUnlock()
	if err := s.persistCollectionLimit(id); err != nil {
		return err
	}
	return writeKeeperRuntime(s.files.runtimePath(id, key.model), r)
}

func writeKeeperRuntime(path string, value any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".runtime-*.tmp")
	if err != nil {
		return err
	}
	defer func() { _ = f.Close(); _ = os.Remove(f.Name()) }()
	if _, err = f.Write(body); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

// Called under saveMu, including when the feature is disabled. Pauses belong
// to the account and must survive a configuration edit or a process restart.
func (s *OpenAIStateKeeperService) restoreRuntime() {
	if s.files == nil {
		return
	}
	s.flushProxySuccessesLocked()
	cfg := s.config.Load()
	for _, id := range s.collectionAccountIDs() {
		limitBody, limitErr := os.ReadFile(s.files.collectionLimitPath(id))
		var savedLimit openAIStateCollectionLimit
		if limitErr == nil {
			limitErr = json.Unmarshal(limitBody, &savedLimit)
		}
		s.mu.Lock()
		limit := s.collectionLimitLocked(id)
		if limitErr == nil && savedLimit.UpdatedAt.After(limit.UpdatedAt) {
			*limit = savedLimit
		}
		s.mu.Unlock()
		for _, model := range cfg.modelNames() {
			s.mu.RLock()
			loaded := s.entryLocked(id, model).runtimeLoaded
			s.mu.RUnlock()
			if loaded {
				continue
			}
			body, err := os.ReadFile(s.files.runtimePath(id, model))
			if errors.Is(err, os.ErrNotExist) {
				legacy, _ := s.files.load(id, cfg.forModel(model))
				if cfg.Models == nil || legacy != nil {
					body, err = os.ReadFile(s.files.runtimePath(id))
				}
			}
			var r openAIStateRuntime
			if err == nil {
				if len(body) > 256<<10 {
					err = errors.New("runtime too large")
				} else {
					err = json.Unmarshal(body, &r)
				}
				if err == nil && (r.AccountID != id || (r.Model != "" && r.Model != model)) {
					err = errors.New("runtime account or model mismatch")
				}
			}
			// Older runtime files did not track credential changes. Seed from the
			// saved State so reauthorization during downtime also gets recollected.
			observed := r.ObservedCredentialStamp
			if observed == "" {
				observed = r.BlockedCredentialStamp
				if record, loadErr := s.files.load(id, cfg.forModel(model)); loadErr == nil && record != nil && observed == "" {
					observed = record.CredentialStamp
				}
			}
			s.mu.Lock()
			e := s.entryLocked(id, model)
			e.runtimeLoaded = true
			if observed != "" {
				e.observedCredentialStamp = observed
			}
			e.credentialRefreshPending = e.credentialRefreshPending || r.CredentialRefreshPending
			if err == nil {
				if e.blockedCredentialStamp == "" {
					e.row.AccountStatus, e.blockedCredentialStamp = r.AccountStatus, r.BlockedCredentialStamp
				}
				if e.blockedCredentialStamp != "" {
					e.row.AccountUnavailable, e.row.AccountUnavailableReason = true, keeperCredentialPause
				}
				e.row.Paused, e.row.PauseReason = r.Paused, r.PauseReason
				e.row.AutoRetryPending, e.row.NextRetryAt, e.row.RetryReason = r.AutoRetryPending, r.NextRetryAt, r.RetryReason
				e.row.FailureCycles, e.nextProxyID = r.FailureCycles, r.NextProxyID
				limit := s.collectionLimitLocked(id)
				if r.CollectionLimit.UpdatedAt.After(limit.UpdatedAt) {
					*limit = r.CollectionLimit
				}
				if e.row.AccountUnavailable {
					pauseStateCollectionForAccount(e, e.row.AccountUnavailableReason)
					e.row.AutoRetryPending, e.row.NextRetryAt = false, nil
				} else if (r.InProgress && !r.Paused) || (r.Version < 2 && r.Paused && keeperLegacyTransientPause(r.PauseReason)) {
					s.deferCollectionLocked(e, time.Now().UTC().Add(keeperBackoff(cfg.OpenAIStateKeeperSettings, max(1, r.FailureCycles))), "上次采集未完成，冷却后自动继续")
				}
				e.row.RoundID, e.row.RoundAttempts, e.row.RoundSource = r.RoundID, r.RoundAttempts, r.RoundSource
				e.row.RetryAttempt = r.RetryAttempt
				e.row.RetryLimit = r.RetryLimit
				e.row.CollectionProxyID, e.row.ProxyAttempt, e.row.ProxyCount = r.CollectionProxyID, r.ProxyAttempt, r.ProxyCount
				e.row.Attempts, e.row.Successes, e.row.Injections = r.Attempts, r.Successes, r.Injections
				e.lastFinishedAt, e.refreshVersion = r.LastFinishedAt, r.RefreshVersion
				if !r.LastFinishedAt.IsZero() {
					finished := r.LastFinishedAt
					e.row.LastCollectionAt = &finished
				}
				e.collections, e.injections = lastKeeperEvents(r.Collections, 10), lastKeeperEvents(r.InjectionEvents, 10)
			} else if !errors.Is(err, os.ErrNotExist) {
				e.row.Paused, e.row.PauseReason = true, "采集轮次文件无法读取，等待人工重试"
			}
			if limitErr != nil && !errors.Is(limitErr, os.ErrNotExist) {
				e.row.Paused, e.row.PauseReason = true, "账号采集限速文件无法读取，请修复后人工重试"
				e.row.AutoRetryPending, e.row.NextRetryAt = false, nil
			}
			if e.row.RoundSource == "response" && (!cfg.InjectionEnabled || !cfg.ResponseRefreshEnabled) {
				e.row.AutoRetryPending, e.row.NextRetryAt, e.row.RetryReason = false, nil, ""
				e.refreshVersion = ""
			}
			e.row.NextAttemptAt = stateKeeperNextAttempt(s.config.Load().OpenAIStateKeeperSettings, e)
			s.mu.Unlock()
		}
	}
}

func lastKeeperEvents(events []OpenAIStateKeeperEvent, n int) []OpenAIStateKeeperEvent {
	if len(events) > n {
		return events[:n]
	}
	return events
}

func (s *OpenAIStateKeeperService) appendEventLocked(e *openAIKeptState, event OpenAIStateKeeperEvent) {
	if event.Kind == "injection" {
		e.injections = lastKeeperEvents(append([]OpenAIStateKeeperEvent{event}, e.injections...), 10)
	} else {
		e.collections = lastKeeperEvents(append([]OpenAIStateKeeperEvent{event}, e.collections...), 10)
		s.events = lastKeeperEvents(append([]OpenAIStateKeeperEvent{event}, s.events...), 200)
	}
	s.dirtyRuntime[s.key(e.row.AccountID, e.row.Model)] = true
}

type OpenAIStateKeeperRecent struct {
	AccountID       int64                    `json:"account_id"`
	Paused          bool                     `json:"paused"`
	PauseReason     string                   `json:"pause_reason"`
	Collections     []OpenAIStateKeeperEvent `json:"collections"`
	Injections      []OpenAIStateKeeperEvent `json:"injections"`
	SchedulingState *StateSchedulingValidity `json:"scheduling_state,omitempty"`
}

func (s *OpenAIStateKeeperService) RecentWithState(ctx context.Context, ids []int64) ([]OpenAIStateKeeperRecent, error) {
	out := s.Recent(ids)
	if !s.stateSchedulingEnabled() {
		return out, nil
	}
	accounts, err := s.accounts.GetByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	byID := make(map[int64]*Account, len(accounts))
	for _, a := range accounts {
		byID[a.ID] = a
	}
	now := time.Now().UTC()
	for i := range out {
		out[i].SchedulingState = s.schedulingValidity(byID[out[i].AccountID], now)
	}
	return out, nil
}

func (s *OpenAIStateKeeperService) Recent(ids []int64) []OpenAIStateKeeperRecent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]OpenAIStateKeeperRecent, 0, len(ids))
	for _, id := range ids {
		item := OpenAIStateKeeperRecent{AccountID: id, Collections: []OpenAIStateKeeperEvent{}, Injections: []OpenAIStateKeeperEvent{}}
		for _, model := range s.config.Load().modelNames() {
			if e := s.entryLocked(id, model); e != nil {
				if e.row.Paused {
					item.Paused, item.PauseReason = true, model+": "+e.row.PauseReason
				}
				item.Collections, item.Injections = append(item.Collections, e.collections...), append(item.Injections, e.injections...)
			}
		}
		sort.SliceStable(item.Collections, func(i, j int) bool { return item.Collections[i].At.After(item.Collections[j].At) })
		sort.SliceStable(item.Injections, func(i, j int) bool { return item.Injections[i].At.After(item.Injections[j].At) })
		item.Collections, item.Injections = lastKeeperEvents(item.Collections, 10), lastKeeperEvents(item.Injections, 10)
		out = append(out, item)
	}
	return out
}
