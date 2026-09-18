package service

import (
	"context"
	"sort"

	"github.com/google/uuid"
)

func (cfg *openAIStateKeeperConfig) includesCollectionAccount(a *Account) bool {
	if a == nil {
		return false
	}
	if cfg.accounts[a.ID] {
		return true
	}
	for _, id := range a.GroupIDs {
		if cfg.collectionGroups[id] {
			return true
		}
	}
	return false
}

// Group members are resolved from the repository, never saved as explicit IDs.
func (s *OpenAIStateKeeperService) resolveCollectionAccounts(ctx context.Context, q OpenAIStateKeeperSettings, explicit []*Account) ([]*Account, error) {
	if explicit == nil && len(q.AccountIDs) > 0 {
		var err error
		explicit, err = s.accounts.GetByIDs(ctx, q.AccountIDs)
		if err != nil {
			return nil, err
		}
	}
	selected := make(map[int64]*Account, len(explicit))
	for _, a := range explicit {
		if a != nil {
			selected[a.ID] = a
		}
	}
	for _, id := range q.CollectionGroupIDs {
		members, err := s.accounts.ListAllWithFilters(ctx, PlatformOpenAI, AccountTypeOAuth, "", "", id, "")
		if err != nil {
			return nil, err
		}
		for i := range members {
			a := &members[i]
			if a.Platform == PlatformOpenAI && a.Type == AccountTypeOAuth {
				selected[a.ID] = a
			}
		}
	}
	out := make([]*Account, 0, len(selected))
	for _, a := range selected {
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *OpenAIStateKeeperService) collectionAccountIDs() []int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.collectionAccountIDsLocked()
}

func (s *OpenAIStateKeeperService) collectionAccountIDsLocked() []int64 {
	seen := make(map[int64]bool)
	for key := range s.rows {
		seen[key.accountID] = true
	}
	ids := make([]int64, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// The caller holds saveMu. Membership updates keep the configuration pointer,
// existing State versions, and unrelated collection rounds unchanged.
func (s *OpenAIStateKeeperService) syncCollectionScope(q OpenAIStateKeeperSettings, accounts []*Account) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.config.Load().Revision != q.Revision {
		return
	}
	selected := make(map[int64]bool, len(accounts))
	for _, a := range accounts {
		selected[a.ID] = true
	}
	for key := range s.rows {
		if !selected[key.accountID] {
			if cancel := s.activeCancels[key]; cancel != nil {
				cancel()
			}
			delete(s.rows, key)
			delete(s.pendingSignals, key)
			delete(s.dirtyRuntime, key)
		}
	}
	for _, a := range accounts {
		for _, model := range q.modelNames() {
			key := openAIStateKey{a.ID, model}
			if s.rows[key] == nil {
				s.rows[key] = &openAIKeptState{scopeLoading: true, scopeID: uuid.NewString(), row: OpenAIStateKeeperRow{AccountID: a.ID, Model: model, Status: "empty", QualityStatus: "pending"}}
			}
		}
		s.syncAccountAvailabilityLocked(a)
	}
	s.workerSlots.Broadcast()
}

// Restore paused budgets and State before newly joined accounts can be queued.
func (s *OpenAIStateKeeperService) restoreScopeStateFiles(ctx context.Context) {
	s.mu.RLock()
	ids := make(map[int64]bool)
	for key, e := range s.rows {
		if e.scopeLoading {
			ids[key.accountID] = true
		}
	}
	s.mu.RUnlock()
	for id := range ids {
		s.restoreStateFiles(ctx, id)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.rows {
		e.scopeLoading = false
	}
}
