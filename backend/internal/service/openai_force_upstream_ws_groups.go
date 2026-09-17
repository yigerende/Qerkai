package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

const SettingKeyForceOpenAIUpstreamWSGroupIDs = "force_openai_upstream_ws_group_ids"

type cachedForceUpstreamWSGroups struct {
	all       bool
	ids       map[int64]struct{}
	expiresAt int64
}

var forceUpstreamWSGroupsCache atomic.Pointer[cachedForceUpstreamWSGroups]
var forceUpstreamWSGroupsSF singleflight.Group

// Nil preserves the existing all-groups setting; an explicit empty list enables none.
func normalizeForceUpstreamWSGroupIDs(ids []int64) ([]int64, error) {
	if ids == nil {
		return nil, nil
	}
	result := make([]int64, 0, len(ids))
	seen := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		if id <= 0 {
			return nil, fmt.Errorf("%s must contain positive group IDs", SettingKeyForceOpenAIUpstreamWSGroupIDs)
		}
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			result = append(result, id)
		}
	}
	return result, nil
}

func parseForceUpstreamWSGroupIDs(raw string) []int64 {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var ids []int64
	if json.Unmarshal([]byte(raw), &ids) != nil {
		return []int64{}
	}
	normalized, err := normalizeForceUpstreamWSGroupIDs(ids)
	if err != nil {
		return []int64{}
	}
	return normalized
}

func newForceUpstreamWSGroupsCache(ids []int64, ttl time.Duration) *cachedForceUpstreamWSGroups {
	cached := &cachedForceUpstreamWSGroups{all: ids == nil, ids: make(map[int64]struct{}, len(ids)), expiresAt: time.Now().Add(ttl).UnixNano()}
	for _, id := range ids {
		cached.ids[id] = struct{}{}
	}
	return cached
}

func refreshForceUpstreamWSGroupsCache(ids []int64) {
	forceUpstreamWSGroupsSF.Forget(SettingKeyForceOpenAIUpstreamWSGroupIDs)
	forceUpstreamWSGroupsCache.Store(newForceUpstreamWSGroupsCache(ids, forceUpstreamWSCacheTTL))
}

func forceUpstreamWSGroups() *cachedForceUpstreamWSGroups {
	if cached := forceUpstreamWSGroupsCache.Load(); cached != nil && time.Now().UnixNano() < cached.expiresAt {
		return cached
	}
	value, _, _ := forceUpstreamWSGroupsSF.Do(SettingKeyForceOpenAIUpstreamWSGroupIDs, func() (any, error) {
		previous := forceUpstreamWSGroupsCache.Load()
		if previous != nil && time.Now().UnixNano() < previous.expiresAt {
			return previous, nil
		}
		var ids []int64
		ttl := forceUpstreamWSCacheTTL
		if svc := forceUpstreamWSSettings.Load(); svc != nil && svc.settingRepo != nil {
			ctx, cancel := context.WithTimeout(context.Background(), forceUpstreamWSDBTimeout)
			defer cancel()
			raw, err := svc.settingRepo.GetValue(ctx, SettingKeyForceOpenAIUpstreamWSGroupIDs)
			ids = parseForceUpstreamWSGroupIDs(raw)
			if err != nil && !errors.Is(err, ErrSettingNotFound) {
				ids, ttl = []int64{}, forceUpstreamWSErrorTTL
			}
		}
		cached := newForceUpstreamWSGroupsCache(ids, ttl)
		// A settings save must win over a read that started before it.
		if !forceUpstreamWSGroupsCache.CompareAndSwap(previous, cached) {
			return forceUpstreamWSGroupsCache.Load(), nil
		}
		return cached, nil
	})
	cached, _ := value.(*cachedForceUpstreamWSGroups)
	return cached
}

func ForceUpstreamWSEnabledForGroup(groupID int64) bool {
	if !ForceUpstreamWSEnabled() {
		return false
	}
	cached := forceUpstreamWSGroups()
	if cached == nil {
		return false
	}
	_, selected := cached.ids[groupID]
	return cached.all || selected
}

func firstForceWSGroupID(groupIDs []int64) int64 {
	if len(groupIDs) == 0 {
		return 0
	}
	return groupIDs[0]
}

// Use the request's group, never the selected account's potentially shared groups.
func resolveOpenAIWSProtocolForGroup(resolver OpenAIWSProtocolResolver, account *Account, groupID int64) OpenAIWSProtocolDecision {
	if !ForceUpstreamWSEnabledForGroup(groupID) {
		for {
			wrapper, ok := resolver.(*forceUpstreamWSProtocolResolver)
			if !ok {
				break
			}
			resolver = wrapper.inner
		}
	}
	return resolver.Resolve(account)
}
