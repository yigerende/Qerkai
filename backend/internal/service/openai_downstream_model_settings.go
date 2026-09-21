package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const SettingKeyOpenAIDownstreamModelAlignment = "openai_downstream_model_alignment"

// Nil GroupIDs means all request groups; an empty slice means no groups.
type OpenAIDownstreamModelAlignmentSettings struct {
	Enabled  bool     `json:"enabled"`
	GroupIDs []int64  `json:"group_ids"`
	Models   []string `json:"models"`
}

func defaultOpenAIDownstreamModelAlignment() OpenAIDownstreamModelAlignmentSettings {
	return OpenAIDownstreamModelAlignmentSettings{Models: []string{"gpt-6-astra"}}
}

func NormalizeOpenAIDownstreamModelAlignment(v OpenAIDownstreamModelAlignmentSettings) (OpenAIDownstreamModelAlignmentSettings, error) {
	groups, err := normalizeForceUpstreamWSGroupIDs(v.GroupIDs)
	if err != nil {
		return v, err
	}
	v.GroupIDs = groups
	if len(v.Models) > 200 {
		return v, fmt.Errorf("downstream model alignment supports at most 200 models")
	}
	models := make([]string, 0, len(v.Models))
	seen := make(map[string]bool, len(v.Models))
	for _, model := range v.Models {
		model = strings.TrimSpace(model)
		if model == "" {
			continue
		}
		if len(model) > 200 || strings.ContainsAny(model, "\r\n\t ") {
			return v, fmt.Errorf("invalid downstream alignment model: model names must be at most 200 bytes and contain no whitespace")
		}
		key := strings.ToLower(model)
		if !seen[key] {
			models = append(models, model)
			seen[key] = true
		}
	}
	v.Models = models
	return v, nil
}

func parseOpenAIDownstreamModelAlignment(raw string) OpenAIDownstreamModelAlignmentSettings {
	v := defaultOpenAIDownstreamModelAlignment()
	if strings.TrimSpace(raw) == "" {
		return v
	}
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return defaultOpenAIDownstreamModelAlignment()
	}
	if normalized, err := NormalizeOpenAIDownstreamModelAlignment(v); err == nil {
		return normalized
	}
	return defaultOpenAIDownstreamModelAlignment()
}

type cachedOpenAIDownstreamModelAlignment struct {
	enabled   bool
	allGroups bool
	groups    map[int64]struct{}
	models    map[string]struct{}
	expiresAt time.Time
}

func newCachedOpenAIDownstreamModelAlignment(v OpenAIDownstreamModelAlignmentSettings, ttl time.Duration) *cachedOpenAIDownstreamModelAlignment {
	c := &cachedOpenAIDownstreamModelAlignment{
		enabled: v.Enabled, allGroups: v.GroupIDs == nil, expiresAt: time.Now().Add(ttl),
		groups: make(map[int64]struct{}, len(v.GroupIDs)), models: make(map[string]struct{}, len(v.Models)),
	}
	for _, id := range v.GroupIDs {
		c.groups[id] = struct{}{}
	}
	for _, model := range v.Models {
		c.models[strings.ToLower(strings.TrimSpace(model))] = struct{}{}
	}
	return c
}

func (c *cachedOpenAIDownstreamModelAlignment) matches(groupID int64, sentModel string) bool {
	if c == nil || !c.enabled {
		return false
	}
	if !c.allGroups {
		if _, ok := c.groups[groupID]; !ok {
			return false
		}
	}
	_, ok := c.models[strings.ToLower(strings.TrimSpace(sentModel))]
	return ok
}

func (s *SettingService) openAIDownstreamModelAlignment(ctx context.Context) *cachedOpenAIDownstreamModelAlignment {
	if s == nil || s.settingRepo == nil {
		return nil
	}
	if cached := s.downstreamModelAlignmentCache.Load(); cached != nil && time.Now().Before(cached.expiresAt) {
		return cached
	}
	result, _, _ := s.downstreamModelAlignmentSF.Do("settings", func() (any, error) {
		previous := s.downstreamModelAlignmentCache.Load()
		if previous != nil && time.Now().Before(previous.expiresAt) {
			return previous, nil
		}
		readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		raw, err := s.settingRepo.GetValue(readCtx, SettingKeyOpenAIDownstreamModelAlignment)
		ttl := time.Minute
		if err != nil {
			raw, ttl = "", 5*time.Second
		}
		cached := newCachedOpenAIDownstreamModelAlignment(parseOpenAIDownstreamModelAlignment(raw), ttl)
		// A settings save always wins over an older in-flight read.
		if !s.downstreamModelAlignmentCache.CompareAndSwap(previous, cached) {
			return s.downstreamModelAlignmentCache.Load(), nil
		}
		return cached, nil
	})
	cached, _ := result.(*cachedOpenAIDownstreamModelAlignment)
	return cached
}
