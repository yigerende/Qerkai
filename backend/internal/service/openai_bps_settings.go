package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/basispoints"
)

const SettingKeyOpenAIBPS = "openai_bps"

var ErrInvalidOpenAIBPSSettings = errors.New("invalid OpenAI BPS settings")

type OpenAIBPSSettings struct {
	Enabled  bool    `json:"enabled"`
	GroupIDs []int64 `json:"group_ids"`
	basispoints.Config
}

func DefaultOpenAIBPSSettings() OpenAIBPSSettings {
	c := basispoints.DefaultConfig()
	c.Models = []string{"gpt-6-astra"}
	c.ModelMappings = map[string]string{"gpt-6-astra": "gpt-6-astra"}
	return OpenAIBPSSettings{Config: c}
}

func NormalizeOpenAIBPSSettings(v OpenAIBPSSettings) (OpenAIBPSSettings, error) {
	if v.ResponsesURL == "" && v.Models == nil && v.TimeoutSeconds == 0 {
		enabled, groups := v.Enabled, v.GroupIDs
		transport, handshakeTimeout := v.UpstreamTransport, v.WSHandshakeTimeoutSeconds
		v = DefaultOpenAIBPSSettings()
		v.Enabled, v.GroupIDs = enabled, groups
		if transport != "" {
			v.UpstreamTransport = transport
		}
		if handshakeTimeout != 0 {
			v.WSHandshakeTimeoutSeconds = handshakeTimeout
		}
	}
	groups, err := normalizeForceUpstreamWSGroupIDs(v.GroupIDs)
	if err != nil {
		return v, fmt.Errorf("%w: %v", ErrInvalidOpenAIBPSSettings, err)
	}
	v.GroupIDs = groups
	if len(v.Models) == 0 {
		return v, fmt.Errorf("%w: at least one model is required", ErrInvalidOpenAIBPSSettings)
	}
	mappings := make(map[string]string, len(v.Models))
	for k, value := range v.ModelMappings {
		mappings[k] = value
	}
	models := make([]string, 0, len(v.Models))
	for _, model := range v.Models {
		model = strings.TrimSpace(model)
		if model == "" {
			continue
		}
		if strings.ContainsAny(model, "\r\n\t ") {
			return v, fmt.Errorf("%w: invalid model name", ErrInvalidOpenAIBPSSettings)
		}
		models = append(models, model)
		if _, ok := mappings[model]; !ok && !strings.HasSuffix(model, "-basispoints") && !strings.HasSuffix(model, "-bps") {
			mappings[model] = model
		}
	}
	if len(models) == 0 {
		return v, fmt.Errorf("%w: at least one model is required", ErrInvalidOpenAIBPSSettings)
	}
	v.Models = models
	v.ModelMappings = mappings
	cfg, err := basispoints.NormalizeConfig(v.Config)
	if err != nil {
		return v, fmt.Errorf("%w: %v", ErrInvalidOpenAIBPSSettings, err)
	}
	v.Config = cfg
	return v, nil
}

func parseOpenAIBPSSettings(raw string) OpenAIBPSSettings {
	v := DefaultOpenAIBPSSettings()
	if strings.TrimSpace(raw) == "" {
		return v
	}
	if json.Unmarshal([]byte(raw), &v) != nil {
		return DefaultOpenAIBPSSettings()
	}
	if v, err := NormalizeOpenAIBPSSettings(v); err == nil {
		return v
	}
	return DefaultOpenAIBPSSettings()
}

type cachedOpenAIBPS struct {
	settings  OpenAIBPSSettings
	groups    map[int64]bool
	models    map[string]string
	expiresAt time.Time
}

func newCachedOpenAIBPS(v OpenAIBPSSettings, ttl time.Duration) *cachedOpenAIBPS {
	c := &cachedOpenAIBPS{settings: v, groups: map[int64]bool{}, models: map[string]string{}, expiresAt: time.Now().Add(ttl)}
	for _, id := range v.GroupIDs {
		c.groups[id] = true
	}
	for _, model := range v.Models {
		mapped, _ := v.Config.UpstreamModelFor(model)
		c.models[model] = mapped
	}
	return c
}
func (c *cachedOpenAIBPS) groupMatches(id int64) bool {
	return c != nil && c.settings.Enabled && (c.settings.GroupIDs == nil || c.groups[id])
}
func (c *cachedOpenAIBPS) matches(account *Account, groupID int64, model string) bool {
	if account == nil || !account.IsOpenAIOAuth() || !c.groupMatches(groupID) {
		return false
	}
	_, ok := c.models[model]
	return ok
}

// Background account probes have no client API-key group. Match the account's
// memberships against the same configured group/model scope as live traffic.
func (c *cachedOpenAIBPS) matchesAccount(account *Account, model string) bool {
	if c == nil || !c.settings.Enabled || account == nil || !account.IsOpenAIOAuth() {
		return false
	}
	if _, ok := c.models[model]; !ok {
		return false
	}
	if c.settings.GroupIDs == nil {
		return true
	}
	for _, id := range account.GroupIDs {
		if c.groups[id] {
			return true
		}
	}
	return false
}
func (s *SettingService) bpsSettings(ctx context.Context) *cachedOpenAIBPS {
	if s == nil || s.settingRepo == nil {
		return nil
	}
	if c := s.bpsSettingsCache.Load(); c != nil {
		// Production warms this snapshot before accepting traffic. Refresh stale
		// snapshots in the background so the disabled path never waits on SQL.
		if time.Now().After(c.expiresAt) && s.bpsSettingsRefreshing.CompareAndSwap(false, true) {
			go func() {
				defer s.bpsSettingsRefreshing.Store(false)
				s.loadBPSSettings(context.Background())
			}()
		}
		return c
	}
	return s.loadBPSSettings(ctx)
}

func (s *SettingService) loadBPSSettings(ctx context.Context) *cachedOpenAIBPS {
	result, _, _ := s.bpsSettingsSF.Do("settings", func() (any, error) {
		previous := s.bpsSettingsCache.Load()
		if previous != nil && time.Now().Before(previous.expiresAt) {
			return previous, nil
		}
		readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		raw, err := s.settingRepo.GetValue(readCtx, SettingKeyOpenAIBPS)
		ttl := time.Minute
		if err != nil {
			ttl = 5 * time.Second
		}
		v := parseOpenAIBPSSettings(raw)
		if err != nil && previous != nil {
			v = previous.settings
		}
		cached := newCachedOpenAIBPS(v, ttl)
		if !s.bpsSettingsCache.CompareAndSwap(previous, cached) {
			return s.bpsSettingsCache.Load(), nil
		}
		return cached, nil
	})
	cached, _ := result.(*cachedOpenAIBPS)
	return cached
}

// Validate the persisted + submitted union, including omitted fields. SetMultiple
// stores the three values transactionally; the caller serializes concurrent edits.
func (s *SettingService) validateBPSUpdates(ctx context.Context, updates map[string]string) error {
	_, b := updates[SettingKeyOpenAIBPS]
	_, w := updates[SettingKeyForceOpenAIUpstreamWS]
	_, p := updates[SettingKeyOpenAIWSPoolOptimizationEnabled]
	if !b && !w && !p {
		return nil
	}
	values := make(map[string]string, 3)
	for _, key := range []string{SettingKeyOpenAIBPS, SettingKeyForceOpenAIUpstreamWS, SettingKeyOpenAIWSPoolOptimizationEnabled} {
		if value, ok := updates[key]; ok {
			values[key] = value
			continue
		}
		value, err := s.settingRepo.GetValue(ctx, key)
		if err != nil && !errors.Is(err, ErrSettingNotFound) {
			return err
		}
		values[key] = value
	}
	if parseOpenAIBPSSettings(values[SettingKeyOpenAIBPS]).Enabled && (values[SettingKeyForceOpenAIUpstreamWS] == "true" || values[SettingKeyOpenAIWSPoolOptimizationEnabled] == "true") {
		return fmt.Errorf("%w: BPS cannot be enabled together with forced WebSocket or WS pool optimization", ErrInvalidOpenAIBPSSettings)
	}
	return nil
}

// Publishing the committed key must not depend on the subsequent full settings
// reload succeeding. An explicit disable takes effect as soon as the save lands.
func (s *SettingService) publishBPSUpdate(updates map[string]string) {
	if raw, present := updates[SettingKeyOpenAIBPS]; present {
		s.bpsSettingsCache.Store(newCachedOpenAIBPS(parseOpenAIBPSSettings(raw), time.Minute))
	}
}
