package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const SettingKeyOpenAICongestionMigration = "openai_congestion_migration_enabled"
const SettingKeyOpenAICongestionConfig = "openai_congestion_migration_config"

var ErrInvalidOpenAICongestionSettings = errors.New("invalid congestion settings")

type OpenAICongestionSettings struct {
	Enabled                   bool `json:"enabled"`
	WindowSeconds             int  `json:"window_seconds"`
	SampleLimit               int  `json:"sample_limit"`
	SlowEnabled               bool `json:"slow_enabled"`
	SlowMinSamples            int  `json:"slow_min_samples"`
	SlowSeconds               int  `json:"slow_seconds"`
	SlowPercent               int  `json:"slow_percent"`
	SlowMinLoadPercent        int  `json:"slow_min_load_percent"`
	StalledEnabled            bool `json:"stalled_enabled"`
	StalledSeconds            int  `json:"stalled_seconds"`
	StalledMinRequests        int  `json:"stalled_min_requests"`
	StalledPercent            int  `json:"stalled_percent"`
	ErrorsEnabled             bool `json:"errors_enabled"`
	ErrorMinSamples           int  `json:"error_min_samples"`
	ErrorMinCount             int  `json:"error_min_count"`
	ErrorPercent              int  `json:"error_percent"`
	Error429                  bool `json:"error_429"`
	Error5xx                  bool `json:"error_5xx"`
	ErrorNetwork              bool `json:"error_network"`
	FullEnabled               bool `json:"full_enabled"`
	FullLoadPercent           int  `json:"full_load_percent"`
	FullWaitSeconds           int  `json:"full_wait_seconds"`
	TargetMaxLoadPercent      int  `json:"target_max_load_percent"`
	TargetMinFreeSlots        int  `json:"target_min_free_slots"`
	TargetRequireFaster       bool `json:"target_require_faster"`
	TargetMinSamples          int  `json:"target_min_samples"`
	TargetFasterPercent       int  `json:"target_faster_percent"`
	TargetAllowUnknown        bool `json:"target_allow_unknown"`
	ProtectionSeconds         int  `json:"protection_seconds"`
	PendingSeconds            int  `json:"pending_seconds"`
	AvoidUnhealthyNewSessions bool `json:"avoid_unhealthy_new_sessions"`
}

func DefaultOpenAICongestionSettings() OpenAICongestionSettings {
	return OpenAICongestionSettings{
		WindowSeconds: 60, SampleLimit: 256,
		SlowEnabled: true, SlowMinSamples: 5, SlowSeconds: 15, SlowPercent: 50,
		StalledEnabled: true, StalledSeconds: 20, StalledMinRequests: 5, StalledPercent: 20,
		ErrorsEnabled: true, ErrorMinSamples: 5, ErrorMinCount: 3, ErrorPercent: 30, Error429: true, Error5xx: true, ErrorNetwork: true,
		FullEnabled: true, FullLoadPercent: 100, FullWaitSeconds: 2,
		TargetMaxLoadPercent: 50, TargetMinFreeSlots: 1, TargetRequireFaster: true, TargetMinSamples: 3, TargetFasterPercent: 30,
		ProtectionSeconds: 120, PendingSeconds: 600, AvoidUnhealthyNewSessions: true,
	}
}

func (c OpenAICongestionSettings) Validate() error {
	fields := []struct {
		name            string
		value, min, max int
	}{
		{"window_seconds", c.WindowSeconds, 10, 3600}, {"sample_limit", c.SampleLimit, 5, 4096},
		{"slow_min_samples", c.SlowMinSamples, 1, c.SampleLimit}, {"slow_seconds", c.SlowSeconds, 1, 600}, {"slow_percent", c.SlowPercent, 1, 100}, {"slow_min_load_percent", c.SlowMinLoadPercent, 0, 100},
		{"stalled_seconds", c.StalledSeconds, 1, 600}, {"stalled_min_requests", c.StalledMinRequests, 1, 10000}, {"stalled_percent", c.StalledPercent, 1, 100},
		{"error_min_samples", c.ErrorMinSamples, 1, c.SampleLimit}, {"error_min_count", c.ErrorMinCount, 1, c.SampleLimit}, {"error_percent", c.ErrorPercent, 1, 100},
		{"full_load_percent", c.FullLoadPercent, 1, 100}, {"full_wait_seconds", c.FullWaitSeconds, 0, 30},
		{"target_max_load_percent", c.TargetMaxLoadPercent, 1, 100}, {"target_min_free_slots", c.TargetMinFreeSlots, 1, 10000}, {"target_min_samples", c.TargetMinSamples, 1, c.SampleLimit}, {"target_faster_percent", c.TargetFasterPercent, 1, 99},
		{"protection_seconds", c.ProtectionSeconds, 0, 3600}, {"pending_seconds", c.PendingSeconds, 30, 3600},
	}
	for _, f := range fields {
		if f.value < f.min || f.value > f.max {
			return fmt.Errorf("%w: %s must be between %d and %d", ErrInvalidOpenAICongestionSettings, f.name, f.min, f.max)
		}
	}
	return nil
}

type cachedOpenAICongestionSetting struct {
	enabled bool
	expires time.Time
	config  OpenAICongestionSettings
}

func (s *SettingService) loadOpenAICongestionSettings(ctx context.Context) (OpenAICongestionSettings, error) {
	c := DefaultOpenAICongestionSettings()
	values, err := s.settingRepo.GetMultiple(ctx, []string{SettingKeyOpenAICongestionMigration, SettingKeyOpenAICongestionConfig})
	if err != nil {
		return c, err
	}
	if raw := values[SettingKeyOpenAICongestionConfig]; raw != "" {
		if err := json.Unmarshal([]byte(raw), &c); err != nil {
			return c, err
		}
	}
	c.Enabled = strings.EqualFold(strings.TrimSpace(values[SettingKeyOpenAICongestionMigration]), "true")
	return c, c.Validate()
}
func (s *SettingService) cacheOpenAICongestionSettings(c OpenAICongestionSettings) {
	s.openAICongestionCache.Store(&cachedOpenAICongestionSetting{enabled: c.Enabled, expires: time.Now().Add(5 * time.Second), config: c})
}
func (s *SettingService) GetOpenAICongestionSettings(ctx context.Context) (OpenAICongestionSettings, error) {
	if s == nil || s.settingRepo == nil {
		return DefaultOpenAICongestionSettings(), nil
	}
	s.openAICongestionMu.Lock()
	defer s.openAICongestionMu.Unlock()
	c, err := s.loadOpenAICongestionSettings(ctx)
	if err == nil {
		s.cacheOpenAICongestionSettings(c)
	}
	return c, err
}

// Patch under one lock, preserving omitted fields for older clients. Both keys
// are saved in one repository bulk upsert, so an unsuccessful save changes neither.
func (s *SettingService) UpdateOpenAICongestionSettings(ctx context.Context, patch []byte) (OpenAICongestionSettings, error) {
	s.openAICongestionMu.Lock()
	defer s.openAICongestionMu.Unlock()
	c, err := s.loadOpenAICongestionSettings(ctx)
	if err != nil {
		return c, err
	}
	decoder := json.NewDecoder(bytes.NewReader(patch))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&c); err != nil {
		return c, fmt.Errorf("%w: %v", ErrInvalidOpenAICongestionSettings, err)
	}
	if err = c.Validate(); err != nil {
		return c, err
	}
	raw, err := json.Marshal(c)
	if err != nil {
		return c, err
	}
	err = s.settingRepo.SetMultiple(ctx, map[string]string{SettingKeyOpenAICongestionMigration: strconv.FormatBool(c.Enabled), SettingKeyOpenAICongestionConfig: string(raw)})
	if err == nil {
		s.cacheOpenAICongestionSettings(c)
	}
	return c, err
}
func (s *SettingService) GetOpenAICongestionMigration(ctx context.Context) (bool, error) {
	c, err := s.GetOpenAICongestionSettings(ctx)
	return c.Enabled, err
}
func (s *SettingService) SetOpenAICongestionMigration(ctx context.Context, enabled bool) error {
	_, err := s.UpdateOpenAICongestionSettings(ctx, []byte(`{"enabled":`+strconv.FormatBool(enabled)+`}`))
	return err
}

func (s *SettingService) openAICongestionSettings(ctx context.Context) OpenAICongestionSettings {
	defaults := DefaultOpenAICongestionSettings()
	if s == nil || s.settingRepo == nil {
		return defaults
	}
	cached := s.openAICongestionCache.Load()
	if cached != nil && time.Now().Before(cached.expires) {
		return cached.config
	}
	if !s.openAICongestionMu.TryLock() {
		if cached != nil {
			return cached.config
		}
		return defaults
	}
	defer s.openAICongestionMu.Unlock()
	if cached = s.openAICongestionCache.Load(); cached != nil && time.Now().Before(cached.expires) {
		return cached.config
	}
	dbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancel()
	c, err := s.loadOpenAICongestionSettings(dbCtx)
	if err != nil {
		c = defaults
	} // Settings unavailable: original path, never implicit enable.
	s.cacheOpenAICongestionSettings(c)
	return c
}
func (s *SettingService) openAICongestionEnabled(ctx context.Context) bool {
	if s == nil || s.settingRepo == nil {
		return false
	}
	if cached := s.openAICongestionCache.Load(); cached != nil && time.Now().Before(cached.expires) {
		return cached.enabled
	}
	return s.openAICongestionSettings(ctx).Enabled
}
