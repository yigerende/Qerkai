package service

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"
)

const SettingKeyOpenAICongestionMigration = "openai_congestion_migration_enabled"

type cachedOpenAICongestionSetting struct {
	enabled bool
	expires time.Time
}

// Dedicated setting: saving the account-page switch never rewrites other settings.
func (s *SettingService) GetOpenAICongestionMigration(ctx context.Context) (bool, error) {
	if s == nil || s.settingRepo == nil {
		return false, nil
	}
	s.openAICongestionMu.Lock()
	defer s.openAICongestionMu.Unlock()
	raw, err := s.settingRepo.GetValue(ctx, SettingKeyOpenAICongestionMigration)
	if errors.Is(err, ErrSettingNotFound) {
		err = nil
	}
	if err != nil {
		return false, err
	}
	enabled := strings.EqualFold(strings.TrimSpace(raw), "true")
	s.openAICongestionCache.Store(&cachedOpenAICongestionSetting{enabled, time.Now().Add(5 * time.Second)})
	return enabled, nil
}

func (s *SettingService) SetOpenAICongestionMigration(ctx context.Context, enabled bool) error {
	s.openAICongestionMu.Lock()
	defer s.openAICongestionMu.Unlock()
	if err := s.settingRepo.Set(ctx, SettingKeyOpenAICongestionMigration, strconv.FormatBool(enabled)); err != nil {
		return err
	}
	s.openAICongestionCache.Store(&cachedOpenAICongestionSetting{enabled, time.Now().Add(5 * time.Second)})
	return nil
}

func (s *SettingService) openAICongestionEnabled(ctx context.Context) bool {
	if s == nil || s.settingRepo == nil {
		return false
	}
	if v := s.openAICongestionCache.Load(); v != nil && time.Now().Before(v.expires) {
		return v.enabled
	}
	// Only the refresh winner reads the DB. Other requests use the cached flag
	// instead of queueing behind that query or a concurrent settings save.
	if !s.openAICongestionMu.TryLock() {
		v := s.openAICongestionCache.Load()
		return v != nil && v.enabled
	}
	defer s.openAICongestionMu.Unlock()
	if v := s.openAICongestionCache.Load(); v != nil && time.Now().Before(v.expires) {
		return v.enabled
	}
	dbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancel()
	raw, err := s.settingRepo.GetValue(dbCtx, SettingKeyOpenAICongestionMigration)
	enabled := err == nil && strings.EqualFold(strings.TrimSpace(raw), "true")
	s.openAICongestionCache.Store(&cachedOpenAICongestionSetting{enabled, time.Now().Add(5 * time.Second)})
	return enabled
}
