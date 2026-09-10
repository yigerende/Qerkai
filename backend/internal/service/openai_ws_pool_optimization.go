package service

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

type OpenAIWSPoolOptimizationSettings struct {
	Enabled               bool    `json:"enabled"`
	PrewarmIdle           int     `json:"prewarm_idle_per_account"`
	StandbyIdle           int     `json:"standby_idle_per_account"`
	StandbyMax            int     `json:"standby_max_per_account"`
	MaxConns              int     `json:"max_conns_per_account"`
	QueuePerConn          int     `json:"queue_per_conn"`
	TargetUtilization     float64 `json:"target_utilization"`
	IdleRecycleSeconds    int     `json:"idle_recycle_seconds"`
	MaxAgeSeconds         int     `json:"max_age_seconds"`
	HealthIntervalSeconds int     `json:"health_interval_seconds"`
	SessionTTLSeconds     int     `json:"session_ttl_seconds"`
	DialIntervalMS        int     `json:"dial_interval_ms"`
}

func defaultOpenAIWSPoolOptimizationSettings() OpenAIWSPoolOptimizationSettings {
	return OpenAIWSPoolOptimizationSettings{
		PrewarmIdle: 3, StandbyIdle: 3, StandbyMax: 8, MaxConns: 24,
		QueuePerConn: 1, TargetUtilization: 0.8, IdleRecycleSeconds: 300,
		MaxAgeSeconds: 3600, HealthIntervalSeconds: 30,
		SessionTTLSeconds: 3600, DialIntervalMS: 400,
	}
}

type cachedOpenAIWSPoolOptimization struct {
	settings  OpenAIWSPoolOptimizationSettings
	expiresAt int64
}

var (
	openAIWSPoolOptimizationCache atomic.Pointer[cachedOpenAIWSPoolOptimization]
	openAIWSPoolOptimizationSF    singleflight.Group
)

var openAIWSPoolOptimizationKeys = []string{
	SettingKeyOpenAIWSPoolOptimizationEnabled, SettingKeyOpenAIWSPrewarmIdlePerAccount,
	SettingKeyOpenAIWSStandbyIdlePerAccount, SettingKeyOpenAIWSStandbyMaxPerAccount,
	SettingKeyOpenAIWSOptimizedMaxConns, SettingKeyOpenAIWSOptimizedQueuePerConn,
	SettingKeyOpenAIWSOptimizedTargetUtil, SettingKeyOpenAIWSOptimizedIdleRecycle,
	SettingKeyOpenAIWSOptimizedMaxAge, SettingKeyOpenAIWSOptimizedHealthInterval,
	SettingKeyOpenAIWSOptimizedSessionTTL, SettingKeyOpenAIWSOptimizedDialIntervalMS,
}

func normalizeOpenAIWSPoolOptimizationSettings(v OpenAIWSPoolOptimizationSettings) OpenAIWSPoolOptimizationSettings {
	d := defaultOpenAIWSPoolOptimizationSettings()
	if v.PrewarmIdle < 0 {
		v.PrewarmIdle = d.PrewarmIdle
	}
	if v.StandbyIdle < 0 {
		v.StandbyIdle = d.StandbyIdle
	}
	if v.StandbyMax < v.StandbyIdle {
		v.StandbyMax = max(v.StandbyIdle, d.StandbyMax)
	}
	if v.MaxConns <= 0 {
		v.MaxConns = d.MaxConns
	}
	if v.PrewarmIdle+v.StandbyIdle > v.MaxConns {
		v.StandbyIdle = max(0, v.MaxConns-v.PrewarmIdle)
	}
	if v.QueuePerConn <= 0 {
		v.QueuePerConn = d.QueuePerConn
	}
	if v.TargetUtilization <= 0 || v.TargetUtilization > 1 {
		v.TargetUtilization = d.TargetUtilization
	}
	if v.IdleRecycleSeconds <= 0 {
		v.IdleRecycleSeconds = d.IdleRecycleSeconds
	}
	if v.MaxAgeSeconds <= 0 {
		v.MaxAgeSeconds = d.MaxAgeSeconds
	}
	if v.HealthIntervalSeconds <= 0 {
		v.HealthIntervalSeconds = d.HealthIntervalSeconds
	}
	if v.SessionTTLSeconds <= 0 {
		v.SessionTTLSeconds = d.SessionTTLSeconds
	}
	if v.DialIntervalMS < 400 {
		v.DialIntervalMS = 400
	}
	return v
}

func parseOpenAIWSPoolOptimizationValues(values map[string]string) OpenAIWSPoolOptimizationSettings {
	v := defaultOpenAIWSPoolOptimizationSettings()
	v.Enabled = strings.EqualFold(strings.TrimSpace(values[SettingKeyOpenAIWSPoolOptimizationEnabled]), "true")
	v.PrewarmIdle = parseIntSettingDefault(values, SettingKeyOpenAIWSPrewarmIdlePerAccount, v.PrewarmIdle)
	v.StandbyIdle = parseIntSettingDefault(values, SettingKeyOpenAIWSStandbyIdlePerAccount, v.StandbyIdle)
	v.StandbyMax = parseIntSettingDefault(values, SettingKeyOpenAIWSStandbyMaxPerAccount, v.StandbyMax)
	v.MaxConns = parseIntSettingDefault(values, SettingKeyOpenAIWSOptimizedMaxConns, v.MaxConns)
	v.QueuePerConn = parseIntSettingDefault(values, SettingKeyOpenAIWSOptimizedQueuePerConn, v.QueuePerConn)
	v.TargetUtilization = parseFloatSettingDefault(values, SettingKeyOpenAIWSOptimizedTargetUtil, v.TargetUtilization)
	v.IdleRecycleSeconds = parseIntSettingDefault(values, SettingKeyOpenAIWSOptimizedIdleRecycle, v.IdleRecycleSeconds)
	v.MaxAgeSeconds = parseIntSettingDefault(values, SettingKeyOpenAIWSOptimizedMaxAge, v.MaxAgeSeconds)
	v.HealthIntervalSeconds = parseIntSettingDefault(values, SettingKeyOpenAIWSOptimizedHealthInterval, v.HealthIntervalSeconds)
	v.SessionTTLSeconds = parseIntSettingDefault(values, SettingKeyOpenAIWSOptimizedSessionTTL, v.SessionTTLSeconds)
	v.DialIntervalMS = parseIntSettingDefault(values, SettingKeyOpenAIWSOptimizedDialIntervalMS, v.DialIntervalMS)
	return normalizeOpenAIWSPoolOptimizationSettings(v)
}

func refreshOpenAIWSPoolOptimizationSettings(settings *SystemSettings) {
	if settings == nil {
		return
	}
	v := normalizeOpenAIWSPoolOptimizationSettings(OpenAIWSPoolOptimizationSettings{
		Enabled:               settings.OpenAIWSPoolOptimizationEnabled,
		PrewarmIdle:           settings.OpenAIWSPrewarmIdlePerAccount,
		StandbyIdle:           settings.OpenAIWSStandbyIdlePerAccount,
		StandbyMax:            settings.OpenAIWSStandbyMaxPerAccount,
		MaxConns:              settings.OpenAIWSOptimizedMaxConnsPerAccount,
		QueuePerConn:          settings.OpenAIWSOptimizedQueuePerConn,
		TargetUtilization:     settings.OpenAIWSOptimizedTargetUtilization,
		IdleRecycleSeconds:    settings.OpenAIWSOptimizedIdleRecycleSeconds,
		MaxAgeSeconds:         settings.OpenAIWSOptimizedMaxAgeSeconds,
		HealthIntervalSeconds: settings.OpenAIWSOptimizedHealthIntervalSeconds,
		SessionTTLSeconds:     settings.OpenAIWSOptimizedSessionTTLSeconds,
		DialIntervalMS:        settings.OpenAIWSOptimizedDialIntervalMS,
	})
	openAIWSPoolOptimizationSF.Forget("settings")
	openAIWSPoolOptimizationCache.Store(&cachedOpenAIWSPoolOptimization{settings: v, expiresAt: time.Now().Add(forceUpstreamWSCacheTTL).UnixNano()})
}

func currentOpenAIWSPoolOptimizationSettings() OpenAIWSPoolOptimizationSettings {
	if cached := openAIWSPoolOptimizationCache.Load(); cached != nil && time.Now().UnixNano() < cached.expiresAt {
		return cached.settings
	}
	result, _, _ := openAIWSPoolOptimizationSF.Do("settings", func() (any, error) {
		if cached := openAIWSPoolOptimizationCache.Load(); cached != nil && time.Now().UnixNano() < cached.expiresAt {
			return cached.settings, nil
		}
		svc := forceUpstreamWSSettings.Load()
		if svc == nil || svc.settingRepo == nil {
			return defaultOpenAIWSPoolOptimizationSettings(), nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), forceUpstreamWSDBTimeout)
		defer cancel()
		values, err := svc.settingRepo.GetMultiple(ctx, openAIWSPoolOptimizationKeys)
		if err != nil && !errors.Is(err, ErrSettingNotFound) {
			return nil, err
		}
		v := parseOpenAIWSPoolOptimizationValues(values)
		openAIWSPoolOptimizationCache.Store(&cachedOpenAIWSPoolOptimization{settings: v, expiresAt: time.Now().Add(forceUpstreamWSCacheTTL).UnixNano()})
		return v, nil
	})
	if v, ok := result.(OpenAIWSPoolOptimizationSettings); ok {
		return v
	}
	return defaultOpenAIWSPoolOptimizationSettings()
}

func OpenAIWSPoolOptimizationActive() bool {
	return ForceUpstreamWSEnabled() && currentOpenAIWSPoolOptimizationSettings().Enabled
}

func openAIWSOptimizedDialInterval() time.Duration {
	v := currentOpenAIWSPoolOptimizationSettings().DialIntervalMS
	if v < 400 {
		v = 400
	}
	return time.Duration(v) * time.Millisecond
}
