package service

import (
	"context"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/singleflight"
)

const (
	SettingKeyOpenAIWSFreshConnectionRetryEnabled = "openai_ws_fresh_connection_retry_enabled"
	SettingKeyOpenAIWSFreshConnectionRetryMax     = "openai_ws_fresh_connection_retry_max"
	DefaultOpenAIWSFreshConnectionRetryMax        = 1
	MaxOpenAIWSFreshConnectionRetries             = 100
	openAIWSProcessingErrorMessage                = "An error occurred while processing your request. You can retry your request, or contact us through our help center at help.openai.com if the error persists."
)

type openAIWSFreshConnectionRetryConfig struct {
	enabled bool
	max     int
}

type cachedOpenAIWSFreshConnectionRetry struct {
	config    openAIWSFreshConnectionRetryConfig
	expiresAt int64
}

var (
	openAIWSFreshConnectionRetryCache atomic.Pointer[cachedOpenAIWSFreshConnectionRetry]
	openAIWSFreshConnectionRetrySF    singleflight.Group
)

func parseOpenAIWSFreshConnectionRetry(values map[string]string) openAIWSFreshConnectionRetryConfig {
	count := parseIntSettingDefault(values, SettingKeyOpenAIWSFreshConnectionRetryMax, DefaultOpenAIWSFreshConnectionRetryMax)
	if count < 1 || count > MaxOpenAIWSFreshConnectionRetries {
		count = DefaultOpenAIWSFreshConnectionRetryMax
	}
	return openAIWSFreshConnectionRetryConfig{enabled: values[SettingKeyOpenAIWSFreshConnectionRetryEnabled] == "true", max: count}
}

func refreshOpenAIWSFreshConnectionRetry(settings *SystemSettings) {
	openAIWSFreshConnectionRetrySF.Forget("settings")
	openAIWSFreshConnectionRetryCache.Store(&cachedOpenAIWSFreshConnectionRetry{
		config:    openAIWSFreshConnectionRetryConfig{enabled: settings.OpenAIWSFreshConnectionRetryEnabled, max: settings.OpenAIWSFreshConnectionRetryMax},
		expiresAt: time.Now().Add(forceUpstreamWSCacheTTL).UnixNano(),
	})
}

func currentOpenAIWSFreshConnectionRetry() openAIWSFreshConnectionRetryConfig {
	if cached := openAIWSFreshConnectionRetryCache.Load(); cached != nil && time.Now().UnixNano() < cached.expiresAt {
		return cached.config
	}
	value, _, _ := openAIWSFreshConnectionRetrySF.Do("settings", func() (any, error) {
		cached := openAIWSFreshConnectionRetryCache.Load()
		if cached != nil && time.Now().UnixNano() < cached.expiresAt {
			return cached.config, nil
		}
		cfg := openAIWSFreshConnectionRetryConfig{max: DefaultOpenAIWSFreshConnectionRetryMax}
		ttl := forceUpstreamWSErrorTTL
		if svc := forceUpstreamWSSettings.Load(); svc != nil && svc.settingRepo != nil {
			ctx, cancel := context.WithTimeout(context.Background(), forceUpstreamWSDBTimeout)
			defer cancel()
			values, err := svc.settingRepo.GetMultiple(ctx, []string{SettingKeyOpenAIWSFreshConnectionRetryEnabled, SettingKeyOpenAIWSFreshConnectionRetryMax})
			if err == nil {
				cfg, ttl = parseOpenAIWSFreshConnectionRetry(values), forceUpstreamWSCacheTTL
			}
		}
		// A slow read that began before a settings save must not re-enable a
		// switch that was just disabled by the administrator.
		if !openAIWSFreshConnectionRetryCache.CompareAndSwap(cached, &cachedOpenAIWSFreshConnectionRetry{config: cfg, expiresAt: time.Now().Add(ttl).UnixNano()}) {
			if latest := openAIWSFreshConnectionRetryCache.Load(); latest != nil {
				return latest.config, nil
			}
		}
		return cfg, nil
	})
	if cfg, ok := value.(openAIWSFreshConnectionRetryConfig); ok {
		return cfg
	}
	return openAIWSFreshConnectionRetryConfig{max: DefaultOpenAIWSFreshConnectionRetryMax}
}

// Match the provider's processing failure, not all locally mapped 502/503s or
// generic server errors. Some responses append a support request ID.
func isOpenAIWSFreshConnectionRetryError(payload []byte) bool {
	root := gjson.ParseBytes(payload)
	err := root.Get("error")
	switch root.Get("type").String() {
	case "error":
	case "response.failed":
		err = root.Get("response.error")
	default:
		return false
	}
	if err.Get("code").String() != "server_error" {
		return false
	}
	for _, value := range []gjson.Result{err.Get("status_code"), err.Get("status"), root.Get("status_code"), root.Get("status")} {
		if status := value.Int(); status > 0 && status != 502 && status != 503 {
			return false
		}
	}
	message := strings.Join(strings.Fields(err.Get("message").String()), " ")
	if message == openAIWSProcessingErrorMessage {
		return true
	}
	suffix := strings.TrimPrefix(message, openAIWSProcessingErrorMessage+" Please include the request ID ")
	return suffix != message && len(suffix) > len(" in your message.") && strings.HasSuffix(suffix, " in your message.")
}

type openAIWSFreshConnectionRetryError struct{}

func (*openAIWSFreshConnectionRetryError) Error() string {
	return "retry processing error on a fresh upstream websocket"
}

func (state *openAIWSBusinessRetryState) ownsFreshConnectionError(payload []byte) bool {
	return state != nil && state.freshConnectionRetryMax > 0 && isOpenAIWSFreshConnectionRetryError(payload)
}

func (state *openAIWSBusinessRetryState) beginFreshConnectionRetry(ctx context.Context, c *gin.Context) bool {
	if state == nil || state.outputStarted || state.freshConnectionRetries >= state.freshConnectionRetryMax || ctx.Err() != nil ||
		c == nil || c.Request == nil || c.Request.Context().Err() != nil {
		return false
	}
	state.freshConnectionRetries++
	state.freshConnectionPending = true
	state.managed = true
	return true
}
