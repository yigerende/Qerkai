//go:build unit

package service

import (
	"context"
	"net/http"
	"time"
)

// ConfigureOpenAIWSBusinessRetryForTest redirects actual WS connections to a
// local upstream. This hook is excluded from production builds.
func ConfigureOpenAIWSBusinessRetryForTest(s *OpenAIGatewayService, endpoint string, retry OpenAIUpstream5xxRetryConfig, usage UsageLogRepository) func() {
	previousRetry := openAIUpstream5xxRetryOverride.Load()
	previousForce := forceUpstreamWSOverride.Load()
	previousForwarding := gatewayForwardingCache.Load()
	previousPoolSettings := openAIWSPoolOptimizationCache.Load()
	force := true
	openAIUpstream5xxRetryOverride.Store(&retry)
	forceUpstreamWSOverride.Store(&force)
	gatewayForwardingCache.Store(&cachedGatewayForwardingSettings{openAITTFTMode: OpenAITTFTModeNetwork})
	poolSettings := defaultOpenAIWSPoolOptimizationSettings()
	poolSettings.Enabled = true
	poolSettings.PrewarmIdle, poolSettings.StandbyIdle = 0, 0
	openAIWSPoolOptimizationCache.Store(&cachedOpenAIWSPoolOptimization{settings: poolSettings, expiresAt: time.Now().Add(time.Hour).UnixNano()})
	cfg := s.cfg
	s.usageLogRepo = usage
	s.billingService = NewBillingService(cfg, nil)
	s.deferredService = &DeferredService{}
	cfg.Security.URLAllowlist.Enabled = false
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Gateway.OpenAIWS.Enabled = true
	cfg.Gateway.OpenAIWS.OAuthEnabled = true
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 256
	cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 256
	cfg.Gateway.OpenAIWS.MinIdlePerAccount = 0
	cfg.Gateway.OpenAIWS.QueueLimitPerConn = 1024
	// Local sockets need no WAN wait; the production dial limiter still runs.
	cfg.Gateway.OpenAIWS.DialTimeoutSeconds = 1
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 10
	cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 10
	cfg.Gateway.OpenAIWS.EventFlushBatchSize = 1
	cfg.Gateway.OpenAIWS.RetryBackoffInitialMS = 1
	cfg.Gateway.OpenAIWS.RetryBackoffMaxMS = 1
	s.openaiWSResolver = newForceUpstreamWSProtocolResolver(NewOpenAIWSProtocolResolver(cfg))
	pool := newOpenAIWSConnPool(cfg)
	pool.setClientDialerForTest(&openAIWSBusinessTestDialer{endpoint: endpoint, inner: newDefaultOpenAIWSClientDialer()})
	s.openaiWSPool = pool
	return func() {
		pool.Close()
		openAIUpstream5xxRetryOverride.Store(previousRetry)
		forceUpstreamWSOverride.Store(previousForce)
		openAIWSPoolOptimizationCache.Store(previousPoolSettings)
		if previousForwarding == nil {
			previousForwarding = (*cachedGatewayForwardingSettings)(nil)
		}
		gatewayForwardingCache.Store(previousForwarding)
	}
}

type openAIWSBusinessTestDialer struct {
	endpoint string
	inner    openAIWSClientDialer
}

func (d *openAIWSBusinessTestDialer) Dial(ctx context.Context, _ string, headers http.Header, _ string) (openAIWSClientConn, int, http.Header, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return d.inner.Dial(ctx, d.endpoint, headers, "")
}
