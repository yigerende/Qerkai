package service

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenAI5xxForceNewConnectionGates(t *testing.T) {
	t.Cleanup(setForceUpstreamWSForTest(true))
	setForceWSGroupsForTest(t, []int64{11})
	cfg := OpenAIUpstream5xxRetryConfig{Enabled: true, ForceNewConnection: true, SameAccount: 1, Total: 3}
	t.Cleanup(setOpenAIUpstream5xxRetryForTest(cfg))
	c, _ := freshConnectionTestContext()
	id := int64(11)
	c.Set("api_key", &APIKey{GroupID: &id})
	state := &openAIWSBusinessRetryState{config: cfg}
	c.Set(openAIWSBusinessRetryKey, state)
	account := testOpenAIUpstream5xxOAuthAccount()
	require.False(t, state.forceNewConnectionForBusinessRetry(c), "initial attempt")
	retry, same, _, _ := PrepareOpenAIUpstream5xxBusinessRetry(c, account, businessRetryTestFailure(502), "gpt-6-astra")
	require.True(t, retry)
	require.True(t, same)
	require.False(t, state.forceNewConnectionForBusinessRetry(c), "waiting does not start retry")
	BeginOpenAIUpstream5xxUsageAttempt(c)
	require.True(t, state.forceNewConnectionForBusinessRetry(c))
	BeginOpenAIUpstream5xxUsageAttempt(c)
	require.True(t, state.forceNewConnectionForBusinessRetry(c), "repeated begin preserves the pending connection policy")
	require.Equal(t, 1, OpenAIUpstream5xxRetryCount(c))
	id = 22
	require.False(t, state.forceNewConnectionForBusinessRetry(c), "unselected group")
	id = 11
	off := false
	forceUpstreamWSOverride.Store(&off)
	require.False(t, state.forceNewConnectionForBusinessRetry(c), "forced WS disabled during request")
	on := true
	forceUpstreamWSOverride.Store(&on)
	disabled := cfg
	disabled.ForceNewConnection = false
	openAIUpstream5xxRetryOverride.Store(&disabled)
	require.False(t, state.forceNewConnectionForBusinessRetry(c), "option disabled during request")
	disabled = cfg
	disabled.Enabled = false
	openAIUpstream5xxRetryOverride.Store(&disabled)
	require.False(t, state.forceNewConnectionForBusinessRetry(c), "business retry disabled")
	openAIUpstream5xxRetryOverride.Store(&cfg)
	state.outputStarted = true
	require.False(t, state.forceNewConnectionForBusinessRetry(c))
	state.outputStarted = false
	ctx, cancel := context.WithCancel(c.Request.Context())
	cancel()
	c.Request = c.Request.WithContext(ctx)
	require.False(t, state.forceNewConnectionForBusinessRetry(c))
}

func TestOpenAI5xxForceNewConnectionSkipsStandby(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, status := range []int{502, 503} {
			t.Run(fmt.Sprintf("%t/%d", enabled, status), func(t *testing.T) {
				setFreshConnectionRetryForTest(t, false, 1)
				defer setTTFTModeForTest(t, OpenAITTFTModeNetwork)()
				dialer := &freshConnectionTestDialer{events: func(i int) [][]byte {
					if i == 0 {
						return [][]byte{freshConnectionCreated(i), businessRetryTestPayload(status, "error")}
					}
					return [][]byte{freshConnectionCreated(i), freshConnectionCompleted(i)}
				}}
				svc, account := freshConnectionTestGateway(t, dialer, true)
				cfg := OpenAIUpstream5xxRetryConfig{Enabled: true, ForceNewConnection: enabled, SameAccount: 2, Total: 3}
				t.Cleanup(setOpenAIUpstream5xxRetryForTest(cfg))
				standby := &openAIWSCaptureConn{events: [][]byte{freshConnectionCreated(99), freshConnectionCompleted(99)}}
				dialer.standby = func(headers http.Header) {
					conn := newOpenAIWSConn("standby", account.ID, standby, nil)
					conn.poolRole = openAIWSConnRoleStandby
					conn.handshakeCompatibility = normalizeOpenAIWSOptimizedHandshakeCompatibility(account, headers)
					conn.routingAffinity = normalizeOpenAIWSRoutingAffinity(headers)
					ap := svc.openaiWSPool.getOrCreateAccountPool(account.ID)
					ap.mu.Lock()
					ap.conns[conn.id] = conn
					ap.mu.Unlock()
				}
				c, rec := freshConnectionTestContext()
				body := []byte(`{"model":"gpt-6.1-sol","stream":true,"store":false,"prompt_cache_key":"session-a","instructions":"Write code","input":"implement sum","tools":[{"type":"function","name":"exec","parameters":{"type":"object"}}],"reasoning":{"effort":"high"}}`)
				_, err := svc.Forward(context.Background(), c, account, body)
				var failure *UpstreamFailoverError
				require.ErrorAs(t, err, &failure)
				retry, same, _, reason := PrepareOpenAIUpstream5xxBusinessRetry(c, account, failure, "gpt-6.1-sol")
				require.True(t, retry, reason)
				require.True(t, same)
				BeginOpenAIUpstream5xxUsageAttempt(c)
				result, err := svc.Forward(context.Background(), c, account, body)
				require.NoError(t, err)
				SnapshotOpenAIUpstream5xxUsageRetries(c, result, false)
				require.Equal(t, 1, result.OpenAIUpstream5xxRetryCount)
				require.Equal(t, 12, result.Usage.InputTokens)
				require.Equal(t, 3, result.Usage.OutputTokens)
				require.NotContains(t, rec.Body.String(), "server_error")
				require.False(t, openAIWSBusinessRetryStateFrom(c).businessFreshConnectionPending)
				if enabled {
					require.Len(t, dialer.conns, 2)
					require.Empty(t, standby.writes)
					require.Equal(t, dialer.conns[0].lastWrite, dialer.conns[1].lastWrite)
					require.Equal(t, dialer.headers[0].Get("Authorization"), dialer.headers[1].Get("Authorization"))
				} else {
					require.Len(t, dialer.conns, 1)
					require.Len(t, standby.writes, 1)
				}
			})
		}
	}
}
