package service

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type keeperCaptureWSDialer struct {
	mu      sync.Mutex
	headers []http.Header
}

func (d *keeperCaptureWSDialer) Dial(_ context.Context, _ string, h http.Header, _ string) (openAIWSClientConn, int, http.Header, error) {
	d.mu.Lock()
	d.headers = append(d.headers, h.Clone())
	d.mu.Unlock()
	return &openAIWSFakeConn{}, 101, http.Header{"X-Codex-Turn-State": {h.Get(openAICodexTurnStateHeader)}}, nil
}

func TestStateKeeperWSStateRotationKeepsActiveLeaseAndBaselineIsolated(t *testing.T) {
	for _, optimized := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy-pool", true: "optimized-pool"}[optimized], func(t *testing.T) {
			cfg := &config.Config{}
			cfg.Gateway.OpenAIWS.MaxConnsPerAccount, cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 8, 8
			pool := newOpenAIWSConnPool(cfg)
			defer pool.Close()
			dialer := &keeperCaptureWSDialer{}
			pool.setClientDialerForTest(dialer)
			a := keeperTestAccount(951)
			a.Concurrency = 8
			req := openAIWSAcquireRequest{Account: a, WSURL: "wss://example.test/responses", Headers: http.Header{"X-Codex-Turn-State": {"state-one"}}, StateKeeperVersion: "v1", PoolOptimized: optimized, SessionHash: "session"}
			first, err := pool.Acquire(context.Background(), req)
			require.NoError(t, err)
			require.Equal(t, "state-one", first.HandshakeHeader(openAICodexTurnStateHeader))
			req.StateKeeperVersion = "v2"
			req.Headers = http.Header{"X-Codex-Turn-State": {"state-two"}}
			req.PreferredConnID, req.ForcePreferredConn = first.ConnID(), true
			second, err := pool.Acquire(context.Background(), req)
			require.NoError(t, err)
			require.NotEqual(t, first.ConnID(), second.ConnID())
			require.Equal(t, "state-two", second.HandshakeHeader(openAICodexTurnStateHeader))
			select {
			case <-first.conn.closedCh:
				t.Fatal("state rotation closed an active request")
			default:
			}
			require.NoError(t, first.WriteJSON(map[string]any{"input": "existing request"}, 0))
			second.Release()
			third, err := pool.Acquire(context.Background(), req)
			require.NoError(t, err)
			require.Equal(t, second.ConnID(), third.ConnID(), "compatible state should still use normal pool reuse")
			third.Release()
			req.StateKeeperVersion, req.Headers = "", http.Header{"X-Codex-Turn-State": {"native-state"}}
			req.ForcePreferredConn, req.PreferredConnID = false, ""
			baseline, err := pool.Acquire(context.Background(), req)
			require.NoError(t, err)
			require.NotEqual(t, first.ConnID(), baseline.ConnID())
			require.NotEqual(t, second.ConnID(), baseline.ConnID(), "disabled injection cannot reuse a keeper-injected handshake")
			require.Equal(t, "native-state", baseline.HandshakeHeader(openAICodexTurnStateHeader))
			baseline.Release()
			first.Release()
		})
	}
}

func TestStateKeeperWSPrewarmVersionCompatibility(t *testing.T) {
	req := openAIWSAcquireRequest{Account: keeperTestAccount(1), WSURL: "wss://example.test", StateKeeperVersion: "one"}
	other := cloneOpenAIWSAcquireRequest(req)
	require.True(t, sameOpenAIWSPrewarmTarget(req, other))
	other.StateKeeperVersion = "two"
	require.False(t, sameOpenAIWSPrewarmTarget(req, other), "an old prewarm cannot fill the new state's inventory")
}

func TestStateKeeperWSRotationClaimsCurrentPrewarmAsPrimary(t *testing.T) {
	for _, version := range []string{"new-state", ""} {
		t.Run("version="+version, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.Gateway.OpenAIWS.MaxConnsPerAccount, cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 8, 8
			pool := newOpenAIWSConnPool(cfg)
			defer pool.Close()
			account := keeperTestAccount(952)
			ap := pool.getOrCreateAccountPool(account.ID)
			old := newOpenAIWSConn("old-primary", account.ID, &openAIWSFakeConn{}, nil)
			old.ownerSession, old.poolRole, old.ownerUntil = "session", openAIWSConnRoleSessionPrimary, time.Now().Add(time.Hour)
			old.handshakeCompatibility.stateKeeperVersion = "old-state"
			require.True(t, old.tryAcquire())
			defer old.release()
			ap.conns[old.id] = old
			for _, role := range []string{openAIWSConnRolePrewarm, openAIWSConnRoleStandby} {
				conn := newOpenAIWSConn(role, account.ID, &openAIWSFakeConn{}, nil)
				conn.poolRole, conn.handshakeCompatibility.stateKeeperVersion = role, version
				ap.conns[conn.id] = conn
			}
			lease, err := pool.Acquire(context.Background(), openAIWSAcquireRequest{Account: account, WSURL: "wss://example.test", PoolOptimized: true, SessionHash: "session", StateKeeperVersion: version})
			require.NoError(t, err)
			defer lease.Release()
			require.Equal(t, openAIWSConnRolePrewarm, lease.ConnID(), "old State ownership must not skip current prewarm inventory")
			require.Equal(t, openAIWSConnRoleSessionPrimary, lease.conn.poolRole)
			require.True(t, old.isLeased(), "the old active request must remain untouched")
		})
	}
}

func TestStateKeeperForcedWSForwardingObservesFreshMetadataWithoutChangingOutput(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			s, gateway, account := keeperTestService(t)
			q := s.config.Load().OpenAIStateKeeperSettings
			q.InjectionEnabled, q.DegradedStateLengths = enabled, []int{356}
			require.NoError(t, s.Save(context.Background(), q))
			cfg := &config.Config{}
			cfg.Gateway.OpenAIWS.Enabled, cfg.Gateway.OpenAIWS.OAuthEnabled, cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true, true, true
			cfg.Gateway.OpenAIWS.MaxConnsPerAccount, cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 2, 2
			cfg.Gateway.OpenAIWS.ReadTimeoutSeconds, cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 5, 5
			capture := &openAIWSCaptureConn{events: [][]byte{
				[]byte(`{"type":"response.created","response":{"id":"resp_keeper","status":"in_progress"}}`),
				[]byte(fmt.Sprintf(`{"type":"codex.response.metadata","headers":{"x-codex-turn-state":%q}}`, strings.Repeat("d", 356))),
				[]byte(`{"type":"response.output_text.delta","delta":"original answer"}`),
				[]byte(`{"type":"response.completed","response":{"id":"resp_keeper","status":"completed","output":[],"usage":{"input_tokens":2,"output_tokens":3}}}`),
			}}
			dialer := &openAIWSCaptureDialer{conn: capture, handshake: http.Header{"X-Codex-Turn-State": {"fresh-handshake"}}}
			pool := newOpenAIWSConnPool(cfg)
			t.Cleanup(pool.Close)
			pool.setClientDialerForTest(dialer)
			gateway.cfg, gateway.cache, gateway.openaiWSPool = cfg, &stubGatewayCache{}, pool
			gateway.toolCorrector, gateway.openaiWSResolver = NewCodexToolCorrector(), NewOpenAIWSProtocolResolver(cfg)
			gateway.httpUpstream = &keeperHTTPStub{do: func(*http.Request, string, int64, int) (*http.Response, error) {
				t.Fatal("unexpected HTTP fallback")
				return nil, nil
			}}
			account.Extra = map[string]any{"responses_websockets_v2_enabled": true}
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			c.Request.Header.Set(openAICodexTurnStateHeader, "native-state")
			group := int64(11)
			c.Set("api_key", &APIKey{GroupID: &group})
			result, err := gateway.Forward(context.Background(), c, account, []byte(fmt.Sprintf(`{"model":%q,"stream":true,"input":[{"role":"user","content":"original prompt"}]}`, q.Model)))
			require.NoError(t, err)
			require.True(t, result.OpenAIWSMode)
			require.Contains(t, recorder.Body.String(), "original answer")
			require.Contains(t, recorder.Body.String(), "response.created")
			require.EqualValues(t, 2, result.Usage.InputTokens)
			require.EqualValues(t, 3, result.Usage.OutputTokens)
			want := "native-state"
			if enabled {
				want = "collected-secret"
			}
			require.Equal(t, want, dialer.lastHeaders.Get(openAICodexTurnStateHeader))
			require.NotContains(t, fmt.Sprint(capture.lastWrite), "current_turn_state")
			keeperDrainObservations(s)
			require.Equal(t, enabled, len(s.queue) == 1)
			if enabled {
				require.Equal(t, "degraded_signal", s.Recent([]int64{1})[0].Injections[0].Result)
			} else {
				require.Empty(t, s.observations)
			}
		})
	}
}
