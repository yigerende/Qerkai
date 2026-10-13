package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/sjson"
)

func freshConnectionErrorEvent(event string, status ...int) []byte {
	errorValue := map[string]any{"code": "server_error", "message": openAIWSProcessingErrorMessage}
	if len(status) > 0 && status[0] > 0 {
		errorValue["status_code"] = status[0]
	}
	value := map[string]any{"type": event, "error": errorValue}
	if event == "response.failed" {
		value = map[string]any{"type": event, "response": map[string]any{"id": "failed", "status": "failed", "error": errorValue}}
	}
	raw, _ := json.Marshal(value)
	return raw
}

func TestOpenAIWSFreshConnectionRetryMatchesOnlyProcessingError(t *testing.T) {
	for _, event := range []string{"error", "response.failed"} {
		payload := string(freshConnectionErrorEvent(event))
		require.True(t, isOpenAIWSFreshConnectionRetryError([]byte(payload)))
		require.True(t, isOpenAIWSFreshConnectionRetryError([]byte(strings.ReplaceAll(payload, openAIWSProcessingErrorMessage, openAIWSProcessingErrorMessage+" Please include the request ID req_123 in your message."))))
		errorPath := "error."
		if event == "response.failed" {
			errorPath = "response.error."
		}
		for _, path := range []string{errorPath + "status_code", errorPath + "status", "status_code", "status"} {
			for _, status := range []int{401, 429, 500, 502, 503, 504} {
				withStatus, err := sjson.SetBytes([]byte(payload), path, status)
				require.NoError(t, err)
				require.Equal(t, status == 502 || status == 503, isOpenAIWSFreshConnectionRetryError(withStatus), "%s: %s=%d", event, path, status)
			}
		}
		for _, bad := range []string{
			strings.ReplaceAll(payload, "server_error", "rate_limit_exceeded"),
			strings.ReplaceAll(payload, openAIWSProcessingErrorMessage, "Internal server error"),
			strings.ReplaceAll(payload, openAIWSProcessingErrorMessage, "Our servers are currently overloaded. Please try again later."),
			strings.ReplaceAll(payload, `"code":`, `"status_code":429,"code":`),
			strings.ReplaceAll(payload, `"code":`, `"status_code":504,"code":`),
			strings.ReplaceAll(payload, `"type":"`+event+`"`, `"type":"response.completed"`),
		} {
			require.False(t, isOpenAIWSFreshConnectionRetryError([]byte(bad)), bad)
		}
	}
}

func setFreshConnectionRetryForTest(t *testing.T, enabled bool, count int) {
	t.Helper()
	previous := openAIWSFreshConnectionRetryCache.Load()
	refreshOpenAIWSFreshConnectionRetry(&SystemSettings{OpenAIWSFreshConnectionRetryEnabled: enabled, OpenAIWSFreshConnectionRetryMax: count})
	t.Cleanup(func() { openAIWSFreshConnectionRetryCache.Store(previous) })
}

type freshConnectionTestDialer struct {
	mu      sync.Mutex
	conns   []*openAIWSCaptureConn
	headers []http.Header
	events  func(int) [][]byte
	standby func(http.Header)
}

func (d *freshConnectionTestDialer) Dial(_ context.Context, _ string, headers http.Header, _ string) (openAIWSClientConn, int, http.Header, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	conn := &openAIWSCaptureConn{events: d.events(len(d.conns))}
	d.conns = append(d.conns, conn)
	d.headers = append(d.headers, headers.Clone())
	if len(d.conns) == 1 && d.standby != nil {
		d.standby(headers)
	}
	return conn, http.StatusSwitchingProtocols, nil, nil
}

func freshConnectionTestGateway(t *testing.T, dialer openAIWSClientDialer, optimized bool) (*OpenAIGatewayService, *Account) {
	t.Helper()
	t.Cleanup(setForceUpstreamWSForTest(true))
	setForceWSGroupsForTest(t, nil)
	t.Cleanup(setOpenAIUpstream5xxRetryForTest(OpenAIUpstream5xxRetryConfig{}))
	previous := openAIWSPoolOptimizationCache.Load()
	refreshOpenAIWSPoolOptimizationSettings(&SystemSettings{OpenAIWSPoolOptimizationEnabled: optimized})
	t.Cleanup(func() { openAIWSPoolOptimizationCache.Store(previous) })
	cfg := newOpenAIWSV2TestConfig()
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 128
	cfg.Gateway.OpenAIWS.MinIdlePerAccount = 0
	cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 128
	cfg.Gateway.OpenAIWS.RetryBackoffInitialMS = 1
	cfg.Gateway.OpenAIWS.RetryBackoffMaxMS = 1
	pool := newOpenAIWSConnPool(cfg)
	t.Cleanup(pool.Close)
	pool.setClientDialerForTest(dialer)
	svc := &OpenAIGatewayService{
		cfg: cfg, httpUpstream: &httpUpstreamRecorder{}, cache: &stubGatewayCache{},
		openaiWSResolver: newForceUpstreamWSProtocolResolver(NewOpenAIWSProtocolResolver(cfg)),
		toolCorrector:    NewCodexToolCorrector(), openaiWSPool: pool,
	}
	account := &Account{ID: 95801, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Status: StatusActive, Schedulable: true, Concurrency: 128,
		Credentials: map[string]any{"access_token": "test-oauth-token", "chatgpt_account_id": "account-a"},
	}
	return svc, account
}

func freshConnectionTestContext() (*gin.Context, *httptest.ResponseRecorder) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Request.Header.Set("User-Agent", "codex_cli_rs/1.0")
	return c, rec
}

func freshConnectionCreated(i int) []byte {
	return []byte(fmt.Sprintf(`{"type":"response.created","sequence_number":0,"response":{"id":"resp_%d","status":"in_progress","output":[]}}`, i))
}

func freshConnectionCompleted(i int) []byte {
	return []byte(fmt.Sprintf(`{"type":"response.completed","sequence_number":2,"response":{"id":"resp_%d","status":"completed","model":"gpt-6.1-sol","output":[],"usage":{"input_tokens":12,"output_tokens":3,"input_tokens_details":{"cached_tokens":8}}}}`, i))
}

func TestOpenAIWSFreshConnectionRetryForward(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, failure := range []struct {
		event  string
		status int
	}{
		{"error", 0}, {"error", 502}, {"error", 503},
		{"response.failed", 0}, {"response.failed", 502}, {"response.failed", 503},
	} {
		event, status := failure.event, failure.status
		for _, mode := range []string{OpenAITTFTModeNetwork, OpenAITTFTModeSemantic, "nonstream"} {
			for _, retries := range []int{1, 4} {
				t.Run(fmt.Sprintf("%s/%d/%s/%d", event, status, mode, retries), func(t *testing.T) {
					setFreshConnectionRetryForTest(t, true, retries)
					defer setTTFTModeForTest(t, mode)()
					dialer := &freshConnectionTestDialer{events: func(i int) [][]byte {
						if i < retries {
							return [][]byte{freshConnectionCreated(i), freshConnectionErrorEvent(event, status)}
						}
						return [][]byte{freshConnectionCreated(i), []byte(`{"type":"response.output_text.delta","sequence_number":1,"response_id":"resp_final","delta":"recovered"}`), freshConnectionCompleted(i)}
					}}
					svc, account := freshConnectionTestGateway(t, dialer, true)
					c, rec := freshConnectionTestContext()
					body := []byte(fmt.Sprintf(`{"model":"gpt-6.1-sol","stream":%t,"store":false,"prompt_cache_key":"session-a","instructions":"Write code","input":[{"type":"message","role":"user","content":"implement sum"}],"tools":[{"type":"function","name":"exec","parameters":{"type":"object"}}],"reasoning":{"effort":"high"}}`, mode != "nonstream"))
					original := append([]byte(nil), body...)
					// A compatible, idle standby is present after the initial dial.
					// The retry must skip it, even with pool optimization enabled.
					standbyClient := &openAIWSCaptureConn{events: [][]byte{freshConnectionCompleted(99)}}
					dialer.standby = func(headers http.Header) {
						conn := newOpenAIWSConn("standby", account.ID, standbyClient, nil)
						conn.poolRole = openAIWSConnRoleStandby
						conn.handshakeCompatibility = normalizeOpenAIWSOptimizedHandshakeCompatibility(account, headers)
						conn.routingAffinity = normalizeOpenAIWSRoutingAffinity(headers)
						ap := svc.openaiWSPool.getOrCreateAccountPool(account.ID)
						ap.mu.Lock()
						ap.conns[conn.id] = conn
						ap.mu.Unlock()
					}
					result, err := svc.Forward(context.Background(), c, account, body)
					require.NoError(t, err)
					require.NotNil(t, result)
					require.True(t, result.OpenAIWSMode)
					require.Len(t, dialer.conns, retries+1)
					require.Empty(t, standbyClient.writes)
					require.Equal(t, original, body)
					require.True(t, account.Schedulable)
					require.Equal(t, StatusActive, account.Status)
					require.NotContains(t, rec.Body.String(), "server_error")
					for i, conn := range dialer.conns {
						require.Len(t, conn.writes, 1)
						require.Equal(t, dialer.conns[0].lastWrite, conn.lastWrite, "retry must preserve input, tools, reasoning and cache key")
						require.Equal(t, dialer.headers[0].Get("Authorization"), dialer.headers[i].Get("Authorization"))
						if i < retries {
							require.True(t, conn.closed)
						}
					}
					require.Equal(t, 12, result.Usage.InputTokens)
					require.Equal(t, 3, result.Usage.OutputTokens)
					SnapshotOpenAIUpstream5xxUsageRetries(c, result, false)
					require.Zero(t, result.OpenAIUpstream5xxRetryCount)
					require.Nil(t, existingOpenAIUpstream5xxRetryTracker(c))
					if mode == OpenAITTFTModeNetwork {
						require.Equal(t, 1, strings.Count(rec.Body.String(), `"type":"response.created"`))
						require.NotContains(t, rec.Body.String(), `"response_id":"resp_final"`)
						require.Contains(t, rec.Body.String(), `"response_id":"resp_0"`)
					}
				})
			}
		}
	}
}

func TestOpenAIWSFreshConnectionRetryStopsAtLimitAndOutput(t *testing.T) {
	for _, output := range []string{"", `{"type":"response.output_text.delta","delta":"answer"}`, `{"type":"response.output_item.added","item":{"type":"function_call","name":"exec","arguments":""}}`} {
		t.Run(output, func(t *testing.T) {
			setFreshConnectionRetryForTest(t, true, 1)
			defer setTTFTModeForTest(t, OpenAITTFTModeNetwork)()
			dialer := &freshConnectionTestDialer{events: func(i int) [][]byte {
				frames := [][]byte{freshConnectionCreated(i)}
				if output != "" {
					frames = append(frames, []byte(output))
				}
				return append(frames, freshConnectionErrorEvent("error"))
			}}
			svc, account := freshConnectionTestGateway(t, dialer, false)
			// Even with the old 502/503 switch enabled, this error owns one budget.
			defer setOpenAIUpstream5xxRetryForTest(testOpenAIUpstream5xxRetryConfig())()
			c, rec := freshConnectionTestContext()
			_, err := svc.Forward(context.Background(), c, account, []byte(`{"model":"gpt-6.1-sol","stream":true,"input":"hello"}`))
			require.Error(t, err)
			want := 2
			if output != "" {
				want = 1
			}
			require.Len(t, dialer.conns, want)
			require.Contains(t, rec.Body.String(), "server_error")
			require.Equal(t, 1, strings.Count(rec.Body.String(), `"type":"error"`))
			require.Zero(t, OpenAIUpstream5xxRetryCount(c))
			if output == "" {
				before := rec.Body.String()
				require.True(t, WriteOpenAIWSBusinessRetryFailure(c, 502, "server_error", "server_error", openAIWSProcessingErrorMessage, true))
				require.Equal(t, before, rec.Body.String(), "handler must not append a second terminal error")
			}
		})
	}
}

func TestOpenAIWSFreshConnectionRetryScopeAndCancellation(t *testing.T) {
	setFreshConnectionRetryForTest(t, true, 1)
	svc, account := freshConnectionTestGateway(t, &freshConnectionTestDialer{}, false)
	setForceWSGroupsForTest(t, []int64{11})
	for _, group := range []int64{11, 22} {
		c, _ := freshConnectionTestContext()
		c.Set("api_key", &APIKey{GroupID: &group})
		state := svc.openAIWSBusinessRetryState(c, account, true, time.Now())
		require.Equal(t, group == 11, state != nil)
	}
	other := *account
	other.Type = AccountTypeAPIKey
	group := int64(11)
	otherContext, _ := freshConnectionTestContext()
	otherContext.Set("api_key", &APIKey{GroupID: &group})
	require.Nil(t, svc.openAIWSBusinessRetryState(otherContext, &other, true, time.Now()))
	c, _ := freshConnectionTestContext()
	ctx, cancel := context.WithCancel(c.Request.Context())
	c.Request = c.Request.WithContext(ctx)
	cancel()
	state := &openAIWSBusinessRetryState{freshConnectionRetryMax: 1}
	require.False(t, state.beginFreshConnectionRetry(context.Background(), c))
	require.Zero(t, state.freshConnectionRetries)
	t.Cleanup(setForceUpstreamWSForTest(false))
	require.Nil(t, svc.openAIWSBusinessRetryState(c, account, true, time.Now()))
}

func TestOpenAIWSFreshConnectionRetryDisabledAndOtherErrorsUnchanged(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		setFreshConnectionRetryForTest(t, enabled, 1)
		defer setTTFTModeForTest(t, OpenAITTFTModeNetwork)()
		payload := freshConnectionErrorEvent("error")
		if enabled {
			payload = []byte(`{"type":"error","error":{"code":"server_error","message":"Different server error"}}`)
		}
		dialer := &freshConnectionTestDialer{events: func(i int) [][]byte { return [][]byte{freshConnectionCreated(i), payload} }}
		svc, account := freshConnectionTestGateway(t, dialer, false)
		c, rec := freshConnectionTestContext()
		_, err := svc.Forward(context.Background(), c, account, []byte(`{"model":"gpt-6.1-sol","stream":true,"input":"hello"}`))
		require.Error(t, err)
		require.Len(t, dialer.conns, 1)
		require.Contains(t, rec.Body.String(), string(payload))
		if !enabled {
			require.Nil(t, openAIWSBusinessRetryStateFrom(c))
		}
	}
}

func TestOpenAIWSFreshConnectionRetryDefaults(t *testing.T) {
	value := parseOpenAIWSFreshConnectionRetry(nil)
	require.False(t, value.enabled)
	require.Equal(t, 1, value.max)
	for _, invalid := range []string{"-1", "0", "101", "x"} {
		require.Equal(t, 1, parseOpenAIWSFreshConnectionRetry(map[string]string{SettingKeyOpenAIWSFreshConnectionRetryMax: invalid}).max)
	}
}

type freshConnectionSettingsRepo struct {
	SettingRepository
	get func(context.Context, []string) (map[string]string, error)
}

func (r *freshConnectionSettingsRepo) GetMultiple(ctx context.Context, keys []string) (map[string]string, error) {
	return r.get(ctx, keys)
}

func TestOpenAIWSFreshConnectionRetrySaveWinsOverStaleRead(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	previousService := forceUpstreamWSSettings.Load()
	previousCache := openAIWSFreshConnectionRetryCache.Load()
	t.Cleanup(func() {
		forceUpstreamWSSettings.Store(previousService)
		openAIWSFreshConnectionRetryCache.Store(previousCache)
	})
	forceUpstreamWSSettings.Store(&SettingService{settingRepo: &freshConnectionSettingsRepo{get: func(context.Context, []string) (map[string]string, error) {
		close(started)
		<-release
		return map[string]string{SettingKeyOpenAIWSFreshConnectionRetryEnabled: "true", SettingKeyOpenAIWSFreshConnectionRetryMax: "4"}, nil
	}}})
	openAIWSFreshConnectionRetryCache.Store(nil)
	result := make(chan openAIWSFreshConnectionRetryConfig, 1)
	go func() { result <- currentOpenAIWSFreshConnectionRetry() }()
	<-started
	refreshOpenAIWSFreshConnectionRetry(&SystemSettings{OpenAIWSFreshConnectionRetryMax: 1})
	close(release)
	require.False(t, (<-result).enabled)
	require.False(t, currentOpenAIWSFreshConnectionRetry().enabled)
}

func TestOpenAIWSFreshConnectionRetryKeepsTransportRecoveryPayload(t *testing.T) {
	setFreshConnectionRetryForTest(t, true, 1)
	defer setTTFTModeForTest(t, OpenAITTFTModeNetwork)()
	dialer := &freshConnectionTestDialer{events: func(i int) [][]byte {
		switch i {
		case 0: // Transport read fails before any output, using the original reconnect logic.
			return nil
		case 1:
			return [][]byte{freshConnectionCreated(i), freshConnectionErrorEvent("error")}
		case 2: // A transport failure also occurs while recovering the processing error.
			return nil
		default:
			return [][]byte{freshConnectionCreated(i), freshConnectionCompleted(i)}
		}
	}}
	svc, account := freshConnectionTestGateway(t, dialer, false)
	c, rec := freshConnectionTestContext()
	result, err := svc.Forward(context.Background(), c, account, []byte(`{"model":"gpt-6.1-sol","stream":true,"input":"hello","include":["reasoning.encrypted_content"],"tools":[{"type":"function","name":"exec","parameters":{"type":"object"}}]}`))
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, dialer.conns, 4)
	require.Contains(t, dialer.conns[0].lastWrite, "include")
	require.NotContains(t, dialer.conns[1].lastWrite, "include", "original transport recovery trims optional include")
	require.Equal(t, dialer.conns[1].lastWrite, dialer.conns[2].lastWrite)
	require.Equal(t, dialer.conns[1].lastWrite, dialer.conns[3].lastWrite)
	require.Contains(t, rec.Body.String(), "response.completed")
	require.NotContains(t, rec.Body.String(), "server_error")
	require.Equal(t, 1, openAIWSBusinessRetryStateFrom(c).freshConnectionRetries)
}
