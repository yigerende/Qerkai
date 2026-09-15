package service

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type openAIUsageRetryDialer struct {
	attempts atomic.Int32
	conn     *openAIWSCaptureConn
}

func (d *openAIUsageRetryDialer) Dial(context.Context, string, http.Header, string) (openAIWSClientConn, int, http.Header, error) {
	switch d.attempts.Add(1) {
	case 1:
		return nil, http.StatusBadGateway, nil, errors.New("bad gateway")
	case 2:
		return nil, http.StatusServiceUnavailable, nil, errors.New("service unavailable")
	default:
		return d.conn, http.StatusSwitchingProtocols, nil, nil
	}
}

func TestOpenAIUpstream5xxUsageRetriesExcludeWSHandshakeReconnects(t *testing.T) {
	restore := setOpenAIUpstream5xxRetryForTest(testOpenAIUpstream5xxRetryConfig())
	defer restore()
	c := newOpenAIUpstream5xxRetryTestContext(t)
	c.Request.Header.Set("User-Agent", "unit-test-agent/1.0")
	cfg := newOpenAIWSV2TestConfig()
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
	cfg.Gateway.OpenAIWS.MinIdlePerAccount = 0
	cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1
	cfg.Gateway.OpenAIWS.RetryBackoffInitialMS = 1
	cfg.Gateway.OpenAIWS.RetryBackoffMaxMS = 1
	dialer := &openAIUsageRetryDialer{conn: &openAIWSCaptureConn{events: [][]byte{
		[]byte(`{"type":"response.completed","response":{"id":"resp_retry","model":"gpt-5.1","status":"completed","usage":{"input_tokens":3,"output_tokens":2}}}`),
	}}}
	pool := newOpenAIWSConnPool(cfg)
	t.Cleanup(pool.Close)
	pool.setClientDialerForTest(dialer)
	svc := &OpenAIGatewayService{
		cfg:              cfg,
		httpUpstream:     &httpUpstreamRecorder{},
		cache:            &stubGatewayCache{},
		openaiWSResolver: NewOpenAIWSProtocolResolver(cfg),
		toolCorrector:    NewCodexToolCorrector(),
		openaiWSPool:     pool,
	}
	account := testOpenAIUpstream5xxOAuthAccount()
	account.Status, account.Schedulable, account.Concurrency = StatusActive, true, 1
	account.Credentials = map[string]any{"access_token": "test-oauth-token"}
	account.Extra = map[string]any{"responses_websockets_v2_enabled": true}
	ArmOpenAIUpstream5xxUsageRetry(c, account, &UpstreamFailoverError{StatusCode: http.StatusBadGateway})
	BeginOpenAIUpstream5xxUsageAttempt(c)
	result, err := svc.Forward(context.Background(), c, account, []byte(`{"model":"gpt-5.1","stream":false,"input":[{"type":"input_text","text":"hello"}]}`))
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, result.OpenAIWSMode)
	require.EqualValues(t, 3, dialer.attempts.Load())
	SnapshotOpenAIUpstream5xxUsageRetries(c, result, false)
	require.Zero(t, result.OpenAIUpstream5xxRetryCount, "legacy handler and internal handshake retries are not owned by this switch")
}

func TestOpenAIUpstream5xxUsageRetriesCountStartedAttempts(t *testing.T) {
	restore := setOpenAIUpstream5xxRetryForTest(testOpenAIUpstream5xxRetryConfig())
	defer restore()
	c := newOpenAIUpstream5xxRetryTestContext(t)
	account := testOpenAIUpstream5xxOAuthAccount()
	result := &OpenAIForwardResult{}
	BeginOpenAIUpstream5xxUsageAttempt(c)
	SnapshotOpenAIUpstream5xxUsageRetries(c, result, false)
	require.Zero(t, result.OpenAIUpstream5xxRetryCount, "initial attempt is not a retry")

	ArmOpenAIUpstream5xxUsageRetry(c, account, businessRetryTestFailure(http.StatusBadGateway))
	SnapshotOpenAIUpstream5xxUsageRetries(c, result, false)
	require.Zero(t, result.OpenAIUpstream5xxRetryCount, "pending wait or account selection is not a retry")
	BeginOpenAIUpstream5xxUsageAttempt(c)
	BeginOpenAIUpstream5xxUsageAttempt(c)
	SnapshotOpenAIUpstream5xxUsageRetries(c, result, false)
	require.Equal(t, 1, result.OpenAIUpstream5xxRetryCount)

	ArmOpenAIUpstream5xxUsageRetry(c, account, businessRetryTestFailure(http.StatusServiceUnavailable))
	BeginOpenAIUpstream5xxUsageAttempt(c)
	SnapshotOpenAIUpstream5xxUsageRetries(c, result, false)
	require.Equal(t, 2, result.OpenAIUpstream5xxRetryCount, "account switches and same-account retries accumulate")

	ArmOpenAIUpstream5xxUsageRetry(c, account, &UpstreamFailoverError{StatusCode: http.StatusTooManyRequests})
	BeginOpenAIUpstream5xxUsageAttempt(c)
	SnapshotOpenAIUpstream5xxUsageRetries(c, result, false)
	require.Equal(t, 2, result.OpenAIUpstream5xxRetryCount, "429 must not inflate the 502/503 count")
}

func TestOpenAIUpstream5xxUsageRetriesWSTurns(t *testing.T) {
	restore := setOpenAIUpstream5xxRetryForTest(testOpenAIUpstream5xxRetryConfig())
	defer restore()
	c := newOpenAIUpstream5xxRetryTestContext(t)
	account := testOpenAIUpstream5xxOAuthAccount()
	failure := businessRetryTestFailure(http.StatusBadGateway)
	ArmOpenAIUpstream5xxUsageRetry(c, account, failure)
	BeginOpenAIUpstream5xxUsageAttempt(c)
	SnapshotOpenAIUpstream5xxUsageRetries(c, nil, false)
	ArmOpenAIUpstream5xxUsageRetry(c, account, failure)
	BeginOpenAIUpstream5xxUsageAttempt(c)

	completed := &OpenAIForwardResult{}
	SnapshotOpenAIUpstream5xxUsageRetries(c, completed, true)
	require.Equal(t, 2, completed.OpenAIUpstream5xxRetryCount, "hidden failover retains the logical turn count")
	BeginOpenAIUpstream5xxUsageAttempt(c)
	nextTurn := &OpenAIForwardResult{}
	SnapshotOpenAIUpstream5xxUsageRetries(c, nextTurn, false)
	require.Zero(t, nextTurn.OpenAIUpstream5xxRetryCount)

	ArmOpenAIUpstream5xxUsageRetry(c, account, failure)
	SnapshotOpenAIUpstream5xxUsageRetries(c, nil, true)
	BeginOpenAIUpstream5xxUsageAttempt(c)
	SnapshotOpenAIUpstream5xxUsageRetries(c, nextTurn, true)
	require.Zero(t, nextTurn.OpenAIUpstream5xxRetryCount, "finished turns discard pending retries")
	require.Equal(t, 2, completed.OpenAIUpstream5xxRetryCount, "async billing snapshot is immutable across turns")
}

func TestOpenAIUpstream5xxUsageRetriesScope(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		config := testOpenAIUpstream5xxRetryConfig()
		config.Enabled = enabled
		restore := setOpenAIUpstream5xxRetryForTest(config)
		for _, status := range []int{429, 500, 502, 503, 504} {
			c := newOpenAIUpstream5xxRetryTestContext(t)
			ArmOpenAIUpstream5xxUsageRetry(c, testOpenAIUpstream5xxOAuthAccount(), &UpstreamFailoverError{StatusCode: status})
			BeginOpenAIUpstream5xxUsageAttempt(c)
			result := &OpenAIForwardResult{}
			SnapshotOpenAIUpstream5xxUsageRetries(c, result, false)
			require.Zero(t, result.OpenAIUpstream5xxRetryCount, "unowned retries never count, regardless of status or settings")
			if status == 502 || status == 503 {
				ArmOpenAIUpstream5xxUsageRetry(c, testOpenAIUpstream5xxOAuthAccount(), businessRetryTestFailure(status))
				BeginOpenAIUpstream5xxUsageAttempt(c)
				SnapshotOpenAIUpstream5xxUsageRetries(c, result, false)
				want := 0
				if enabled {
					want = 1
				}
				require.Equal(t, want, result.OpenAIUpstream5xxRetryCount)
			}
		}
		restore()
	}
	ArmOpenAIUpstream5xxUsageRetry(nil, nil, nil)
	BeginOpenAIUpstream5xxUsageAttempt(nil)
	SnapshotOpenAIUpstream5xxUsageRetries(nil, nil, true)
}

func TestOpenAIUpstream5xxUsageRetriesRecorded(t *testing.T) {
	for _, count := range []int{0, 3} {
		usageRepo := &openAIRecordUsageLogRepoStub{inserted: true}
		svc := newOpenAIRecordUsageServiceForTest(usageRepo, &openAIRecordUsageUserRepoStub{}, &openAIRecordUsageSubRepoStub{}, nil)
		svc.RecordCyberPolicyUsageLog(context.Background(), CyberPolicyUsageInput{
			APIKey:                      &APIKey{ID: 2, User: &User{ID: 1}},
			Account:                     &Account{ID: 3},
			RequestID:                   "retry-usage",
			Model:                       "gpt-5.1",
			InputTokens:                 10,
			OpenAIUpstream5xxRetryCount: count,
		})
		require.Equal(t, 1, usageRepo.calls)
		require.NotNil(t, usageRepo.lastLog.OpenAIUpstream5xxRetryCount)
		require.Equal(t, count, *usageRepo.lastLog.OpenAIUpstream5xxRetryCount)
	}
}
