package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type keeperSettingsStub struct {
	SettingRepository
	mu    sync.Mutex
	value string
}

func (r *keeperSettingsStub) GetValue(context.Context, string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.value == "" {
		return "", ErrSettingNotFound
	}
	return r.value, nil
}
func (r *keeperSettingsStub) Set(_ context.Context, _ string, v string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.value = v
	return nil
}

type keeperAccountsStub struct {
	AccountRepository
	accounts map[int64]*Account
}

func (r *keeperAccountsStub) GetByID(_ context.Context, id int64) (*Account, error) {
	return r.accounts[id], nil
}
func (r *keeperAccountsStub) GetByIDs(_ context.Context, ids []int64) ([]*Account, error) {
	var out []*Account
	for _, id := range ids {
		if a := r.accounts[id]; a != nil {
			out = append(out, a)
		}
	}
	return out, nil
}

type keeperProxiesStub struct {
	ProxyRepository
	proxy *Proxy
}

func (r *keeperProxiesStub) GetByID(context.Context, int64) (*Proxy, error) { return r.proxy, nil }

type keeperHTTPStub struct {
	HTTPUpstream
	do func(*http.Request, string, int64, int) (*http.Response, error)
}

type keeperWSResolver struct{}

func (keeperWSResolver) Resolve(*Account) OpenAIWSProtocolDecision {
	return OpenAIWSProtocolDecision{Transport: OpenAIUpstreamTransportResponsesWebsocketV2}
}

func (r *keeperHTTPStub) Do(req *http.Request, proxy string, id int64, concurrency int) (*http.Response, error) {
	return r.do(req, proxy, id, concurrency)
}

func keeperTestAccount(id int64) *Account {
	return &Account{ID: id, Name: fmt.Sprintf("test-%d", id), Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Concurrency: 1, Credentials: map[string]any{"access_token": fmt.Sprintf("secret-%d", id), "chatgpt_account_id": fmt.Sprintf("chatgpt-%d", id)}}
}
func keeperTestContext(group int64) *gin.Context {
	c := &gin.Context{Request: httptest.NewRequest(http.MethodPost, "/v1/responses", nil)}
	c.Set("api_key", &APIKey{ID: 1, GroupID: &group})
	return c
}
func keeperTestService(t *testing.T) (*OpenAIStateKeeperService, *OpenAIGatewayService, *Account) {
	t.Helper()
	a := keeperTestAccount(1)
	accounts := &keeperAccountsStub{accounts: map[int64]*Account{1: a}}
	gateway := &OpenAIGatewayService{accountRepo: accounts}
	s := newOpenAIStateKeeper(&keeperSettingsStub{}, accounts, &keeperProxiesStub{proxy: &Proxy{ID: 1, Protocol: "http", Host: "127.0.0.1", Port: 8888, Status: StatusActive}}, gateway)
	gateway.stateKeeper.Store(s)
	t.Cleanup(s.Stop)
	q := DefaultOpenAIStateKeeperSettings()
	q.Enabled = true
	q.InjectionEnabled = true
	q.AutoRefresh = false
	q.AccountIDs = []int64{1}
	q.GroupIDs = []int64{11}
	q.ProxyID = 1
	q.Revision = "test"
	s.install(q)
	s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
		return openAIStateProbeResult{status: 292, value: "collected-secret", result: "collected", message: "collected", credentialStamp: stateKeeperCredentialStamp(a)}
	}
	s.run(openAIStateKeeperJob{1, "test"})
	return s, gateway, a
}

func TestStateKeeperProbeNeverConfusesTurnStateWithCollectedState(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		headers http.Header
		body    string
		want    string
		codex   bool
	}{
		{"actual header", 292, http.Header{"Current_turn_state": {"signed-state"}}, "", "collected", false},
		{"body cannot replace response header", 292, http.Header{}, `{"current_turn_state":"signed-state"}`, "not_observed", false},
		{"ordinary header", 200, http.Header{"X-Codex-Turn-State": {"turn-only"}}, "data: {\"type\":\"response.completed\"}\n", "not_observed", true},
		{"ordinary metadata", 200, http.Header{}, "data: {\"type\":\"response.metadata\",\"headers\":{\"x-codex-turn-state\":\"turn-only\"}}\n", "not_observed", true},
		{"200 is not 292", 200, http.Header{"Current_turn_state": {"signed-state"}}, "", "not_observed", false},
		{"292 without value", 292, http.Header{}, `{}`, "not_observed", false},
		{"invalid header", 292, http.Header{"Current_turn_state": {"bad\r\nvalue"}}, "", "not_observed", false},
		{"failure", 503, http.Header{"Current_turn_state": {"signed-state"}}, `{}`, "upstream_error", false},
		{"stream failure", 200, http.Header{}, "data: {\"type\":\"response.failed\"}\n", "upstream_error", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := parseOpenAIStateProbeResponse(&http.Response{StatusCode: tc.status, Header: tc.headers, Body: io.NopCloser(strings.NewReader(tc.body))})
			require.Equal(t, tc.want, r.result)
			require.Equal(t, tc.codex, r.hasCodexTurnState)
			if tc.want != "collected" {
				require.Empty(t, r.value)
			}
		})
	}
}

func TestStateKeeperInjectionScopeAndOff(t *testing.T) {
	s, gateway, a := keeperTestService(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	for _, tc := range []struct {
		name    string
		group   int64
		account *Account
		model   string
		want    bool
	}{
		{"selected", 11, a, q.Model, true}, {"other group", 12, a, q.Model, false}, {"other account", 11, keeperTestAccount(2), q.Model, false}, {"other model", 11, a, "different-model", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			headers := http.Header{}
			body := []byte(fmt.Sprintf(`{"model":%q,"input":"unchanged"}`, tc.model))
			gateway.injectCollectedStateHTTP(keeperTestContext(tc.group), tc.account, body, headers)
			require.Equal(t, tc.want, headers.Get(openAICollectedStateHeader) != "")
		})
	}
	q.Enabled = false
	q.Revision = "off"
	s.install(q)
	headers := http.Header{"X-Codex-Turn-State": {"original"}}
	gateway.injectCollectedStateHTTP(keeperTestContext(11), a, []byte(fmt.Sprintf(`{"model":%q}`, q.Model)), headers)
	require.Equal(t, http.Header{"X-Codex-Turn-State": {"original"}}, headers)
	payload := map[string]any{"model": q.Model, "input": "original", "client_metadata": map[string]any{"other": "value"}}
	before, _ := json.Marshal(payload)
	gateway.injectCollectedStateWS(keeperTestContext(11), a, payload)
	after, _ := json.Marshal(payload)
	require.JSONEq(t, string(before), string(after))
	require.Error(t, s.Schedule(nil))
}

func TestStateKeeperWSUsesFreshStatePerMessageWithoutMutatingInput(t *testing.T) {
	s, gateway, a := keeperTestService(t)
	model := s.config.Load().Model
	metadata := map[string]any{"x-codex-turn-state": "native-turn", "x-codex-turn-metadata": "native-metadata"}
	for _, value := range []string{"first-state", "refreshed-state"} {
		s.mu.Lock()
		s.rows[1].value = value
		s.mu.Unlock()
		payload := map[string]any{"model": model, "input": []string{"original prompt"}, "client_metadata": metadata, "type": "response.create"}
		gateway.injectCollectedStateWS(keeperTestContext(11), a, payload)
		require.Equal(t, value, payload["client_metadata"].(map[string]any)[openAICollectedStateHeader])
		require.Equal(t, "native-turn", payload["client_metadata"].(map[string]any)[openAICodexTurnStateHeader])
		require.NotContains(t, metadata, openAICollectedStateHeader)
	}
	q := s.config.Load().OpenAIStateKeeperSettings
	q.InjectionEnabled = false
	q.Revision = "no-inject"
	s.install(q)
	payload := map[string]any{"model": model, "client_metadata": metadata}
	gateway.injectCollectedStateWS(keeperTestContext(11), a, payload)
	require.Equal(t, metadata, payload["client_metadata"])
}

func TestStateKeeperExpiryReauthorizationAndProbeIsolation(t *testing.T) {
	s, _, a := keeperTestService(t)
	model := s.config.Load().Model
	c := keeperTestContext(11)
	c.Set(openAIStateProbeContextKey, true)
	require.Empty(t, s.valueFor(c, a, model))
	reauth := *a
	reauth.Credentials = map[string]any{"access_token": "another-owner"}
	require.Empty(t, s.valueFor(keeperTestContext(11), &reauth, model))
	past := time.Now().Add(-time.Second)
	s.rows[1].row.ExpiresAt = &past
	require.Empty(t, s.valueFor(keeperTestContext(11), a, model))
	require.Equal(t, "expired", s.Snapshot().Rows[0].Status)
}

func TestStateKeeperSavePreservesCollectedStateForInjectionToggle(t *testing.T) {
	s, _, a := keeperTestService(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.InjectionEnabled = false
	require.NoError(t, s.Save(context.Background(), q))
	require.Empty(t, s.valueFor(keeperTestContext(11), a, q.Model))
	q.InjectionEnabled = true
	require.NoError(t, s.Save(context.Background(), q))
	require.Equal(t, "collected-secret", s.valueFor(keeperTestContext(11), a, q.Model))
	encoded, err := json.Marshal(s.Snapshot())
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "collected-secret")
	require.NotContains(t, string(encoded), "secret-1")
	repo := s.settings.(*keeperSettingsStub)
	require.NotContains(t, repo.value, "collected-secret")
}

func TestStateKeeperDisableDiscardsInFlightCollection(t *testing.T) {
	s, _, _ := keeperTestService(t)
	s.files = keeperTestFileStore(t)
	started := make(chan struct{})
	done := make(chan struct{})
	s.probe = func(ctx context.Context, _ OpenAIStateKeeperSettings, _ int64) openAIStateProbeResult {
		close(started)
		<-ctx.Done()
		return openAIStateProbeResult{status: 292, result: "collected", value: "late-secret"}
	}
	go func() { s.run(openAIStateKeeperJob{1, "test"}); close(done) }()
	<-started
	q := s.config.Load().OpenAIStateKeeperSettings
	q.Enabled = false
	q.Revision = "disabled"
	s.install(q)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("in-flight probe was not cancelled")
	}
	require.Empty(t, s.rows[1].value)
	require.False(t, s.rows[1].row.Collecting)
	_, err := os.Stat(s.files.path(1))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestStateKeeperDeduplicatesManualQueueAndBacksOff(t *testing.T) {
	s, _, _ := keeperTestService(t)
	require.NoError(t, s.Schedule([]int64{1, 1}))
	require.NoError(t, s.Schedule([]int64{1}))
	require.Len(t, s.queue, 1)
	s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
		return openAIStateProbeResult{status: 503, result: "upstream_error", message: "temporary"}
	}
	s.run(openAIStateKeeperJob{1, "test"})
	first := s.Snapshot().Rows[0]
	s.run(openAIStateKeeperJob{1, "test"})
	second := s.Snapshot().Rows[0]
	require.Equal(t, "refresh_failed", first.Status)
	require.Greater(t, second.NextAttemptAt.Sub(*second.LastAttemptAt), first.NextAttemptAt.Sub(*first.LastAttemptAt))
	require.NoError(t, s.config.Load().Validate())
}

func TestStateKeeperConcurrentFortyAccounts(t *testing.T) {
	s, gateway, _ := keeperTestService(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.AccountIDs = nil
	q.Revision = "forty"
	for id := int64(1); id <= 40; id++ {
		q.AccountIDs = append(q.AccountIDs, id)
	}
	s.install(q)
	s.probe = func(_ context.Context, _ OpenAIStateKeeperSettings, id int64) openAIStateProbeResult {
		return openAIStateProbeResult{status: 292, result: "collected", value: fmt.Sprintf("state-%d", id), credentialStamp: stateKeeperCredentialStamp(keeperTestAccount(id))}
	}
	for id := int64(1); id <= 40; id++ {
		s.run(openAIStateKeeperJob{id, "forty"})
	}
	var wg sync.WaitGroup
	failures := make(chan string, 2000)
	for i := 0; i < 2000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := int64(i%40 + 1)
			a := keeperTestAccount(id)
			payload := map[string]any{"model": q.Model, "input": "keep this"}
			gateway.injectCollectedStateWS(keeperTestContext(11), a, payload)
			metadata, ok := payload["client_metadata"].(map[string]any)
			if !ok || metadata[openAICollectedStateHeader] != fmt.Sprintf("state-%d", id) {
				failures <- fmt.Sprintf("account %d state mismatch", id)
			}
		}(i)
	}
	wg.Wait()
	close(failures)
	for failure := range failures {
		t.Error(failure)
	}
	for _, row := range s.Snapshot().Rows {
		require.EqualValues(t, 50, row.Injections)
	}
}

func TestStateKeeperCollectorUsesChosenProxyAndRealResponse(t *testing.T) {
	s, gateway, account := keeperTestService(t)
	gateway.openaiWSResolver = keeperWSResolver{}
	require.True(t, gateway.shouldRouteChatCompletionsViaWS(keeperTestContext(11), account))
	originalProxy := &Proxy{ID: 9, Protocol: "http", Host: "business-proxy", Port: 8080}
	account.ProxyID, account.Proxy = &originalProxy.ID, originalProxy
	account.Concurrency = 50
	gateway.httpUpstream = &keeperHTTPStub{do: func(req *http.Request, proxy string, id int64, concurrency int) (*http.Response, error) {
		require.Equal(t, "https://chatgpt.com/backend-api/codex/responses", req.URL.String())
		require.Equal(t, http.MethodPost, req.Method)
		body, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		require.Equal(t, s.config.Load().Model, gjson.GetBytes(body, "model").String())
		require.Equal(t, "user", gjson.GetBytes(body, "input.0.role").String())
		require.Equal(t, "hi", gjson.GetBytes(body, "input.0.content").String())
		require.False(t, gjson.GetBytes(body, "messages").Exists())
		require.True(t, gjson.GetBytes(body, "stream").Bool())
		require.False(t, gjson.GetBytes(body, "store").Bool())
		require.Equal(t, "application/json", req.Header.Get("Content-Type"))
		require.Equal(t, "text/event-stream", req.Header.Get("Accept"))
		require.Equal(t, "chatgpt-1", req.Header.Get("ChatGPT-Account-Id"))
		require.Empty(t, req.Header.Get(openAICodexTurnStateHeader))
		require.Equal(t, "http://127.0.0.1:8888", proxy)
		require.EqualValues(t, 1, id)
		require.Equal(t, 1, concurrency)
		require.Empty(t, req.Header.Get(openAICollectedStateHeader), "collector must never feed its own prior state back")
		require.Equal(t, "Bearer secret-1", req.Header.Get("Authorization"))
		return &http.Response{StatusCode: 292, Header: http.Header{"Current_turn_state": {"actual-upstream-state"}}, Body: io.NopCloser(strings.NewReader(""))}, nil
	}}
	result := s.collect(context.Background(), s.config.Load().OpenAIStateKeeperSettings, 1)
	require.Equal(t, "collected", result.result)
	require.Equal(t, "actual-upstream-state", result.value)
	require.Same(t, originalProxy, account.Proxy)
	require.EqualValues(t, 9, *account.ProxyID)
	require.Equal(t, 50, account.Concurrency)
	require.True(t, gateway.shouldRouteChatCompletionsViaWS(keeperTestContext(11), account), "collection must not disable business WS routing")
}

func TestStateKeeperCollectorKeepsAccountAndOriginalError(t *testing.T) {
	s, gateway, _ := keeperTestService(t)
	s.accounts.(*keeperAccountsStub).accounts[2] = keeperTestAccount(2)
	calls := 0
	gateway.httpUpstream = &keeperHTTPStub{do: func(req *http.Request, proxy string, id int64, concurrency int) (*http.Response, error) {
		calls++
		require.EqualValues(t, 2, id)
		require.Equal(t, "Bearer secret-2", req.Header.Get("Authorization"))
		require.Equal(t, "chatgpt-2", req.Header.Get("ChatGPT-Account-Id"))
		return &http.Response{StatusCode: 503, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"error":{"message":"private-upstream-error"}}`))}, nil
	}}
	result := s.collect(context.Background(), s.config.Load().OpenAIStateKeeperSettings, 2)
	require.Equal(t, 1, calls, "collection must not fail over to a different account")
	require.Equal(t, 503, result.status)
	require.Equal(t, "upstream_error", result.result)
	require.NotContains(t, result.message, "private-upstream-error")
}

func TestStateKeeperCollectorPreservesCancellation(t *testing.T) {
	s, gateway, _ := keeperTestService(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	gateway.httpUpstream = &keeperHTTPStub{do: func(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
		deadline, ok := req.Context().Deadline()
		require.True(t, ok, "probe deadline must not be detached")
		wantDeadline, _ := ctx.Deadline()
		require.Equal(t, wantDeadline, deadline)
		cancel()
		require.ErrorIs(t, req.Context().Err(), context.Canceled)
		return nil, req.Context().Err()
	}}
	result := s.collect(ctx, s.config.Load().OpenAIStateKeeperSettings, 1)
	require.Equal(t, "failed", result.result)
	require.Contains(t, result.message, "取消")
}

func TestStateKeeperCollectorNeverFallsBackFromUnavailableProxy(t *testing.T) {
	s, gateway, _ := keeperTestService(t)
	s.proxies.(*keeperProxiesStub).proxy = nil
	gateway.httpUpstream = &keeperHTTPStub{do: func(*http.Request, string, int64, int) (*http.Response, error) {
		t.Fatal("an unavailable collection proxy must not trigger direct access")
		return nil, nil
	}}
	result := s.collect(context.Background(), s.config.Load().OpenAIStateKeeperSettings, 1)
	require.Equal(t, "failed", result.result)
	require.Contains(t, result.message, "采集代理不可用")
}

func TestStateKeeperChatCompletionsBillingFailure(t *testing.T) {
	response := &http.Response{StatusCode: 429, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{
  "error": {"code":"billing_not_active","type":"billing_not_active","message":"secret-upstream-details"}
}`))}
	r := parseOpenAIStateProbeResponse(response)
	require.Equal(t, "upstream_error", r.result)
	require.Equal(t, 429, r.status)
	require.Contains(t, r.message, "billing_not_active")
	require.Contains(t, r.message, "API 计费未启用")
	require.NotContains(t, r.message, "secret-upstream-details")
	require.Empty(t, r.value)
}

func keeperTestFileStore(t *testing.T) *openAIStateFileStore {
	t.Helper()
	return &openAIStateFileStore{dir: filepath.Join(t.TempDir(), "states"), cipher: &liveAttestationAES{key: [32]byte{1}}}
}

func TestStateKeeperPersistsSeparateEncryptedFilesAndRestores(t *testing.T) {
	s, _, a := keeperTestService(t)
	s.files = keeperTestFileStore(t)
	s.run(openAIStateKeeperJob{1, "test"})
	q := s.config.Load().OpenAIStateKeeperSettings
	first, err := os.ReadFile(s.files.path(1))
	require.NoError(t, err)
	require.NotContains(t, string(first), "collected-secret")
	require.NotContains(t, string(first), "secret-1")
	require.True(t, s.Snapshot().Rows[0].StateFileSaved)
	record, err := s.files.load(1, q)
	require.NoError(t, err)
	require.NotNil(t, record)
	require.Equal(t, "collected-secret", record.Value)

	// A second account has its own file, and its encrypted identity is checked.
	second := *record
	second.AccountID = 2
	second.Value = "second-account-state"
	require.NoError(t, s.files.save(second))
	secondLoaded, err := s.files.load(2, q)
	require.NoError(t, err)
	require.Equal(t, "second-account-state", secondLoaded.Value)
	require.NoError(t, os.WriteFile(s.files.path(2), first, 0600))
	secondLoaded, err = s.files.load(2, q)
	require.NoError(t, err)
	require.Nil(t, secondLoaded, "copying an account's file must not make it usable by another account")

	// A new keeper process recovers state without issuing an upstream request.
	restarted := newOpenAIStateKeeper(s.settings, s.accounts, s.proxies, s.gateway)
	t.Cleanup(restarted.Stop)
	restarted.files = s.files
	restarted.install(q)
	restarted.restoreStateFiles(context.Background())
	require.Equal(t, "collected-secret", restarted.valueFor(keeperTestContext(11), a, q.Model))
	require.True(t, restarted.Snapshot().Rows[0].StateFileSaved)
	require.Equal(t, "ready", restarted.Snapshot().Rows[0].Status)
	require.WithinDuration(t, record.ExpiresAt.Add(-5*time.Minute), *restarted.Snapshot().Rows[0].NextAttemptAt, time.Second)

	// Atomic replacement publishes the refreshed value rather than a partial file.
	record.Value = "refreshed-state"
	require.NoError(t, s.files.save(*record))
	refreshed, err := s.files.load(1, q)
	require.NoError(t, err)
	require.Equal(t, "refreshed-state", refreshed.Value)
}

func TestStateKeeperFilesRejectExpiredChangedPolicyAndCredentials(t *testing.T) {
	s, _, a := keeperTestService(t)
	s.files = keeperTestFileStore(t)
	s.run(openAIStateKeeperJob{1, "test"})
	q := s.config.Load().OpenAIStateKeeperSettings
	for _, change := range []func(*OpenAIStateKeeperSettings){
		func(q *OpenAIStateKeeperSettings) { q.Enabled = false },
		func(q *OpenAIStateKeeperSettings) { q.Model = "other" },
		func(q *OpenAIStateKeeperSettings) { q.ProxyID = 2 },
		func(q *OpenAIStateKeeperSettings) { q.TTLSeconds = 7200 },
	} {
		changed := q
		change(&changed)
		r, err := s.files.load(1, changed)
		require.NoError(t, err)
		require.Nil(t, r)
	}
	a.Credentials["access_token"] = "reauthorized"
	restarted := newOpenAIStateKeeper(s.settings, s.accounts, s.proxies, s.gateway)
	t.Cleanup(restarted.Stop)
	restarted.files = s.files
	restarted.install(q)
	restarted.restoreStateFiles(context.Background())
	require.Empty(t, restarted.rows[1].value)

	record, err := s.files.load(1, q)
	require.NoError(t, err)
	record.ExpiresAt = time.Now().Add(-time.Minute)
	record.CollectedAt = record.ExpiresAt.Add(-time.Duration(q.TTLSeconds) * time.Second)
	require.NoError(t, s.files.save(*record))
	expired, err := s.files.load(1, q)
	require.NoError(t, err)
	require.Nil(t, expired)
}

func TestStateKeeperFileWriteFailureKeepsPreviousState(t *testing.T) {
	s, _, a := keeperTestService(t)
	s.files = keeperTestFileStore(t)
	s.run(openAIStateKeeperJob{1, "test"})
	before, err := os.ReadFile(s.files.path(1))
	require.NoError(t, err)
	s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
		return openAIStateProbeResult{status: 292, value: "replacement", result: "collected", credentialStamp: stateKeeperCredentialStamp(a)}
	}
	// A file cannot be used as a directory; force a real filesystem failure.
	blocked := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(blocked, []byte("test"), 0600))
	oldDir := s.files.dir
	s.files.dir = blocked
	s.run(openAIStateKeeperJob{1, "test"})
	s.files.dir = oldDir
	require.Equal(t, "collected-secret", s.valueFor(keeperTestContext(11), a, s.config.Load().Model))
	require.Equal(t, "refresh_failed", s.Snapshot().Rows[0].Status)
	require.Contains(t, s.Snapshot().Rows[0].Message, "文件保存失败")
	after, err := os.ReadFile(s.files.path(1))
	require.NoError(t, err)
	require.Equal(t, before, after)
}
