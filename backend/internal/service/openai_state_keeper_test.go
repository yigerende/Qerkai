package service

import (
	"bytes"
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

func (r *keeperAccountsStub) ListAllWithFilters(_ context.Context, platform, accountType, status, search string, groupID int64, privacyMode string) ([]Account, error) {
	var out []Account
	for _, a := range r.accounts {
		if a.Platform != platform || a.Type != accountType {
			continue
		}
		for _, id := range a.GroupIDs {
			if id == groupID {
				out = append(out, *a)
				break
			}
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

func keeperAddTestAccounts(s *OpenAIStateKeeperService, ids []int64) {
	accounts := s.accounts.(*keeperAccountsStub).accounts
	for _, id := range ids {
		if accounts[id] == nil {
			accounts[id] = keeperTestAccount(id)
		}
	}
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
	q.MaxAttempts = 1
	q.AccountIDs = []int64{1}
	q.GroupIDs = []int64{11}
	q.ProxyID = 1
	q.Revision = "test"
	s.install(q)
	keeperMarkDegraded(s, 1)
	s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
		return openAIStateProbeResult{status: 200, value: "collected-secret", result: "collected", message: "collected", credentialStamp: stateKeeperCredentialStamp(a)}
	}
	s.run(openAIStateKeeperJob{accountID: 1, revision: "test", source: "manual"})
	return s, gateway, a
}

func TestStateKeeperProbeCollectsHTTP200TurnState(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		headers http.Header
		body    string
		want    string
		codex   bool
	}{
		{"length 292", 200, http.Header{"X-Codex-Turn-State": {strings.Repeat("a", 292)}}, "", "collected", true},
		{"length 312", 200, http.Header{"X-Codex-Turn-State": {strings.Repeat("b", 312)}}, "", "collected", true},
		{"length 356", 200, http.Header{"X-Codex-Turn-State": {strings.Repeat("c", 356)}}, "", "collected", true},
		{"body cannot replace response header", 200, http.Header{}, `{"x-codex-turn-state":"signed-state"}`, "not_observed", false},
		{"metadata is not an HTTP header", 200, http.Header{}, "data: {\"type\":\"response.metadata\",\"headers\":{\"x-codex-turn-state\":\"turn-only\"}}\n", "not_observed", true},
		{"legacy header name", 200, http.Header{"Current_turn_state": {"signed-state"}}, "", "not_observed", false},
		{"292 is not the HTTP status", 292, http.Header{"X-Codex-Turn-State": {"signed-state"}}, "", "not_observed", true},
		{"empty header", 200, http.Header{"X-Codex-Turn-State": {""}}, "", "not_observed", false},
		{"invalid header", 200, http.Header{"X-Codex-Turn-State": {"bad\r\nvalue"}}, "", "not_observed", true},
		{"oversized header", 200, http.Header{"X-Codex-Turn-State": {strings.Repeat("s", 16385)}}, "", "not_observed", true},
		{"failure", 503, http.Header{"X-Codex-Turn-State": {"signed-state"}}, `{}`, "upstream_error", true},
		{"stream failure", 200, http.Header{}, "data: {\"type\":\"response.failed\"}\n", "upstream_error", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := parseOpenAIStateProbeResponse(&http.Response{StatusCode: tc.status, Header: tc.headers, Body: io.NopCloser(strings.NewReader(tc.body))})
			require.Equal(t, tc.want, r.result)
			require.Equal(t, tc.codex, r.hasCodexTurnState)
			require.Equal(t, len(tc.headers.Get(openAICodexTurnStateHeader)), r.turnStateLength)
			if tc.want != "collected" {
				require.Empty(t, r.value)
			} else {
				require.Equal(t, tc.headers.Get(openAICodexTurnStateHeader), r.value)
			}
		})
	}
}

type keeperHeaderOnlyBody struct{ t *testing.T }

func (b keeperHeaderOnlyBody) Read([]byte) (int, error) {
	b.t.Fatal("header collection must not wait for the model response body")
	return 0, io.EOF
}

func (keeperHeaderOnlyBody) Close() error { return nil }

func TestStateKeeperCapturesHeaderWithoutWaitingForBody(t *testing.T) {
	value := strings.Repeat("s", 356)
	result := parseOpenAIStateProbeResponse(&http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"X-Codex-Turn-State": {value}},
		Body:       keeperHeaderOnlyBody{t: t},
	})
	require.Equal(t, "collected", result.result)
	require.Equal(t, value, result.value)
	require.Equal(t, 356, result.turnStateLength)
}

func TestStateKeeperPublishesHeaderLengthWithoutStateContents(t *testing.T) {
	s, _, a := keeperTestService(t)
	value := strings.Repeat("s", 356)
	s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
		return openAIStateProbeResult{status: http.StatusOK, value: value, result: "collected", credentialStamp: stateKeeperCredentialStamp(a)}
	}
	s.run(openAIStateKeeperJob{accountID: 1, revision: "test", source: "manual"})
	snapshot := s.Snapshot()
	require.Equal(t, 356, snapshot.Rows[0].TurnStateLength)
	require.Equal(t, 356, snapshot.Events[0].TurnStateLength)
	require.Equal(t, http.StatusOK, snapshot.Rows[0].HTTPStatus)
	require.Equal(t, "ready", snapshot.Rows[0].Status)
	encoded, err := json.Marshal(snapshot)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), value)

	s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
		return openAIStateProbeResult{status: http.StatusServiceUnavailable, result: "upstream_error"}
	}
	s.run(openAIStateKeeperJob{accountID: 1, revision: "test", source: "manual"})
	snapshot = s.Snapshot()
	require.Zero(t, snapshot.Rows[0].TurnStateLength, "latest failed attempt must not display the previous response's header length")
	require.Zero(t, snapshot.Events[0].TurnStateLength)
	require.Equal(t, 356, snapshot.Events[1].TurnStateLength)
	require.Equal(t, value, s.entryLocked(a.ID).value, "a failed refresh preserves the previous stored capture")
}

func TestStateKeeperDetailReturnsOnlyTheAccountsStoredCapture(t *testing.T) {
	s, _, _ := keeperTestService(t)
	detail, ok := s.Detail(1)
	require.True(t, ok)
	require.Equal(t, int64(1), detail.AccountID)
	require.Equal(t, s.config.Load().Model, detail.Model)
	require.Equal(t, http.StatusOK, detail.HTTPStatus)
	require.Equal(t, openAICodexTurnStateHeader, detail.HeaderName)
	require.Equal(t, "collected-secret", detail.HeaderValue)
	require.Equal(t, len(detail.HeaderValue), detail.TurnStateLength)
	require.Equal(t, *s.entryLocked(1).row.CollectedAt, detail.CollectedAt)
	_, ok = s.Detail(2)
	require.False(t, ok, "another account must not receive this account's capture")

	s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
		return openAIStateProbeResult{status: http.StatusServiceUnavailable, result: "upstream_error"}
	}
	s.run(openAIStateKeeperJob{accountID: 1, revision: "test", source: "manual"})
	afterFailure, ok := s.Detail(1)
	require.True(t, ok)
	require.Equal(t, detail, afterFailure, "a failed refresh must not change the stored capture's metadata")
	require.Equal(t, http.StatusServiceUnavailable, s.Snapshot().Rows[0].HTTPStatus)

	q := s.config.Load().OpenAIStateKeeperSettings
	q.Enabled = false
	q.Revision = "disabled"
	s.install(q)
	_, ok = s.Detail(1)
	require.False(t, ok, "cleared captures must not remain accessible")
}

func TestStateKeeperAllowedLengthsControlFilesAndRestore(t *testing.T) {
	s, _, a := keeperTestService(t)
	s.files = keeperTestFileStore(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.AllowedStateLengths = []int{292, 332}
	require.NoError(t, s.Save(context.Background(), q))
	require.Empty(t, s.entryLocked(1).value, "a newly restricted policy clears an incompatible cached state")
	probe := func(length int) {
		s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
			return openAIStateProbeResult{status: http.StatusOK, value: strings.Repeat("s", length), result: "collected", credentialStamp: stateKeeperCredentialStamp(a)}
		}
		s.run(openAIStateKeeperJob{accountID: 1, revision: s.config.Load().Revision, source: "manual"})
	}
	probe(356)
	_, err := os.Stat(s.files.path(1, s.config.Load().Model))
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Empty(t, s.entryLocked(1).value)
	require.Equal(t, "filtered", s.Snapshot().Rows[0].Status)
	require.Equal(t, 356, s.Snapshot().Rows[0].TurnStateLength)
	detail, ok := s.Detail(1)
	require.True(t, ok)
	require.False(t, detail.SaveAllowed)
	require.False(t, detail.StateFileSaved)
	require.Equal(t, strings.Repeat("s", 356), detail.HeaderValue)

	probe(292)
	before, err := os.ReadFile(s.files.path(1, s.config.Load().Model))
	require.NoError(t, err)
	require.Len(t, s.entryLocked(1).value, 292)
	probe(356)
	after, err := os.ReadFile(s.files.path(1, s.config.Load().Model))
	require.NoError(t, err)
	require.Equal(t, before, after, "a filtered capture must never overwrite a previously allowed file")
	require.Len(t, s.entryLocked(1).value, 292)
	encoded, err := json.Marshal(s.Snapshot())
	require.NoError(t, err)
	require.NotContains(t, string(encoded), strings.Repeat("s", 356))

	probe(332)
	detail, ok = s.Detail(1)
	require.True(t, ok)
	require.True(t, detail.SaveAllowed)
	require.True(t, detail.StateFileSaved)
	require.Len(t, detail.HeaderValue, 332)
	restarted := newOpenAIStateKeeper(s.settings, s.accounts, s.proxies, s.gateway)
	t.Cleanup(restarted.Stop)
	restarted.files = s.files
	restarted.reload(context.Background())
	restarted.restoreStateFiles(context.Background())
	require.Equal(t, []int{292, 332}, restarted.Snapshot().Settings.AllowedStateLengths)
	require.Len(t, restarted.entryLocked(1).value, 332)
	restoredDetail, ok := restarted.Detail(1)
	require.True(t, ok)
	require.Equal(t, detail, restoredDetail)

	q = s.config.Load().OpenAIStateKeeperSettings
	q.AllowedStateLengths = []int{292}
	require.NoError(t, s.Save(context.Background(), q))
	require.Empty(t, s.entryLocked(1).value)
	require.False(t, s.Snapshot().Rows[0].StateFileSaved)
	restarted.reload(context.Background())
	restarted.restoreStateFiles(context.Background())
	require.Empty(t, restarted.entryLocked(1).value, "restart must reject files outside the current length policy")
	record, err := s.files.load(1, s.config.Load().OpenAIStateKeeperSettings)
	require.NoError(t, err)
	require.Nil(t, record)
}

func TestStateKeeperAllowedLengthsValidationAndDefaults(t *testing.T) {
	q := DefaultOpenAIStateKeeperSettings()
	require.Empty(t, q.AllowedStateLengths)
	require.True(t, q.allowsStateLength(356), "an empty list preserves unrestricted collection")
	for _, lengths := range [][]int{{0}, {-1}, {16385}, {292, 292}, make([]int, 101)} {
		q.AllowedStateLengths = lengths
		require.Error(t, q.Validate())
	}
	q.AllowedStateLengths = []int{292, 332}
	require.NoError(t, q.Validate())
	require.True(t, q.allowsStateLength(292))
	require.True(t, q.allowsStateLength(332))
	require.False(t, q.allowsStateLength(356))
}

func TestStateKeeperFileDetailReadsSavedFileInsteadOfLatestHeader(t *testing.T) {
	s, _, a := keeperTestService(t)
	s.files = keeperTestFileStore(t)
	detail, err := s.FileDetail(1)
	require.NoError(t, err)
	require.Nil(t, detail, "in-memory collection alone must not enable file viewing")
	q := s.config.Load().OpenAIStateKeeperSettings
	q.AllowedStateLengths = []int{292, 332}
	require.NoError(t, s.Save(context.Background(), q))
	s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
		return openAIStateProbeResult{status: http.StatusOK, value: strings.Repeat("a", 332), result: "collected", credentialStamp: stateKeeperCredentialStamp(a)}
	}
	s.run(openAIStateKeeperJob{accountID: 1, revision: s.config.Load().Revision, source: "manual"})
	s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
		return openAIStateProbeResult{status: http.StatusOK, value: strings.Repeat("b", 356), result: "collected", credentialStamp: stateKeeperCredentialStamp(a)}
	}
	s.run(openAIStateKeeperJob{accountID: 1, revision: s.config.Load().Revision, source: "manual"})
	header, ok := s.Detail(1)
	require.True(t, ok)
	require.Len(t, header.HeaderValue, 356)
	detail, err = s.FileDetail(1)
	require.NoError(t, err)
	require.NotNil(t, detail)
	require.Equal(t, filepath.Base(s.files.path(1, q.Model)), detail.FileName)
	require.Equal(t, strings.Repeat("a", 332), detail.Content.Value)
	require.Equal(t, int64(1), detail.Content.AccountID)
	require.Equal(t, openAICodexTurnStateHeader, detail.Content.HeaderName)
	other, err := s.FileDetail(2)
	require.NoError(t, err)
	require.Nil(t, other)

	// Both inspection and restoration accept older captures without a fixed lifetime.
	record := detail.Content
	record.CollectedAt = time.Now().Add(-30 * 24 * time.Hour).UTC()
	require.NoError(t, s.files.save(record))
	detail, err = s.FileDetail(1)
	require.NoError(t, err)
	require.NotNil(t, detail)
	require.Equal(t, record.CollectedAt, detail.Content.CollectedAt)
	restored, err := s.files.load(1, s.config.Load().OpenAIStateKeeperSettings)
	require.NoError(t, err)
	require.NotNil(t, restored)
	require.Equal(t, record.CollectedAt, restored.CollectedAt)

	require.NoError(t, os.Remove(s.files.path(1, s.config.Load().Model)))
	detail, err = s.FileDetail(1)
	require.NoError(t, err)
	require.Nil(t, detail, "a missing file must not be replaced with in-memory state")
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
			headers := http.Header{"X-Codex-Turn-State": {"original-client-state"}}
			body := []byte(fmt.Sprintf(`{"model":%q,"input":"unchanged"}`, tc.model))
			req := httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))
			req.Header = headers
			out := gateway.prepareCollectedStateHTTP(keeperTestContext(tc.group), tc.account, body, req)
			want := "original-client-state"
			if tc.want {
				want = "collected-secret"
			}
			require.Equal(t, want, out.Header.Get(openAICodexTurnStateHeader))
			sent, err := io.ReadAll(out.Body)
			require.NoError(t, err)
			require.Equal(t, body, sent)
			require.Empty(t, out.Header.Get("current_turn_state"))
		})
	}
	q.InjectionEnabled = false
	q.Revision = "off"
	s.install(q)
	headers := http.Header{"X-Codex-Turn-State": {"original"}}
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	req.Header = headers
	require.Same(t, req, gateway.prepareCollectedStateHTTP(keeperTestContext(11), a, []byte(fmt.Sprintf(`{"model":%q}`, q.Model)), req))
	require.Equal(t, http.Header{"X-Codex-Turn-State": {"original"}}, headers)
	payload := map[string]any{"model": q.Model, "input": "original", "client_metadata": map[string]any{"other": "value"}}
	before, _ := json.Marshal(payload)
	require.Nil(t, gateway.prepareCollectedStateWS(keeperTestContext(11), a, q.Model, headers))
	after, _ := json.Marshal(payload)
	require.JSONEq(t, string(before), string(after))
	q.Enabled = false
	q.Revision = "collection-off"
	s.install(q)
	require.Error(t, s.Schedule(nil))
}

func TestStateKeeperWSUsesHandshakeWithoutMutatingInput(t *testing.T) {
	s, gateway, a := keeperTestService(t)
	model := s.config.Load().Model
	metadata := map[string]any{"x-codex-turn-state": "native-turn", "x-codex-turn-metadata": "native-metadata"}
	for _, value := range []string{"first-state", "refreshed-state"} {
		s.mu.Lock()
		s.entryLocked(1).value = value
		s.mu.Unlock()
		payload := map[string]any{"model": model, "input": []string{"original prompt"}, "client_metadata": metadata, "type": "response.create"}
		before, _ := json.Marshal(payload)
		headers := http.Header{}
		ticket := gateway.prepareCollectedStateWS(keeperTestContext(11), a, model, headers)
		require.NotNil(t, ticket)
		require.Equal(t, value, headers.Get(openAICodexTurnStateHeader))
		after, _ := json.Marshal(payload)
		require.Equal(t, before, after)
		require.Equal(t, "native-turn", payload["client_metadata"].(map[string]any)[openAICodexTurnStateHeader])
		require.NotContains(t, metadata, "current_turn_state")
	}
	q := s.config.Load().OpenAIStateKeeperSettings
	q.InjectionEnabled = false
	q.Revision = "no-inject"
	s.install(q)
	payload := map[string]any{"model": model, "client_metadata": metadata}
	require.Nil(t, gateway.prepareCollectedStateWS(keeperTestContext(11), a, model, http.Header{}))
	require.Equal(t, metadata, payload["client_metadata"])
}

func TestStateKeeperNoFixedExpiryAndReauthorizationAndProbeIsolation(t *testing.T) {
	s, _, a := keeperTestService(t)
	model := s.config.Load().Model
	c := keeperTestContext(11)
	c.Set(openAIStateProbeContextKey, true)
	require.Empty(t, s.valueFor(c, a, model))
	reauth := *a
	reauth.Credentials = map[string]any{"access_token": "another-owner"}
	require.Empty(t, s.valueFor(keeperTestContext(11), &reauth, model))
	past := time.Now().Add(-30 * 24 * time.Hour)
	s.entryLocked(1).row.CollectedAt = &past
	require.Equal(t, "collected-secret", s.valueFor(keeperTestContext(11), a, model))
	require.Equal(t, "ready", s.Snapshot().Rows[0].Status)
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
	require.Equal(t, "collected-secret", s.entryLocked(a.ID).value)
	require.True(t, s.Snapshot().Settings.InjectionEnabled)
	encoded, err := json.Marshal(s.Snapshot())
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "collected-secret")
	require.NotContains(t, string(encoded), "secret-1")
	repo := s.settings.(*keeperSettingsStub)
	require.NotContains(t, repo.value, "collected-secret")
	require.True(t, gjson.Get(repo.value, "injection_enabled").Bool())
	restarted := newOpenAIStateKeeper(s.settings, s.accounts, s.proxies, s.gateway)
	t.Cleanup(restarted.Stop)
	restarted.reload(context.Background())
	require.True(t, restarted.Snapshot().Settings.InjectionEnabled)
}

func TestStateKeeperDisableDiscardsInFlightCollection(t *testing.T) {
	s, _, _ := keeperTestService(t)
	s.files = keeperTestFileStore(t)
	started := make(chan struct{})
	done := make(chan struct{})
	s.probe = func(ctx context.Context, _ OpenAIStateKeeperSettings, _ int64) openAIStateProbeResult {
		close(started)
		<-ctx.Done()
		return openAIStateProbeResult{status: 200, result: "collected", value: "late-secret"}
	}
	go func() { s.run(openAIStateKeeperJob{accountID: 1, revision: "test", source: "manual"}); close(done) }()
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
	require.Empty(t, s.entryLocked(1).value)
	require.False(t, s.entryLocked(1).row.Collecting)
	_, err := os.Stat(s.files.path(1, s.config.Load().Model))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestStateKeeperDeduplicatesManualQueueWithoutImplicitRetries(t *testing.T) {
	s, _, _ := keeperTestService(t)
	require.NoError(t, s.Schedule([]int64{1, 1}))
	require.NoError(t, s.Schedule([]int64{1}))
	require.Len(t, s.queue, 1)
	s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
		return openAIStateProbeResult{status: 503, result: "upstream_error", message: "temporary"}
	}
	s.run(openAIStateKeeperJob{accountID: 1, revision: "test", source: "manual"})
	first := s.Snapshot().Rows[0]
	s.run(openAIStateKeeperJob{accountID: 1, revision: "test", source: "manual"})
	second := s.Snapshot().Rows[0]
	require.Equal(t, "refresh_failed", first.Status)
	require.Nil(t, first.NextAttemptAt)
	require.Nil(t, second.NextAttemptAt)
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
	keeperAddTestAccounts(s, q.AccountIDs)
	s.install(q)
	s.probe = func(_ context.Context, _ OpenAIStateKeeperSettings, id int64) openAIStateProbeResult {
		return openAIStateProbeResult{status: 200, result: "collected", value: fmt.Sprintf("state-%d", id), credentialStamp: stateKeeperCredentialStamp(keeperTestAccount(id))}
	}
	for id := int64(1); id <= 40; id++ {
		keeperMarkDegraded(s, id)
		s.run(openAIStateKeeperJob{accountID: id, revision: "forty", source: "manual"})
	}
	s.startWorkers()
	var wg sync.WaitGroup
	failures := make(chan string, 2000)
	for i := 0; i < 2000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := int64(i%40 + 1)
			a := keeperTestAccount(id)
			headers := http.Header{}
			ticket := gateway.prepareCollectedStateWS(keeperTestContext(11), a, q.Model, headers)
			ticket.noteSent()
			if headers.Get(openAICodexTurnStateHeader) != fmt.Sprintf("state-%d", id) {
				failures <- fmt.Sprintf("account %d received another account's state", id)
			}
		}(i)
	}
	wg.Wait()
	close(failures)
	for failure := range failures {
		t.Error(failure)
	}
	s.Stop()
	for _, row := range s.Snapshot().Rows {
		require.Equal(t, int64(50), row.Injections)
		require.Equal(t, fmt.Sprintf("state-%d", row.AccountID), s.entryLocked(row.AccountID).value)
	}
}

func TestStateKeeperCollectorUsesChosenProxyAndRealResponse(t *testing.T) {
	s, gateway, account := keeperTestService(t)
	gateway.openaiWSResolver = keeperWSResolver{}
	require.True(t, gateway.shouldRouteChatCompletionsViaWS(keeperTestContext(11), account))
	originalProxy := &Proxy{ID: 9, Protocol: "http", Host: "business-proxy", Port: 8080}
	account.ProxyID, account.Proxy = &originalProxy.ID, originalProxy
	account.Concurrency = 50
	collectionConfig := s.config.Load().OpenAIStateKeeperSettings
	collectionConfig.AccountConcurrency, collectionConfig.MaxAttempts = 4, 9
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
		require.Equal(t, 4, concurrency)
		require.Equal(t, HTTPUpstreamProfileOpenAIStateCollection, HTTPUpstreamProfileFromContext(req.Context()))
		require.Empty(t, req.Header.Get(openAICollectedStateHeader), "collector must never feed its own prior state back")
		require.Equal(t, "Bearer secret-1", req.Header.Get("Authorization"))
		return &http.Response{StatusCode: 200, Header: http.Header{"X-Codex-Turn-State": {"actual-upstream-state"}}, Body: io.NopCloser(strings.NewReader(""))}, nil
	}}
	result := s.collect(context.Background(), collectionConfig, 1)
	require.Equal(t, "collected", result.result)
	require.Equal(t, "actual-upstream-state", result.value)
	require.Equal(t, len("actual-upstream-state"), result.turnStateLength)
	require.Same(t, originalProxy, account.Proxy)
	require.EqualValues(t, 9, *account.ProxyID)
	require.Equal(t, 50, account.Concurrency)
	require.True(t, gateway.shouldRouteChatCompletionsViaWS(keeperTestContext(11), account), "collection must not disable business WS routing")
}

func TestStateKeeperCollectorKeepsAccountAndOriginalError(t *testing.T) {
	s, gateway, _ := keeperTestService(t)
	s.accounts.(*keeperAccountsStub).accounts[2] = keeperTestAccount(2)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.AccountIDs = []int64{1, 2}
	require.NoError(t, s.Save(context.Background(), q))
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
	s.run(openAIStateKeeperJob{accountID: 1, revision: "test", source: "manual"})
	q := s.config.Load().OpenAIStateKeeperSettings
	first, err := os.ReadFile(s.files.path(1, s.config.Load().Model))
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
	require.NoError(t, os.WriteFile(s.files.path(2, s.config.Load().Model), first, 0600))
	secondLoaded, err = s.files.load(2, q)
	require.NoError(t, err)
	require.Nil(t, secondLoaded, "copying an account's file must not make it usable by another account")

	// A new keeper process recovers state without issuing an upstream request.
	restarted := newOpenAIStateKeeper(s.settings, s.accounts, s.proxies, s.gateway)
	t.Cleanup(restarted.Stop)
	restarted.files = s.files
	restarted.install(q)
	restarted.restoreStateFiles(context.Background())
	require.Equal(t, "collected-secret", restarted.entryLocked(a.ID).value)
	keeperMarkDegraded(restarted, a.ID)
	require.Equal(t, "collected-secret", restarted.valueFor(keeperTestContext(11), a, q.Model))
	require.Equal(t, http.StatusOK, restarted.Snapshot().Rows[0].HTTPStatus)
	require.Equal(t, len("collected-secret"), restarted.Snapshot().Rows[0].TurnStateLength)
	require.True(t, restarted.Snapshot().Rows[0].HasCodexTurnState)
	require.True(t, restarted.Snapshot().Rows[0].StateFileSaved)
	require.Equal(t, "ready", restarted.Snapshot().Rows[0].Status)
	require.Nil(t, restarted.Snapshot().Rows[0].NextAttemptAt)

	// Atomic replacement publishes the refreshed value rather than a partial file.
	record.Value = "refreshed-state"
	require.NoError(t, s.files.save(*record))
	refreshed, err := s.files.load(1, q)
	require.NoError(t, err)
	require.Equal(t, "refreshed-state", refreshed.Value)
}

func TestStateKeeperFilesRejectChangedPolicyAndCredentials(t *testing.T) {
	s, _, a := keeperTestService(t)
	s.files = keeperTestFileStore(t)
	s.run(openAIStateKeeperJob{accountID: 1, revision: "test", source: "manual"})
	q := s.config.Load().OpenAIStateKeeperSettings
	for _, change := range []func(*OpenAIStateKeeperSettings){
		func(q *OpenAIStateKeeperSettings) { q.Enabled = false },
		func(q *OpenAIStateKeeperSettings) { q.Model = "other" },
		func(q *OpenAIStateKeeperSettings) { q.ProxyID = 2 },
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
	require.Empty(t, restarted.entryLocked(1).value)

	record, err := s.files.load(1, q)
	require.NoError(t, err)
	record.CollectedAt = time.Now().Add(-30 * 24 * time.Hour).UTC()
	require.NoError(t, s.files.save(*record))
	old, err := s.files.load(1, q)
	require.NoError(t, err)
	require.NotNil(t, old)
	require.Equal(t, record.CollectedAt, old.CollectedAt)
}

func TestStateKeeperFilesRejectLegacyCaptureFormat(t *testing.T) {
	s, _, _ := keeperTestService(t)
	s.files = keeperTestFileStore(t)
	s.run(openAIStateKeeperJob{accountID: 1, revision: "test", source: "manual"})
	q := s.config.Load().OpenAIStateKeeperSettings
	record, err := s.files.load(1, q)
	require.NoError(t, err)
	require.NotNil(t, record)
	require.Equal(t, openAICodexTurnStateHeader, record.HeaderName)
	record.HeaderName = "current_turn_state"
	require.Error(t, s.files.save(*record))
	legacy, err := json.Marshal(record)
	require.NoError(t, err)
	legacy = bytes.Replace(legacy, []byte(`"turn_state":`), []byte(`"current_turn_state":`), 1)
	encrypted, err := s.files.cipher.Encrypt(string(legacy))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(s.files.path(1, s.config.Load().Model), []byte(encrypted), 0600))
	restored, err := s.files.load(1, q)
	require.NoError(t, err)
	require.Nil(t, restored)
}

func TestStateKeeperIgnoresLegacyFixedExpiry(t *testing.T) {
	s, _, _ := keeperTestService(t)
	s.files = keeperTestFileStore(t)
	s.run(openAIStateKeeperJob{accountID: 1, revision: "test", source: "manual"})
	q := s.config.Load().OpenAIStateKeeperSettings
	record, err := s.files.load(1, q)
	require.NoError(t, err)
	encoded, err := json.Marshal(record)
	require.NoError(t, err)
	var legacy map[string]any
	require.NoError(t, json.Unmarshal(encoded, &legacy))
	legacy["ttl_seconds"] = 3600
	legacy["expires_at"] = time.Now().Add(-24 * time.Hour).UTC().Format(time.RFC3339Nano)
	encoded, err = json.Marshal(legacy)
	require.NoError(t, err)
	encrypted, err := s.files.cipher.Encrypt(string(encoded))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(s.files.path(1, s.config.Load().Model), []byte(encrypted), 0600))
	restored, err := s.files.load(1, q)
	require.NoError(t, err)
	require.NotNil(t, restored)
	require.Equal(t, record.Value, restored.Value)
	current, err := json.Marshal(restored)
	require.NoError(t, err)
	require.NotContains(t, string(current), "expires_at")
	require.NotContains(t, string(current), "ttl_seconds")
}

func TestStateKeeperFileWriteFailureKeepsPreviousState(t *testing.T) {
	s, _, a := keeperTestService(t)
	s.files = keeperTestFileStore(t)
	s.run(openAIStateKeeperJob{accountID: 1, revision: "test", source: "manual"})
	before, err := os.ReadFile(s.files.path(1, s.config.Load().Model))
	require.NoError(t, err)
	s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
		return openAIStateProbeResult{status: 200, value: "replacement", result: "collected", credentialStamp: stateKeeperCredentialStamp(a)}
	}
	// A file cannot be used as a directory; force a real filesystem failure.
	blocked := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(blocked, []byte("test"), 0600))
	oldDir := s.files.dir
	s.files.dir = blocked
	s.run(openAIStateKeeperJob{accountID: 1, revision: "test", source: "manual"})
	s.files.dir = oldDir
	require.Equal(t, "collected-secret", s.entryLocked(a.ID).value)
	require.Equal(t, "refresh_failed", s.Snapshot().Rows[0].Status)
	require.Contains(t, s.Snapshot().Rows[0].Message, "文件保存失败")
	after, err := os.ReadFile(s.files.path(1, s.config.Load().Model))
	require.NoError(t, err)
	require.Equal(t, before, after)
}
