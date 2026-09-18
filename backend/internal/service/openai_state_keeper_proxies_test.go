package service

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type keeperProxySet struct {
	ProxyRepository
	items map[int64]*Proxy
	errs  map[int64]error
}

func (p *keeperProxySet) GetByID(_ context.Context, id int64) (*Proxy, error) {
	if err := p.errs[id]; err != nil {
		return nil, err
	}
	return p.items[id], nil
}

type keeperSelectionAccountsFailure struct{ AccountRepository }

func (*keeperSelectionAccountsFailure) GetByIDs(context.Context, []int64) ([]*Account, error) {
	return nil, context.DeadlineExceeded
}

func TestStateKeeperDeletedAccountsLeaveStatusAndManualRetryScope(t *testing.T) {
	s, _, _ := keeperTestService(t)
	accounts := s.accounts.(*keeperAccountsStub)
	accounts.accounts[2], accounts.accounts[3] = keeperTestAccount(2), keeperTestAccount(3)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.AccountIDs = []int64{1, 2, 3}
	require.NoError(t, s.Save(context.Background(), q))
	s.entryLocked(1).row.Paused, s.entryLocked(3).row.Paused = true, true
	count, err := s.SchedulePaused()
	require.NoError(t, err)
	require.Equal(t, 2, count)
	delete(accounts.accounts, 1)
	accounts.accounts[2].Status = "disabled"
	require.NoError(t, s.SyncSelection(context.Background()))
	require.Equal(t, []int64{2, 3}, s.config.Load().AccountIDs)
	require.True(t, s.config.Load().Enabled)
	require.Nil(t, s.entryLocked(1))
	require.Len(t, s.Snapshot().Rows, 2)
	require.Empty(t, s.queue, "old queued plans are removed with the deleted account")
	count, err = s.SchedulePaused()
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, int64(3), (<-s.queue).accountID)
	delete(accounts.accounts, 2)
	delete(accounts.accounts, 3)
	require.NoError(t, s.SyncSelection(context.Background()))
	require.Empty(t, s.Snapshot().Rows)
	require.Empty(t, s.config.Load().AccountIDs)
	require.False(t, s.config.Load().Enabled)
	s.reload(context.Background())
	require.Empty(t, s.Snapshot().Rows, "deleted accounts do not reappear after reload")
}

func TestStateKeeperAccountLookupFailureDoesNotCleanAnySelection(t *testing.T) {
	s, _, _ := keeperTestService(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	require.NoError(t, s.Save(context.Background(), q))
	s.proxies = &keeperProxySet{}
	s.accounts = &keeperSelectionAccountsFailure{}
	before := s.config.Load()
	raw, err := s.settings.GetValue(context.Background(), openAIStateKeeperSettingKey)
	require.NoError(t, err)
	require.ErrorIs(t, s.SyncSelection(context.Background()), context.DeadlineExceeded)
	require.Same(t, before, s.config.Load())
	after, err := s.settings.GetValue(context.Background(), openAIStateKeeperSettingKey)
	require.NoError(t, err)
	require.Equal(t, raw, after)
}

func TestStateKeeperDeletedProxySelectionIsPersistedInPriorityOrder(t *testing.T) {
	s, gateway, _ := keeperTestService(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.ProxyIDs, q.RetryIntervalSeconds = []int64{3, 2, 1}, 1
	require.NoError(t, s.Save(context.Background(), q))
	proxies := &keeperProxySet{items: map[int64]*Proxy{
		1: {ID: 1, Status: StatusActive},
		2: {ID: 2, Status: "inactive"},
	}, errs: map[int64]error{3: ErrProxyNotFound}}
	s.proxies = proxies
	version := s.entryLocked(1).version
	require.NoError(t, s.SyncSelection(context.Background()))
	current := s.config.Load()
	require.Equal(t, []int64{2, 1}, current.proxyIDs(), "inactive proxies are not deleted proxies")
	require.Equal(t, int64(2), current.ProxyID)
	require.True(t, current.Enabled)
	require.True(t, current.InjectionEnabled)
	require.Equal(t, version, s.entryLocked(1).version, "State from a retained proxy is preserved")
	require.NoError(t, s.SyncSelection(context.Background()))
	require.Same(t, current, s.config.Load(), "an unchanged selection does not replace the configuration")
	restarted := newOpenAIStateKeeper(s.settings, s.accounts, s.proxies, gateway)
	t.Cleanup(restarted.Stop)
	restarted.reload(context.Background())
	require.Equal(t, []int64{2, 1}, restarted.config.Load().proxyIDs())
	delete(proxies.items, 1)
	delete(proxies.items, 2)
	require.NoError(t, s.SyncSelection(context.Background()))
	require.Empty(t, s.config.Load().proxyIDs())
	require.Zero(t, s.config.Load().ProxyID)
	require.False(t, s.config.Load().Enabled)
	require.True(t, s.config.Load().InjectionEnabled, "do not rewrite the injection checkbox")
	require.Error(t, s.Schedule(nil))
	restarted.reload(context.Background())
	require.Empty(t, restarted.config.Load().proxyIDs(), "legacy proxy_id cannot resurrect a removed proxy")
	require.False(t, restarted.config.Load().Enabled)
}

func TestStateKeeperProxyLookupFailureDoesNotDeleteSelection(t *testing.T) {
	s, _, _ := keeperTestService(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.ProxyIDs, q.RetryIntervalSeconds = []int64{1, 2}, 1
	require.NoError(t, s.Save(context.Background(), q))
	s.proxies = &keeperProxySet{errs: map[int64]error{1: ErrProxyNotFound, 2: context.DeadlineExceeded}}
	before := s.config.Load()
	raw, err := s.settings.GetValue(context.Background(), openAIStateKeeperSettingKey)
	require.NoError(t, err)
	require.ErrorIs(t, s.SyncSelection(context.Background()), context.DeadlineExceeded)
	require.Same(t, before, s.config.Load())
	after, err := s.settings.GetValue(context.Background(), openAIStateKeeperSettingKey)
	require.NoError(t, err)
	require.Equal(t, raw, after)
}

func TestStateKeeperProxySyncUsesLatestPersistedSettings(t *testing.T) {
	s, _, _ := keeperTestService(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.ProxyIDs, q.RetryIntervalSeconds = []int64{1, 2}, 1
	require.NoError(t, s.Save(context.Background(), q))
	q.RetryCount, q.RetryIntervalSeconds, q.Revision = 7, 17, "external-update"
	encoded, err := json.Marshal(q)
	require.NoError(t, err)
	require.NoError(t, s.settings.Set(context.Background(), openAIStateKeeperSettingKey, string(encoded)))
	s.proxies = &keeperProxySet{items: map[int64]*Proxy{2: {ID: 2, Status: StatusActive}}}
	require.NoError(t, s.SyncSelection(context.Background()))
	require.Equal(t, []int64{2}, s.config.Load().proxyIDs())
	require.Equal(t, 7, s.config.Load().RetryCount)
	require.Equal(t, 17, s.config.Load().RetryIntervalSeconds)
}

func TestStateKeeperProxyPriorityCompletesRetriesBeforeFailover(t *testing.T) {
	s, gateway, account := keeperTestService(t)
	s.files = &openAIStateFileStore{dir: t.TempDir(), cipher: &liveAttestationAES{key: [32]byte{1}}}
	q := s.config.Load().OpenAIStateKeeperSettings
	q.ProxyIDs, q.MaxAttempts, q.RetryCount, q.RetryIntervalSeconds = []int64{3, 2, 1}, 2, 1, 1
	require.NoError(t, s.Save(context.Background(), q))
	var calls []int64
	var at []time.Time
	s.probe = func(_ context.Context, q OpenAIStateKeeperSettings, _ int64) openAIStateProbeResult {
		calls = append(calls, q.ProxyID)
		at = append(at, time.Now())
		value := strings.Repeat("x", 356)
		if q.ProxyID == 2 {
			value = strings.Repeat("s", 332)
		}
		return openAIStateProbeResult{status: 200, result: "collected", value: value, credentialStamp: stateKeeperCredentialStamp(account)}
	}
	q.AllowedStateLengths = []int{332}
	require.NoError(t, s.Save(context.Background(), q))
	s.run(openAIStateKeeperJob{accountID: 1, revision: s.config.Load().Revision})
	require.Equal(t, []int64{3, 3, 3, 3, 2}, calls)
	require.GreaterOrEqual(t, at[2].Sub(at[1]), time.Second)
	require.GreaterOrEqual(t, at[4].Sub(at[3]), time.Second)
	row := s.Snapshot().Rows[0]
	require.False(t, row.Paused)
	require.Equal(t, 2, row.ProxyAttempt)
	require.Equal(t, 3, row.ProxyCount)
	require.Equal(t, int64(2), row.SavedProxyID)
	file, err := s.FileDetail(1)
	require.NoError(t, err)
	require.Equal(t, int64(2), file.Content.ProxyID)
	version := s.entryLocked(1).version
	q.ProxyIDs = []int64{1, 2, 3}
	require.NoError(t, s.Save(context.Background(), q))
	require.Equal(t, version, s.entryLocked(1).version, "priority edits keep State from a still-selected proxy")
	require.Len(t, gateway.stateKeeper.Load().valueFor(keeperTestContext(11), account, q.Model), 332)
	restarted := newOpenAIStateKeeper(s.settings, s.accounts, s.proxies, gateway)
	t.Cleanup(restarted.Stop)
	restarted.files = s.files
	restarted.reload(context.Background())
	restarted.restoreStateFiles(context.Background())
	require.Len(t, restarted.entryLocked(1).value, 332)
	q.ProxyIDs = []int64{1, 3}
	require.NoError(t, s.Save(context.Background(), q))
	require.Empty(t, s.entryLocked(1).value, "removed collection proxy invalidates its saved State")
}

func TestStateKeeperProxyFailoverChangesOnlyCollectorTransport(t *testing.T) {
	s, gateway, account := keeperTestService(t)
	gateway.openaiWSResolver = keeperWSResolver{}
	business := &Proxy{ID: 9, Protocol: "http", Host: "business-proxy", Port: 8080}
	account.ProxyID, account.Proxy = &business.ID, business
	s.proxies = &keeperProxySet{items: map[int64]*Proxy{
		1: {ID: 1, Protocol: "http", Host: "first-proxy", Port: 8001, Status: StatusActive},
		2: {ID: 2, Protocol: "http", Host: "second-proxy", Port: 8002, Status: StatusActive},
	}}
	q := s.config.Load().OpenAIStateKeeperSettings
	q.ProxyIDs, q.MaxAttempts, q.RetryCount, q.RetryIntervalSeconds, q.AllowedStateLengths = []int64{1, 2}, 1, 0, 1, []int{332}
	require.NoError(t, s.Save(context.Background(), q))
	var exits []string
	gateway.httpUpstream = &keeperHTTPStub{do: func(req *http.Request, proxy string, id int64, _ int) (*http.Response, error) {
		exits = append(exits, proxy)
		require.Equal(t, account.ID, id)
		require.Empty(t, req.Header.Get(openAICodexTurnStateHeader))
		length := 356
		if len(exits) == 2 {
			length = 332
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"X-Codex-Turn-State": {strings.Repeat("x", length)}}, Body: io.NopCloser(strings.NewReader(""))}, nil
	}}
	s.probe = s.collect
	s.run(openAIStateKeeperJob{accountID: account.ID, revision: s.config.Load().Revision})
	require.Equal(t, []string{"http://first-proxy:8001", "http://second-proxy:8002"}, exits)
	require.False(t, s.Snapshot().Rows[0].Paused)
	require.Same(t, business, account.Proxy)
	require.Equal(t, business.ID, *account.ProxyID)
	require.True(t, gateway.shouldRouteChatCompletionsViaWS(keeperTestContext(11), account))
}

func TestStateKeeperExhaustsAllProxiesAndValidatesSelection(t *testing.T) {
	s, _, _ := keeperTestService(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.ProxyIDs, q.MaxAttempts, q.RetryCount, q.RetryIntervalSeconds = []int64{2, 1}, 1, 0, 1
	require.NoError(t, s.Save(context.Background(), q))
	var calls []int64
	s.probe = func(_ context.Context, q OpenAIStateKeeperSettings, _ int64) openAIStateProbeResult {
		calls = append(calls, q.ProxyID)
		return openAIStateProbeResult{result: "failed"}
	}
	s.run(openAIStateKeeperJob{accountID: 1, revision: s.config.Load().Revision})
	require.Equal(t, []int64{2, 1}, calls)
	require.True(t, s.Snapshot().Rows[0].Paused)
	for _, ids := range [][]int64{{}, {1, 1}, {-1}, {0}} {
		q.ProxyIDs = ids
		require.Error(t, q.Validate())
	}
	q.ProxyIDs, q.RetryIntervalSeconds = []int64{1, 2}, 0
	require.Error(t, q.Validate())
}
