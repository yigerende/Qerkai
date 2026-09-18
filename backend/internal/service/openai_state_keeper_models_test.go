package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStateKeeperModelsPersistAndInjectOnlyMatchingAccountModel(t *testing.T) {
	s, gateway, a := keeperTestService(t)
	s.files = &openAIStateFileStore{dir: t.TempDir(), cipher: &liveAttestationAES{key: [32]byte{1}}}
	accounts := s.accounts.(*keeperAccountsStub)
	accounts.accounts[2] = keeperTestAccount(2)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.AccountIDs, q.Models = []int64{1, 2}, defaultStateKeeperModels()
	require.NoError(t, s.Save(context.Background(), q))
	s.probe = func(_ context.Context, q OpenAIStateKeeperSettings, id int64) openAIStateProbeResult {
		return openAIStateProbeResult{status: 200, result: "collected", value: fmt.Sprintf("gAAAAA-account-%d-model-%s", id, q.Model), credentialStamp: stateKeeperCredentialStamp(accounts.accounts[id])}
	}
	require.NoError(t, s.Schedule(nil))
	for len(s.queue) > 0 {
		s.run(<-s.queue)
	}
	snapshot := s.Snapshot()
	require.Len(t, snapshot.Rows, 2)
	for _, row := range snapshot.Rows {
		require.Len(t, row.Models, 4)
		for _, model := range row.Models {
			require.True(t, model.StateFileSaved)
			require.NotEmpty(t, model.StatePreview)
			require.NotContains(t, model.StatePreview, "account-")
		}
	}
	restarted := newOpenAIStateKeeper(s.settings, s.accounts, s.proxies, gateway)
	t.Cleanup(restarted.Stop)
	restarted.files = s.files
	restarted.reload(context.Background())
	restarted.restoreStateFiles(context.Background())
	gateway.stateKeeper.Store(restarted)
	versions := map[string]bool{}
	for _, account := range []*Account{a, accounts.accounts[2]} {
		for _, model := range q.Models {
			want := fmt.Sprintf("gAAAAA-account-%d-model-%s", account.ID, model)
			file, err := restarted.FileDetail(account.ID, model)
			require.NoError(t, err)
			require.Equal(t, want, file.Content.Value)
			body := []byte(fmt.Sprintf(`{"model":%q,"stream":true,"input":"unchanged"}`, model))
			req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(string(body)))
			out := gateway.prepareCollectedStateHTTP(keeperTestContext(11), account, body, req)
			require.Equal(t, want, out.Header.Get(openAICodexTurnStateHeader))
			got, err := io.ReadAll(out.Body)
			require.NoError(t, err)
			require.Equal(t, body, got)
			headers := http.Header{}
			ticket := gateway.prepareCollectedStateWS(keeperTestContext(11), account, model, headers)
			require.Equal(t, want, headers.Get(openAICodexTurnStateHeader))
			require.False(t, versions[ticket.poolVersion()])
			versions[ticket.poolVersion()] = true
			require.True(t, ticket.poolCurrentCheck()())
		}
	}
	require.Nil(t, gateway.prepareCollectedStateWS(keeperTestContext(11), a, "unconfigured", http.Header{}))
	q.InjectionEnabled = false
	require.NoError(t, restarted.Save(context.Background(), q))
	for _, model := range q.Models {
		headers := http.Header{"X-Codex-Turn-State": {"native"}}
		require.Nil(t, gateway.prepareCollectedStateWS(keeperTestContext(11), a, model, headers))
		require.Equal(t, "native", headers.Get(openAICodexTurnStateHeader))
	}
}

func TestStateKeeperLegacyFileIsOnlyRestoredForItsModel(t *testing.T) {
	s, _, a := keeperTestService(t)
	s.files = &openAIStateFileStore{dir: t.TempDir(), cipher: &liveAttestationAES{key: [32]byte{1}}}
	q := s.config.Load().OpenAIStateKeeperSettings
	s.run(openAIStateKeeperJob{accountID: a.ID, revision: q.Revision})
	require.NoError(t, os.Rename(s.files.path(a.ID, q.Model), s.files.path(a.ID)))
	require.NoError(t, os.Rename(s.files.runtimePath(a.ID, q.Model), s.files.runtimePath(a.ID)))
	q.Models = defaultStateKeeperModels()
	require.NoError(t, s.Save(context.Background(), q))
	restarted := newOpenAIStateKeeper(s.settings, s.accounts, s.proxies, s.gateway)
	t.Cleanup(restarted.Stop)
	restarted.files = s.files
	restarted.reload(context.Background())
	restarted.restoreStateFiles(context.Background())
	for _, model := range q.Models {
		file, err := restarted.FileDetail(a.ID, model)
		require.NoError(t, err)
		if model == "gpt-6-astra" {
			require.Equal(t, "collected-secret", file.Content.Value)
		} else {
			require.Nil(t, file)
		}
	}
}

func TestStateKeeperAllSourcesUseConfiguredRetryInterval(t *testing.T) {
	for _, source := range []string{"manual", "timer", "degradation_scan", "response"} {
		t.Run(source, func(t *testing.T) {
			s, _, _ := keeperTestService(t)
			q := s.config.Load().OpenAIStateKeeperSettings
			q.ResponseRefreshEnabled = true
			q.RetryCount, q.RetryIntervalSeconds, q.MaxAttempts = 2, 1, 1
			require.NoError(t, s.Save(context.Background(), q))
			var calls []time.Time
			s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
				calls = append(calls, time.Now())
				if len(calls) < 3 {
					return openAIStateProbeResult{status: 200, result: "not_observed"}
				}
				return openAIStateProbeResult{status: 200, result: "collected", value: "gAAAAA-retry-success"}
			}
			s.run(openAIStateKeeperJob{accountID: 1, revision: s.config.Load().Revision, source: source})
			require.Len(t, calls, 3)
			require.GreaterOrEqual(t, calls[1].Sub(calls[0]), time.Second)
			require.GreaterOrEqual(t, calls[2].Sub(calls[1]), time.Second)
			row := s.Snapshot().Rows[0].Models[0]
			require.False(t, row.Paused)
			require.Equal(t, 2, row.RetryAttempt)
			require.Equal(t, 2, row.RetryLimit)
			require.Equal(t, 3, row.RoundAttempts)
			require.Nil(t, row.NextRetryAt)
		})
	}
}

func TestStateKeeperRetryExhaustionAndCancellation(t *testing.T) {
	s, _, _ := keeperTestService(t)
	s.files = &openAIStateFileStore{dir: t.TempDir(), cipher: &liveAttestationAES{key: [32]byte{1}}}
	q := s.config.Load().OpenAIStateKeeperSettings
	q.RetryCount, q.RetryIntervalSeconds, q.MaxAttempts = 1, 1, 2
	require.NoError(t, s.Save(context.Background(), q))
	var calls atomic.Int64
	s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
		calls.Add(1)
		return openAIStateProbeResult{status: 503, result: "upstream_error"}
	}
	s.run(openAIStateKeeperJob{accountID: 1, revision: s.config.Load().Revision})
	require.Equal(t, int64(4), calls.Load())
	require.True(t, s.Snapshot().Rows[0].Paused)
	restarted := newOpenAIStateKeeper(s.settings, s.accounts, s.proxies, s.gateway)
	t.Cleanup(restarted.Stop)
	restarted.files = s.files
	restarted.reload(context.Background())
	require.True(t, restarted.Snapshot().Rows[0].Paused)
	require.Equal(t, 1, restarted.Snapshot().Rows[0].RetryLimit)
	q.RetryIntervalSeconds = 60
	q.RetryCount = 2
	require.NoError(t, s.Save(context.Background(), q))
	require.Equal(t, 1, s.Snapshot().Rows[0].RetryLimit, "editing policy cannot rewrite the completed round's limit")
	done := make(chan struct{})
	go func() { s.run(openAIStateKeeperJob{accountID: 1, revision: s.config.Load().Revision}); close(done) }()
	require.Eventually(t, func() bool { return s.Snapshot().Rows[0].NextRetryAt != nil }, time.Second, time.Millisecond)
	q.Enabled = false
	require.NoError(t, s.Save(context.Background(), q))
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("disabled collector did not cancel retry wait")
	}
	require.Equal(t, int64(6), calls.Load())
}

func TestStateKeeperMultiModelConcurrentLoadKeepsAccountAndGlobalLimits(t *testing.T) {
	s, _, _ := keeperTestService(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.Models, q.AccountIDs = defaultStateKeeperModels(), nil
	for id := int64(1); id <= 100; id++ {
		q.AccountIDs = append(q.AccountIDs, id)
	}
	q.Concurrency, q.AccountConcurrency, q.MaxAttempts, q.Revision = 50, 4, 3, "multi-load"
	keeperAddTestAccounts(s, q.AccountIDs)
	s.install(q)
	var mu sync.Mutex
	active, peak := 0, 0
	accountActive, calls := map[int64]int{}, map[openAIStateKey]int{}
	overLimit := false
	s.probe = func(_ context.Context, q OpenAIStateKeeperSettings, id int64) openAIStateProbeResult {
		mu.Lock()
		active++
		accountActive[id]++
		calls[openAIStateKey{id, q.Model}]++
		peak = max(peak, active)
		if active > 50 || accountActive[id] > 4 {
			overLimit = true
		}
		mu.Unlock()
		time.Sleep(3 * time.Millisecond)
		mu.Lock()
		active--
		accountActive[id]--
		mu.Unlock()
		return openAIStateProbeResult{status: 200, result: "not_observed"}
	}
	s.startWorkers()
	require.NoError(t, s.Schedule(nil))
	require.Eventually(t, func() bool {
		for _, account := range s.Snapshot().Rows {
			for _, row := range account.Models {
				if !row.Paused || row.Collecting || row.Queued {
					return false
				}
			}
		}
		return true
	}, 15*time.Second, 10*time.Millisecond)
	s.Stop()
	mu.Lock()
	defer mu.Unlock()
	require.False(t, overLimit)
	require.Len(t, calls, 400)
	for _, n := range calls {
		require.Equal(t, 3, n)
	}
	t.Logf("100 accounts x 4 models, 1200 probes, peak %d/50, per-account limit 4", peak)
}

func TestStateKeeperResponseRefreshTargetsOnlyItsModel(t *testing.T) {
	s, gateway, a := keeperTestService(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.ResponseRefreshEnabled = true
	q.Models, q.DegradedStateLengths = defaultStateKeeperModels(), []int{356}
	require.NoError(t, s.Save(context.Background(), q))
	ticket := gateway.prepareCollectedStateWS(keeperTestContext(11), a, "gpt-5.6-sol", http.Header{})
	ticket.observe(strings.Repeat("x", 356), 200)
	keeperDrainObservations(s)
	require.Len(t, s.queue, 1)
	job := <-s.queue
	require.Equal(t, "gpt-5.6-sol", job.model)
	require.False(t, s.entryLocked(1, "gpt-6-astra").row.Queued)
}
