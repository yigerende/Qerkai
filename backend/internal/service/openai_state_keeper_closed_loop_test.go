package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func keeperMarkDegraded(s *OpenAIStateKeeperService, id int64) {
	now := time.Now().UTC()
	q := AccountQualitySettings{Enabled: true, Revision: "quality-test", QuestionEnabled: true, DegradationMode: "any", DegradationConditions: []string{"question"}}
	s.observeQualityResult(q, AccountQualityResult{AccountID: id, Revision: q.Revision, Question: QualityQuestionResult{QualityVerdict: QualityVerdict{Status: "degraded", Degraded: true, CheckedAt: &now}}})
}

func keeperDrainObservations(s *OpenAIStateKeeperService) {
	for {
		select {
		case event := <-s.observations:
			s.processObservation(event)
		default:
			return
		}
	}
}

func TestStateKeeperParallelRoundHardBudgetAndPersistentPause(t *testing.T) {
	s, _, a := keeperTestService(t)
	s.files = keeperTestFileStore(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.Concurrency, q.AccountConcurrency, q.MaxAttempts = 4, 3, 7
	q.AutoRefresh, q.AutoCollectIntervalSeconds = true, 1
	q.AllowedStateLengths = []int{332}
	require.NoError(t, s.Save(context.Background(), q))
	var active, peak, calls atomic.Int64
	s.probe = func(ctx context.Context, _ OpenAIStateKeeperSettings, _ int64) openAIStateProbeResult {
		n := active.Add(1)
		for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
		}
		calls.Add(1)
		time.Sleep(10 * time.Millisecond)
		active.Add(-1)
		return openAIStateProbeResult{status: 200, result: "collected", value: strings.Repeat("x", 356)}
	}
	s.run(openAIStateKeeperJob{accountID: 1, revision: s.config.Load().Revision, source: "manual"})
	require.Equal(t, int64(7), calls.Load())
	require.Equal(t, int64(3), peak.Load())
	row := s.Snapshot().Rows[0]
	require.Equal(t, 7, row.RoundAttempts)
	require.True(t, row.Paused)
	s.scheduleDue(time.Now().Add(time.Hour))
	require.Empty(t, s.queue)

	restarted := newOpenAIStateKeeper(s.settings, s.accounts, s.proxies, s.gateway)
	t.Cleanup(restarted.Stop)
	restarted.files = s.files
	restarted.reload(context.Background())
	keeperMarkDegraded(restarted, 1)
	require.True(t, restarted.Snapshot().Rows[0].Paused)
	restarted.scheduleDue(time.Now().Add(24 * time.Hour))
	require.Empty(t, restarted.queue)
	q = restarted.config.Load().OpenAIStateKeeperSettings
	q.Enabled = false
	require.NoError(t, restarted.Save(context.Background(), q))
	q.Enabled = true
	require.NoError(t, restarted.Save(context.Background(), q))
	restarted.scheduleDue(time.Now().Add(24 * time.Hour))
	require.Empty(t, restarted.queue, "configuration changes must not reset the paused budget")
	restarted.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
		return openAIStateProbeResult{status: 200, result: "collected", value: strings.Repeat("a", 332), credentialStamp: stateKeeperCredentialStamp(a)}
	}
	require.NoError(t, restarted.Schedule([]int64{1}))
	restarted.run(<-restarted.queue)
	require.False(t, restarted.Snapshot().Rows[0].Paused)
	require.Len(t, restarted.entryLocked(1).value, 332)
	require.Equal(t, "degraded", restarted.Snapshot().Rows[0].QualityStatus, "collection is not a quality recovery")
}

func TestStateKeeperFirstSavedStateWinsAndSiblingsDrain(t *testing.T) {
	s, _, a := keeperTestService(t)
	s.files = keeperTestFileStore(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.AccountConcurrency, q.MaxAttempts = 3, 10
	require.NoError(t, s.Save(context.Background(), q))
	started := make(chan int, 3)
	allowWinner, drain := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	s.probe = func(ctx context.Context, _ OpenAIStateKeeperSettings, _ int64) openAIStateProbeResult {
		n := int(calls.Add(1))
		started <- n
		value := "winner"
		if n == 1 {
			<-allowWinner
		} else {
			<-ctx.Done()
			<-drain
			value = "late-result"
		}
		return openAIStateProbeResult{status: 200, result: "collected", value: value, credentialStamp: stateKeeperCredentialStamp(a)}
	}
	done := make(chan struct{})
	go func() {
		s.run(openAIStateKeeperJob{accountID: 1, revision: s.config.Load().Revision, source: "manual"})
		close(done)
	}()
	for range 3 {
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			t.Fatal("single-account concurrency did not start")
		}
	}
	close(allowWinner)
	require.Eventually(t, func() bool { s.mu.RLock(); defer s.mu.RUnlock(); return s.entryLocked(1).value == "winner" }, 3*time.Second, time.Millisecond)
	require.NoError(t, s.Schedule([]int64{1}))
	require.Empty(t, s.queue, "account remains reserved while cancelled siblings drain")
	close(drain)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("round did not finish")
	}
	require.Equal(t, int64(3), calls.Load())
	require.Equal(t, "winner", s.entryLocked(1).value)
	file, err := s.FileDetail(1)
	require.NoError(t, err)
	require.Equal(t, "winner", file.Content.Value)
}

func TestStateKeeperConcurrentAccountsShareGlobalRequestLimit(t *testing.T) {
	s, _, _ := keeperTestService(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.AccountIDs = nil
	for id := int64(1); id <= 100; id++ {
		q.AccountIDs = append(q.AccountIDs, id)
	}
	q.Concurrency, q.AccountConcurrency, q.MaxAttempts, q.Revision = 50, 4, 9, "load"
	keeperAddTestAccounts(s, q.AccountIDs)
	s.install(q)
	var mu sync.Mutex
	active, peak := 0, 0
	accountActive, accountCalls := map[int64]int{}, map[int64]int{}
	overLimit := false
	s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
		return openAIStateProbeResult{}
	}
	s.probe = func(ctx context.Context, _ OpenAIStateKeeperSettings, id int64) openAIStateProbeResult {
		mu.Lock()
		active++
		accountActive[id]++
		accountCalls[id]++
		peak = max(peak, active)
		if active > q.Concurrency || accountActive[id] > q.AccountConcurrency || accountCalls[id] > q.MaxAttempts {
			overLimit = true
		}
		mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		mu.Lock()
		active--
		accountActive[id]--
		mu.Unlock()
		return openAIStateProbeResult{status: 200, result: "not_observed"}
	}
	s.startWorkers()
	require.NoError(t, s.Schedule(nil))
	require.Eventually(t, func() bool {
		for _, row := range s.Snapshot().Rows {
			if !row.Paused || row.Collecting || row.Queued {
				return false
			}
		}
		return true
	}, 10*time.Second, 10*time.Millisecond)
	s.Stop()
	mu.Lock()
	defer mu.Unlock()
	require.False(t, overLimit)
	require.Greater(t, peak, 1)
	for id := int64(1); id <= 100; id++ {
		require.Equal(t, 9, accountCalls[id])
	}
	t.Logf("100 accounts, 900 attempts, global peak %d/50, per-account limit 4, exact round budget 9", peak)
}

func TestStateKeeperResponseRefreshDeduplicatedAndStaleResponseIgnored(t *testing.T) {
	s, gateway, a := keeperTestService(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.ResponseRefreshEnabled = true
	q.DegradedStateLengths = []int{356}
	require.NoError(t, s.Save(context.Background(), q))
	headers := http.Header{}
	ticket := gateway.prepareCollectedStateWS(keeperTestContext(11), a, q.Model, headers)
	ticket.noteSent()
	for range 1000 {
		ticket.observe(strings.Repeat("x", 356), 200)
	}
	keeperDrainObservations(s)
	require.Len(t, s.queue, 1)
	require.Equal(t, int64(1), s.Snapshot().Rows[0].Injections)
	require.Equal(t, "degraded_signal", s.Recent([]int64{1})[0].Injections[0].Result)
	s.run(<-s.queue)
	require.NotEqual(t, ticket.version, s.entryLocked(1).version)
	ticket.observe(strings.Repeat("x", 356), 200)
	keeperDrainObservations(s)
	require.Empty(t, s.queue, "old request cannot refresh newly collected State")
	newTicket := gateway.prepareCollectedStateWS(keeperTestContext(11), a, q.Model, http.Header{})
	newTicket.observe(strings.Repeat("x", 356), 200)
	keeperDrainObservations(s)
	require.Len(t, s.queue, 1, "a fresh response may trigger the next round")
	q.InjectionEnabled = false
	require.NoError(t, s.Save(context.Background(), q))
	newTicket.observe(strings.Repeat("x", 356), 200)
	keeperDrainObservations(s)
	require.Empty(t, s.queue, "disabled injection stops response-triggered work")
	q.InjectionEnabled = true
	require.NoError(t, s.Save(context.Background(), q))
	reenabled := gateway.prepareCollectedStateWS(keeperTestContext(11), a, q.Model, http.Header{})
	reenabled.observe(strings.Repeat("x", 356), 200)
	keeperDrainObservations(s)
	require.Len(t, s.queue, 1, "a discarded queued refresh must not permanently suppress later signals")
}

func TestStateKeeperDisabledHTTPAndWSKeepOriginalRequests(t *testing.T) {
	s, gateway, a := keeperTestService(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.InjectionEnabled = false
	require.NoError(t, s.Save(context.Background(), q))
	for _, path := range []string{"/v1/responses", "/v1/chat/completions"} {
		body := fmt.Sprintf(`{"model":%q,"input":"original","stream":true}`, q.Model)
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
		req.Header.Set(openAICodexTurnStateHeader, "client-state")
		out := gateway.prepareCollectedStateHTTP(keeperTestContext(11), a, []byte(body), req)
		require.Same(t, req, out)
		require.Nil(t, out.Context().Value(openAIStateTicketKey{}))
		gateway.httpUpstream = &keeperHTTPStub{do: func(sent *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
			require.Same(t, req, sent)
			require.Equal(t, "client-state", sent.Header.Get(openAICodexTurnStateHeader))
			got, err := io.ReadAll(sent.Body)
			require.NoError(t, err)
			require.Equal(t, body, string(got))
			return &http.Response{StatusCode: 503, Header: http.Header{"X-Codex-Turn-State": {strings.Repeat("b", 356)}}, Body: io.NopCloser(strings.NewReader("unchanged-response"))}, nil
		}}
		response, err := gateway.doOpenAIUpstream(out, "original-proxy", a)
		require.NoError(t, err)
		require.Equal(t, 503, response.StatusCode)
		got, err := io.ReadAll(response.Body)
		require.NoError(t, err)
		require.Equal(t, "unchanged-response", string(got))
	}
	headers := http.Header{"X-Codex-Turn-State": {"native-state"}}
	ticket := gateway.prepareCollectedStateWS(keeperTestContext(11), a, q.Model, headers)
	require.Nil(t, ticket)
	require.Empty(t, ticket.poolVersion())
	require.Equal(t, "native-state", headers.Get(openAICodexTurnStateHeader))
	require.Empty(t, s.observations)
	require.Empty(t, s.queue)
}

func TestStateKeeperSaturatedHistoryKeepsDegradationSignal(t *testing.T) {
	s, gateway, a := keeperTestService(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.ResponseRefreshEnabled = true
	q.DegradedStateLengths = []int{356}
	require.NoError(t, s.Save(context.Background(), q))
	for range cap(s.observations) {
		s.observations <- openAIStateObservation{}
	}
	ticket := gateway.prepareCollectedStateWS(keeperTestContext(11), a, q.Model, http.Header{})
	for range 10000 {
		ticket.observe(strings.Repeat("d", 356), 200)
	}
	require.Empty(t, s.queue, "overflow still waits for background processing")
	require.Len(t, s.pendingSignals, 1, "signals are bounded by account count")
	s.processPendingSignals()
	require.Len(t, s.queue, 1, "a full history queue must not lose the refresh trigger")
	require.Empty(t, s.pendingSignals)
}

func TestStateKeeperOnlyDegradationScanRequiresOverallDegradation(t *testing.T) {
	for _, detectionEnabled := range []bool{false, true} {
		for _, state := range []string{"pending", "normal", "degraded"} {
			t.Run(fmt.Sprintf("detection=%t/status=%s", detectionEnabled, state), func(t *testing.T) {
				s, gateway, a := keeperTestService(t)
				q := s.config.Load().OpenAIStateKeeperSettings
				q.DegradationScanEnabled, q.DegradationScanIntervalSeconds = true, 1
				require.NoError(t, s.Save(context.Background(), q))
				s.mu.Lock()
				s.qualityPolicy.Enabled = detectionEnabled
				s.entryLocked(1).row.QualityStatus = state
				s.mu.Unlock()
				s.scheduleDegradationScan(time.Now().Add(time.Hour))
				require.Equal(t, detectionEnabled && state == "degraded", len(s.queue) == 1)
				headers := http.Header{"X-Codex-Turn-State": {"native-state"}}
				ticket := gateway.prepareCollectedStateWS(keeperTestContext(11), a, q.Model, headers)
				require.NotNil(t, ticket)
				require.Equal(t, "collected-secret", headers.Get(openAICodexTurnStateHeader))
				require.Equal(t, "collected-secret", ticket.value)
				current := ticket.poolCurrentCheck()
				require.NotNil(t, current)
				require.True(t, current(), "WS state remains reusable independently of quality results")
				body := []byte(fmt.Sprintf(`{"model":%q,"stream":true,"input":"original"}`, q.Model))
				req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(string(body)))
				req.Header.Set(openAICodexTurnStateHeader, "native-state")
				out := gateway.prepareCollectedStateHTTP(keeperTestContext(11), a, body, req)
				require.Equal(t, "collected-secret", out.Header.Get(openAICodexTurnStateHeader))
				savedBody, err := io.ReadAll(out.Body)
				require.NoError(t, err)
				require.Equal(t, body, savedBody)
				q.InjectionEnabled = false
				require.NoError(t, s.Save(context.Background(), q))
				require.False(t, current(), "disabling injection must still invalidate the prewarm target")
			})
		}
	}
}

func TestStateKeeperThreeTriggerSettingsAreIndependent(t *testing.T) {
	s, gateway, a := keeperTestService(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	q.AutoRefresh, q.AutoCollectIntervalSeconds = true, 120
	q.DegradationScanEnabled, q.DegradationScanIntervalSeconds = true, 30
	q.DegradedStateLengths = []int{356}
	require.NoError(t, s.Save(context.Background(), q))
	s.mu.Lock()
	s.entryLocked(1).row.QualityStatus = "normal"
	s.mu.Unlock()
	now := time.Now().Add(time.Hour)
	s.scheduleDegradationScan(now)
	require.Empty(t, s.queue, "the degradation scan excludes normal accounts")
	s.scheduleDue(now)
	require.Len(t, s.queue, 1, "ordinary automatic collection applies to selected accounts")
	job := <-s.queue
	require.Equal(t, "timer", job.source)
	s.run(job)
	s.scheduleDue(s.entryLocked(1).lastFinishedAt.Add(119 * time.Second))
	require.Empty(t, s.queue)
	q.AutoRefresh, q.DegradationScanEnabled = false, false
	q.ResponseRefreshEnabled = true
	require.NoError(t, s.Save(context.Background(), q))
	ticket := gateway.prepareCollectedStateWS(keeperTestContext(11), a, q.Model, http.Header{})
	require.NotNil(t, ticket)
	require.Equal(t, "collected-secret", ticket.value, "injection is independent of the quality conclusion and both timers")
	ticket.observe(strings.Repeat("d", 356), 200)
	keeperDrainObservations(s)
	require.Len(t, s.queue, 1, "response signals do not require either timer to be enabled")
	require.Equal(t, "response", (<-s.queue).source)
	q.InjectionEnabled, q.AutoRefresh, q.DegradationScanEnabled = false, true, true
	q.ResponseRefreshEnabled = false
	require.NoError(t, s.Save(context.Background(), q))
	keeperMarkDegraded(s, a.ID)
	s.scheduleDegradationScan(now)
	require.Len(t, s.queue, 1, "degradation scanning is independent of business injection")
	require.Equal(t, "degradation_scan", (<-s.queue).source)
	require.Nil(t, gateway.prepareCollectedStateWS(keeperTestContext(11), a, q.Model, http.Header{}))
}

func TestStateKeeperHTTPResponseQueuesRefreshWithoutReadingOrResendingBody(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			s, gateway, a := keeperTestService(t)
			q := s.config.Load().OpenAIStateKeeperSettings
			q.ResponseRefreshEnabled = enabled
			q.DegradedStateLengths = []int{356}
			require.NoError(t, s.Save(context.Background(), q))
			body := []byte(fmt.Sprintf(`{"model":%q,"input":"original"}`, q.Model))
			req := gateway.prepareCollectedStateHTTP(keeperTestContext(11), a, body, httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(string(body))))
			calls := 0
			response := &http.Response{StatusCode: 200, Header: http.Header{"X-Codex-Turn-State": {strings.Repeat("x", 356)}}, Body: keeperHeaderOnlyBody{t: t}}
			gateway.httpUpstream = &keeperHTTPStub{do: func(r *http.Request, proxy string, _ int64, _ int) (*http.Response, error) {
				calls++
				require.Equal(t, "original-proxy", proxy)
				require.Equal(t, "collected-secret", r.Header.Get(openAICodexTurnStateHeader))
				return response, nil
			}}
			got, err := gateway.doOpenAIUpstream(req, "original-proxy", a)
			require.NoError(t, err)
			require.Same(t, response, got)
			require.Equal(t, 1, calls)
			keeperDrainObservations(s)
			if enabled {
				require.Len(t, s.queue, 1)
				require.Equal(t, "response", (<-s.queue).source)
			} else {
				require.Empty(t, s.queue)
				require.Empty(t, s.pendingSignals)
			}
		})
	}
}

func TestStateKeeperInterruptedRoundStaysPausedAfterRestart(t *testing.T) {
	s, _, _ := keeperTestService(t)
	s.files = keeperTestFileStore(t)
	q := s.config.Load().OpenAIStateKeeperSettings
	require.NoError(t, s.Save(context.Background(), q))
	s.mu.Lock()
	s.entryLocked(1).row.Collecting = true
	s.entryLocked(1).row.RoundID = "interrupted"
	s.mu.Unlock()
	require.NoError(t, s.persistRuntime(1))
	restarted := newOpenAIStateKeeper(s.settings, s.accounts, s.proxies, s.gateway)
	t.Cleanup(restarted.Stop)
	restarted.files = s.files
	restarted.reload(context.Background())
	require.True(t, restarted.Snapshot().Rows[0].Paused)
	require.Contains(t, restarted.Snapshot().Rows[0].PauseReason, "未完成")
}

func BenchmarkStateKeeperDisabledRequest(b *testing.B) {
	s := newOpenAIStateKeeper(nil, nil, nil, nil)
	defer s.Stop()
	gateway := &OpenAIGatewayService{}
	gateway.stateKeeper.Store(s)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		req := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
		body := []byte(`{"model":"example"}`)
		for pb.Next() {
			gateway.prepareCollectedStateHTTP(nil, nil, body, req)
		}
	})
}
