//go:build unit

package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type congestionHTTPFixture struct {
	HTTPUpstream
	client *http.Client
}

type congestionSlotsFixture struct {
	ConcurrencyCache
	mu      sync.Mutex
	active  map[int64]int
	maximum map[int64]int
}

func (c *congestionSlotsFixture) AcquireAccountSlot(_ context.Context, id int64, limit int, _ string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	occupied := c.active[id]
	if id == 1 {
		occupied += 80
	}
	if occupied >= limit {
		return false, nil
	}
	c.active[id]++
	if c.active[id] > c.maximum[id] {
		c.maximum[id] = c.active[id]
	}
	return true, nil
}
func (c *congestionSlotsFixture) ReleaseAccountSlot(_ context.Context, id int64, _ string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.active[id]--
	return nil
}
func (c *congestionSlotsFixture) GetAccountWaitingCount(context.Context, int64) (int, error) {
	return 0, nil
}
func (c *congestionSlotsFixture) GetAccountsLoadBatch(_ context.Context, accounts []AccountWithConcurrency) (map[int64]*AccountLoadInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[int64]*AccountLoadInfo{}
	for _, a := range accounts {
		used := c.active[a.ID]
		if a.ID == 1 {
			used += 80
		}
		out[a.ID] = &AccountLoadInfo{AccountID: a.ID, CurrentConcurrency: used, LoadRate: used * 100 / a.MaxConcurrency}
	}
	return out, nil
}

func TestOpenAICongestionBurstRespectsCapacityAndReleasesSlots(t *testing.T) {
	s, c, accounts := congestionFixture(true, 80, 0)
	accounts[1].Concurrency = 20
	slots := &congestionSlotsFixture{active: map[int64]int{}, maximum: map[int64]int{}}
	s.concurrencyService = NewConcurrencyService(slots)
	congestionBad(s, 1)
	const n = 200
	contexts := make([]context.Context, n)
	for i := range contexts {
		session := fmt.Sprintf("burst-%d", i%20)
		require.NoError(t, c.SetSessionAccountID(context.Background(), 7, "openai:"+session, 1, time.Hour))
		contexts[i] = congestionContext(t, s)
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, n)
	var acquired, queued atomic.Int64
	for i, ctx := range contexts {
		wg.Add(1)
		go func(i int, ctx context.Context) {
			defer wg.Done()
			<-start
			sel, err := congestionSelect(s, ctx, fmt.Sprintf("burst-%d", i%20))
			if err != nil {
				errs <- err
				return
			}
			defer releaseLegacySchedulerDecisionSelection(sel)
			if !sel.Acquired {
				if sel.WaitPlan == nil || sel.WaitPlan.Timeout <= 0 {
					errs <- errors.New("missing bounded queue")
				}
				queued.Add(1)
				return
			}
			acquired.Add(1)
			_, finish := s.beginCongestionForward(ctx, sel.Account, nil)
			time.Sleep(3 * time.Millisecond)
			if err := finish(&OpenAIForwardResult{}, nil); err != nil {
				errs <- err
			}
		}(i, ctx)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.EqualValues(t, n, acquired.Load()+queued.Load())
	require.Positive(t, acquired.Load())
	require.LessOrEqual(t, slots.maximum[1], 20)
	require.LessOrEqual(t, slots.maximum[2], 20)
	require.Zero(t, slots.active[1])
	require.Zero(t, slots.active[2])
}

func (u congestionHTTPFixture) Do(r *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	return u.client.Do(r)
}

func TestOpenAICongestionForwardObservationDoesNotChangeWireOrCancel(t *testing.T) {
	body := []byte(`{"model":"gpt-6-astra","stream":true,"input":[{"role":"user","content":"write code"}],"tools":[{"type":"function","name":"read_file","parameters":{"type":"object"}}],"prompt_cache_key":"keep-me"}`)
	wire := []byte("event: response.created\ndata: {\"type\":\"response.created\"}\n\nevent: response.completed\ndata: {\"type\":\"response.completed\"}\n\n")
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		received, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Equal(body, received) || r.Header.Get("X-Test-Session") != "keep-me" {
			t.Error("wire request changed")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write(wire)
	}))
	defer server.Close()
	for _, enabled := range []bool{false, true} {
		s, _, a := congestionFixture(enabled, 10, 10)
		s.httpUpstream = congestionHTTPFixture{client: server.Client()}
		base := congestionContext(t, s)
		base, cancel := context.WithTimeout(base, time.Minute)
		defer cancel()
		ctx, finish := s.beginCongestionForward(base, &a[0], body)
		require.Equal(t, base.Done(), ctx.Done())
		request, err := http.NewRequestWithContext(ctx, "POST", server.URL, bytes.NewReader(body))
		require.NoError(t, err)
		request.Header.Set("X-Test-Session", "keep-me")
		response, err := s.doOpenAIUpstream(request, "", &a[0])
		require.NoError(t, err)
		received, err := io.ReadAll(response.Body)
		response.Body.Close()
		require.NoError(t, err)
		require.Equal(t, wire, received)
		require.NoError(t, finish(&OpenAIForwardResult{}, nil))
		require.NoError(t, base.Err())
		if enabled {
			require.Empty(t, s.congestionMonitor.accounts[1].active)
		}
	}
	require.EqualValues(t, 2, calls.Load(), "one upstream request per forward; observation never retries")
}

func TestOpenAICongestionOpaqueReferenceDoesNotMigrate(t *testing.T) {
	s, c, _ := congestionFixture(true, 100, 0)
	require.NoError(t, c.SetSessionAccountID(context.Background(), 7, "openai:chat", 1, time.Hour))
	g, _ := gin.CreateTestContext(httptest.NewRecorder())
	g.Request = httptest.NewRequest("POST", "/v1/responses", nil)
	cleanup := s.PrepareOpenAICongestionRequest(g, []byte(`{"input":[{"type":"item_reference","id":"item_owned_by_A"}]}`))
	defer cleanup()
	selected, err := congestionSelect(s, g.Request.Context(), "chat")
	require.NoError(t, err)
	require.EqualValues(t, 1, selected.Account.ID)
	require.NotNil(t, selected.WaitPlan)
	require.Zero(t, c.calls.Load())
}

// Thread-safe cache double for exercising real selection/forward lifecycle.
// The actual Redis scripts are tested separately against miniredis.
type congestionCacheFixture struct {
	GatewayCache
	mu        sync.Mutex
	bindings  map[string]int64
	pending   map[string]string
	protected map[string]bool
	calls     atomic.Int64
}

func congestionFixtureKey(group int64, key string) string { return fmt.Sprintf("%d:%s", group, key) }
func (c *congestionCacheFixture) GetSessionAccountID(_ context.Context, g int64, k string) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v := c.bindings[congestionFixtureKey(g, k)]
	if v == 0 {
		return 0, ErrStickySessionNotFound
	}
	return v, nil
}
func (c *congestionCacheFixture) SetSessionAccountID(_ context.Context, g int64, k string, v int64, _ time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bindings[congestionFixtureKey(g, k)] = v
	return nil
}
func (c *congestionCacheFixture) DeleteSessionAccountID(_ context.Context, g int64, k string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.bindings, congestionFixtureKey(g, k))
	return nil
}
func (c *congestionCacheFixture) RefreshSessionTTL(context.Context, int64, string, time.Duration) error {
	return nil
}
func (c *congestionCacheFixture) ReadCongestionSession(_ context.Context, g int64, k string) (string, bool, error) {
	c.calls.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	key := congestionFixtureKey(g, k)
	return c.pending[key], c.protected[key], nil
}
func (c *congestionCacheFixture) ReserveCongestionSession(_ context.Context, g int64, k string, from int64, p string, _ time.Duration) (bool, error) {
	c.calls.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	key := congestionFixtureKey(g, k)
	if c.bindings[key] != from || c.pending[key] != "" || c.protected[key] {
		return false, nil
	}
	c.pending[key] = p
	return true, nil
}
func (c *congestionCacheFixture) CommitCongestionSession(_ context.Context, g int64, k, p string, from, to int64, _, _ time.Duration, legacy string, _ time.Duration) (bool, error) {
	c.calls.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	key := congestionFixtureKey(g, k)
	if c.bindings[key] != from || c.pending[key] != p {
		return false, nil
	}
	c.bindings[key] = to
	c.protected[key] = true
	delete(c.pending, key)
	if legacy != "" {
		c.bindings[congestionFixtureKey(g, legacy)] = to
	}
	return true, nil
}
func (c *congestionCacheFixture) ReleaseCongestionSession(_ context.Context, g int64, k, p string) error {
	c.calls.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	key := congestionFixtureKey(g, k)
	if c.pending[key] == p {
		delete(c.pending, key)
	}
	return nil
}
func (c *congestionCacheFixture) SetSessionAccountIDIfUnchanged(_ context.Context, g int64, k string, from, to int64, _ time.Duration, legacy string, _ time.Duration) (bool, error) {
	c.calls.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	key := congestionFixtureKey(g, k)
	if c.bindings[key] != from {
		return false, nil
	}
	c.bindings[key] = to
	if legacy != "" {
		c.bindings[congestionFixtureKey(g, legacy)] = to
	}
	return true, nil
}
func (c *congestionCacheFixture) DeleteSessionAccountIDIfUnchanged(_ context.Context, g int64, k string, from int64, legacy string) (bool, error) {
	c.calls.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	key := congestionFixtureKey(g, k)
	if c.bindings[key] != from || c.pending[key] != "" {
		return false, nil
	}
	delete(c.bindings, key)
	if legacy != "" {
		delete(c.bindings, congestionFixtureKey(g, legacy))
	}
	return true, nil
}

// Production Redis decoding / repository queries return a fresh Account per
// read. The older scheduler double returns shared pointers, which incorrectly
// shares the account's request-local lazy model-mapping cache across workers.
type congestionAccountRepo struct {
	schedulerGroupAwareOpenAIAccountRepo
}

func (r congestionAccountRepo) GetByID(ctx context.Context, id int64) (*Account, error) {
	account, err := r.schedulerGroupAwareOpenAIAccountRepo.GetByID(ctx, id)
	if err != nil || account == nil {
		return account, err
	}
	copy := *account
	return &copy, nil
}

func congestionFixture(enabled bool, loadA, loadB int) (*OpenAIGatewayService, *congestionCacheFixture, []Account) {
	resetOpenAIAdvancedSchedulerSettingCacheForTest()
	accounts := []Account{
		{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, Concurrency: 100, GroupIDs: []int64{7}, Priority: 0},
		{ID: 2, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, Concurrency: 100, GroupIDs: []int64{7}, Priority: 1},
	}
	cache := &congestionCacheFixture{bindings: map[string]int64{}, pending: map[string]string{}, protected: map[string]bool{}}
	settings := &SettingService{settingRepo: &openAIAdvancedSchedulerSettingRepoStub{}}
	settings.cacheOpenAICongestionSettings(func() OpenAICongestionSettings { c := DefaultOpenAICongestionSettings(); c.Enabled = enabled; return c }())
	cfg := newSchedulerTestOpenAIWSV2Config()
	cfg.Gateway.Scheduling.LoadBatchEnabled = true
	cfg.Gateway.Scheduling.StickySessionMaxWaiting = 3
	cfg.Gateway.Scheduling.StickySessionWaitTimeout = 120 * time.Second
	cfg.Gateway.Scheduling.FallbackWaitTimeout = 30 * time.Second
	cfg.Gateway.Scheduling.FallbackMaxWaiting = 100
	svc := &OpenAIGatewayService{accountRepo: congestionAccountRepo{schedulerGroupAwareOpenAIAccountRepo{schedulerTestOpenAIAccountRepo{accounts: accounts}}}, cache: cache, settingService: settings, cfg: cfg,
		concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{loadMap: map[int64]*AccountLoadInfo{1: {AccountID: 1, CurrentConcurrency: loadA, LoadRate: loadA}, 2: {AccountID: 2, CurrentConcurrency: loadB, LoadRate: loadB}}, acquireResults: map[int64]bool{1: loadA < 100, 2: loadB < 100}})}
	return svc, cache, accounts
}
func congestionContext(t *testing.T, s *OpenAIGatewayService) context.Context {
	t.Helper()
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/responses", nil)
	cleanup := s.PrepareOpenAICongestionRequest(c)
	t.Cleanup(cleanup)
	return c.Request.Context()
}
func congestionSelect(s *OpenAIGatewayService, ctx context.Context, session string) (*AccountSelectionResult, error) {
	g := int64(7)
	sel, _, err := s.SelectAccountWithSchedulerForCapability(ctx, &g, "", session, "gpt-6-astra", nil, OpenAIUpstreamTransportAny, OpenAIEndpointCapabilityResponses, false, false, true)
	return sel, err
}
func congestionBad(s *OpenAIGatewayService, id int64) {
	s.congestionMonitor.mu.Lock()
	defer s.congestionMonitor.mu.Unlock()
	if s.congestionMonitor.accounts == nil {
		s.congestionMonitor.accounts = map[int64]*congestionAccountHealth{}
	}
	h := &congestionAccountHealth{active: map[*openAICongestionAttempt]struct{}{}, last: time.Now()}
	for i := 0; i < 6; i++ {
		h.samples = append(h.samples, congestionSample{at: time.Now(), bad: true, status: 502, model: "gpt-6-astra"})
	}
	s.congestionMonitor.accounts[id] = h
}
func congestionAssertBinding(t *testing.T, c *congestionCacheFixture, id int64) {
	t.Helper()
	actual, err := c.GetSessionAccountID(context.Background(), 7, "openai:chat")
	require.NoError(t, err)
	require.Equal(t, id, actual)
}

func TestOpenAICongestionDisabledIsOriginalSelection(t *testing.T) {
	for _, load := range []int{20, 80, 100} {
		t.Run(fmt.Sprint(load), func(t *testing.T) {
			s, c, accounts := congestionFixture(false, load, 0)
			congestionBad(s, 1)
			require.NoError(t, c.SetSessionAccountID(context.Background(), 7, "openai:chat", 1, time.Hour))
			ctx := congestionContext(t, s)
			require.Nil(t, congestionRequest(ctx))
			selected, err := congestionSelect(s, ctx, "chat")
			require.NoError(t, err)
			defer releaseLegacySchedulerDecisionSelection(selected)
			g := int64(7)
			original, _, err := s.selectAccountWithSchedulerOnce(ctx, &g, "", "chat", "gpt-6-astra", nil, OpenAIUpstreamTransportAny, OpenAIEndpointCapabilityResponses, "", false, PlatformOpenAI, false, true)
			require.NoError(t, err)
			defer releaseLegacySchedulerDecisionSelection(original)
			require.Equal(t, original.Account.ID, selected.Account.ID)
			require.Equal(t, original.WaitPlan, selected.WaitPlan)
			require.Equal(t, original.Acquired, selected.Acquired)
			forwarded, finish := s.beginCongestionForward(ctx, &accounts[0], nil)
			require.Equal(t, ctx, forwarded)
			sentinel := errors.New("original error")
			require.Same(t, sentinel, finish(nil, sentinel))
			require.Len(t, s.congestionMonitor.accounts[1].samples, 6)
			require.Empty(t, s.congestionMonitor.accounts[1].active)
			require.Zero(t, c.calls.Load())
		})
	}
}

func TestOpenAICongestionHealthyBusySessionStays(t *testing.T) {
	s, c, _ := congestionFixture(true, 80, 10)
	require.NoError(t, c.SetSessionAccountID(context.Background(), 7, "openai:chat", 1, time.Hour))
	sel, err := congestionSelect(s, congestionContext(t, s), "chat")
	require.NoError(t, err)
	defer releaseLegacySchedulerDecisionSelection(sel)
	require.EqualValues(t, 1, sel.Account.ID)
	require.Empty(t, c.pending)
}

func TestOpenAICongestionMigrationCommitsOnlySuccessAndStays(t *testing.T) {
	s, c, accounts := congestionFixture(true, 80, 10)
	congestionBad(s, 1)
	require.NoError(t, c.SetSessionAccountID(context.Background(), 7, "openai:chat", 1, time.Hour))
	old := congestionContext(t, s)
	ot := congestionRequest(old)
	ot.group, ot.session, ot.expected, ot.bound = 7, "openai:chat", 1, true
	ctx := congestionContext(t, s)
	sel, err := congestionSelect(s, ctx, "chat")
	require.NoError(t, err)
	defer releaseLegacySchedulerDecisionSelection(sel)
	require.EqualValues(t, 2, sel.Account.ID)
	congestionAssertBinding(t, c, 1)
	// Handler eager binding must not publish an unproven migration.
	g := int64(7)
	require.NoError(t, s.BindStickySessionAfterProfitAdmission(ctx, &g, "chat", 2))
	congestionAssertBinding(t, c, 1)
	follower := congestionContext(t, s)
	second, err := congestionSelect(s, follower, "chat")
	require.NoError(t, err)
	defer releaseLegacySchedulerDecisionSelection(second)
	require.EqualValues(t, 2, second.Account.ID)
	forward, finish := s.beginCongestionForward(ctx, &accounts[1], nil)
	observeCongestionEvent(forward, []byte(`{"type":"response.created"}`), "response.created")
	require.NoError(t, finish(&OpenAIForwardResult{}, nil))
	congestionAssertBinding(t, c, 2)
	require.NoError(t, s.setStickySessionAccountID(old, &g, "chat", 1, time.Hour))
	require.NoError(t, s.deleteStickySessionAccountID(old, &g, "chat"))
	congestionAssertBinding(t, c, 2)
	// A recovered source must not pull the session back, including after protection.
	s.congestionMonitor.mu.Lock()
	delete(s.congestionMonitor.accounts, 1)
	s.congestionMonitor.mu.Unlock()
	for _, protected := range []bool{true, false} {
		c.protected[congestionFixtureKey(7, "openai:chat")] = protected
		next, err := congestionSelect(s, congestionContext(t, s), "chat")
		require.NoError(t, err)
		require.EqualValues(t, 2, next.Account.ID)
		releaseLegacySchedulerDecisionSelection(next)
	}
}

func TestOpenAICongestionFailureAndDisableNeverCommit(t *testing.T) {
	for _, disable := range []bool{false, true} {
		t.Run(fmt.Sprint(disable), func(t *testing.T) {
			s, c, a := congestionFixture(true, 80, 0)
			congestionBad(s, 1)
			require.NoError(t, c.SetSessionAccountID(context.Background(), 7, "openai:chat", 1, time.Hour))
			ctx := congestionContext(t, s)
			sel, err := congestionSelect(s, ctx, "chat")
			require.NoError(t, err)
			defer releaseLegacySchedulerDecisionSelection(sel)
			_, finish := s.beginCongestionForward(ctx, &a[1], nil)
			sentinel := errors.New("original failed forward")
			if disable {
				s.settingService.cacheOpenAICongestionSettings(DefaultOpenAICongestionSettings())
				require.NoError(t, finish(&OpenAIForwardResult{}, nil))
			} else {
				require.Same(t, sentinel, finish(nil, sentinel))
			}
			congestionAssertBinding(t, c, 1)
			require.Empty(t, c.pending)
		})
	}
}

func TestOpenAICongestionFullShortWaitAndAllBusyFallback(t *testing.T) {
	for _, b := range []int{10, 100} {
		t.Run(fmt.Sprint(b), func(t *testing.T) {
			s, c, _ := congestionFixture(true, 100, b)
			require.NoError(t, c.SetSessionAccountID(context.Background(), 7, "openai:chat", 1, time.Hour))
			start := time.Now()
			sel, err := congestionSelect(s, congestionContext(t, s), "chat")
			require.NoError(t, err)
			defer releaseLegacySchedulerDecisionSelection(sel)
			if b == 10 {
				require.EqualValues(t, 2, sel.Account.ID)
				require.GreaterOrEqual(t, time.Since(start), 2*time.Second)
			} else {
				require.EqualValues(t, 1, sel.Account.ID)
				require.NotNil(t, sel.WaitPlan)
				require.Equal(t, 120*time.Second, sel.WaitPlan.Timeout)
				require.Less(t, time.Since(start), time.Second)
				require.Empty(t, c.pending)
			}
		})
	}
}

func TestOpenAICongestionOneAccountAndUnsafeContinuation(t *testing.T) {
	s, c, a := congestionFixture(true, 100, 0)
	s.accountRepo = schedulerTestOpenAIAccountRepo{accounts: a[:1]}
	require.NoError(t, c.SetSessionAccountID(context.Background(), 7, "openai:chat", 1, time.Hour))
	sel, err := congestionSelect(s, congestionContext(t, s), "chat")
	require.NoError(t, err)
	require.EqualValues(t, 1, sel.Account.ID)
	require.NotNil(t, sel.WaitPlan)
	require.Empty(t, c.pending)
	s, c, _ = congestionFixture(true, 80, 0)
	congestionBad(s, 1)
	ctx := congestionContext(t, s)
	g := int64(7)
	_, _, _, handled := s.selectWithCongestion(ctx, &g, "resp_owned_by_A", "chat", "gpt-6-astra", nil, OpenAIUpstreamTransportAny, OpenAIEndpointCapabilityResponses, "", false, PlatformOpenAI, true, true)
	require.False(t, handled)
	require.Zero(t, c.calls.Load())
}

func TestOpenAICongestionNewSessionAndCandidateEligibility(t *testing.T) {
	s, _, _ := congestionFixture(true, 80, 0)
	congestionBad(s, 1)
	sel, err := congestionSelect(s, congestionContext(t, s), "new-chat")
	require.NoError(t, err)
	require.EqualValues(t, 2, sel.Account.ID)
	releaseLegacySchedulerDecisionSelection(sel)
	for _, kind := range []string{"different_group", "disabled", "unsupported_model", "api_key"} {
		t.Run(kind, func(t *testing.T) {
			s, c, a := congestionFixture(true, 80, 0)
			congestionBad(s, 1)
			switch kind {
			case "different_group":
				a[1].GroupIDs = []int64{9}
			case "disabled":
				a[1].Schedulable = false
			case "unsupported_model":
				a[1].Credentials = map[string]any{"model_mapping": map[string]any{"gpt-other": "gpt-other"}}
			case "api_key":
				a[1].Type = AccountTypeAPIKey
			}
			require.NoError(t, c.SetSessionAccountID(context.Background(), 7, "openai:chat", 1, time.Hour))
			sel, err := congestionSelect(s, congestionContext(t, s), "chat")
			require.NoError(t, err)
			defer releaseLegacySchedulerDecisionSelection(sel)
			require.EqualValues(t, 1, sel.Account.ID)
			require.Empty(t, c.pending)
		})
	}
}

func TestOpenAICongestionHealthDoesNotMistakeLongOutputForStall(t *testing.T) {
	s, _, a := congestionFixture(true, 90, 0)
	ctx := congestionContext(t, s)
	var finishes []func(*OpenAIForwardResult, error) error
	var attempts []*openAICongestionAttempt
	for i := 0; i < 5; i++ {
		forward, finish := s.beginCongestionForward(ctx, &a[0], nil)
		attempt := forward.Value(congestionAttemptKey{}).(*openAICongestionAttempt)
		attempt.started = time.Now().Add(-2 * time.Minute)
		attempts = append(attempts, attempt)
		finishes = append(finishes, finish)
	}
	require.Equal(t, "stalled_upstream_requests", s.congestionMonitor.snapshot(1, "", 90, time.Now(), DefaultOpenAICongestionSettings()).reason)
	for _, attempt := range attempts {
		attempt.started = time.Now().Add(-time.Second)
		observeCongestionEvent(context.WithValue(ctx, congestionAttemptKey{}, attempt), nil, "")
	}
	require.Empty(t, s.congestionMonitor.snapshot(1, "", 90, time.Now(), DefaultOpenAICongestionSettings()).reason)
	for _, finish := range finishes {
		require.NoError(t, finish(&OpenAIForwardResult{}, nil))
	}
	require.Empty(t, s.congestionMonitor.accounts[1].active)
}

func TestOpenAICongestionConcurrentSameSessionOneDestination(t *testing.T) {
	s, c, a := congestionFixture(true, 80, 0)
	congestionBad(s, 1)
	require.NoError(t, c.SetSessionAccountID(context.Background(), 7, "openai:chat", 1, time.Hour))
	const n = 100
	contexts := make([]context.Context, n)
	for i := range contexts {
		contexts[i] = congestionContext(t, s)
	}
	var wg sync.WaitGroup
	results := make(chan error, n)
	start := make(chan struct{})
	for _, ctx := range contexts {
		wg.Add(1)
		go func(ctx context.Context) {
			defer wg.Done()
			<-start
			sel, err := congestionSelect(s, ctx, "chat")
			if err != nil {
				results <- err
				return
			}
			defer releaseLegacySchedulerDecisionSelection(sel)
			if sel.Account.ID != 2 {
				results <- fmt.Errorf("selected %d", sel.Account.ID)
				return
			}
			_, finish := s.beginCongestionForward(ctx, &a[1], nil)
			if err := finish(&OpenAIForwardResult{}, nil); err != nil {
				results <- err
			}
		}(ctx)
	}
	close(start)
	wg.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}
	congestionAssertBinding(t, c, 2)
	require.Empty(t, c.pending)
}
