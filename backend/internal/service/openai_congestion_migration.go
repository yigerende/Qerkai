package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/tidwall/gjson"
)

const (
	congestionFullWait   = 2 * time.Second
	congestionProtection = 2 * time.Minute
	congestionLeaseTTL   = 10 * time.Minute
	congestionSlowFrame  = 60 * time.Second
)

type congestionRequestKey struct{}
type congestionAttemptKey struct{}

type congestionMigration struct {
	From  int64  `json:"from"`
	To    int64  `json:"to"`
	Owner string `json:"owner"`
}

type openAICongestionRequest struct {
	s            *OpenAIGatewayService
	owner        string
	group        int64
	session      string
	expected     int64
	bound        bool
	migration    string
	move         congestionMigration
	tried        bool
	deferBinding bool
	races        int
	legacy       string
	allowRebind  bool
	pinned       bool
}

func congestionRequest(ctx context.Context) *openAICongestionRequest {
	t, _ := ctx.Value(congestionRequestKey{}).(*openAICongestionRequest)
	return t
}

func (t *openAICongestionRequest) enabled(ctx context.Context) bool {
	return t != nil && t.s.settingService.openAICongestionEnabled(ctx)
}

// No request state, timers or Redis migration calls are installed when disabled.
func (s *OpenAIGatewayService) PrepareOpenAICongestionRequest(c *gin.Context, body ...[]byte) func() {
	if s == nil || c == nil || c.Request == nil || !s.settingService.openAICongestionEnabled(c.Request.Context()) {
		return func() {}
	}
	if _, ok := s.cache.(OpenAICongestionCache); !ok {
		return func() {}
	}
	t := &openAICongestionRequest{s: s, owner: uuid.NewString()}
	if len(body) > 0 {
		t.pinned = gjson.GetBytes(body[0], "previous_response_id").String() != ""
		gjson.GetBytes(body[0], "input").ForEach(func(_, item gjson.Result) bool {
			if item.Get("type").String() == "item_reference" {
				t.pinned = true
			}
			return !t.pinned
		})
	}
	c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), congestionRequestKey{}, t))
	return func() { t.release() }
}

func (t *openAICongestionRequest) release() {
	if t == nil || t.migration == "" || t.move.Owner != t.owner {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = t.s.cache.(OpenAICongestionCache).ReleaseCongestionSession(ctx, t.group, t.session, t.migration)
	t.migration = ""
}

// Suppress eager candidate bindings; publish a migration only after a successful
// forward. A delayed request may refresh its old binding, but cannot overwrite B.
func (s *OpenAIGatewayService) congestionBind(ctx context.Context, group int64, session string, account int64, ttl time.Duration) (bool, error) {
	t := congestionRequest(ctx)
	if !t.enabled(ctx) {
		return false, nil
	}
	if t.deferBinding {
		return true, nil
	}
	if !t.bound || t.group != group || t.session != session {
		return false, nil
	}
	cache, ok := s.cache.(OpenAICongestionCache)
	if !ok {
		return false, nil
	}
	if t.migration != "" || (t.expected > 0 && account != t.expected && !t.allowRebind) {
		return true, nil
	}
	ok, err := cache.SetSessionAccountIDIfUnchanged(ctx, group, session, t.expected, account, ttl, t.legacy, s.openAIStickyLegacyTTL(ttl))
	if ok {
		t.expected = account
	}
	return true, err
}

type congestionSample struct {
	at    time.Time
	bad   bool
	first time.Duration
}
type congestionAccountHealth struct {
	samples []congestionSample
	active  map[*openAICongestionAttempt]struct{}
	last    time.Time
}
type openAICongestionMonitor struct {
	mu        sync.Mutex
	accounts  map[int64]*congestionAccountHealth
	lastSweep time.Time
}

func (m *openAICongestionMonitor) start(a *openAICongestionAttempt) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.accounts == nil {
		m.accounts = make(map[int64]*congestionAccountHealth)
	}
	now := time.Now()
	if now.Sub(m.lastSweep) > time.Minute {
		for id, h := range m.accounts {
			if len(h.active) == 0 && now.Sub(h.last) > 2*time.Minute {
				delete(m.accounts, id)
			}
		}
		m.lastSweep = now
	}
	h := m.accounts[a.account]
	if h == nil {
		h = &congestionAccountHealth{active: make(map[*openAICongestionAttempt]struct{})}
		m.accounts[a.account] = h
	}
	h.active[a] = struct{}{}
	h.last = now
}

func (m *openAICongestionMonitor) finish(a *openAICongestionAttempt, result *OpenAIForwardResult, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h := m.accounts[a.account]
	if h == nil {
		return
	}
	delete(h.active, a)
	h.last = time.Now()
	if (err != nil && (errors.Is(err, context.Canceled) || (a.client != nil && a.client.Err() != nil))) || (err == nil && result == nil) {
		return
	}
	bad := false
	var failover *UpstreamFailoverError
	var network net.Error
	if errors.As(err, &failover) {
		bad = failover.StatusCode == 429 || failover.StatusCode >= 500
	}
	if errors.As(err, &network) {
		bad = true
	}
	if err != nil && !bad {
		return
	} // client errors and invalid prompts are not congestion.
	first := time.Duration(0)
	if ns := a.firstFrameNS.Load(); ns > 0 {
		first = time.Duration(ns - 1)
	} else if result != nil && result.FirstTokenMs != nil {
		first = time.Duration(*result.FirstTokenMs) * time.Millisecond
	}
	h.samples = append(h.samples, congestionSample{time.Now(), bad, first})
	if len(h.samples) > 64 {
		copy(h.samples, h.samples[len(h.samples)-64:])
		h.samples = h.samples[:64]
	}
}

func (m *openAICongestionMonitor) reason(id int64, load int, now time.Time) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	h := m.accounts[id]
	if h == nil {
		return ""
	}
	n, bad, slow := 0, 0, 0
	for _, v := range h.samples {
		if now.Sub(v.at) <= time.Minute {
			n++
			if v.bad {
				bad++
			}
			if v.first >= 15*time.Second {
				slow++
			}
		}
	}
	if n >= 5 && bad >= 3 && bad*100/n >= 30 {
		return "repeated_upstream_errors"
	}
	stalled := 0
	for a := range h.active {
		if !a.frame.Load() && now.Sub(a.started) >= congestionSlowFrame {
			stalled++
		}
	}
	if stalled >= 3 && stalled*5 >= len(h.active) {
		return "stalled_upstream_requests"
	}
	if load >= 80 && n >= 5 && slow*100/n >= 30 {
		return "high_load_and_slow_first_frame"
	}
	return ""
}

// One invocation surrounds one real forward, including HTTP -> forced WS.
// Nested format adapters share the attempt instead of double-counting it.
type openAICongestionAttempt struct {
	client       context.Context
	request      *openAICongestionRequest
	account      int64
	started      time.Time
	frame        atomic.Bool
	firstFrameNS atomic.Int64
}

// Observe only. Do not change upstream contexts, timeouts, output or retry policy.
func (s *OpenAIGatewayService) beginCongestionForward(ctx context.Context, account *Account, _ []byte) (context.Context, func(*OpenAIForwardResult, error) error) {
	t := congestionRequest(ctx)
	if !t.enabled(ctx) || account == nil || !account.IsOpenAIOAuth() || ctx.Value(congestionAttemptKey{}) != nil {
		return ctx, func(_ *OpenAIForwardResult, e error) error { return e }
	}
	a := &openAICongestionAttempt{request: t, account: account.ID, started: time.Now(), client: ctx}
	ctx = context.WithValue(ctx, congestionAttemptKey{}, a)
	s.congestionMonitor.start(a)
	return ctx, func(result *OpenAIForwardResult, err error) error {
		s.congestionMonitor.finish(a, result, err)
		if t.migration != "" && t.move.To == account.ID {
			if err == nil && result != nil && t.enabled(ctx) {
				commitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				ok, e := s.cache.(OpenAICongestionCache).CommitCongestionSession(commitCtx, t.group, t.session, t.migration, t.move.From, t.move.To, openaiStickySessionTTL, congestionProtection, t.legacy, s.openAIStickyLegacyTTL(openaiStickySessionTTL))
				if e != nil {
					slog.Warn("openai_congestion_binding_commit_failed", "account_id", account.ID, "error", e)
				}
				if ok {
					slog.Info("openai_congestion_migrated", "group_id", t.group, "from_account_id", t.move.From, "to_account_id", t.move.To)
					t.expected = t.move.To
					t.migration = ""
				}
			} else {
				t.release()
				t.migration = ""
			}
		}
		return err
	}
}

// Read observation preserves all bytes and the original stream/close behavior.
type congestionObservedBody struct {
	io.ReadCloser
	attempt *openAICongestionAttempt
}

func (b *congestionObservedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if n > 0 {
		b.attempt.observeFrame()
	}
	return n, err
}
func (a *openAICongestionAttempt) observeFrame() {
	if a != nil && a.firstFrameNS.Load() == 0 && a.firstFrameNS.CompareAndSwap(0, time.Since(a.started).Nanoseconds()+1) {
		a.frame.Store(true)
	}
}

func observeCongestionEvent(ctx context.Context, _ []byte, _ string) {
	a, _ := ctx.Value(congestionAttemptKey{}).(*openAICongestionAttempt)
	a.observeFrame()
}

// Congestion never replays a request. It only narrows a selection before the
// existing forward, which retains its own output guards and failover budget.
func (s *OpenAIGatewayService) selectWithCongestion(ctx context.Context, groupID *int64, previous, session, model string, excluded map[int64]struct{}, transport OpenAIUpstreamTransport, capability OpenAIEndpointCapability, imageCapability OpenAIImagesCapability, compact bool, platform string, canMove, cost bool) (*AccountSelectionResult, OpenAIAccountScheduleDecision, error, bool) {
	t := congestionRequest(ctx)
	fallback := func() (*AccountSelectionResult, OpenAIAccountScheduleDecision, error, bool) {
		return nil, OpenAIAccountScheduleDecision{}, nil, false
	}
	if !t.enabled(ctx) || t.pinned || NormalizeOpenAICompatiblePlatform(platform) != PlatformOpenAI || previous != "" || imageCapability != "" || compact || preserveOpenAIGuardianParentBinding(ctx, session) || s.resolveOpenAIGuardianParentAccountID(ctx, groupID) > 0 {
		return fallback()
	}
	cache, ok := s.cache.(OpenAICongestionCache)
	if !ok || s.concurrencyService == nil {
		return fallback()
	}
	ctx = s.withOpenAIProfitControlGate(ctx, groupID)
	bound, err := s.getStickySessionAccountID(ctx, groupID, session)
	if err != nil && !errors.Is(err, ErrStickySessionNotFound) {
		return fallback()
	}
	var source *Account
	if bound > 0 {
		source, err = s.getSchedulableAccount(ctx, bound)
		if err != nil || source == nil || !source.IsOpenAIOAuth() {
			t.bound = false
			return fallback()
		}
	}
	key := s.openAISessionCacheKey(session)
	if !t.bound && key != "" {
		t.group, t.session, t.expected, t.bound = derefGroupID(groupID), key, bound, true
		if s.openAISessionHashDualWriteOldEnabled() {
			t.legacy = s.openAILegacySessionCacheKey(ctx, session)
		}
	}
	if _, skip := excluded[bound]; skip {
		t.allowRebind = true
		return fallback()
	}
	pending, protected := "", false
	if key != "" {
		pending, protected, err = cache.ReadCongestionSession(ctx, derefGroupID(groupID), key)
		if err != nil {
			return fallback()
		}
	}
	if protected || (t.tried && pending == "") {
		return fallback()
	}
	// Healthy sticky sessions need only their own cached load. Enumerate alternatives
	// only for an actual congestion signal or a new session.
	reason := ""
	sourceLoad := 0
	if bound > 0 && pending == "" {
		if veto, _ := openAIProfitControlVetoReason(ctx, source); veto {
			return fallback()
		}
		loads, e := s.concurrencyService.GetAccountsLoadBatch(ctx, []AccountWithConcurrency{{ID: bound, MaxConcurrency: source.Concurrency}})
		if e != nil || loads[bound] == nil {
			return fallback()
		}
		sourceLoad = loads[bound].LoadRate
		reason = s.congestionMonitor.reason(bound, sourceLoad, time.Now())
		if sourceLoad >= 100 {
			reason = "concurrency_full"
		}
		if reason == "" {
			return fallback()
		}
	}
	accounts, err := s.listSchedulableAccounts(ctx, groupID, PlatformOpenAI)
	if err != nil {
		return fallback()
	}
	// Always use the original selector for final eligibility, ordering, DB recheck,
	// profit admission and atomic slot acquisition. No separate scheduling policy.
	selectAllowed := func(allowed map[int64]struct{}, keepSession string) (*AccountSelectionResult, OpenAIAccountScheduleDecision, error) {
		x := cloneExcludedAccountIDs(excluded)
		if x == nil {
			x = make(map[int64]struct{})
		}
		for _, a := range accounts {
			if _, ok := allowed[a.ID]; !ok {
				x[a.ID] = struct{}{}
			}
		}
		t.deferBinding = true
		defer func() { t.deferBinding = false }()
		selection, decision, err := s.selectAccountWithSchedulerOnce(ctx, groupID, "", keepSession, model, x, transport, capability, imageCapability, compact, platform, canMove, cost)
		if selection != nil && selection.Account != nil {
			if _, ok := allowed[selection.Account.ID]; !ok {
				if selection.ReleaseFunc != nil {
					selection.ReleaseFunc()
				}
				return nil, decision, ErrNoAvailableAccounts
			}
		}
		return selection, decision, err
	}
	if pending != "" {
		var move congestionMigration
		if json.Unmarshal([]byte(pending), &move) == nil && move.From == bound {
			if _, skip := excluded[move.To]; !skip && t.enabled(ctx) {
				sel, dec, e := selectAllowed(map[int64]struct{}{move.To: {}}, "")
				if e == nil && sel != nil && sel.Account != nil && sel.Account.ID == move.To && sel.Account.IsOpenAIOAuth() {
					t.migration, t.move, t.tried = pending, move, true
					return sel, dec, nil, true
				}
				if sel != nil && sel.ReleaseFunc != nil {
					sel.ReleaseFunc()
				}
			}
		}
		return fallback()
	}
	loads := make([]AccountWithConcurrency, 0, len(accounts))
	for _, a := range accounts {
		loads = append(loads, AccountWithConcurrency{ID: a.ID, MaxConcurrency: a.Concurrency})
	}
	loadMap, err := s.concurrencyService.GetAccountsLoadBatch(ctx, loads)
	if err != nil {
		return fallback()
	}
	targets := make(map[int64]struct{})
	healthy := false
	now := time.Now()
	for i := range accounts {
		a := &accounts[i]
		if _, skip := excluded[a.ID]; skip {
			continue
		}
		if a.ID == bound || !a.IsOpenAIOAuth() {
			continue
		}
		l := loadMap[a.ID]
		if l == nil || l.LoadRate > 50 || s.congestionMonitor.reason(a.ID, l.LoadRate, now) != "" {
			continue
		}
		if !isOpenAICompatibleAccountEligibleForRequest(ctx, a, platform, model, false, capability) || !s.openAIAccountMatchesSchedulingGroup(a, groupID) {
			continue
		}
		if veto, _ := openAIProfitControlVetoReason(ctx, a); veto {
			continue
		}
		if reason == "high_load_and_slow_first_frame" && sourceLoad-l.LoadRate < 25 {
			continue
		}
		targets[a.ID] = struct{}{}
		healthy = true
	}
	if !healthy {
		return fallback()
	} // One account/all congested: original bounded queue.
	if bound == 0 {
		// Unbound sessions preserve priority/cost ordering; only confirmed bad accounts
		// are filtered, and a usable healthy alternative must actually acquire a slot.
		allowed := make(map[int64]struct{}, len(accounts))
		removed := false
		for _, a := range accounts {
			l := loadMap[a.ID]
			if a.IsOpenAIOAuth() && l != nil && s.congestionMonitor.reason(a.ID, l.LoadRate, now) != "" {
				removed = true
				continue
			}
			allowed[a.ID] = struct{}{}
		}
		if !removed || !t.enabled(ctx) {
			return fallback()
		}
		sel, dec, e := selectAllowed(allowed, session)
		if e == nil && sel != nil && sel.Acquired {
			return sel, dec, nil, true
		}
		if sel != nil && sel.ReleaseFunc != nil {
			sel.ReleaseFunc()
		}
		return fallback()
	}
	t.tried = true
	if reason == "concurrency_full" {
		timer := time.NewTimer(congestionFullWait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, OpenAIAccountScheduleDecision{}, ctx.Err(), true
		case <-timer.C:
		}
		fresh, e := s.concurrencyService.GetAccountsLoadBatchFresh(ctx, loads)
		if e != nil || fresh[bound] == nil || fresh[bound].LoadRate < 100 {
			return fallback()
		}
	}
	if !t.enabled(ctx) {
		return fallback()
	}
	sel, dec, e := selectAllowed(targets, "")
	if e != nil || sel == nil || !sel.Acquired {
		if sel != nil && sel.ReleaseFunc != nil {
			sel.ReleaseFunc()
		}
		return fallback()
	}
	if !t.enabled(ctx) {
		if sel.ReleaseFunc != nil {
			sel.ReleaseFunc()
		}
		return fallback()
	}
	move := congestionMigration{From: bound, To: sel.Account.ID, Owner: t.owner}
	payload, _ := json.Marshal(move)
	reserved, e := cache.ReserveCongestionSession(ctx, t.group, key, bound, string(payload), congestionLeaseTTL)
	if e == nil && reserved {
		t.migration, t.move = string(payload), move
		slog.Info("openai_congestion_migration_started", "from_account_id", bound, "to_account_id", sel.Account.ID, "reason", reason)
		return sel, dec, nil, true
	}
	if sel.ReleaseFunc != nil {
		sel.ReleaseFunc()
	}
	if e == nil && t.races < 1 {
		t.races++
		// Re-read the winning binding/lease once, never create a second target.
		return s.selectWithCongestion(ctx, groupID, previous, session, model, excluded, transport, capability, imageCapability, compact, platform, canMove, cost)
	}
	return fallback()
}

func (s *OpenAIGatewayService) congestionDeleteBinding(ctx context.Context, group int64, session string) (bool, error) {
	t := congestionRequest(ctx)
	if !t.enabled(ctx) || !t.bound || t.group != group || t.session != session {
		return false, nil
	}
	ok, err := s.cache.(OpenAICongestionCache).DeleteSessionAccountIDIfUnchanged(ctx, group, session, t.expected, t.legacy)
	if ok {
		t.expected = 0
		t.allowRebind = true
	}
	return true, err
}
