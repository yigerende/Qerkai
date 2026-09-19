package service

import (
	"context"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/tidwall/gjson"
)

type openAIStateTicketKey struct{}

const openAIStateUsageTicketKey = "openai_state_usage_ticket"

// Keep only the current upstream attempt; a previous account or retry must
// not mark the final usage record as injected.
func resetOpenAIStateUsage(c *gin.Context) {
	if c != nil {
		if _, exists := c.Get(openAIStateUsageTicketKey); exists {
			c.Set(openAIStateUsageTicketKey, (*openAIStateTicket)(nil))
		}
	}
}

// SnapshotOpenAIStateUsage runs before asynchronous usage recording. It does
// not retain gin.Context or copy the sensitive State value into usage logs.
func SnapshotOpenAIStateUsage(c *gin.Context, result *OpenAIForwardResult) {
	if c == nil || result == nil {
		return
	}
	value, _ := c.Get(openAIStateUsageTicketKey)
	ticket, _ := value.(*openAIStateTicket)
	result.StateInjected = ticket != nil && ticket.sent.Load()
}

type openAIStateTicket struct {
	keeper         *OpenAIStateKeeperService
	config         *openAIStateKeeperConfig
	accountID      int64
	model          string
	version        string
	value          string
	id             string
	transport      string
	sent           atomic.Bool
	suppressed     atomic.Bool
	responseLength atomic.Int64
	responseStatus atomic.Int64
	originalHeader []string
}

type openAIStateObservation struct {
	ticket *openAIStateTicket
	at     time.Time
	length int
	status int
	sent   bool
}

func (s *OpenAIStateKeeperService) ticketFor(c *gin.Context, a *Account, model, transport string) *openAIStateTicket {
	if s == nil {
		return nil
	}
	cfg := s.config.Load()
	if cfg == nil || !cfg.Enabled || !cfg.InjectionEnabled {
		return nil
	}
	if !stateKeeperAccountEligible(a) || !cfg.includesCollectionAccount(a) || !cfg.models[model] || c == nil || c.Request == nil || c.GetBool(openAIStateProbeContextKey) {
		return nil
	}
	path := strings.TrimRight(c.Request.URL.Path, "/")
	if !strings.HasSuffix(path, "/responses") && !strings.HasSuffix(path, "/chat/completions") {
		return nil
	}
	if !cfg.AllGroups && !cfg.groups[getOpenAIGroupIDFromContext(c)] {
		return nil
	}
	return s.accountTicket(cfg, a, model, transport)
}

func (s *OpenAIStateKeeperService) accountTicket(cfg *openAIStateKeeperConfig, a *Account, model, transport string) *openAIStateTicket {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e := s.entryLocked(a.ID, model)
	if s.config.Load() != cfg || e == nil || e.scopeLoading || e.row.AccountUnavailable {
		return nil
	}
	value := e.value
	if value != "" && !stateKeeperStateMatchesAccount(cfg.OpenAIStateKeeperSettings, e.credentialStamp, e.identityStamp, a) {
		value = ""
	}
	return &openAIStateTicket{keeper: s, config: cfg, accountID: a.ID, model: model, version: e.version, value: value, id: uuid.NewString(), transport: transport}
}

// Quality probes have no API-key group; use the account's group membership.
func (s *OpenAIStateKeeperService) prepareQualityState(a *Account, model string, headers http.Header) *openAIStateTicket {
	if s == nil || headers == nil {
		return nil
	}
	cfg := s.config.Load()
	if cfg == nil || !cfg.Enabled || !cfg.InjectionEnabled || !stateKeeperAccountEligible(a) || !cfg.includesCollectionAccount(a) || !cfg.models[model] {
		return nil
	}
	allowed := cfg.AllGroups
	for _, id := range a.GroupIDs {
		allowed = allowed || cfg.groups[id]
	}
	if !allowed {
		return nil
	}
	t := s.accountTicket(cfg, a, model, "quality_http")
	if t != nil && t.value != "" {
		headers.Set(openAICodexTurnStateHeader, t.value)
	}
	return t
}

func (s *OpenAIStateKeeperService) valueFor(c *gin.Context, a *Account, model string) string {
	if ticket := s.ticketFor(c, a, model, "http"); ticket != nil {
		return ticket.value
	}
	return ""
}

func (s *OpenAIGatewayService) prepareCollectedStateHTTP(c *gin.Context, a *Account, body []byte, req *http.Request) *http.Request {
	resetOpenAIStateUsage(c)
	if s == nil || req == nil {
		return req
	}
	keeper := s.stateKeeper.Load()
	if keeper == nil {
		return req
	}
	cfg := keeper.config.Load()
	if cfg == nil || !cfg.Enabled || !cfg.InjectionEnabled {
		return req
	}
	t := keeper.ticketFor(c, a, gjson.GetBytes(body, "model").String(), "http")
	if t == nil {
		return req
	}
	c.Set(openAIStateUsageTicketKey, t)
	if t.value != "" {
		t.originalHeader = append([]string(nil), req.Header.Values(openAICodexTurnStateHeader)...)
		req.Header.Set(openAICodexTurnStateHeader, t.value)
	}
	ctx := context.WithValue(req.Context(), openAIStateTicketKey{}, t)
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) {
		if info.Err == nil {
			t.noteSent()
		}
	}})
	return req.WithContext(ctx)
}

func (s *OpenAIGatewayService) prepareCollectedStateWS(c *gin.Context, a *Account, model string, headers http.Header) *openAIStateTicket {
	resetOpenAIStateUsage(c)
	if s == nil || headers == nil {
		return nil
	}
	t := s.stateKeeper.Load().ticketFor(c, a, model, "ws")
	if t != nil {
		c.Set(openAIStateUsageTicketKey, t)
	}
	if t != nil && t.value != "" {
		headers.Set(openAICodexTurnStateHeader, t.value)
	}
	return t
}

func (t *openAIStateTicket) poolVersion() string {
	if t == nil || t.value == "" {
		return ""
	}
	return t.version
}

func (t *openAIStateTicket) poolCurrentCheck() func() bool {
	if t == nil || t.value == "" {
		return nil
	}
	return func() bool {
		s := t.keeper
		if s.config.Load() != t.config {
			return false
		}
		s.mu.RLock()
		defer s.mu.RUnlock()
		e := s.entryLocked(t.accountID, t.model)
		return e != nil && e.version == t.version && e.value == t.value
	}
}

func (t *openAIStateTicket) noteSent() {
	if t == nil || t.value == "" || t.suppressed.Load() || !t.sent.CompareAndSwap(false, true) {
		return
	}
	t.enqueue(openAIStateObservation{ticket: t, at: time.Now().UTC(), sent: true, length: int(t.responseLength.Load()), status: int(t.responseStatus.Load())})
}

func (t *openAIStateTicket) observe(value string, status int) {
	if t == nil || !t.config.ResponseRefreshEnabled || t.keeper.config.Load() != t.config || !validCollectedState(value) {
		return
	}
	t.responseLength.Store(int64(len(value)))
	t.responseStatus.Store(int64(status))
	t.enqueue(openAIStateObservation{ticket: t, at: time.Now().UTC(), length: len(value), status: status})
}

func (t *openAIStateTicket) enqueue(event openAIStateObservation) {
	if cfg := t.keeper.config.Load(); cfg != t.config || !cfg.Enabled || !cfg.InjectionEnabled || (!event.sent && !cfg.ResponseRefreshEnabled) {
		return
	}
	select {
	case t.keeper.observations <- event:
		return
	default:
	}
	if event.sent || !t.config.isDegradedLength(event.length) {
		return
	}
	// Keep one refresh signal per account when observation traffic saturates
	// the history queue. Business requests never wait for disk or collection.
	s := t.keeper
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.entryLocked(t.accountID, t.model); s.config.Load() == t.config && e != nil && e.version == t.version {
		s.pendingSignals[s.key(t.accountID, t.model)] = event
		select {
		case s.signalWake <- struct{}{}:
		default:
		}
	}
}

func (s *OpenAIStateKeeperService) processPendingSignals() {
	s.mu.Lock()
	pending := s.pendingSignals
	s.pendingSignals = make(map[openAIStateKey]openAIStateObservation)
	s.mu.Unlock()
	for _, event := range pending {
		s.processObservation(event)
	}
}

func (s *OpenAIStateKeeperService) processObservation(o openAIStateObservation) {
	t := o.ticket
	if t == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg, e := s.config.Load(), s.entryLocked(t.accountID, t.model)
	if cfg != t.config || !cfg.Enabled || !cfg.InjectionEnabled || e == nil {
		return
	}
	if o.sent {
		e.row.Injections++
		event := OpenAIStateKeeperEvent{ID: t.id, At: o.at, AccountID: t.accountID, Model: t.model, Kind: "injection", Source: t.transport, InjectedLength: len(t.value), TurnStateLength: o.length, HTTPStatus: o.status, Result: "sent", Message: "已携带 State 发送，恢复情况以降智检测为准"}
		if cfg.ResponseRefreshEnabled && cfg.isDegradedLength(o.length) {
			event.Result, event.Message = "degraded_signal", "返回长度命中降智配置"
		}
		s.appendEventLocked(e, event)
		return
	}
	if !cfg.ResponseRefreshEnabled {
		return
	}
	degraded := cfg.isDegradedLength(o.length)
	for i := range e.injections {
		if e.injections[i].ID != t.id {
			continue
		}
		e.injections[i].TurnStateLength, e.injections[i].HTTPStatus = o.length, o.status
		if degraded {
			e.injections[i].Result, e.injections[i].Message = "degraded_signal", "返回长度命中降智配置"
		} else {
			e.injections[i].Message = "已取得返回响应头；恢复情况以降智检测为准"
		}
		s.dirtyRuntime[s.key(t.accountID, t.model)] = true
		break
	}
	// The response only signals background work. It does not consume the body,
	// alter downstream output, or re-send the user's business request.
	if !degraded || e.row.Paused || e.version != t.version {
		return
	}
	refreshKey := t.version
	if refreshKey == "" {
		refreshKey = "no-state"
	}
	if e.refreshVersion == refreshKey || e.row.Collecting || e.row.Queued {
		return
	}
	if err := s.enqueueSourceLocked(cfg, []int64{t.accountID}, "response", t.model); err == nil && e.row.Queued {
		e.refreshVersion = refreshKey
		s.dirtyRuntime[s.key(t.accountID, t.model)] = true
	}
}

func (s *OpenAIStateKeeperService) observationWorker() {
	defer s.wg.Done()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	flush := func() {
		s.mu.Lock()
		ids := make([]openAIStateKey, 0, len(s.dirtyRuntime))
		for id := range s.dirtyRuntime {
			ids = append(ids, id)
		}
		s.dirtyRuntime = make(map[openAIStateKey]bool)
		s.mu.Unlock()
		for _, id := range ids {
			if err := s.persistRuntime(id.accountID, id.model); err != nil {
				s.mu.Lock()
				s.dirtyRuntime[id] = true
				s.mu.Unlock()
			}
		}
	}
	for {
		select {
		case <-s.ctx.Done():
			for {
				select {
				case event := <-s.observations:
					s.processObservation(event)
				default:
					s.processPendingSignals()
					flush()
					return
				}
			}
		case event := <-s.observations:
			s.processObservation(event)
		case <-s.signalWake:
			s.processPendingSignals()
		case <-ticker.C:
			flush()
		}
	}
}
