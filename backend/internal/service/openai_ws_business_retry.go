package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const openAIWSBusinessRetryKey = "openai_ws_business_retry"

// This state belongs to one HTTP response, never to a pooled WS connection.
type openAIWSBusinessRetryState struct {
	config           OpenAIUpstream5xxRetryConfig
	startedAt        time.Time
	firstFrameAt     time.Time
	capacityFirstAt  time.Time
	outputStarted    bool
	outputEvent      string
	managed          bool
	clientResponseID string
	createdSent      bool
	inProgressSent   bool
	hasSequence      bool
	sequence         int64
	terminalEvent    string
}

func openAIWSBusinessRetryStateFrom(c *gin.Context) *openAIWSBusinessRetryState {
	if c == nil {
		return nil
	}
	value, _ := c.Get(openAIWSBusinessRetryKey)
	state, _ := value.(*openAIWSBusinessRetryState)
	return state
}

func (s *OpenAIGatewayService) openAIWSBusinessRetryState(c *gin.Context, account *Account, stream bool, start time.Time) *openAIWSBusinessRetryState {
	if state := openAIWSBusinessRetryStateFrom(c); state != nil && isOpenAIUpstream5xxRetryAccount(account) {
		return state
	}
	if c == nil || c.Request == nil || !stream || openAIWSChatBridgeFromContext(c) != nil ||
		!strings.HasSuffix(strings.TrimRight(c.Request.URL.Path, "/"), "/responses") ||
		!isOpenAIUpstream5xxRetryAccount(account) || !ForceUpstreamWSEnabledForGroup(getOpenAIGroupIDFromContext(c)) {
		return nil
	}
	config := OpenAIUpstream5xxRetrySettings()
	if !config.Enabled || config.Total <= 0 || !hasEnabledOpenAIUpstream5xxRetryRule(config.Rules) {
		return nil
	}
	state := &openAIWSBusinessRetryState{config: config, startedAt: start}
	c.Set(openAIWSBusinessRetryKey, state)
	return state
}

// Only these two provider messages opt in. A numeric 502/503 or a generic
// server_error is insufficient, including locally mapped transport errors.
func openAIUpstream5xxBusinessErrorStatus(payload []byte) int {
	return MatchOpenAIUpstream5xxRetryRules(payload, nil).StatusCode
}

func openAIWSRetryMetadata(payload []byte) bool {
	switch gjson.GetBytes(payload, "type").String() {
	case "codex.rate_limits", "codex.response.metadata":
		return true
	case "response.created", "response.in_progress":
		output := gjson.GetBytes(payload, "response.output")
		return (!output.Exists() || (output.IsArray() && len(output.Array()) == 0)) &&
			gjson.GetBytes(payload, "response.error").Type == gjson.Null
	default:
		return false
	}
}

// The first attempt is byte-preserving. After a hidden failure, keep the
// already advertised response identity and monotonically increasing sequence.
func (state *openAIWSBusinessRetryState) prepareFrame(payload []byte) []byte {
	if state == nil {
		return payload
	}
	eventType := gjson.GetBytes(payload, "type").String()
	if state.managed && ((eventType == "response.created" && state.createdSent) ||
		(eventType == "response.in_progress" && state.inProgressSent)) && openAIWSRetryMetadata(payload) {
		return nil
	}
	id := gjson.GetBytes(payload, "response.id").String()
	if state.clientResponseID == "" {
		state.clientResponseID = id
	}
	if state.managed && state.clientResponseID != "" {
		if id != "" && id != state.clientResponseID {
			payload, _ = sjson.SetBytes(payload, "response.id", state.clientResponseID)
		}
		if gjson.GetBytes(payload, "response_id").Exists() {
			payload, _ = sjson.SetBytes(payload, "response_id", state.clientResponseID)
		}
	}
	if seq := gjson.GetBytes(payload, "sequence_number"); seq.Exists() {
		if state.managed && state.hasSequence {
			payload, _ = sjson.SetBytes(payload, "sequence_number", state.sequence+1)
		}
		state.sequence = gjson.GetBytes(payload, "sequence_number").Int()
		state.hasSequence = true
	}
	state.createdSent = state.createdSent || eventType == "response.created"
	state.inProgressSent = state.inProgressSent || eventType == "response.in_progress"
	if isOpenAIWSTerminalEvent(eventType) {
		state.terminalEvent = eventType
	}
	if !openAIWSRetryMetadata(payload) && !state.outputStarted {
		state.outputStarted = true
		state.outputEvent = eventType
	}
	return payload
}

func (s *OpenAIGatewayService) newOpenAIWSBusinessRetryError(c *gin.Context, state *openAIWSBusinessRetryState, account *Account, requestID string, payload []byte, model string, headers http.Header) *UpstreamFailoverError {
	if state == nil {
		return nil
	}
	match := MatchOpenAIUpstream5xxRetryRules(payload, state.config.Rules)
	if !match.Matched {
		return nil
	}
	status := match.StatusCode
	message := extractOpenAISSEErrorMessage(payload)
	failure := s.newOpenAIStreamFailoverErrorWithModel(c, account, true, requestID, payload, message, model, headers)
	failure.StatusCode = status
	failure.ResponseBody = append([]byte(nil), payload...)
	failure.ClientStatusCode = status
	failure.ClientMessage = sanitizeUpstreamErrorMessage(message)
	failure.Stage = GatewayFailureStageInference
	failure.Scope = GatewayFailureScopeProvider
	failure.OpenAIUpstream5xxRetry = &state.config
	failure.OpenAIUpstream5xxRuleID = match.RuleID
	failure.OpenAIUpstream5xxRuleName = match.RuleName
	failure.RetryableOnSameAccount = state.config.SameAccount > 0
	failure.RequestScopedTransient = true
	failure.SameAccountRetryMax = state.config.SameAccount
	failure.SameAccountRetryDelay = state.config.Delay
	failure.SameAccountRetryDeadline = time.Time{}
	failure.OpenAIWSRetryAfterMetadata = !state.outputStarted
	state.managed = true
	if status == 503 && state.capacityFirstAt.IsZero() {
		state.capacityFirstAt = time.Now()
	}
	if state.outputStarted {
		failure.NextAccountAction = NextAccountStop
	}
	return failure
}

// Prepare leaves the count unchanged; BeginOpenAIUpstream5xxUsageAttempt commits it.
func PrepareOpenAIUpstream5xxBusinessRetry(c *gin.Context, account *Account, failure *UpstreamFailoverError, model string) (retry, sameAccount bool, delay time.Duration, reason string) {
	if !IsOpenAIUpstream5xxRetryOwned(failure, account) {
		return false, false, 0, "not_owned"
	}
	if c.Request.Context().Err() != nil {
		reason = "client_disconnected"
	} else if !failure.OpenAIWSRetryAfterMetadata {
		reason = "output_started"
		if state := openAIWSBusinessRetryStateFrom(c); state != nil {
			reason += ":" + state.outputEvent
		}
	}
	tracker := openAIUpstream5xxRetryTrackerFrom(c)
	tracker.mu.Lock()
	used, sameUsed := tracker.intercepts, tracker.sameAccountAttempts[account.ID]
	tracker.mu.Unlock()
	config := failure.OpenAIUpstream5xxRetry
	if reason == "" && used >= config.Total {
		reason = "retry_count_exhausted"
	}
	if state := openAIWSBusinessRetryStateFrom(c); reason == "" && state != nil && !state.capacityFirstAt.IsZero() {
		if budget := openAICapacityShedBudget(); budget > 0 && time.Since(state.capacityFirstAt)+config.Delay >= budget {
			reason = "retry_time_exhausted"
		}
	}
	if reason != "" {
		stopOpenAIUpstream5xxRetry(c, reason)
		RecordOpenAIUpstream5xxRetrySkipped(c, account, failure.StatusCode, model, reason)
		return false, false, 0, reason
	}
	sameAccount = sameUsed < config.SameAccount
	nextSame := 0
	if sameAccount {
		nextSame = sameUsed + 1
	}
	NoteOpenAIUpstream5xxRetryIntercept(c, account, failure, model, nextSame, used, config.Delay)
	return true, sameAccount, config.Delay, ""
}

func OpenAIUpstream5xxRetryOutputStopReason(c *gin.Context) string {
	reason := "output_started"
	if state := openAIWSBusinessRetryStateFrom(c); state != nil && state.outputEvent != "" {
		reason += ":" + state.outputEvent
	}
	return reason
}

func OpenAIUpstream5xxRetryBudgetAllowsStart(c *gin.Context) bool {
	state := openAIWSBusinessRetryStateFrom(c)
	if state == nil || !state.managed {
		return true
	}
	if c.Request.Context().Err() != nil {
		stopOpenAIUpstream5xxRetry(c, "client_disconnected")
		return false
	}
	if !state.capacityFirstAt.IsZero() {
		if budget := openAICapacityShedBudget(); budget > 0 && time.Since(state.capacityFirstAt) >= budget {
			stopOpenAIUpstream5xxRetry(c, "retry_time_exhausted")
			return false
		}
	}
	return true
}

// Revalidate the same account and preserve its selection's profit gate. The
// normal handler admission still acquires its concurrency slot on each attempt.
func (s *OpenAIGatewayService) SelectOpenAIUpstream5xxRetryAccount(ctx context.Context, previous *AccountSelectionResult, groupID *int64, model string, capability OpenAIEndpointCapability, compact bool) (*AccountSelectionResult, error) {
	if previous == nil || previous.Account == nil {
		return nil, ErrNoAvailableAccounts
	}
	ctx = ContextWithSelectionProfitGate(ctx, previous)
	account := s.recheckSelectedOpenAIAccountFromDB(ctx, previous.Account, groupID, PlatformOpenAI, model, compact, capability)
	if account == nil || !account.IsSchedulable() || !s.openAIAccountMatchesSchedulingGroup(account, groupID) {
		return nil, ErrNoAvailableAccounts
	}
	selection := *previous
	selection.Account, selection.Acquired, selection.ReleaseFunc = account, false, nil
	cfg := s.schedulingConfig()
	selection.WaitPlan = &AccountWaitPlan{AccountID: account.ID, MaxConcurrency: account.Concurrency, Timeout: cfg.StickySessionWaitTimeout, MaxWaiting: cfg.StickySessionMaxWaiting}
	return &selection, nil
}

func OpenAIUpstream5xxRetrySwitchPendingAccount(c *gin.Context) {
	if tracker := existingOpenAIUpstream5xxRetryTracker(c); tracker != nil {
		tracker.mu.Lock()
		if tracker.pending != nil {
			tracker.pending.SameAccountAttempt = 0
		}
		tracker.mu.Unlock()
	}
}

func WriteOpenAIWSBusinessRetryFailure(c *gin.Context, status int, errType, code, message string, countTowardsSLA bool) bool {
	state := openAIWSBusinessRetryStateFrom(c)
	if state == nil || !state.managed || c.Writer == nil || !c.Writer.Written() {
		return false
	}
	if state.terminalEvent == "response.completed" || state.terminalEvent == "response.done" {
		return true
	}
	if tracker := existingOpenAIUpstream5xxRetryTracker(c); tracker != nil {
		tracker.mu.Lock()
		if tracker.stopReason == "" {
			tracker.stopReason = "upstream_error"
		}
		if tracker.finalMessage == "" {
			tracker.finalMessage = sanitizeUpstreamErrorMessage(message)
		}
		tracker.mu.Unlock()
	}
	if countTowardsSLA {
		MarkOpsStreamFailure(c, errType, code, message, status)
	} else {
		MarkOpsStreamError(c, errType, message, status)
	}
	MarkOpenAIUpstream5xxRetryCompleted(c, time.Now())
	if state.terminalEvent != "" {
		return true
	}
	if code == "" {
		code = errType
	}
	response := map[string]any{"status": "failed", "output": []any{}, "error": map[string]any{"type": errType, "code": code, "message": message, "status_code": status}}
	if state.clientResponseID != "" {
		response["id"] = state.clientResponseID
	}
	payload, _ := json.Marshal(map[string]any{"type": "response.failed", "response": response, "sequence_number": state.sequence + 1})
	payload = state.prepareFrame(payload)
	_, _ = c.Writer.Write(append(append([]byte("data: "), payload...), '\n', '\n'))
	c.Writer.Flush()
	return true
}

func OpenAIWSBusinessRetryActive(c *gin.Context) bool {
	return openAIWSBusinessRetryStateFrom(c) != nil
}

func StopOpenAIUpstream5xxBusinessRetry(c *gin.Context, reason string) {
	stopOpenAIUpstream5xxRetry(c, reason)
}

func openAIWSBusinessTurnStateKey(c *gin.Context, state string) string {
	seed := openAICodexTurnStateSeed(c)
	if seed == "" || state == "" {
		return ""
	}
	return fmt.Sprintf("%s\x00ws:%x", seed, sha256.Sum256([]byte(state)))
}

func (s *OpenAIGatewayService) noteOpenAIWSBusinessTurnState(c *gin.Context, account *Account, value string) {
	key := openAIWSBusinessTurnStateKey(c, value)
	if key == "" {
		return
	}
	s.openaiCodexTurnStateOrigins.Store(key, openAICodexTurnStateOrigin{accountID: account.ID, expiresAt: time.Now().Add(s.openAIWSSessionStickyTTL())})
	s.sweepOpenAICodexTurnStateOrigins()
}

func (s *OpenAIGatewayService) guardOpenAIWSBusinessTurnState(c *gin.Context, account *Account, value string) string {
	if key := openAIWSBusinessTurnStateKey(c, value); key != "" {
		if raw, ok := s.openaiCodexTurnStateOrigins.Load(key); ok {
			if origin, valid := raw.(openAICodexTurnStateOrigin); valid && time.Now().Before(origin.expiresAt) && origin.accountID != account.ID {
				return ""
			}
		}
	}
	return value
}
