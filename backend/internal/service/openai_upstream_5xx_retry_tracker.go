package service

import (
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/gin-gonic/gin"
)

const openAIUpstream5xxRetryTrackerKey = "openai_upstream_5xx_retry_tracker"

// One ledger supplies the retry panel, usage records and request-wide budget.
// A scheduled retry is committed only when the next forwarding attempt starts.
type openAIUpstream5xxRetryTracker struct {
	mu                  sync.Mutex
	firstInterceptAt    time.Time
	completedAt         time.Time
	intercepts          int
	sameAccountAttempts map[int64]int
	pending             *OpenAIUpstream5xxRetryLogEntry
	lastStatus          int
	lastAccountID       int64
	lastAccountName     string
	lastModel           string
	lastMessage         string
	stopReason          string
	flushed             bool
}

func NoteOpenAIUpstream5xxRetryIntercept(c *gin.Context, account *Account, failoverErr *UpstreamFailoverError, model string, sameAccountAttempt, _ int, retryDelay time.Duration) {
	if c == nil || !IsOpenAIUpstream5xxRetryOwned(failoverErr, account) {
		return
	}
	accountID, accountName := openAIUpstream5xxRetryAccountIdentity(account)
	entry := OpenAIUpstream5xxRetryLogEntry{
		Event: OpenAIUpstream5xxRetryEventIntercepted, StatusCode: failoverErr.StatusCode,
		UpstreamStatus: failoverErr.StatusCode, AccountID: accountID, AccountName: accountName,
		RequestID: openAIUpstream5xxRetryRequestID(c), ClientRequestID: openAIUpstream5xxRetryClientRequestID(c),
		Model: model, Transport: openAIUpstream5xxRetryTransport(c), Path: openAIUpstream5xxRetryPath(c),
		SameAccountAttempt: sameAccountAttempt, SameAccountMax: failoverErr.OpenAIUpstream5xxRetry.SameAccount,
		RetryDelayMS: retryDelay.Milliseconds(), UpstreamMessage: extractOpenAISSEErrorMessage(failoverErr.ResponseBody),
	}
	tracker := openAIUpstream5xxRetryTrackerFrom(c)
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.firstInterceptAt.IsZero() {
		tracker.firstInterceptAt = time.Now()
	}
	tracker.pending = &entry
}

func RecordOpenAIUpstream5xxRetrySkipped(c *gin.Context, account *Account, statusCode int, model, reason string) {
	if !IsOpenAIUpstream5xxRetryCandidate(statusCode, account) {
		return
	}
	accountID, accountName := openAIUpstream5xxRetryAccountIdentity(account)
	recordOpenAIUpstream5xxRetryEvent(OpenAIUpstream5xxRetryLogEntry{
		AtUnixMS: time.Now().UnixMilli(), Event: OpenAIUpstream5xxRetryEventSkipped,
		StatusCode: statusCode, UpstreamStatus: statusCode, AccountID: accountID, AccountName: accountName,
		RequestID: openAIUpstream5xxRetryRequestID(c), ClientRequestID: openAIUpstream5xxRetryClientRequestID(c),
		Model: model, Transport: openAIUpstream5xxRetryTransport(c), Path: openAIUpstream5xxRetryPath(c),
		RetryCount: OpenAIUpstream5xxRetryCount(c), UpstreamMessage: reason, StopReason: reason,
	})
}

func MarkOpenAIUpstream5xxRetryCompleted(c *gin.Context, completedAt time.Time) {
	tracker := existingOpenAIUpstream5xxRetryTracker(c)
	if tracker == nil {
		return
	}
	if completedAt.IsZero() {
		completedAt = time.Now()
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.intercepts > 0 && tracker.completedAt.IsZero() {
		tracker.completedAt = completedAt
	}
}

func stopOpenAIUpstream5xxRetry(c *gin.Context, reason string) {
	if tracker := existingOpenAIUpstream5xxRetryTracker(c); tracker != nil {
		tracker.mu.Lock()
		tracker.stopReason = reason
		tracker.pending = nil
		tracker.mu.Unlock()
	}
	MarkOpenAIUpstream5xxRetryCompleted(c, time.Now())
}

func FlushOpenAIUpstream5xxRetryTracker(c *gin.Context, clientStatus int) {
	tracker := existingOpenAIUpstream5xxRetryTracker(c)
	if tracker == nil {
		return
	}
	if streamErr, ok := GetOpsStreamError(c); ok && streamErr.IntendedStatus > 0 {
		clientStatus = streamErr.IntendedStatus
	}
	if c.Request != nil && c.Request.Context().Err() != nil {
		clientStatus = 499
	}
	if state := openAIWSBusinessRetryStateFrom(c); state != nil &&
		state.terminalEvent != "response.completed" && state.terminalEvent != "response.done" && clientStatus < 400 {
		clientStatus = http.StatusBadGateway
	}
	tracker.mu.Lock()
	if tracker.flushed || tracker.intercepts == 0 {
		tracker.mu.Unlock()
		return
	}
	tracker.flushed = true
	entry := OpenAIUpstream5xxRetryLogEntry{
		AtUnixMS: time.Now().UnixMilli(), StatusCode: clientStatus, UpstreamStatus: tracker.lastStatus,
		AccountID: tracker.lastAccountID, AccountName: tracker.lastAccountName, Model: tracker.lastModel,
		Attempt: tracker.intercepts, RetryCount: tracker.intercepts, UpstreamMessage: tracker.lastMessage,
		StopReason: tracker.stopReason,
	}
	endAt := tracker.completedAt
	if endAt.IsZero() {
		endAt = time.Now()
	}
	if !tracker.firstInterceptAt.IsZero() && endAt.After(tracker.firstInterceptAt) {
		entry.ExtraLatencyMS = endAt.Sub(tracker.firstInterceptAt).Milliseconds()
	}
	tracker.mu.Unlock()
	entry.RequestID = openAIUpstream5xxRetryRequestID(c)
	entry.ClientRequestID = openAIUpstream5xxRetryClientRequestID(c)
	entry.Transport = openAIUpstream5xxRetryTransport(c)
	entry.Path = openAIUpstream5xxRetryPath(c)
	entry.Event = OpenAIUpstream5xxRetryEventExhausted
	if clientStatus >= 200 && clientStatus < 300 {
		entry.Event = OpenAIUpstream5xxRetryEventSucceeded
	}
	recordOpenAIUpstream5xxRetryEvent(entry)
}

func existingOpenAIUpstream5xxRetryTracker(c *gin.Context) *openAIUpstream5xxRetryTracker {
	if c == nil {
		return nil
	}
	raw, _ := c.Get(openAIUpstream5xxRetryTrackerKey)
	tracker, _ := raw.(*openAIUpstream5xxRetryTracker)
	return tracker
}

func openAIUpstream5xxRetryTrackerFrom(c *gin.Context) *openAIUpstream5xxRetryTracker {
	if c == nil {
		return nil
	}
	if tracker := existingOpenAIUpstream5xxRetryTracker(c); tracker != nil {
		return tracker
	}
	tracker := &openAIUpstream5xxRetryTracker{sameAccountAttempts: make(map[int64]int)}
	c.Set(openAIUpstream5xxRetryTrackerKey, tracker)
	return tracker
}

func openAIUpstream5xxRetryAccountIdentity(account *Account) (int64, string) {
	if account == nil {
		return 0, ""
	}
	return account.ID, strings.TrimSpace(account.Name)
}

func openAIUpstream5xxRetryTransport(c *gin.Context) string { return GetOpenAIUpstreamTransport(c) }

func openAIUpstream5xxRetryPath(c *gin.Context) string {
	if c == nil || c.Request == nil || c.Request.URL == nil {
		return ""
	}
	return c.Request.URL.Path
}

func openAIUpstream5xxRetryRequestID(c *gin.Context) string {
	if c == nil || c.Request == nil {
		return ""
	}
	value, _ := c.Request.Context().Value(ctxkey.RequestID).(string)
	return strings.TrimSpace(value)
}

func openAIUpstream5xxRetryClientRequestID(c *gin.Context) string {
	if c == nil || c.Request == nil {
		return ""
	}
	value, _ := c.Request.Context().Value(ctxkey.ClientRequestID).(string)
	return strings.TrimSpace(value)
}
