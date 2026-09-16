package service

import (
	"time"

	"github.com/gin-gonic/gin"
)

// Only explicitly owned retries enter the shared ledger. Legacy retries and
// connection establishment never increment the usage column or retry panel.
func ArmOpenAIUpstream5xxUsageRetry(c *gin.Context, account *Account, failoverErr *UpstreamFailoverError) {
	if c == nil || !IsOpenAIUpstream5xxRetryOwned(failoverErr, account) {
		return
	}
	if tracker := existingOpenAIUpstream5xxRetryTracker(c); tracker != nil {
		tracker.mu.Lock()
		pending := tracker.pending != nil
		tracker.mu.Unlock()
		if pending {
			return
		}
	}
	NoteOpenAIUpstream5xxRetryIntercept(c, account, failoverErr, "", 0, 0, failoverErr.SameAccountRetryDelay)
}

func BeginOpenAIUpstream5xxUsageAttempt(c *gin.Context) {
	tracker := existingOpenAIUpstream5xxRetryTracker(c)
	if tracker == nil {
		return
	}
	tracker.mu.Lock()
	if tracker.pending == nil || (c.Request != nil && c.Request.Context().Err() != nil) {
		tracker.mu.Unlock()
		return
	}
	entry := *tracker.pending
	tracker.pending = nil
	tracker.intercepts++
	if entry.SameAccountAttempt > 0 {
		if tracker.sameAccountAttempts == nil {
			tracker.sameAccountAttempts = make(map[int64]int)
		}
		tracker.sameAccountAttempts[entry.AccountID]++
	}
	tracker.lastStatus, tracker.lastAccountID = entry.UpstreamStatus, entry.AccountID
	tracker.lastAccountName, tracker.lastModel, tracker.lastMessage = entry.AccountName, entry.Model, entry.UpstreamMessage
	tracker.lastRuleID, tracker.lastRuleName = entry.RuleID, entry.RuleName
	entry.WSRetryCount, entry.WSRetryStatus, entry.WSRetryReason = tracker.wsRetryCount, tracker.wsRetryStatus, tracker.wsRetryReason
	entry.AtUnixMS = time.Now().UnixMilli()
	entry.Attempt, entry.RetryCount = tracker.intercepts, tracker.intercepts
	tracker.mu.Unlock()
	recordOpenAIUpstream5xxRetryEvent(entry)
}

func OpenAIUpstream5xxRetryCount(c *gin.Context) int {
	tracker := existingOpenAIUpstream5xxRetryTracker(c)
	if tracker == nil {
		return 0
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	return tracker.intercepts
}

// Freeze scalar usage data before asynchronous billing reads it.
func SnapshotOpenAIUpstream5xxUsageRetries(c *gin.Context, result *OpenAIForwardResult, finishTurn bool) {
	if result != nil {
		result.OpenAIUpstream5xxRetryCount = OpenAIUpstream5xxRetryCount(c)
	}
	if finishTurn && c != nil {
		c.Set(openAIUpstream5xxRetryTrackerKey, (*openAIUpstream5xxRetryTracker)(nil))
	}
}
