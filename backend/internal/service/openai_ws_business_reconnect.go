package service

import (
	"errors"
	"strings"

	"github.com/gin-gonic/gin"
)

// Only an already owned business retry may replay past metadata notifications.
func openAIWSBusinessRetryCanReconnect(c *gin.Context) bool {
	state := openAIWSBusinessRetryStateFrom(c)
	return state != nil && state.managed && !state.outputStarted &&
		c.Request != nil && c.Request.Context().Err() == nil
}

func openAIWSBusinessTransportFailure(err error) bool {
	var fallback *openAIWSFallbackError
	if !errors.As(err, &fallback) || fallback == nil {
		return false
	}
	switch strings.TrimPrefix(strings.TrimSpace(fallback.Reason), "prewarm_") {
	case "read_event", "write_request", "write", "acquire_timeout", "acquire_conn",
		"conn_queue_full", "dial_failed", "upstream_5xx", "ws_connection_limit_reached", openAIWSStaleConnReason:
		return true
	default:
		return false
	}
}

func beginOpenAIWSBusinessReconnect(c *gin.Context, reason string) {
	if tracker := existingOpenAIUpstream5xxRetryTracker(c); tracker != nil {
		tracker.mu.Lock()
		tracker.wsRetryCount++
		tracker.wsRetryStatus = "retrying"
		tracker.wsRetryReason = reason
		tracker.mu.Unlock()
	}
}

func markOpenAIWSBusinessReconnectRecovered(c *gin.Context) {
	if tracker := existingOpenAIUpstream5xxRetryTracker(c); tracker != nil {
		tracker.mu.Lock()
		if tracker.wsRetryStatus == "retrying" {
			tracker.wsRetryStatus = "recovered"
		}
		tracker.mu.Unlock()
	}
}

func finishOpenAIWSBusinessReconnect(c *gin.Context, err error, reason string) {
	var business *UpstreamFailoverError
	if err == nil || errors.As(err, &business) {
		return
	}
	tracker := existingOpenAIUpstream5xxRetryTracker(c)
	if tracker == nil {
		return
	}
	if c.Request.Context().Err() != nil {
		reason = "client_disconnected"
	} else if state := openAIWSBusinessRetryStateFrom(c); state != nil && state.outputStarted {
		reason = OpenAIUpstream5xxRetryOutputStopReason(c)
	} else if reason == "" {
		reason = "ws_error"
	}
	tracker.mu.Lock()
	if tracker.wsRetryCount > 0 && (tracker.wsRetryStatus == "retrying" || openAIWSBusinessTransportFailure(err)) {
		tracker.wsRetryStatus = "failed"
	}
	tracker.finalMessage = sanitizeUpstreamErrorMessage(err.Error())
	tracker.mu.Unlock()
	stopOpenAIUpstream5xxRetry(c, reason)
}
