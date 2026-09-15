package service

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

const businessProcessingMessage = "An error occurred while processing your request. You can retry your request, or contact us through our help center at help.openai.com if the error persists. Please include the request ID request-example in your message."

func businessRetryTestPayload(status int, event string) []byte {
	message := businessProcessingMessage
	if status == 503 {
		message = overloadEventMessage
	}
	err := map[string]any{"type": "server_error", "message": message, "status_code": status}
	value := map[string]any{"type": event, "error": err}
	if event == "response.failed" {
		value = map[string]any{"type": event, "response": map[string]any{"id": "resp_failed", "status": "failed", "error": err}}
	}
	payload, _ := json.Marshal(value)
	return payload
}

func businessRetryTestFailure(status int) *UpstreamFailoverError {
	config := OpenAIUpstream5xxRetrySettings()
	return &UpstreamFailoverError{StatusCode: status, ResponseBody: businessRetryTestPayload(status, "error"), OpenAIUpstream5xxRetry: &config,
		OpenAIWSRetryAfterMetadata: true, SameAccountRetryMax: config.SameAccount, SameAccountRetryDelay: config.Delay}
}

func TestOpenAIWSBusinessRetryClassifiesOnlyRequestedErrors(t *testing.T) {
	for _, status := range []int{502, 503} {
		for _, event := range []string{"error", "response.failed"} {
			require.Equal(t, status, openAIUpstream5xxBusinessErrorStatus(businessRetryTestPayload(status, event)))
		}
	}
	for _, raw := range []string{
		`{"type":"error","error":{"status_code":502,"message":"bad gateway"}}`,
		`{"type":"response.failed","response":{"error":{"status_code":503,"message":"try again"}}}`,
		`{"type":"error","error":{"code":"server_is_overloaded","message":"different capacity message"}}`,
		`{"type":"error","error":{"status_code":502,"message":"Your input exceeds the context window of this model. Please adjust your input and try again."}}`,
		`{"type":"error","error":{"code":"rate_limit_exceeded","message":"Our servers are currently overloaded. Please try again later."}}`,
		`{"type":"error","error":{"status_code":429,"message":"Our servers are currently overloaded. Please try again later."}}`,
		`{"type":"response.completed","response":{"output":[{"text":"Our servers are currently overloaded. Please try again later."}]}}`,
		`not json`,
	} {
		require.Zero(t, openAIUpstream5xxBusinessErrorStatus([]byte(raw)), raw)
	}
}

func TestOpenAIWSBusinessRetryPreservesFirstNotificationsAndResponseIdentity(t *testing.T) {
	state := &openAIWSBusinessRetryState{}
	firstFrames := []string{
		`{"type":"codex.rate_limits","rate_limits":{"allowed":true,"limit_reached":false}}`,
		`{"type":"codex.response.metadata","headers":{"x-codex-turn-state":"state-a"}}`,
		`{"type":"response.created","sequence_number":0,"response":{"id":"resp_a","status":"in_progress","error":null,"output":[]}}`,
		`{"type":"response.in_progress","sequence_number":1,"response":{"id":"resp_a","output":[]}}`,
	}
	for _, raw := range firstFrames {
		require.Equal(t, raw, string(state.prepareFrame([]byte(raw))))
		require.False(t, state.outputStarted)
	}
	state.managed = true
	require.Empty(t, state.prepareFrame([]byte(`{"type":"response.created","sequence_number":0,"response":{"id":"resp_b","output":[]}}`)))
	require.Empty(t, state.prepareFrame([]byte(`{"type":"response.in_progress","sequence_number":1,"response":{"id":"resp_b","output":[]}}`)))
	frame := state.prepareFrame([]byte(`{"type":"response.output_item.added","sequence_number":2,"response_id":"resp_b","item":{"id":"msg_b","type":"message","content":[]}}`))
	require.True(t, state.outputStarted)
	require.Equal(t, "resp_a", gjson.GetBytes(frame, "response_id").String())
	require.Equal(t, "msg_b", gjson.GetBytes(frame, "item.id").String())
	require.EqualValues(t, 2, gjson.GetBytes(frame, "sequence_number").Int())
	frame = state.prepareFrame([]byte(`{"type":"response.completed","sequence_number":3,"response":{"id":"resp_b","status":"completed","output":[{"type":"message","content":[{"text":"resp_b is literal user text"}]}]}}`))
	require.Equal(t, "resp_a", gjson.GetBytes(frame, "response.id").String())
	require.Equal(t, "resp_b is literal user text", gjson.GetBytes(frame, "response.output.0.content.0.text").String())
	require.Equal(t, "response.completed", state.terminalEvent)
}

func TestOpenAIWSBusinessRetryOutputBoundary(t *testing.T) {
	for _, raw := range []string{
		`{"type":"response.created","response":{"output":[{"type":"message","content":[{"text":"already generated"}]}]}}`,
		`{"type":"response.output_item.added","item":{"type":"function_call","name":"exec","arguments":""}}`,
		`{"type":"response.output_text.delta","delta":"hello"}`,
		`{"type":"response.reasoning_summary_text.delta","delta":"thinking"}`,
		`{"type":"new.unknown.event"}`,
	} {
		state := &openAIWSBusinessRetryState{}
		state.prepareFrame([]byte(raw))
		require.True(t, state.outputStarted, raw)
	}
}

func TestOpenAIWSBusinessRetryBudgetAndSharedCounters(t *testing.T) {
	config := OpenAIUpstream5xxRetryConfig{Enabled: true, SameAccount: 3, Total: 5, Delay: time.Millisecond}
	t.Cleanup(setOpenAIUpstream5xxRetryForTest(config))
	ClearOpenAIUpstream5xxRetryLog()
	t.Cleanup(ClearOpenAIUpstream5xxRetryLog)
	c := newOpenAIUpstream5xxRetryTestContext(t)
	account := testOpenAIUpstream5xxOAuthAccount()
	failure := businessRetryTestFailure(503)
	for attempt := 1; attempt <= 5; attempt++ {
		retry, same, delay, reason := PrepareOpenAIUpstream5xxBusinessRetry(c, account, failure, "gpt-5.6-terra")
		require.True(t, retry, reason)
		require.Equal(t, attempt != 4, same)
		require.Equal(t, config.Delay, delay)
		require.Equal(t, attempt-1, OpenAIUpstream5xxRetryCount(c))
		if !same {
			account = &Account{ID: 8, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
		}
		BeginOpenAIUpstream5xxUsageAttempt(c)
		BeginOpenAIUpstream5xxUsageAttempt(c)
		result := &OpenAIForwardResult{}
		SnapshotOpenAIUpstream5xxUsageRetries(c, result, false)
		require.Equal(t, attempt, result.OpenAIUpstream5xxRetryCount)
	}
	retry, _, _, reason := PrepareOpenAIUpstream5xxBusinessRetry(c, account, failure, "m")
	require.False(t, retry)
	require.Equal(t, "retry_count_exhausted", reason)
	MarkOpsStreamError(c, "server_error", overloadEventMessage, 503)
	FlushOpenAIUpstream5xxRetryTracker(c, 200)
	page := GetOpenAIUpstream5xxRetryLog(OpenAIUpstream5xxRetryLogFilter{})
	require.Equal(t, 5, page.Stats.Intercepted)
	require.Equal(t, 1, page.Stats.Exhausted)
	require.Equal(t, 5, page.Entries[0].RetryCount)
	require.Equal(t, 503, page.Entries[0].StatusCode)
}

func TestOpenAIWSBusinessRetryCanceledWaitDoesNotCount(t *testing.T) {
	t.Cleanup(setOpenAIUpstream5xxRetryForTest(testOpenAIUpstream5xxRetryConfig()))
	ClearOpenAIUpstream5xxRetryLog()
	t.Cleanup(ClearOpenAIUpstream5xxRetryLog)
	c := newOpenAIUpstream5xxRetryTestContext(t)
	ctx, cancel := context.WithCancel(c.Request.Context())
	c.Request = c.Request.WithContext(ctx)
	retry, _, _, _ := PrepareOpenAIUpstream5xxBusinessRetry(c, testOpenAIUpstream5xxOAuthAccount(), businessRetryTestFailure(502), "m")
	require.True(t, retry)
	cancel()
	BeginOpenAIUpstream5xxUsageAttempt(c)
	FlushOpenAIUpstream5xxRetryTracker(c, http.StatusOK)
	require.Zero(t, OpenAIUpstream5xxRetryCount(c))
	require.Zero(t, GetOpenAIUpstream5xxRetryLog(OpenAIUpstream5xxRetryLogFilter{}).Total)
}

func TestOpenAIWSBusinessRetryZeroSameAccountAndTimeBudget(t *testing.T) {
	config := OpenAIUpstream5xxRetryConfig{Enabled: true, SameAccount: 0, Total: 2}
	t.Cleanup(setOpenAIUpstream5xxRetryForTest(config))
	c := newOpenAIUpstream5xxRetryTestContext(t)
	retry, same, _, _ := PrepareOpenAIUpstream5xxBusinessRetry(c, testOpenAIUpstream5xxOAuthAccount(), businessRetryTestFailure(503), "m")
	require.True(t, retry)
	require.False(t, same)
	t.Setenv(openAICapacityShedBudgetEnv, "1")
	c.Set(openAIWSBusinessRetryKey, &openAIWSBusinessRetryState{managed: true, capacityFirstAt: time.Now().Add(-2 * time.Second)})
	require.False(t, OpenAIUpstream5xxRetryBudgetAllowsStart(c))
	BeginOpenAIUpstream5xxUsageAttempt(c)
	require.Zero(t, OpenAIUpstream5xxRetryCount(c))
}

func TestOpenAIWSBusinessRetryTurnStateFollowsItsMintingAccount(t *testing.T) {
	c := newOpenAIUpstream5xxRetryTestContext(t)
	c.Request.Header.Set("session_id", "session-one")
	svc := &OpenAIGatewayService{}
	a, b := &Account{ID: 1}, &Account{ID: 2}
	svc.noteOpenAIWSBusinessTurnState(c, a, "blob-a")
	svc.noteOpenAIWSBusinessTurnState(c, b, "blob-b")
	require.Equal(t, "blob-a", svc.guardOpenAIWSBusinessTurnState(c, a, "blob-a"))
	require.Empty(t, svc.guardOpenAIWSBusinessTurnState(c, b, "blob-a"))
	require.Equal(t, "blob-b", svc.guardOpenAIWSBusinessTurnState(c, b, "blob-b"))
	c.Request.Header.Set("session_id", "session-two")
	require.Equal(t, "blob-a", svc.guardOpenAIWSBusinessTurnState(c, b, "blob-a"), "provenance is scoped to the client session")
}
