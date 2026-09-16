package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOpenAIUpstream5xxRetryRulesDefaultsAndExclusions(t *testing.T) {
	for _, event := range []string{"error", "response.failed"} {
		for _, status := range []int{502, 503} {
			match := MatchOpenAIUpstream5xxRetryRules(businessRetryTestPayload(status, event), nil)
			require.True(t, match.Matched)
			require.Equal(t, status, match.StatusCode)
		}
	}
	for _, payload := range []string{
		`{"type":"error","error":{"status_code":429,"message":"Our servers are currently overloaded. Please try again later."}}`,
		`{"type":"error","error":{"code":"rate_limit_exceeded","message":"Our servers are currently overloaded. Please try again later."}}`,
		`{"type":"response.completed","message":"Our servers are currently overloaded. Please try again later."}`,
		`{"type":"error","error":{"status_code":503,"message":"different failure"}}`,
	} {
		require.False(t, MatchOpenAIUpstream5xxRetryRules([]byte(payload), nil).Matched, payload)
	}
}

func TestOpenAIUpstream5xxRetryRulesCustomizationAndPersistence(t *testing.T) {
	payload := []byte(`{"type":"error","error":{"message":" CUSTOM   failure, please retry "}}`)
	rules := []OpenAIUpstream5xxRetryRule{{ID: "custom", Name: "Custom", Enabled: true, StatusCode: 502, MatchMode: "all", Keywords: []string{"custom failure", "retry"}}}
	match := MatchOpenAIUpstream5xxRetryRules(payload, rules)
	require.True(t, match.Matched)
	require.Equal(t, "custom", match.RuleID)
	rules[0].Keywords = append(rules[0].Keywords, "absent")
	require.False(t, MatchOpenAIUpstream5xxRetryRules(payload, rules).Matched)
	rules[0].MatchMode = "any"
	require.True(t, MatchOpenAIUpstream5xxRetryRules(payload, rules).Matched)
	rules[0].Enabled = false
	require.False(t, MatchOpenAIUpstream5xxRetryRules(payload, rules).Matched)
	encoded, err := json.Marshal(rules)
	require.NoError(t, err)
	require.Equal(t, rules, parseOpenAIUpstream5xxRetryRules(string(encoded)))
	require.Len(t, parseOpenAIUpstream5xxRetryRules(""), 2)
	for _, raw := range []string{"[]", "null", "invalid"} {
		parsed := parseOpenAIUpstream5xxRetryRules(raw)
		require.NotNil(t, parsed)
		require.Empty(t, parsed)
		require.False(t, MatchOpenAIUpstream5xxRetryRules(businessRetryTestPayload(503, "error"), parsed).Matched)
	}
	rules[0].Keywords = []string{" "}
	_, err = NormalizeOpenAIUpstream5xxRetryRules(rules)
	require.Error(t, err)
	_, err = NormalizeOpenAIUpstream5xxRetryRules(append(DefaultOpenAIUpstream5xxRetryRules(), DefaultOpenAIUpstream5xxRetryRules()[0]))
	require.Error(t, err)
}

func TestOpenAIWSBusinessReconnectRequiresOwnedMetadataOnlyRequest(t *testing.T) {
	c := newOpenAIUpstream5xxRetryTestContext(t)
	require.False(t, openAIWSBusinessRetryCanReconnect(c))
	state := &openAIWSBusinessRetryState{config: testOpenAIUpstream5xxRetryConfig()}
	c.Set(openAIWSBusinessRetryKey, state)
	require.False(t, openAIWSBusinessRetryCanReconnect(c))
	state.managed = true
	state.prepareFrame([]byte(`{"type":"response.created","response":{"output":[]}}`))
	require.True(t, openAIWSBusinessRetryCanReconnect(c))
	state.prepareFrame([]byte(`{"type":"response.output_text.delta","delta":"answer"}`))
	require.False(t, openAIWSBusinessRetryCanReconnect(c))
	state.outputStarted = false
	ctx, cancel := context.WithCancel(c.Request.Context())
	c.Request = c.Request.WithContext(ctx)
	cancel()
	require.False(t, openAIWSBusinessRetryCanReconnect(c))
}

func TestOpenAIWSBusinessRetryUsesRuleSnapshot(t *testing.T) {
	ClearOpenAIUpstream5xxRetryLog()
	t.Cleanup(ClearOpenAIUpstream5xxRetryLog)
	c := newOpenAIUpstream5xxRetryTestContext(t)
	state := &openAIWSBusinessRetryState{config: OpenAIUpstream5xxRetryConfig{Enabled: true, Total: 5, Rules: []OpenAIUpstream5xxRetryRule{}}}
	svc := &OpenAIGatewayService{}
	account := testOpenAIUpstream5xxOAuthAccount()
	require.Nil(t, svc.newOpenAIWSBusinessRetryError(c, state, account, "", businessRetryTestPayload(503, "error"), "m", nil))
	state.config.Rules = DefaultOpenAIUpstream5xxRetryRules()
	failure := svc.newOpenAIWSBusinessRetryError(c, state, account, "", businessRetryTestPayload(503, "error"), "m", nil)
	require.NotNil(t, failure)
	require.Equal(t, "server_overloaded", failure.OpenAIUpstream5xxRuleID)
	NoteOpenAIUpstream5xxRetryIntercept(c, account, failure, "m", 1, 0, time.Millisecond)
	BeginOpenAIUpstream5xxUsageAttempt(c)
	require.Equal(t, 1, OpenAIUpstream5xxRetryCount(c))
}
