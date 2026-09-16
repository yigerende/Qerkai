//go:build unit

package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestOpenAIRetryRulesSettingsRoundTrip(t *testing.T) {
	h, repo := newStepUpSwitchTestHandler(t, nil)
	rules := service.DefaultOpenAIUpstream5xxRetryRules()
	rules[0].Enabled = false
	rules[1].Name = "Overload retry"
	rec := doUpdateSettings(t, h, map[string]any{"openai_upstream_5xx_retry_enabled": true, "openai_upstream_5xx_retry_rules": rules}, nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.True(t, service.OpenAIUpstream5xxRetrySettings().Enabled)
	require.Equal(t, rules, service.OpenAIUpstream5xxRetrySettings().Rules, "saved selections must update the runtime cache")
	var stored []service.OpenAIUpstream5xxRetryRule
	require.NoError(t, json.Unmarshal([]byte(repo.values[service.SettingKeyOpenAIUpstream5xxRetryRules]), &stored))
	require.Equal(t, rules, stored)
	rec = doUpdateSettings(t, h, map[string]any{"risk_control_enabled": true}, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal([]byte(repo.values[service.SettingKeyOpenAIUpstream5xxRetryRules]), &stored))
	require.Equal(t, rules, stored, "omitted rules must retain the stored selection")
	rec = doUpdateSettings(t, h, map[string]any{"openai_upstream_5xx_retry_enabled": false}, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	require.False(t, service.OpenAIUpstream5xxRetrySettings().Enabled)
	require.Equal(t, rules, service.OpenAIUpstream5xxRetrySettings().Rules)
	rec = doUpdateSettings(t, h, map[string]any{"openai_upstream_5xx_retry_rules": []any{}}, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "[]", repo.values[service.SettingKeyOpenAIUpstream5xxRetryRules])
	require.NotNil(t, service.OpenAIUpstream5xxRetrySettings().Rules)
	require.Empty(t, service.OpenAIUpstream5xxRetrySettings().Rules)
	rules[1].Keywords = []string{""}
	rec = doUpdateSettings(t, h, map[string]any{"openai_upstream_5xx_retry_rules": rules}, nil)
	require.NotEqual(t, http.StatusOK, rec.Code)
	require.Equal(t, "[]", repo.values[service.SettingKeyOpenAIUpstream5xxRetryRules])
}

func TestOpenAIRetryRulesPreviewUsesDraftRules(t *testing.T) {
	rules := []service.OpenAIUpstream5xxRetryRule{{ID: "draft", Name: "Draft", Enabled: true, StatusCode: 503, MatchMode: "all", Keywords: []string{"temporary", "retry"}}}
	for _, tc := range []struct{ payload, reason string }{
		{`{"type":"error","error":{"message":"temporary error, retry"}}`, "matched"},
		{`{"type":"error","error":{"status_code":429,"message":"temporary error, retry"}}`, "excluded_status"},
		{`{broken`, "invalid_json"},
	} {
		body, err := json.Marshal(map[string]any{"rules": rules, "payload": tc.payload})
		require.NoError(t, err)
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/preview", strings.NewReader(string(body)))
		c.Request.Header.Set("Content-Type", "application/json")
		(&OpsHandler{}).PreviewOpenAIUpstream5xxRetryRule(c)
		require.Equal(t, 200, rec.Code)
		require.Equal(t, tc.reason, gjson.GetBytes(rec.Body.Bytes(), "data.reason").String())
	}
}
