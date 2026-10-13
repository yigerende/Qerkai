package admin

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestSettingHandlerWSFreshConnectionRetry(t *testing.T) {
	repo := &settingHandlerRepoStub{values: map[string]string{}}
	h := NewSettingHandler(service.NewSettingService(repo, &config.Config{}), nil, nil, nil, nil, nil, nil)
	update := func(body map[string]any) *httptest.ResponseRecorder { return doUpdateSettings(t, h, body, nil) }
	t.Cleanup(func() { update(map[string]any{"force_openai_upstream_ws": false}) })
	get := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(get)
	c.Request = httptest.NewRequest("GET", "/api/v1/admin/settings", nil)
	h.GetSettings(c)
	require.Equal(t, int64(1), gjson.GetBytes(get.Body.Bytes(), "data.openai_ws_fresh_connection_retry_max").Int())
	require.False(t, gjson.GetBytes(get.Body.Bytes(), "data.openai_ws_fresh_connection_retry_enabled").Bool())
	// API clients cannot bypass the forced-WS prerequisite.
	rec := update(map[string]any{"openai_ws_fresh_connection_retry_enabled": true})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, "false", repo.values[service.SettingKeyOpenAIWSFreshConnectionRetryEnabled])
	rec = update(map[string]any{"force_openai_upstream_ws": true, "openai_ws_fresh_connection_retry_enabled": true, "openai_ws_fresh_connection_retry_max": 4})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.True(t, gjson.GetBytes(rec.Body.Bytes(), "data.openai_ws_fresh_connection_retry_enabled").Bool())
	require.Equal(t, "4", repo.values[service.SettingKeyOpenAIWSFreshConnectionRetryMax])
	for _, count := range []any{-1, 0, 101, 1.5} {
		rec = update(map[string]any{"openai_ws_fresh_connection_retry_max": count})
		require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		require.Equal(t, "4", repo.values[service.SettingKeyOpenAIWSFreshConnectionRetryMax])
	}
	rec = update(map[string]any{"force_openai_upstream_ws": false})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.False(t, gjson.GetBytes(rec.Body.Bytes(), "data.openai_ws_fresh_connection_retry_enabled").Bool())
	rec = update(map[string]any{"force_openai_upstream_ws": true})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.False(t, gjson.GetBytes(rec.Body.Bytes(), "data.openai_ws_fresh_connection_retry_enabled").Bool(), "re-enabling forced WS does not silently enable retries")
	require.Equal(t, int64(4), gjson.GetBytes(rec.Body.Bytes(), "data.openai_ws_fresh_connection_retry_max").Int())
}
