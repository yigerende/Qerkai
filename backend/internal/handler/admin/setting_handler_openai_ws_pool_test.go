package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestSettingHandlerOpenAIWSPoolDefaultsAndPartialUpdate(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repo := &settingHandlerRepoStub{values: map[string]string{}}
	svc := service.NewSettingService(repo, &config.Config{Default: config.DefaultConfig{UserConcurrency: 5}})
	handler := NewSettingHandler(svc, nil, nil, nil, nil, nil, nil)

	get := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(get)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/v1/admin/settings", nil)
	handler.GetSettings(c)
	require.Equal(t, http.StatusOK, get.Code)

	var resp response.Response
	require.NoError(t, json.Unmarshal(get.Body.Bytes(), &resp))
	data := resp.Data.(map[string]any)
	require.Equal(t, false, data["openai_ws_pool_optimization_enabled"])
	require.Equal(t, float64(3), data["openai_ws_prewarm_idle_per_account"])
	require.Equal(t, float64(3), data["openai_ws_standby_idle_per_account"])
	require.Equal(t, float64(8), data["openai_ws_standby_max_per_account"])
	require.Equal(t, float64(24), data["openai_ws_optimized_max_conns_per_account"])
	require.Equal(t, float64(300), data["openai_ws_optimized_session_idle_timeout_seconds"])
	require.Equal(t, float64(400), data["openai_ws_optimized_dial_interval_ms"])

	updated := doUpdateSettings(t, handler, map[string]any{
		"openai_ws_pool_optimization_enabled": true,
		"openai_ws_prewarm_idle_per_account":  4,
	}, nil)
	require.Equal(t, http.StatusOK, updated.Code, updated.Body.String())
	require.Equal(t, "true", repo.values[service.SettingKeyOpenAIWSPoolOptimizationEnabled])
	require.Equal(t, "4", repo.values[service.SettingKeyOpenAIWSPrewarmIdlePerAccount])
	require.Equal(t, "3", repo.values[service.SettingKeyOpenAIWSStandbyIdlePerAccount])
	require.Equal(t, "300", repo.values[service.SettingKeyOpenAIWSOptimizedSessionIdleTTL])
	require.Equal(t, "400", repo.values[service.SettingKeyOpenAIWSOptimizedDialIntervalMS])
}

func TestSettingHandlerOpenAIWSPoolRejectsDialIntervalBelowFloor(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repo := &settingHandlerRepoStub{values: map[string]string{}}
	svc := service.NewSettingService(repo, &config.Config{Default: config.DefaultConfig{UserConcurrency: 5}})
	handler := NewSettingHandler(svc, nil, nil, nil, nil, nil, nil)

	rec := doUpdateSettings(t, handler, map[string]any{
		"openai_ws_optimized_dial_interval_ms": 399,
	}, nil)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Empty(t, repo.lastUpdates)
}
