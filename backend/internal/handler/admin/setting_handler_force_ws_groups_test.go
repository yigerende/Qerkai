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

func TestSettingHandlerForceWSGroupScope(t *testing.T) {
	repo := &settingHandlerRepoStub{values: map[string]string{}}
	svc := service.NewSettingService(repo, &config.Config{})
	h := NewSettingHandler(svc, nil, nil, nil, nil, nil, nil)
	update := func(body map[string]any) *httptest.ResponseRecorder {
		return doUpdateSettings(t, h, body, nil)
	}
	defer update(map[string]any{"force_openai_upstream_ws": false, "force_openai_upstream_ws_group_ids": nil})
	rec := update(map[string]any{"force_openai_upstream_ws": true})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.True(t, service.ForceUpstreamWSEnabledForGroup(22), "missing selection preserves legacy all-groups behavior")
	rec = update(map[string]any{"force_openai_upstream_ws_group_ids": []int64{11, 11}})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, "[11]", repo.values[service.SettingKeyForceOpenAIUpstreamWSGroupIDs])
	require.True(t, service.ForceUpstreamWSEnabledForGroup(11))
	require.False(t, service.ForceUpstreamWSEnabledForGroup(22))
	rec = update(map[string]any{"force_openai_upstream_ws": false})
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "[11]", repo.values[service.SettingKeyForceOpenAIUpstreamWSGroupIDs])
	require.False(t, service.ForceUpstreamWSEnabledForGroup(11))
	rec = update(map[string]any{"force_openai_upstream_ws": true, "force_openai_upstream_ws_group_ids": []int64{}})
	require.Equal(t, http.StatusOK, rec.Code)
	require.False(t, service.ForceUpstreamWSEnabledForGroup(11))
	rec = update(map[string]any{"force_openai_upstream_ws_group_ids": nil})
	require.Equal(t, http.StatusOK, rec.Code)
	require.True(t, service.ForceUpstreamWSEnabledForGroup(22))
	get := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(get)
	c.Request = httptest.NewRequest("GET", "/api/v1/admin/settings", nil)
	h.GetSettings(c)
	require.Contains(t, get.Body.String(), `"force_openai_upstream_ws_group_ids":null`)
	for _, invalid := range []any{[]int64{-1}, []int64{0}, "all", []string{"11"}} {
		rec = update(map[string]any{"force_openai_upstream_ws_group_ids": invalid})
		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.Equal(t, "null", repo.values[service.SettingKeyForceOpenAIUpstreamWSGroupIDs])
	}
	rec = update(map[string]any{"force_openai_upstream_ws_group_ids": []int64{22}})
	require.Equal(t, "[22]", gjson.GetBytes(rec.Body.Bytes(), "data.force_openai_upstream_ws_group_ids").Raw)
}
