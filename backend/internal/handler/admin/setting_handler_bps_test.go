package admin

import (
	"net/http"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestBPSSettingsMutualExclusionAndPartialUpdates(t *testing.T) {
	repo := &settingHandlerRepoStub{values: map[string]string{}}
	h := NewSettingHandler(service.NewSettingService(repo, &config.Config{}), nil, nil, nil, nil, nil, nil)
	bps := service.DefaultOpenAIBPSSettings()
	bps.Enabled = true
	bps.GroupIDs = []int64{11}
	update := func(payload map[string]any, status int) {
		t.Helper()
		rec := doUpdateSettings(t, h, payload, nil)
		require.Equal(t, status, rec.Code, rec.Body.String())
	}
	update(map[string]any{"openai_bps": bps}, http.StatusOK)
	require.True(t, gjson.Get(repo.values[service.SettingKeyOpenAIBPS], "enabled").Bool())
	update(map[string]any{"force_openai_upstream_ws": true}, http.StatusBadRequest)
	update(map[string]any{"openai_ws_pool_optimization_enabled": true}, http.StatusBadRequest)
	bps.Enabled = false
	update(map[string]any{"openai_bps": bps, "force_openai_upstream_ws": true, "openai_ws_pool_optimization_enabled": true}, http.StatusOK)
	bps.Enabled = true
	update(map[string]any{"openai_bps": bps}, http.StatusBadRequest)
	update(map[string]any{"openai_bps": bps, "force_openai_upstream_ws": false, "openai_ws_pool_optimization_enabled": false}, http.StatusOK)
	old := repo.values[service.SettingKeyOpenAIBPS]
	update(map[string]any{"force_openai_upstream_ws": false}, http.StatusOK)
	require.JSONEq(t, old, repo.values[service.SettingKeyOpenAIBPS])
	update(map[string]any{"openai_bps": map[string]any{"enabled": true, "models": []string{"bad model"}}}, http.StatusBadRequest)
}
