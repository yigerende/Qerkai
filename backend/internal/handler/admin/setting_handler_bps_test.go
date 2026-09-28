package admin

import (
	"encoding/json"
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

func TestBPSSettingsTransportValidationAndLegacyDefaults(t *testing.T) {
	repo := &settingHandlerRepoStub{values: map[string]string{}}
	h := NewSettingHandler(service.NewSettingService(repo, &config.Config{}), nil, nil, nil, nil, nil, nil)
	defaults := service.DefaultOpenAIBPSSettings()
	defaults.Enabled = true
	raw, err := json.Marshal(defaults)
	require.NoError(t, err)
	var legacy map[string]any
	require.NoError(t, json.Unmarshal(raw, &legacy))
	delete(legacy, "upstream_transport")
	delete(legacy, "ws_handshake_timeout_seconds")
	rec := doUpdateSettings(t, h, map[string]any{"openai_bps": legacy}, nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, "http", gjson.Get(repo.values[service.SettingKeyOpenAIBPS], "upstream_transport").String())
	require.EqualValues(t, 5, gjson.Get(repo.values[service.SettingKeyOpenAIBPS], "ws_handshake_timeout_seconds").Int())
	defaults.UpstreamTransport, defaults.WSHandshakeTimeoutSeconds = "auto", 7
	rec = doUpdateSettings(t, h, map[string]any{"openai_bps": defaults}, nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	saved := repo.values[service.SettingKeyOpenAIBPS]
	require.Equal(t, "auto", gjson.Get(saved, "upstream_transport").String())
	require.EqualValues(t, 7, gjson.Get(saved, "ws_handshake_timeout_seconds").Int())
	for _, timeout := range []int{-1, 31} {
		defaults.WSHandshakeTimeoutSeconds = timeout
		rec = doUpdateSettings(t, h, map[string]any{"openai_bps": defaults}, nil)
		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.Equal(t, saved, repo.values[service.SettingKeyOpenAIBPS])
	}
	defaults.WSHandshakeTimeoutSeconds, defaults.UpstreamTransport = 5, "invalid"
	rec = doUpdateSettings(t, h, map[string]any{"openai_bps": defaults}, nil)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, saved, repo.values[service.SettingKeyOpenAIBPS])
}
