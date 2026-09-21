package admin

import (
	"net/http"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestSettingHandlerDownstreamModelAlignment(t *testing.T) {
	repo := &settingHandlerRepoStub{values: map[string]string{}}
	svc := service.NewSettingService(repo, &config.Config{})
	h := NewSettingHandler(svc, nil, nil, nil, nil, nil, nil)
	update := func(config map[string]any) {
		rec := doUpdateSettings(t, h, map[string]any{"openai_downstream_model_alignment": config}, nil)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	}
	update(map[string]any{"enabled": true, "group_ids": []int64{11, 11}, "models": []string{" gpt-6-astra ", "", "gpt-6-astra", "gpt-5.6-terra"}})
	raw := repo.values[service.SettingKeyOpenAIDownstreamModelAlignment]
	require.True(t, gjson.Get(raw, "enabled").Bool())
	require.Equal(t, "[11]", gjson.Get(raw, "group_ids").Raw)
	require.Equal(t, `["gpt-6-astra","gpt-5.6-terra"]`, gjson.Get(raw, "models").Raw)
	rec := doUpdateSettings(t, h, map[string]any{"force_openai_upstream_ws": false}, nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.JSONEq(t, raw, repo.values[service.SettingKeyOpenAIDownstreamModelAlignment], "unrelated saves preserve the policy")
	update(map[string]any{"enabled": false, "group_ids": nil, "models": []string{"gpt-6-astra"}})
	require.False(t, gjson.Get(repo.values[service.SettingKeyOpenAIDownstreamModelAlignment], "enabled").Bool())
	update(map[string]any{"enabled": true, "group_ids": []int64{}, "models": []string{}})
	require.Equal(t, "[]", gjson.Get(repo.values[service.SettingKeyOpenAIDownstreamModelAlignment], "group_ids").Raw)
	for _, invalid := range []map[string]any{
		{"enabled": true, "group_ids": []int64{-1}, "models": []string{"gpt-6-astra"}},
		{"enabled": true, "group_ids": nil, "models": []string{"not a model"}},
	} {
		rec := doUpdateSettings(t, h, map[string]any{"openai_downstream_model_alignment": invalid}, nil)
		require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	}
}
