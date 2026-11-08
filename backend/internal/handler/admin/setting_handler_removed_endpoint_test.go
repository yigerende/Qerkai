package admin

import (
	"net/http"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestRemovedEndpointSettingDoesNotBlockForcedWS(t *testing.T) {
	// Existing installations may retain this inert historical setting.
	repo := &settingHandlerRepoStub{values: map[string]string{"openai_bps": `{"enabled":true}`}}
	h := NewSettingHandler(service.NewSettingService(repo, &config.Config{}), nil, nil, nil, nil, nil, nil)
	t.Cleanup(func() {
		doUpdateSettings(t, h, map[string]any{"force_openai_upstream_ws": false, "openai_ws_pool_optimization_enabled": false}, nil)
	})
	for _, enabled := range []bool{true, false} {
		rec := doUpdateSettings(t, h, map[string]any{
			"force_openai_upstream_ws": enabled, "openai_ws_pool_optimization_enabled": enabled,
		}, nil)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.Equal(t, enabled, gjson.GetBytes(rec.Body.Bytes(), "data.force_openai_upstream_ws").Bool())
		require.False(t, gjson.GetBytes(rec.Body.Bytes(), "data.openai_bps").Exists())
	}
}
