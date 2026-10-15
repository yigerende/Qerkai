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

func TestSettingHandler5xxForceNewConnection(t *testing.T) {
	repo := &settingHandlerRepoStub{values: map[string]string{}}
	h := NewSettingHandler(service.NewSettingService(repo, &config.Config{}), nil, nil, nil, nil, nil, nil)
	key := service.SettingKeyOpenAIUpstream5xxRetryForceNewConnection
	update := func(body map[string]any, want bool) {
		t.Helper()
		rec := doUpdateSettings(t, h, body, nil)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.Equal(t, want, gjson.GetBytes(rec.Body.Bytes(), "data."+key).Bool())
		rec = httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest("GET", "/api/v1/admin/settings", nil)
		h.GetSettings(c)
		require.Equal(t, want, gjson.GetBytes(rec.Body.Bytes(), "data."+key).Bool())
		require.Equal(t, want, service.OpenAIUpstream5xxRetrySettings().ForceNewConnection)
	}
	t.Cleanup(func() {
		update(map[string]any{"force_openai_upstream_ws": false, "openai_upstream_5xx_retry_enabled": false}, false)
	})
	update(map[string]any{key: true}, false)
	update(map[string]any{key: true, "openai_upstream_5xx_retry_enabled": true}, false)
	update(map[string]any{key: true, "force_openai_upstream_ws": true}, true)
	require.Equal(t, "true", repo.values[key])
	update(map[string]any{"openai_upstream_5xx_retry_total": 7}, true)
	update(map[string]any{"force_openai_upstream_ws": false}, false)
	update(map[string]any{"force_openai_upstream_ws": true}, false)
	update(map[string]any{key: true}, true)
	update(map[string]any{"openai_upstream_5xx_retry_enabled": false}, false)
	update(map[string]any{"openai_upstream_5xx_retry_enabled": true}, false)
}
