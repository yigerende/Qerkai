package admin

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestOpenAICongestionSettingDefaultAndExplicitDisable(t *testing.T) {
	repo := newTestSettingRepo()
	repo.values["unrelated_setting"] = "preserved"
	handler := &SettingHandler{settingService: service.NewSettingService(repo, nil)}
	router := gin.New()
	router.GET("/setting", handler.GetOpenAICongestionMigration)
	router.PUT("/setting", handler.UpdateOpenAICongestionMigration)
	call := func(method, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(method, "/setting", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, r)
		return w
	}
	w := call(http.MethodGet, "")
	require.Equal(t, 200, w.Code)
	require.Contains(t, w.Body.String(), `"enabled":false`)
	for _, value := range []string{"true", "false"} {
		w = call(http.MethodPut, `{"enabled":`+value+`}`)
		require.Equal(t, 200, w.Code)
		require.Equal(t, value, repo.values[service.SettingKeyOpenAICongestionMigration])
		require.Equal(t, "preserved", repo.values["unrelated_setting"])
	}
	for _, invalid := range []string{`{}`, `{"enabled":null}`, `{"enabled":"false"}`} {
		w = call(http.MethodPut, invalid)
		require.Equal(t, 400, w.Code)
		require.Equal(t, "false", repo.values[service.SettingKeyOpenAICongestionMigration])
	}
	// Fresh process/settings instance reads the persisted disabled flag.
	enabled, err := service.NewSettingService(repo, nil).GetOpenAICongestionMigration(httptest.NewRequest(http.MethodGet, "/", nil).Context())
	require.NoError(t, err)
	require.False(t, enabled)
}

type failingCongestionSettingRepo struct {
	*testSettingRepo
	fail bool
}

func (r *failingCongestionSettingRepo) SetMultiple(ctx context.Context, values map[string]string) error {
	if r.fail {
		return errors.New("storage unavailable")
	}
	return r.testSettingRepo.SetMultiple(ctx, values)
}

func TestOpenAICongestionSettingsPersistenceValidationAndFailedSave(t *testing.T) {
	repo := &failingCongestionSettingRepo{testSettingRepo: newTestSettingRepo()}
	settings := service.NewSettingService(repo, nil)
	handler := &SettingHandler{settingService: settings}
	router := gin.New()
	router.PUT("/setting", handler.UpdateOpenAICongestionMigration)
	call := func(body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("PUT", "/setting", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, r)
		return w
	}
	require.Equal(t, 200, call(`{"enabled":true,"slow_seconds":23,"slow_min_load_percent":0,"target_allow_unknown":true,"protection_seconds":0}`).Code)
	require.Equal(t, 200, call(`{"enabled":false}`).Code)
	restored, err := service.NewSettingService(repo, nil).GetOpenAICongestionSettings(context.Background())
	require.NoError(t, err)
	require.False(t, restored.Enabled)
	require.Equal(t, 23, restored.SlowSeconds)
	require.True(t, restored.TargetAllowUnknown)
	require.Zero(t, restored.ProtectionSeconds)
	raw := repo.values[service.SettingKeyOpenAICongestionConfig]
	for _, body := range []string{
		`{"enabled":true,"slow_seconds":0}`, `{"enabled":true,"slow_percent":101}`, `{"enabled":true,"sample_limit":5,"slow_min_samples":6}`,
		`{"enabled":true,"window_seconds":null}`, `{"enabled":true,"typo_field":15}`, `{"enabled":true,"pending_seconds":1}`, `{"enabled":true,"full_wait_seconds":1.5}`,
	} {
		require.Equal(t, 400, call(body).Code, body)
		require.Equal(t, raw, repo.values[service.SettingKeyOpenAICongestionConfig])
		require.Equal(t, "false", repo.values[service.SettingKeyOpenAICongestionMigration])
	}
	repo.fail = true
	require.Equal(t, 500, call(`{"enabled":true,"slow_seconds":8}`).Code)
	require.Equal(t, raw, repo.values[service.SettingKeyOpenAICongestionConfig])
	require.Equal(t, "false", repo.values[service.SettingKeyOpenAICongestionMigration])
}
