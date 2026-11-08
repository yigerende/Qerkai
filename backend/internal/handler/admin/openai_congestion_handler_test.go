package admin

import (
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
