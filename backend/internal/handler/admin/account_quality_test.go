package admin

import (
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAccountQualityHandlerBoundaries(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := NewAccountQualityHandler(nil)
	router := gin.New()
	router.POST("/results", h.Results)
	router.GET("/capabilities", h.Capabilities)
	for _, body := range []string{`{}`, `{"account_ids":[0]}`, `{"account_ids":[1,1]}`, strings.Repeat("x", 17000)} {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "/results", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, r)
		require.Equal(t, 400, w.Code)
	}
	h.slots <- struct{}{}
	h.slots <- struct{}{}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/results", strings.NewReader(`{"account_ids":[1]}`)))
	require.Equal(t, 429, w.Code)
	require.Equal(t, "1", w.Header().Get("Retry-After"))
	w = httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/capabilities", nil))
	require.Equal(t, 200, w.Code)
	require.Contains(t, w.Body.String(), `"read_only":true`)
}
