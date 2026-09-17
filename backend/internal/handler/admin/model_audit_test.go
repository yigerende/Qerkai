//go:build unit

package admin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type modelAuditRepoStub struct {
	service.UsageLogRepository
	calls int
}

func (r *modelAuditRepoStub) LatestModelAudit(ctx context.Context, input service.ModelAuditInput) ([]service.ModelAuditResult, error) {
	r.calls++
	_, ok := ctx.Deadline()
	if !ok {
		panic("query needs a deadline")
	}
	out := []service.ModelAuditResult{}
	for _, a := range input.Accounts {
		out = append(out, service.ModelAuditResult{AccountID: a.AccountID, Logs: []service.ModelAuditLog{}})
	}
	return out, nil
}
func TestModelAuditHandlerBoundsAndBusy(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repo := &modelAuditRepoStub{}
	h := &UsageHandler{usageService: service.NewUsageService(repo, nil, nil, nil)}
	router := gin.New()
	router.POST("/audit", h.ModelAudit)
	call := func(body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "/audit", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, r)
		return w
	}
	body := `{"model":"gpt-6-astra","accounts":[{"account_id":1,"since":"2026-09-17T00:00:00Z"}]}`
	w := call(body)
	require.Equal(t, 200, w.Code)
	require.Contains(t, w.Body.String(), `"logs":[]`)
	require.Equal(t, 1, repo.calls)
	for _, invalid := range []string{`{}`, `{"model":"x","accounts":[{"account_id":1}]}`, strings.Repeat("x", 17000)} {
		require.Equal(t, 400, call(invalid).Code)
	}
	modelAuditSlots <- struct{}{}
	modelAuditSlots <- struct{}{}
	defer func() { <-modelAuditSlots; <-modelAuditSlots }()
	w = call(body)
	require.Equal(t, 429, w.Code)
	require.Equal(t, "1", w.Header().Get("Retry-After"))
	require.Equal(t, 1, repo.calls)
}
