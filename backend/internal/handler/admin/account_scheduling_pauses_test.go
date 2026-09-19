package admin

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type schedulingPauseAdmin struct {
	service.AdminService
	input service.AccountSchedulingPausesInput
}

func (s *schedulingPauseAdmin) ListAccountSchedulingPauses(_ context.Context, input service.AccountSchedulingPausesInput) (*service.AccountSchedulingPauses, error) {
	s.input = input
	return &service.AccountSchedulingPauses{Version: 1, ServerNow: time.Now(), Accounts: []service.AccountSchedulingPause{}}, nil
}

func TestListSchedulingPausesQueryValidation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &schedulingPauseAdmin{}
	h := &AccountHandler{adminService: svc}
	for _, tc := range []struct {
		query  string
		status int
	}{
		{"", 200}, {"?limit=10&after_id=2&account_ids=3,4", 200},
		{"?limit=101", 400}, {"?limit=0", 400}, {"?limit=x", 400}, {"?after_id=-1", 400},
		{"?account_ids=1,1", 400}, {"?account_ids=0", 400}, {"?account_ids=wat", 400},
	} {
		r := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(r)
		c.Request = httptest.NewRequest("GET", "/accounts/scheduling-paused"+tc.query, nil)
		h.ListSchedulingPauses(c)
		require.Equal(t, tc.status, r.Code, tc.query)
		if tc.status == 200 {
			require.Equal(t, "no-store", r.Header().Get("Cache-Control"))
		}
	}
}
