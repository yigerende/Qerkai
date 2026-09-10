package admin

import (
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// GetOpenAIWSPoolOps returns a lock-bounded in-memory snapshot. It never dials
// upstream and never queries historical request data.
func (h *SettingHandler) GetOpenAIWSPoolOps(c *gin.Context) {
	response.Success(c, service.GetOpenAIWSPoolOpsSnapshot())
}
