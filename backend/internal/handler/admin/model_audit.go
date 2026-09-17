package admin

import (
	"net/http"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

var modelAuditSlots = make(chan struct{}, 2)

func (h *UsageHandler) ModelAuditCapabilities(c *gin.Context) {
	response.Success(c, gin.H{"version": 1, "max_accounts": 10, "logs_per_account": 3})
}
func (h *UsageHandler) ModelAudit(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 16<<10)
	var input service.ModelAuditInput
	if err := c.ShouldBindJSON(&input); err != nil {
		response.BadRequest(c, "Invalid model audit body")
		return
	}
	if err := input.Validate(); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	select {
	case modelAuditSlots <- struct{}{}:
		defer func() { <-modelAuditSlots }()
	default:
		c.Header("Retry-After", "1")
		c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"code": 429, "message": "Model audit busy"})
		return
	}
	results, err := h.usageService.LatestModelAudit(c.Request.Context(), input)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, gin.H{"accounts": results})
}
