package admin

import (
	"strconv"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// ListSchedulingPauses returns real switch pauses, not temporary rate-limit cooldowns.
func (h *AccountHandler) ListSchedulingPauses(c *gin.Context) {
	input := service.AccountSchedulingPausesInput{Limit: 100}
	var err error
	if raw, exists := c.GetQuery("limit"); exists {
		input.Limit, err = strconv.Atoi(raw)
		if err != nil {
			response.BadRequest(c, "Invalid limit")
			return
		}
	}
	if raw, exists := c.GetQuery("after_id"); exists {
		input.AfterID, err = strconv.ParseInt(raw, 10, 64)
		if err != nil {
			response.BadRequest(c, "Invalid after_id")
			return
		}
	}
	if raw, exists := c.GetQuery("account_ids"); exists {
		if len(raw) > 2100 {
			response.BadRequest(c, "Too many account IDs")
			return
		}
		for _, part := range strings.Split(raw, ",") {
			id, parseErr := strconv.ParseInt(strings.TrimSpace(part), 10, 64)
			if parseErr != nil {
				response.BadRequest(c, "Invalid account IDs")
				return
			}
			input.AccountIDs = append(input.AccountIDs, id)
		}
	}
	if err = input.Validate(); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	reader, ok := h.adminService.(service.AccountSchedulingPausesReader)
	if !ok {
		response.Error(c, 503, "Account scheduling pauses unavailable")
		return
	}
	result, err := reader.ListAccountSchedulingPauses(c.Request.Context(), input)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	c.Header("Cache-Control", "no-store")
	response.Success(c, result)
}
