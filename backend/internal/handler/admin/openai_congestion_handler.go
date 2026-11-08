package admin

import (
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/gin-gonic/gin"
	"net/http"
)

func (h *SettingHandler) GetOpenAICongestionMigration(c *gin.Context) {
	enabled, err := h.settingService.GetOpenAICongestionMigration(c.Request.Context())
	if err != nil {
		response.Error(c, http.StatusInternalServerError, "Unable to read congestion migration setting")
		return
	}
	response.Success(c, gin.H{"enabled": enabled})
}

func (h *SettingHandler) UpdateOpenAICongestionMigration(c *gin.Context) {
	var body struct {
		Enabled *bool `json:"enabled" binding:"required"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		response.BadRequest(c, "enabled must be a boolean")
		return
	}
	if err := h.settingService.SetOpenAICongestionMigration(c.Request.Context(), *body.Enabled); err != nil {
		response.Error(c, http.StatusInternalServerError, "Unable to save congestion migration setting")
		return
	}
	response.Success(c, gin.H{"enabled": *body.Enabled})
}
