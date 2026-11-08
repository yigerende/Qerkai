package admin

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"net/http"
)

func (h *SettingHandler) GetOpenAICongestionMigration(c *gin.Context) {
	settings, err := h.settingService.GetOpenAICongestionSettings(c.Request.Context())
	if err != nil {
		response.Error(c, http.StatusInternalServerError, "Unable to read congestion settings")
		return
	}
	response.Success(c, settings)
}
func (h *SettingHandler) UpdateOpenAICongestionMigration(c *gin.Context) {
	var body map[string]json.RawMessage
	if err := c.ShouldBindJSON(&body); err != nil {
		response.BadRequest(c, "Expected a settings object")
		return
	}
	var enabled *bool
	if err := json.Unmarshal(body["enabled"], &enabled); err != nil || enabled == nil {
		response.BadRequest(c, "enabled must be a boolean")
		return
	}
	for _, value := range body {
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			response.BadRequest(c, "Settings must not be null")
			return
		}
	}
	raw, _ := json.Marshal(body)
	settings, err := h.settingService.UpdateOpenAICongestionSettings(c.Request.Context(), raw)
	if errors.Is(err, service.ErrInvalidOpenAICongestionSettings) {
		response.BadRequest(c, err.Error())
		return
	}
	if err != nil {
		response.Error(c, http.StatusInternalServerError, "Unable to save congestion settings")
		return
	}
	response.Success(c, settings)
}
