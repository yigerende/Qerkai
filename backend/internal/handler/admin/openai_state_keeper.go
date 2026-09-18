package admin

import (
	"net/http"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

type OpenAIStateKeeperHandler struct {
	svc *service.OpenAIStateKeeperService
}

func NewOpenAIStateKeeperHandler(svc *service.OpenAIStateKeeperService) *OpenAIStateKeeperHandler {
	return &OpenAIStateKeeperHandler{svc: svc}
}

func (h *OpenAIStateKeeperHandler) Get(c *gin.Context) { response.Success(c, h.svc.Snapshot()) }

func (h *OpenAIStateKeeperHandler) Save(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 64<<10)
	q := service.DefaultOpenAIStateKeeperSettings()
	if err := c.ShouldBindJSON(&q); err != nil {
		response.BadRequest(c, "配置格式不正确")
		return
	}
	if err := h.svc.Save(c.Request.Context(), q); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	response.Success(c, h.svc.Snapshot())
}

func (h *OpenAIStateKeeperHandler) Collect(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 16<<10)
	var input struct {
		AccountIDs []int64 `json:"account_ids"`
	}
	if err := c.ShouldBindJSON(&input); err != nil {
		response.BadRequest(c, "采集请求格式不正确")
		return
	}
	if err := h.svc.Schedule(input.AccountIDs); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	response.Success(c, gin.H{"scheduled": true})
}
