package admin

import (
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"net/http"
	"strconv"
)

type AccountQualityHandler struct {
	svc   *service.AccountQualityService
	slots chan struct{}
}

func NewAccountQualityHandler(svc *service.AccountQualityService) *AccountQualityHandler {
	return &AccountQualityHandler{svc: svc, slots: make(chan struct{}, 2)}
}
func (h *AccountQualityHandler) Capabilities(c *gin.Context) {
	response.Success(c, gin.H{"version": 1, "max_accounts": 100, "read_only": true, "logs_per_account": 3})
}
func (h *AccountQualityHandler) Settings(c *gin.Context) {
	q, err := h.svc.Settings(c.Request.Context())
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	summary, err := h.svc.Summary(c.Request.Context())
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	progress, err := h.svc.Progress(c.Request.Context(), q)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, gin.H{"settings": q, "summary": summary, "progress": progress})
}
func (h *AccountQualityHandler) Save(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20)
	q := service.DefaultAccountQualitySettings()
	if err := c.ShouldBindJSON(&q); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	if err := q.Validate(); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	q, err := h.svc.SaveSettings(c.Request.Context(), q)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, gin.H{"settings": q})
}
func (h *AccountQualityHandler) Progress(c *gin.Context) {
	q, err := h.svc.Settings(c.Request.Context())
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	progress, err := h.svc.Progress(c.Request.Context(), q)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	summary, err := h.svc.Summary(c.Request.Context())
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, gin.H{"progress": progress, "summary": summary})
}
func (h *AccountQualityHandler) Results(c *gin.Context) {
	select {
	case h.slots <- struct{}{}:
		defer func() { <-h.slots }()
	default:
		c.Header("Retry-After", "1")
		response.Error(c, 429, "Quality result queries busy")
		return
	}
	var body struct {
		AccountIDs []int64 `json:"account_ids"`
	}
	if c.Param("id") != "" {
		id, err := strconv.ParseInt(c.Param("id"), 10, 64)
		if err != nil || id < 1 {
			response.BadRequest(c, "Invalid account ID")
			return
		}
		body.AccountIDs = []int64{id}
	} else {
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 16<<10)
		if err := c.ShouldBindJSON(&body); err != nil {
			response.BadRequest(c, err.Error())
			return
		}
	}
	if len(body.AccountIDs) < 1 || len(body.AccountIDs) > 100 {
		response.BadRequest(c, "Require 1-100 account IDs")
		return
	}
	seen := map[int64]bool{}
	for _, id := range body.AccountIDs {
		if id < 1 || seen[id] {
			response.BadRequest(c, "Account IDs must be positive and unique")
			return
		}
		seen[id] = true
	}
	out, err := h.svc.Results(c.Request.Context(), body.AccountIDs)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, out)
}
func (h *AccountQualityHandler) Run(c *gin.Context) {
	var body struct {
		AccountIDs []int64 `json:"account_ids"`
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 16<<10)
	if err := c.ShouldBindJSON(&body); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	if err := h.svc.Schedule(c.Request.Context(), body.AccountIDs); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	response.Success(c, gin.H{"scheduled": true})
}
func (h *AccountQualityHandler) RunSelected(c *gin.Context) {
	var body struct {
		AccountIDs []int64 `json:"account_ids"`
		Revision   string  `json:"revision"`
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 16<<10)
	if err := c.ShouldBindJSON(&body); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	out, err := h.svc.ScheduleSelected(c.Request.Context(), body.AccountIDs, body.Revision)
	if err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	response.Success(c, out)
}
func (h *AccountQualityHandler) History(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id < 1 {
		response.BadRequest(c, "Invalid account ID")
		return
	}
	q, err := h.svc.Settings(c.Request.Context())
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	out, err := h.svc.History(c.Request.Context(), id, q.HistoryLimit)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, gin.H{"items": out})
}
