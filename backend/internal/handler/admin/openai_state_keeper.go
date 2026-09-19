package admin

import (
	"net/http"
	"strconv"

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

func (h *OpenAIStateKeeperHandler) Get(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	err := h.svc.SyncSelection(c.Request.Context())
	snapshot := h.svc.Snapshot()
	if err != nil && snapshot.ConfigError == "" {
		snapshot.ConfigError = "采集账号或代理同步失败，保留原配置，请稍后刷新"
	}
	response.Success(c, snapshot)
}

func (h *OpenAIStateKeeperHandler) Recent(c *gin.Context) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 16<<10)
	var input struct {
		AccountIDs []int64 `json:"account_ids"`
	}
	if err := c.ShouldBindJSON(&input); err != nil || len(input.AccountIDs) == 0 || len(input.AccountIDs) > 100 {
		response.BadRequest(c, "请选择 1–100 个账号")
		return
	}
	seen := map[int64]bool{}
	for _, id := range input.AccountIDs {
		if id <= 0 || seen[id] {
			response.BadRequest(c, "账号 ID 无效或重复")
			return
		}
		seen[id] = true
	}
	c.Header("Cache-Control", "no-store")
	items, err := h.svc.RecentWithState(c.Request.Context(), input.AccountIDs)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, gin.H{"accounts": items})
}

func (h *OpenAIStateKeeperHandler) Detail(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		response.BadRequest(c, "账号 ID 必须为正整数")
		return
	}
	detail, ok := h.svc.Detail(id, c.Query("model"))
	if !ok {
		response.NotFound(c, "该账号暂无已采集的 Turn-State 响应头")
		return
	}
	response.Success(c, detail)
}

func (h *OpenAIStateKeeperHandler) FileDetail(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		response.BadRequest(c, "账号 ID 必须为正整数")
		return
	}
	detail, err := h.svc.FileDetail(id, c.Query("model"))
	if err != nil {
		response.InternalError(c, "State 文件读取失败")
		return
	}
	if detail == nil {
		response.NotFound(c, "该账号暂无符合当前配置的已写入 State 文件")
		return
	}
	response.Success(c, detail)
}

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
		AccountIDs  []int64 `json:"account_ids"`
		Model       string  `json:"model"`
		PausedOnly  bool    `json:"paused_only"`
		CoolingOnly bool    `json:"cooling_only"`
	}
	if err := c.ShouldBindJSON(&input); err != nil {
		response.BadRequest(c, "采集请求格式不正确")
		return
	}
	if input.CoolingOnly {
		if input.PausedOnly || len(input.AccountIDs) > 0 || input.Model != "" {
			response.BadRequest(c, "批量重试冷却中不支持同时指定待人工项、账号或模型")
			return
		}
		count, err := h.svc.ScheduleCooling()
		if err != nil {
			response.BadRequest(c, err.Error())
			return
		}
		response.Success(c, gin.H{"scheduled": true, "scheduled_count": count})
		return
	}
	if input.PausedOnly {
		if len(input.AccountIDs) > 0 || input.Model != "" {
			response.BadRequest(c, "批量重试待人工项不支持指定账号或模型")
			return
		}
		count, err := h.svc.SchedulePaused()
		if err != nil {
			response.BadRequest(c, err.Error())
			return
		}
		response.Success(c, gin.H{"scheduled": true, "scheduled_count": count})
		return
	}
	if err := h.svc.Schedule(input.AccountIDs, input.Model); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	response.Success(c, gin.H{"scheduled": true})
}
