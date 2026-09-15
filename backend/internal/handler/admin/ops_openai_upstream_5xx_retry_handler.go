package admin

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// 上游 502/503 重试观测接口（二次开发功能，非上游代码）
//
// 数据源是进程内环形缓冲（service/openai_upstream_5xx_retry_log.go），
// 不查库，因此无需 ops monitoring 开关校验 —— 该校验的目的是避免昂贵的
// 监控表查询，这里不存在这个成本。
//
// 上游不存在同名文件，合并冲突代价为零。

// openAIUpstream5xxRetryEvents 限定 event 过滤的取值，避免任意字符串进入查询。
var openAIUpstream5xxRetryEvents = map[string]struct{}{
	service.OpenAIUpstream5xxRetryEventIntercepted: {},
	service.OpenAIUpstream5xxRetryEventSucceeded:   {},
	service.OpenAIUpstream5xxRetryEventExhausted:   {},
	service.OpenAIUpstream5xxRetryEventSkipped:     {},
}

// GetOpenAIUpstream5xxRetryLog 返回 502/503 重试观测记录、聚合统计与当前生效配置。
//
// 查询参数（全部可选）：
//   - event       intercepted / succeeded / exhausted / skipped
//   - status_code 502 或 503
//   - account_id  账号 ID
//   - page / page_size 分页（page_size 上限 200）
func (h *OpsHandler) GetOpenAIUpstream5xxRetryLog(c *gin.Context) {
	page, pageSize := response.ParsePagination(c)
	if pageSize > 200 {
		pageSize = 200
	}

	filter := service.OpenAIUpstream5xxRetryLogFilter{
		Limit:  pageSize,
		Offset: (page - 1) * pageSize,
	}

	if raw := strings.TrimSpace(c.Query("event")); raw != "" {
		if _, ok := openAIUpstream5xxRetryEvents[raw]; !ok {
			response.BadRequest(c, "Invalid event")
			return
		}
		filter.Event = raw
	}
	if raw := strings.TrimSpace(c.Query("status_code")); raw != "" {
		statusCode, err := strconv.Atoi(raw)
		if err != nil || (statusCode != http.StatusBadGateway && statusCode != http.StatusServiceUnavailable) {
			response.BadRequest(c, "Invalid status_code")
			return
		}
		filter.StatusCode = statusCode
	}
	if raw := strings.TrimSpace(c.Query("account_id")); raw != "" {
		accountID, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || accountID <= 0 {
			response.BadRequest(c, "Invalid account_id")
			return
		}
		filter.AccountID = accountID
	}

	response.Success(c, service.GetOpenAIUpstream5xxRetryLog(filter))
}

// ClearOpenAIUpstream5xxRetryLog 清空观测缓冲。
//
// 只影响进程内观测数据，不改变任何重试行为、账号状态或计费，因此无需二次确认。
func (h *OpsHandler) ClearOpenAIUpstream5xxRetryLog(c *gin.Context) {
	service.ClearOpenAIUpstream5xxRetryLog()
	response.Success(c, gin.H{"cleared": true})
}
