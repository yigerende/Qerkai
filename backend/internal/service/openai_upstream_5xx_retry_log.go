package service

import (
	"strings"
	"sync"
	"time"
)

// 上游 502/503 重试观测日志（二次开发功能，非上游代码）
//
// 需求：管理后台需要看到「哪次请求被拦下、重试了几次、最终成功还是失败、
// 相比不重试多花了多少毫秒」。
//
// 存储选型：进程内定长环形缓冲。
//   - 不落库：写入发生在网关热路径上，任何 SQL 都会把重试省下的时间又搭进去；
//     且这是排障用的近期观测数据，没有长期留存价值。
//   - 定长数组 + 写指针：append 是「一次锁 + 一次数组赋值」，不分配、不扩容、
//     不需要清理任务。满 5000 条后自然覆盖最旧记录。
//   - 单 mutex 而非 RWMutex：写路径远多于读路径（读只有管理页面轮询），
//     RWMutex 的额外开销在此没有收益。

// openAIUpstream5xxRetryLogCapacity 是环形缓冲的固定容量。
//
// 5000 条在默认配置下约覆盖数千次 502/503 事件 —— 足以回溯一次完整的上游
// 过载窗口。刻意不做成可配置项：容量变化需要重建数组，而这块内存
// （5000 × ~200B ≈ 1MB）常驻代价可以忽略。
const openAIUpstream5xxRetryLogCapacity = 5000

// openAIUpstream5xxRetryLogMessageMax 限制上游消息的留存长度。
// 截断避免单条异常长的上游错误体把整个缓冲的内存占用放大。
const openAIUpstream5xxRetryLogMessageMax = 200

// 事件类型。与前端展示文案一一对应。
const (
	// OpenAIUpstream5xxRetryEventIntercepted 502/503 被拦下，准备重试。
	OpenAIUpstream5xxRetryEventIntercepted = "intercepted"
	// OpenAIUpstream5xxRetryEventSucceeded 重试后上游返回成功。
	OpenAIUpstream5xxRetryEventSucceeded = "succeeded"
	// OpenAIUpstream5xxRetryEventExhausted 重试预算用尽，最终失败返回客户端。
	OpenAIUpstream5xxRetryEventExhausted = "exhausted"
	// OpenAIUpstream5xxRetryEventSkipped 命中 502/503 但未重试（开关关闭以外的原因，
	// 例如已向客户端写出语义字节、凭证类失败、账号类型不在覆盖范围）。
	OpenAIUpstream5xxRetryEventSkipped = "skipped"
)

// 传输协议标识。与访问日志的 upstream_transport 语义一致。
const (
	OpenAIUpstream5xxRetryTransportHTTP = "http_sse"
	OpenAIUpstream5xxRetryTransportWS   = "responses_websockets_v2"
)

// OpenAIUpstream5xxRetryLogEntry 是一条重试观测记录。
//
// 全部为值类型，无指针、无切片：写入时整体赋值进数组槽位，不产生逃逸分配。
type OpenAIUpstream5xxRetryLogEntry struct {
	StopReason string `json:"stop_reason,omitempty"`
	Seq        uint64 `json:"seq"`
	AtUnixMS   int64  `json:"at_unix_ms"`
	Event      string `json:"event"`
	// StatusCode 本条事件对外呈现的状态码：
	// intercepted / skipped 为上游状态码；succeeded / exhausted 为最终返回客户端的状态码。
	StatusCode int `json:"status_code"`
	// UpstreamStatus 触发本次重试链路的上游状态码（502/503），终态事件也保留，
	// 便于在「重试成功」一行上直接看到当初拦下的是什么。
	UpstreamStatus  int    `json:"upstream_status"`
	RequestID       string `json:"request_id"`
	ClientRequestID string `json:"client_request_id"`
	AccountID       int64  `json:"account_id"`
	AccountName     string `json:"account_name"`
	Model           string `json:"model"`
	Transport       string `json:"transport"`
	Path            string `json:"path"`
	// Attempt 本次请求内的累计拦截序号（1 = 第一次被拦下）。
	Attempt int `json:"attempt"`
	// SameAccountAttempt 当前账号上的同账号重试序号（1 = 该账号第一次重试）。
	SameAccountAttempt int `json:"same_account_attempt"`
	// SameAccountMax 本次生效的同账号重试上限，便于确认配置是否按预期生效。
	SameAccountMax int `json:"same_account_max"`
	// RetryDelayMS 本次重试前的等待间隔。
	RetryDelayMS int64 `json:"retry_delay_ms"`
	// ExtraLatencyMS 因重试额外付出的耗时（毫秒）。
	//
	// 口径：从「首个 502/503 被拦下」到「重试链路终点」的墙钟时间差。
	// 成功流式请求的终点是最终重试首个有效输出，不包含其后的完整生成时间；
	// intercepted 事件恒为 0（此刻还没有额外开销）。
	ExtraLatencyMS int64 `json:"extra_latency_ms"`
	// RetryCount 到本条事件为止，本次请求累计发生的重试次数。
	RetryCount int `json:"retry_count"`
	// UpstreamMessage 上游错误消息（截断到 openAIUpstream5xxRetryLogMessageMax）。
	UpstreamMessage string `json:"upstream_message"`
}

// OpenAIUpstream5xxRetryLogStats 是窗口内的聚合统计。
type OpenAIUpstream5xxRetryLogStats struct {
	Total       int `json:"total"`
	Intercepted int `json:"intercepted"`
	Succeeded   int `json:"succeeded"`
	Exhausted   int `json:"exhausted"`
	Skipped     int `json:"skipped"`
	// ExtraLatencyMSTotal / AvgExtraLatencyMS 仅统计终态事件
	// （succeeded / exhausted），intercepted 的 0 值不参与平均，否则会把
	// 平均额外耗时稀释掉一半。
	ExtraLatencyMSTotal int64 `json:"extra_latency_ms_total"`
	AvgExtraLatencyMS   int64 `json:"avg_extra_latency_ms"`
	MaxExtraLatencyMS   int64 `json:"max_extra_latency_ms"`
}

type openAIUpstream5xxRetryLogStore struct {
	mu      sync.Mutex
	entries [openAIUpstream5xxRetryLogCapacity]OpenAIUpstream5xxRetryLogEntry
	// next 下一个写入位置；总写入量用 seq 追踪。
	next int
	// count 已写入的有效记录数，上限为容量。
	count int
	seq   uint64
}

var openAIUpstream5xxRetryLog openAIUpstream5xxRetryLogStore

func (s *openAIUpstream5xxRetryLogStore) append(entry OpenAIUpstream5xxRetryLogEntry) {
	s.mu.Lock()
	s.seq++
	entry.Seq = s.seq
	s.entries[s.next] = entry
	s.next = (s.next + 1) % openAIUpstream5xxRetryLogCapacity
	if s.count < openAIUpstream5xxRetryLogCapacity {
		s.count++
	}
	s.mu.Unlock()
}

// snapshot 按时间倒序（最新在前）复制出全部有效记录。
func (s *openAIUpstream5xxRetryLogStore) snapshot() []OpenAIUpstream5xxRetryLogEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.count == 0 {
		return nil
	}
	out := make([]OpenAIUpstream5xxRetryLogEntry, 0, s.count)
	// 从最近写入的槽位往回走：next-1 是最新一条。
	for i := 0; i < s.count; i++ {
		idx := (s.next - 1 - i + openAIUpstream5xxRetryLogCapacity*2) % openAIUpstream5xxRetryLogCapacity
		out = append(out, s.entries[idx])
	}
	return out
}

func (s *openAIUpstream5xxRetryLogStore) reset() {
	s.mu.Lock()
	s.next = 0
	s.count = 0
	s.seq = 0
	s.entries = [openAIUpstream5xxRetryLogCapacity]OpenAIUpstream5xxRetryLogEntry{}
	s.mu.Unlock()
}

func truncateOpenAIUpstream5xxRetryMessage(message string) string {
	message = strings.TrimSpace(message)
	if len(message) <= openAIUpstream5xxRetryLogMessageMax {
		return message
	}
	// 按 rune 边界截断，避免把多字节字符切成非法 UTF-8（前端会显示成乱码）。
	runes := []rune(message)
	limit := openAIUpstream5xxRetryLogMessageMax
	if len(runes) < limit {
		limit = len(runes)
	}
	return string(runes[:limit])
}

// recordOpenAIUpstream5xxRetryEvent 写入一条观测记录。
//
// 调用方必须已确认开关开启 —— 本函数不再重复判定，因为它的每个调用点都紧跟在
// 一次 IsOpenAIUpstream5xxRetryCandidate 之后，重复判定会多一次 atomic.Load。
func recordOpenAIUpstream5xxRetryEvent(entry OpenAIUpstream5xxRetryLogEntry) {
	if entry.AtUnixMS == 0 {
		entry.AtUnixMS = time.Now().UnixMilli()
	}
	entry.UpstreamMessage = truncateOpenAIUpstream5xxRetryMessage(entry.UpstreamMessage)
	entry.AccountName = truncateOpenAIUpstream5xxRetryMessage(entry.AccountName)
	entry.Model = truncateOpenAIUpstream5xxRetryMessage(entry.Model)
	openAIUpstream5xxRetryLog.append(entry)
}

// OpenAIUpstream5xxRetryLogFilter 描述查询条件。零值表示不过滤。
type OpenAIUpstream5xxRetryLogFilter struct {
	Event      string
	StatusCode int
	AccountID  int64
	Limit      int
	Offset     int
}

// OpenAIUpstream5xxRetryLogPage 是一页查询结果。
type OpenAIUpstream5xxRetryLogPage struct {
	Entries  []OpenAIUpstream5xxRetryLogEntry `json:"entries"`
	Total    int                              `json:"total"`
	Capacity int                              `json:"capacity"`
	Stats    OpenAIUpstream5xxRetryLogStats   `json:"stats"`
	Config   OpenAIUpstream5xxRetryLogConfig  `json:"config"`
}

// OpenAIUpstream5xxRetryLogConfig 回显当前生效配置，便于在页面上确认开关状态。
type OpenAIUpstream5xxRetryLogConfig struct {
	Enabled     bool `json:"enabled"`
	SameAccount int  `json:"same_account"`
	Total       int  `json:"total"`
	DelayMS     int  `json:"delay_ms"`
}

// GetOpenAIUpstream5xxRetryLog 返回过滤后的一页记录与全量聚合统计。
//
// 统计口径是「过滤后的全集」而非当前页，这样翻页时顶部统计卡不会跳变。
func GetOpenAIUpstream5xxRetryLog(filter OpenAIUpstream5xxRetryLogFilter) OpenAIUpstream5xxRetryLogPage {
	config := OpenAIUpstream5xxRetrySettings()
	page := OpenAIUpstream5xxRetryLogPage{
		Entries:  []OpenAIUpstream5xxRetryLogEntry{},
		Capacity: openAIUpstream5xxRetryLogCapacity,
		Config: OpenAIUpstream5xxRetryLogConfig{
			Enabled:     config.Enabled,
			SameAccount: config.SameAccount,
			Total:       config.Total,
			DelayMS:     int(config.Delay / time.Millisecond),
		},
	}

	all := openAIUpstream5xxRetryLog.snapshot()
	matched := make([]OpenAIUpstream5xxRetryLogEntry, 0, len(all))
	var terminalCount int
	for _, entry := range all {
		if filter.Event != "" && entry.Event != filter.Event {
			continue
		}
		if filter.StatusCode != 0 && entry.StatusCode != filter.StatusCode {
			continue
		}
		if filter.AccountID != 0 && entry.AccountID != filter.AccountID {
			continue
		}
		matched = append(matched, entry)

		page.Stats.Total++
		switch entry.Event {
		case OpenAIUpstream5xxRetryEventIntercepted:
			page.Stats.Intercepted++
		case OpenAIUpstream5xxRetryEventSucceeded:
			page.Stats.Succeeded++
		case OpenAIUpstream5xxRetryEventExhausted:
			page.Stats.Exhausted++
		case OpenAIUpstream5xxRetryEventSkipped:
			page.Stats.Skipped++
		}
		if entry.Event == OpenAIUpstream5xxRetryEventSucceeded || entry.Event == OpenAIUpstream5xxRetryEventExhausted {
			terminalCount++
			page.Stats.ExtraLatencyMSTotal += entry.ExtraLatencyMS
			if entry.ExtraLatencyMS > page.Stats.MaxExtraLatencyMS {
				page.Stats.MaxExtraLatencyMS = entry.ExtraLatencyMS
			}
		}
	}
	if terminalCount > 0 {
		page.Stats.AvgExtraLatencyMS = page.Stats.ExtraLatencyMSTotal / int64(terminalCount)
	}
	page.Total = len(matched)

	limit := filter.Limit
	if limit <= 0 {
		limit = 200
	}
	if limit > openAIUpstream5xxRetryLogCapacity {
		limit = openAIUpstream5xxRetryLogCapacity
	}
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}
	if offset >= len(matched) {
		return page
	}
	end := offset + limit
	if end > len(matched) {
		end = len(matched)
	}
	page.Entries = matched[offset:end]
	return page
}

// ClearOpenAIUpstream5xxRetryLog 清空环形缓冲。
func ClearOpenAIUpstream5xxRetryLog() {
	openAIUpstream5xxRetryLog.reset()
}
