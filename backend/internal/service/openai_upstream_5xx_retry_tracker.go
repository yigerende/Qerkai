package service

import (
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
)

// 502/503 重试的请求级追踪（二次开发功能，非上游代码）
//
// 职责：把「拦截」与「最终结果」串成一条可读链路，并算出本次重试为客户端
// 额外增加了多少毫秒。
//
// 为什么需要 tracker 而不是在拦截点直接写完整日志：
//   - 「重试成功 / 重试耗尽」只有请求走完才知道；
//   - 「多了多少 ms」需要「第一次拦截时刻 → 请求结束时刻」的差值；
//   - 6 个 handler failover 循环各有多个成功/失败出口，逐个接管会散落十几处改动。
//
// 因此拦截时只在 gin context 上挂一个 tracker，终局事件统一由访问日志中间件
// （请求结束后必然执行且只执行一次）通过 FlushOpenAIUpstream5xxRetryTracker 落盘。
//
// 关闭开关后的行为保证：所有入口的第一件事都是 IsOpenAIUpstream5xxRetryCandidate
// 判定，关闭时立即返回，tracker 永不创建；Flush 读不到 tracker 时直接返回。
// 整条链路零副作用、零分配。

// openAIUpstream5xxRetryTrackerKey 是 gin context 中 tracker 的键。
const openAIUpstream5xxRetryTrackerKey = "openai_upstream_5xx_retry_tracker"

// openAIUpstream5xxRetryTracker 汇总单次请求内的全部 502/503 拦截。
//
// 并发安全：WS 入站链路存在读写两个 goroutine 都可能触发上游失败的情形，
// 因此用 mutex 保护。锁只在拦截与 flush 时各持有一次，不在字节转发热路径上。
type openAIUpstream5xxRetryTracker struct {
	mu sync.Mutex
	// firstInterceptAt 第一次拦截的时刻，作为额外耗时的计时起点。
	firstInterceptAt time.Time
	// intercepts 已拦截并准备重试的次数。
	intercepts int
	// lastStatus 最近一次被拦截的上游状态码。
	lastStatus      int
	lastAccountID   int64
	lastAccountName string
	lastModel       string
	lastMessage     string
	// flushed 防止重复落盘。
	flushed bool
}

// NoteOpenAIUpstream5xxRetryIntercept 记录一次「已拦截，准备重试」。
//
// 由 handler 的同账号重试分支调用 —— 那里同时持有 gin context、账号、
// failover 错误与两个重试计数器，是唯一能一次性凑齐全部字段的位置。
func NoteOpenAIUpstream5xxRetryIntercept(
	c *gin.Context,
	account *Account,
	failoverErr *UpstreamFailoverError,
	model string,
	sameAccountAttempt int,
	totalRetries int,
	retryDelay time.Duration,
) {
	if failoverErr == nil || !IsOpenAIUpstream5xxRetryCandidate(failoverErr.StatusCode, account) {
		return
	}
	accountID, accountName := openAIUpstream5xxRetryAccountIdentity(account)
	upstreamMessage := extractUpstreamErrorMessage(failoverErr.ResponseBody)
	now := time.Now()

	attempt := totalRetries
	if tracker := openAIUpstream5xxRetryTrackerFrom(c); tracker != nil {
		tracker.mu.Lock()
		if tracker.firstInterceptAt.IsZero() {
			tracker.firstInterceptAt = now
		}
		tracker.intercepts++
		attempt = tracker.intercepts
		tracker.lastStatus = failoverErr.StatusCode
		tracker.lastAccountID = accountID
		tracker.lastAccountName = accountName
		tracker.lastModel = model
		tracker.lastMessage = upstreamMessage
		tracker.mu.Unlock()
	}

	recordOpenAIUpstream5xxRetryEvent(OpenAIUpstream5xxRetryLogEntry{
		AtUnixMS:           now.UnixMilli(),
		Event:              OpenAIUpstream5xxRetryEventIntercepted,
		StatusCode:         failoverErr.StatusCode,
		UpstreamStatus:     failoverErr.StatusCode,
		RequestID:          openAIUpstream5xxRetryRequestID(c),
		ClientRequestID:    openAIUpstream5xxRetryClientRequestID(c),
		AccountID:          accountID,
		AccountName:        accountName,
		Model:              model,
		Transport:          openAIUpstream5xxRetryTransport(c),
		Path:               openAIUpstream5xxRetryPath(c),
		Attempt:            attempt,
		SameAccountAttempt: sameAccountAttempt,
		RetryCount:         totalRetries,
		RetryDelayMS:       retryDelay.Milliseconds(),
		SameAccountMax:     failoverErr.SameAccountRetryMax,
		UpstreamMessage:    upstreamMessage,
	})
}

// RecordOpenAIUpstream5xxRetrySkipped 记录一次「未重试」。
//
// 语义：状态码与账号都在覆盖范围内，但本次失败不适合原地重试。目前来源是
// 「已向客户端写出语义字节」（流中途 502/503，重试会造成重复输出）与凭证类失败。
// 这条日志让「为什么这次没重试」在后台可见，而不是静默跳过。
func RecordOpenAIUpstream5xxRetrySkipped(c *gin.Context, account *Account, statusCode int, model, reason string) {
	if !IsOpenAIUpstream5xxRetryCandidate(statusCode, account) {
		return
	}
	accountID, accountName := openAIUpstream5xxRetryAccountIdentity(account)
	recordOpenAIUpstream5xxRetryEvent(OpenAIUpstream5xxRetryLogEntry{
		AtUnixMS:        time.Now().UnixMilli(),
		Event:           OpenAIUpstream5xxRetryEventSkipped,
		StatusCode:      statusCode,
		UpstreamStatus:  statusCode,
		RequestID:       openAIUpstream5xxRetryRequestID(c),
		ClientRequestID: openAIUpstream5xxRetryClientRequestID(c),
		AccountID:       accountID,
		AccountName:     accountName,
		Model:           model,
		Transport:       openAIUpstream5xxRetryTransport(c),
		Path:            openAIUpstream5xxRetryPath(c),
		UpstreamMessage: reason,
	})
}

// FlushOpenAIUpstream5xxRetryTracker 在请求结束后落盘终局事件。
//
// 由访问日志中间件调用（请求结束后必然执行且只执行一次）。
// clientStatus 是实际返回给客户端的 HTTP 状态码：
//   - 2xx  → 重试成功；
//   - 其他 → 重试耗尽（等待时间已白花）。
//
// 额外耗时 = 现在 - 第一次拦截时刻，即相比「首次 502 直接失败」多花的墙钟时间。
//
// 本次请求没有任何拦截时直接返回，不产生任何写入。
func FlushOpenAIUpstream5xxRetryTracker(c *gin.Context, clientStatus int) {
	if c == nil {
		return
	}
	raw, ok := c.Get(openAIUpstream5xxRetryTrackerKey)
	if !ok {
		return
	}
	tracker, ok := raw.(*openAIUpstream5xxRetryTracker)
	if !ok || tracker == nil {
		return
	}

	tracker.mu.Lock()
	if tracker.flushed || tracker.intercepts == 0 {
		tracker.mu.Unlock()
		return
	}
	tracker.flushed = true
	entry := OpenAIUpstream5xxRetryLogEntry{
		AtUnixMS:        time.Now().UnixMilli(),
		StatusCode:      clientStatus,
		UpstreamStatus:  tracker.lastStatus,
		AccountID:       tracker.lastAccountID,
		AccountName:     tracker.lastAccountName,
		Model:           tracker.lastModel,
		Attempt:         tracker.intercepts,
		RetryCount:      tracker.intercepts,
		UpstreamMessage: tracker.lastMessage,
	}
	if !tracker.firstInterceptAt.IsZero() {
		entry.ExtraLatencyMS = time.Since(tracker.firstInterceptAt).Milliseconds()
	}
	tracker.mu.Unlock()

	entry.RequestID = openAIUpstream5xxRetryRequestID(c)
	entry.ClientRequestID = openAIUpstream5xxRetryClientRequestID(c)
	entry.Transport = openAIUpstream5xxRetryTransport(c)
	entry.Path = openAIUpstream5xxRetryPath(c)
	if clientStatus >= http.StatusOK && clientStatus < http.StatusMultipleChoices {
		entry.Event = OpenAIUpstream5xxRetryEventSucceeded
	} else {
		entry.Event = OpenAIUpstream5xxRetryEventExhausted
	}
	recordOpenAIUpstream5xxRetryEvent(entry)
}

// openAIUpstream5xxRetryTrackerFrom 取出（必要时创建）本次请求的 tracker。
func openAIUpstream5xxRetryTrackerFrom(c *gin.Context) *openAIUpstream5xxRetryTracker {
	if c == nil {
		return nil
	}
	if raw, ok := c.Get(openAIUpstream5xxRetryTrackerKey); ok {
		if tracker, ok := raw.(*openAIUpstream5xxRetryTracker); ok {
			return tracker
		}
	}
	tracker := &openAIUpstream5xxRetryTracker{}
	c.Set(openAIUpstream5xxRetryTrackerKey, tracker)
	return tracker
}

func openAIUpstream5xxRetryAccountIdentity(account *Account) (int64, string) {
	if account == nil {
		return 0, ""
	}
	return account.ID, strings.TrimSpace(account.Name)
}

// openAIUpstream5xxRetryTransport 复用访问日志的上游协议判定。
// 空值表示本次请求尚未完成协议决策，前端按「未知」展示而非误报 http_sse。
func openAIUpstream5xxRetryTransport(c *gin.Context) string {
	return GetOpenAIUpstreamTransport(c)
}

func openAIUpstream5xxRetryPath(c *gin.Context) string {
	if c == nil || c.Request == nil || c.Request.URL == nil {
		return ""
	}
	return c.Request.URL.Path
}

func openAIUpstream5xxRetryRequestID(c *gin.Context) string {
	if c == nil || c.Request == nil {
		return ""
	}
	value, _ := c.Request.Context().Value(ctxkey.RequestID).(string)
	return strings.TrimSpace(value)
}

func openAIUpstream5xxRetryClientRequestID(c *gin.Context) string {
	if c == nil || c.Request == nil {
		return ""
	}
	value, _ := c.Request.Context().Value(ctxkey.ClientRequestID).(string)
	return strings.TrimSpace(value)
}
