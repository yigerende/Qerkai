package handler

import (
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

// 上游 502/503 过载重试的 handler 侧接入（二次开发功能，非上游代码）
//
// 本文件把「记录拦截」与「累计预算判定」收敛成两个单行调用，使 6 个
// OpenAI failover 循环各自只需插入极少的代码。上游不存在同名文件，
// 合并冲突代价为零。
//
// 关闭开关后的行为保证：两个函数内部第一件事都是
// service.IsOpenAIUpstream5xxRetryCandidate 判定，关闭时
//   - noteOpenAIUpstream5xxRetry 不写任何日志、不建 tracker；
//   - openAIUpstream5xxRetryBudgetExhausted 恒返回 false，调用方完全沿用
//     上游原有的换号预算判定。
//
// 即：开关关闭时链路与上游原始逻辑逐字节一致。

// noteOpenAIUpstream5xxRetry 在同账号重试即将发生时记录一条「已拦截，准备重试」。
//
// 调用位置：handler 同账号重试分支中，sameAccountRetryCount 自增之后、
// time.After 等待之前 —— 此刻已确定「这次一定会重试」，且两个计数器都是最终值。
func noteOpenAIUpstream5xxRetry(
	c *gin.Context,
	account *service.Account,
	failoverErr *service.UpstreamFailoverError,
	model string,
	sameAccountAttempt int,
	sameAccountRetryCount map[int64]int,
	switchCount int,
	retryDelay time.Duration,
) {
	service.NoteOpenAIUpstream5xxRetryIntercept(
		c,
		account,
		failoverErr,
		model,
		sameAccountAttempt,
		openAIUpstream5xxRetriesUsed(sameAccountRetryCount, switchCount),
		retryDelay,
	)
}

// markOpenAIUpstream5xxRetryCompleted 把成功重试的终点标记在首个有效输出处。
// 流式 Forward 会等整条流结束后才返回，但 result.FirstTokenMs 保留了首个输出
// 相对本次 attempt 开始的时间，因此可以避免把后续生成时间计入重试额外耗时。
func markOpenAIUpstream5xxRetryCompleted(
	c *gin.Context,
	attemptStartedAt time.Time,
	result *service.OpenAIForwardResult,
	err error,
) {
	if err != nil || result == nil {
		return
	}
	completedAt := time.Now()
	if result.FirstTokenMs != nil && !attemptStartedAt.IsZero() && *result.FirstTokenMs >= 0 {
		completedAt = attemptStartedAt.Add(time.Duration(*result.FirstTokenMs) * time.Millisecond)
		if completedAt.After(time.Now()) {
			completedAt = time.Now()
		}
	}
	service.MarkOpenAIUpstream5xxRetryCompleted(c, completedAt)
}

// openAIUpstream5xxRetryBudgetExhausted 报告本次请求的累计重试预算是否用尽。
//
// 调用位置：handler 换号预算检查之前。返回 true 时调用方应按「failover 耗尽」
// 处理，即把上游错误正常返回客户端。
//
// 返回 false 时调用方必须完全沿用上游原有的换号预算判定 —— 本函数只收紧、
// 从不放宽预算。
func openAIUpstream5xxRetryBudgetExhausted(
	account *service.Account,
	failoverErr *service.UpstreamFailoverError,
	sameAccountRetryCount map[int64]int,
	switchCount int,
) bool {
	return service.OpenAIUpstream5xxTotalRetryBudgetExhausted(failoverErr, account, sameAccountRetryCount, switchCount)
}

// openAIUpstream5xxRetriesUsed 汇总本次请求已发生的重试次数。
//
// 口径与 service.OpenAIUpstream5xxTotalRetryBudgetExhausted 保持一致：
// 所有账号的同账号重试次数之和 + 已发生的换号次数。
func openAIUpstream5xxRetriesUsed(sameAccountRetryCount map[int64]int, switchCount int) int {
	used := switchCount
	for _, count := range sameAccountRetryCount {
		used += count
	}
	return used
}

// openAIUpstream5xxRetrySkipReasonStreamStarted 是「已向客户端写出语义字节」的
// 统一原因文案。流中途收到 502/503 时重试会造成重复输出，因此只能把错误按已提交
// 的响应返回客户端。
const openAIUpstream5xxRetrySkipReasonStreamStarted = "已向客户端写出内容，重试会造成重复输出"

// recordOpenAIUpstream5xxRetrySkipped 记录一次「未重试」。
//
// 语义：状态码与账号都在覆盖范围内，但本次失败不适合原地重试 —— 主要来源是
// 「已向客户端写出语义字节」（流中途 502/503，重试会造成重复输出）。
// 这条记录让「为什么这次没重试」在后台可见，而不是静默跳过。
func recordOpenAIUpstream5xxRetrySkipped(
	c *gin.Context,
	account *service.Account,
	failoverErr *service.UpstreamFailoverError,
	model string,
	reason string,
) {
	if failoverErr == nil {
		return
	}
	service.RecordOpenAIUpstream5xxRetrySkipped(c, account, failoverErr.StatusCode, model, reason)
}
