package service

import (
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// 容量降载重试的墙钟预算。
//
// 二次开发新增文件，上游不存在，不产生合并冲突。
//
// 问题：容量降载被判为 RequestScopedTransient，同时获得「同账号退避重试」与
// 「换账号重试」两级恢复能力。两级预算都按【次数】计：
//
//	每账号尝试数 = 1 + account.GetPoolModeRetryCount()        默认 4
//	账号数       = 1 + gateway.max_account_switches           默认 11
//
// 次数预算在快失败下合理，在慢失败下失控。生产实测（2026-09-08 01:25-01:40，
// 60 个降载请求的 upstream_errors 时间戳，dropped_earlier_attempts=0 无截断）：
//
//	单次尝试耗时     11.7 ~ 27.3s（上游要跑一段才放弃，不是准入拒绝）
//	重试总时长       p50 28.2s   p90 144.9s   max 544.5s（16 次尝试，跨 4 账号）
//
// 544 秒的请求客户端早已超时，服务端仍在为一个无人接收的响应消耗上游配额。
// 单位错了：现有机制数次数，而客户端在意的是墙钟。
//
// 重试本身是有效的，不能砍掉——同窗口实测救回率 73.8%（error_log.message 带
// "Recovered upstream error 503" 前缀的行）。有效的原因不是「换到了好账号」
// （降载在账号间均匀分布，每分钟 11.2/13 个账号同时降载），而是【等待了时间】
// ——尝试间隔 30~62s，上游容量在这期间恢复。
//
// 因此这里只给重试加墙钟封顶，不改变它的判定与顺序：预算内完全保留原有的
// 同账号退避 + 换账号能力，预算耗尽即停止重试并把上游原文返回客户端
// （ClientMessage 已由 newOpenAIUpstreamFailoverError 填好）。
//
// 预算按 90s 取值的依据（同批 30 个多次尝试请求的 span 分布）：
//
//	90s 内完成 22/30，被砍掉 8/30
//	   被砍掉的 span：544.5 / 215.3 / 202.6 / 144.9 / 139.9 / 127.0 / 99.7 / 94.8s
//	连首次即成功的 25 个一并计入，保住原救回量的 47/55 ≈ 85%
//	最坏总时长从 544s 降到「首次尝试 + 90s」≈ 120s
//
// 时间口径不可换成「按次数再收紧」：单次尝试耗时本身就有 11~27s 的离散，
// 同样的次数在不同上游状态下对应的墙钟差两倍以上，这正是原缺陷的成因。

const (
	// openAICapacityShedStartKey 存放本请求首次遇到容量降载的时刻。
	openAICapacityShedStartKey = "openai_capacity_shed_first_seen_at"

	// openAICapacityShedBudgetDefault 是默认墙钟预算，从首次降载起算。
	//
	// 不从请求开始起算：首次尝试的 11~27s 是无法避免的成本（上游必须跑完才
	// 告诉我们降载），把它算进预算等于让预算的实际可用部分随上游状态波动。
	// 因此请求的最坏总时长 ≈ 首次尝试 + 本预算 ≈ 120s。
	openAICapacityShedBudgetDefault = 90 * time.Second

	// openAICapacityShedSameAccountMaxRetries 是同账号退避重试的次数上限。
	//
	// 低于 pool_mode_retry_count 默认值 3：那个默认值服务的是 Google 间歇性
	// 400、空响应这类重试成本近似为零的错误，用在 11~31s 才返回的容量降载上
	// 属于量纲错配——3 次就把整个墙钟预算吃光，换号那一级永远走不到。
	//
	// 实际生效次数由时间闸决定（见 applyOpenAICapacityShedSameAccountLimit），
	// 本常量只是慢档失效时的兜底上限。
	openAICapacityShedSameAccountMaxRetries = 2

	// openAICapacityShedBudgetEnv 允许生产不改代码调整预算，单位秒；
	// 设为 0 表示禁用封顶，回到纯次数预算的上游行为。
	//
	// 用环境变量而不是 config.yaml：后者需要改上游 internal/config/config.go
	// （viper 默认值 + 结构体字段 + 校验三处），而该文件是上游高频改动区。
	openAICapacityShedBudgetEnv = "SUB2API_OPENAI_CAPACITY_SHED_BUDGET_SECONDS"
)

// openAICapacityShedBudget 解析生效的预算值。
//
// 非法值（负数、无法解析）一律回落默认值而不是报错：这是降级路径上的旁路
// 保护，配置写错不应让重试行为变得比没有该功能时更差。
func openAICapacityShedBudget() time.Duration {
	raw := strings.TrimSpace(os.Getenv(openAICapacityShedBudgetEnv))
	if raw == "" {
		return openAICapacityShedBudgetDefault
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds < 0 {
		return openAICapacityShedBudgetDefault
	}
	return time.Duration(seconds) * time.Second
}

// openAICapacityShedElapsed 返回自首次降载起的已耗时，以及是否已有记录。
func openAICapacityShedElapsed(c *gin.Context) (time.Duration, bool) {
	startedAt, ok := openAICapacityShedStartedAt(c)
	if !ok {
		return 0, false
	}
	return time.Since(startedAt), true
}

// openAICapacityShedStartedAt 返回本请求首次降载的时刻。
func openAICapacityShedStartedAt(c *gin.Context) (time.Time, bool) {
	if c == nil {
		return time.Time{}, false
	}
	raw, ok := c.Get(openAICapacityShedStartKey)
	if !ok {
		return time.Time{}, false
	}
	startedAt, ok := raw.(time.Time)
	if !ok || startedAt.IsZero() {
		return time.Time{}, false
	}
	return startedAt, true
}

// applyOpenAICapacityShedBudget 给容量降载的 failover 错误加墙钟封顶。
//
// 返回的仍是同一个 *UpstreamFailoverError（原地修改），使调用方无需区分
// 「被封顶」与「未被封顶」两种返回形态——handler 侧读的是字段，不是类型。
//
// 只处理容量降载：IsOpenAICapacityShed 要求 RequestScopedTransient 为真且
// ResponseBody 命中降载判据，因此限流（429 落库 resets_at）、账号级 4xx
// （401/402/403 走 HandleUpstreamError + 换号）等语义完全不受影响。
func applyOpenAICapacityShedBudget(c *gin.Context, failoverErr *UpstreamFailoverError) *UpstreamFailoverError {
	if failoverErr == nil || c == nil || !failoverErr.IsOpenAICapacityShed() {
		return failoverErr
	}
	budget := openAICapacityShedBudget()
	if budget <= 0 {
		return failoverErr
	}
	startedAt, seen := openAICapacityShedStartedAt(c)
	if !seen {
		// 首次降载：只打起点，保留完整重试能力。此时尚未产生任何重试开销，
		// 立刻封顶会把「一次都不重试」当成常态，丢掉全部救回量。
		startedAt = time.Now()
		c.Set(openAICapacityShedStartKey, startedAt)
	}
	if time.Since(startedAt) < budget {
		// 预算内：同账号阶梯另有限时限次，把剩余预算让给换号。
		applyOpenAICapacityShedSameAccountLimit(failoverErr, startedAt, budget)
		return failoverErr
	}
	// 预算耗尽：两级恢复能力一起收掉。只收一级没有意义——留下同账号重试会
	// 继续按 500ms/1s/2s 退避重试满次数，留下换账号会让新账号重新获得完整的
	// 同账号预算，正是产生 544s 的那个乘法。
	failoverErr.RetryableOnSameAccount = false
	failoverErr.NextAccountAction = NextAccountStop
	// ClientStatusCode/ClientMessage 保持 newOpenAIUpstreamFailoverError 填好的
	// 503 + 上游原文：客户端需要知道是容量问题才能自行退避，而不是一个 502。
	return failoverErr
}

// ApplyOpenAICapacityShedBudgetForTest 仅供 handler 包的端到端模拟测试使用：
// 显式指定本请求首次降载的时刻，以便在不依赖真实时钟的情况下推演重试阶梯。
func ApplyOpenAICapacityShedBudgetForTest(
	c *gin.Context,
	failoverErr *UpstreamFailoverError,
	shedStartedAt time.Time,
) *UpstreamFailoverError {
	if c != nil && !shedStartedAt.IsZero() {
		c.Set(openAICapacityShedStartKey, shedStartedAt)
	}
	return applyOpenAICapacityShedBudget(c, failoverErr)
}

// NewOpenAICapacityShedFailoverErrorForTest 构造一个与生产同形的降载 failover 错误。
func NewOpenAICapacityShedFailoverErrorForTest() *UpstreamFailoverError {
	return &UpstreamFailoverError{
		StatusCode: http.StatusServiceUnavailable,
		ResponseBody: []byte(`{"type":"error","error":{"code":"server_is_overloaded",` +
			`"type":"service_unavailable_error",` +
			`"message":"Our servers are currently overloaded. Please try again later."}}`),
		RetryableOnSameAccount: true,
		RequestScopedTransient: true,
		ClientStatusCode:       http.StatusServiceUnavailable,
		ClientMessage:          "Our servers are currently overloaded. Please try again later.",
	}
}

// applyOpenAICapacityShedSameAccountLimit 给同账号阶梯加时间与次数双闸。
//
// ## 为什么必须限制同账号阶梯
//
// 上一版只给整体加了墙钟预算，结果换号那一级被同账号阶梯饿死。生产实测
// （部署后 1 小时，21 条多次尝试请求）：
//
//	跨账号 0/21      ← 一次都没换过号
//	span   p50 97.5s  p90 124.9s  MAX 127.7s
//
// 对比部署前同一指标：单请求最多跨 4 个账号、MAX 544.5s。也就是说墙钟预算
// 确实把最坏时长压下来了，代价却是把「先同账号退避、不行再换账号」的第二级
// 完全消灭。
//
// 算术很直白：单次尝试约 31s，同账号阶梯 = 1 + pool_mode_retry_count(3)
// ≈ 97s，已经超过 90s 总预算，switchCount 一次都加不上。
//
// ## 为什么不是简单把次数从 3 改成 2
//
// 31s × 3 次 = 93s 仍然超预算，换号照样走不到。而尝试耗时本身在 11~31s
// 之间浮动（同一天不同时段实测），任何固定次数在慢档下都会重蹈覆辙——
// 这与本文件开头论述的「次数预算在慢失败下失控」是同一个病，只是发生在
// 下一层。
//
// ## 做法
//
// 时间闸为主、次数闸兜底，让实际重试次数随尝试耗时自适应：
//
//	慢档（约 31s/次）：t=31 允许重试；t=62 超过半程闸 → 换号。得 1 次
//	快档（约 11s/次）：t=11、t=22 允许；t=33 触次数闸 → 换号。得 2 次
//
// 两档都保证「至少换一次号」，且都落在 1~2 次同账号重试的区间内。
//
// 半程（budget/2）而非三分之一：后者在慢档下第一次失败（31s）就已越线，
// 等于同账号一次都不重试，违背「先同账号退避」的初衷。
func applyOpenAICapacityShedSameAccountLimit(
	failoverErr *UpstreamFailoverError,
	startedAt time.Time,
	budget time.Duration,
) {
	if failoverErr == nil || !failoverErr.RetryableOnSameAccount || budget <= 0 {
		return
	}
	// 次数闸：只收紧不放宽。handler 的 sameAccountRetryAllowed 取
	// min(SameAccountRetryMax, account.GetPoolModeRetryCount())，
	// 账号显式配置更小值时仍以账号为准。
	if failoverErr.SameAccountRetryMax <= 0 ||
		failoverErr.SameAccountRetryMax > openAICapacityShedSameAccountMaxRetries {
		failoverErr.SameAccountRetryMax = openAICapacityShedSameAccountMaxRetries
	}
	// 时间闸：同账号阶梯只能用掉前半程预算，后半程留给换号。
	// 已有更早的截止时间时不放宽（例如 OAuth 429 自带的窗口）。
	deadline := startedAt.Add(budget / 2)
	if failoverErr.SameAccountRetryDeadline.IsZero() ||
		deadline.Before(failoverErr.SameAccountRetryDeadline) {
		failoverErr.SameAccountRetryDeadline = deadline
	}
}
