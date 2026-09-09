package service

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// 大 payload 的写超时与重连预算。
//
// 二次开发新增文件，上游不存在，不产生合并冲突。
//
// ## 问题一：写超时吃光重连预算
//
// 生产日志（2.4MB payload）：
//
//	write_request_fail account_id=54594 payload_bytes=2431550
//	  cause=... context deadline exceeded
//	reconnect_budget_exhausted attempts=1 max_retries=5
//	  reason=write_request elapsed_ms=120004 budget_ms=5000
//
// 两个时长的量级差了 24 倍：
//
//	write_timeout_seconds     默认 120s   ← 单次写操作的上限
//	retry_total_budget_ms     默认 5000ms ← 整个请求的重连预算
//
// openai_gateway_forward.go 的 retryStartedAt 打在【首次尝试之前】，
// 于是首次尝试自身的耗时也算进预算。写超时耗满 120s 后，
// time.Since(retryStartedAt) 已是 120004ms，远超 5000ms 预算，
// 循环在第一次 attempt 后就 break——大 payload 请求实际上没有任何重连保护，
// 而它恰恰是最需要重连的那一类（连接在长写过程中更可能已经失效）。
//
// 预算本身的语义是合理的：限制「因重试而额外付出的时间」。错的是把
// 不可避免的首次尝试也算作重试开销。但 retryStartedAt 在上游高频文件里，
// 且该口径同时服务其它 reason（read_event 等，那些的首次尝试很快，
// 现有口径没有暴露问题）。因此这里不改预算口径，而是从成因入手：
// 让写超时与 payload 大小匹配，不再出现单次写占满两分钟的情况。
//
// ## 问题二：写超时对大 payload 过长
//
// 120s 是给「任意大小 payload」定的统一上限，对 2.4MB 明显过宽：即使
// 按较差的 1MB/s 估算也只需 2.4s。真实情况是连接已经不可写（对端静默
// 消失），而 120s 让这个事实推迟两分钟才被发现——期间客户端在等，
// 上游连接槽被占，重连预算被消耗殆尽。
//
// 按字节数给出上限，取 max(下限, 字节数/吞吐 × 安全系数)，并整体不超过
// 配置的 write_timeout_seconds：既不会让小 payload 因超时过短而误判，
// 也不会让大 payload 卡满两分钟。

const (
	// openAIWSWriteBudgetFloor 是写超时下限。
	//
	// 小 payload 的写失败几乎都是连接已死，不需要给足时间；但也不能太短，
	// 否则跨洲链路的正常抖动会被误判为失败。
	openAIWSWriteBudgetFloor = 10 * time.Second

	// openAIWSWriteBudgetBytesPerSecond 是估算用的保守吞吐。
	//
	// 取 512KB/s——远低于真实链路能力，留足余量给 TLS、代理与拥塞控制。
	// 按此估算 2.4MB 得 4.8s，乘安全系数后约 14s，与下限同阶。
	openAIWSWriteBudgetBytesPerSecond = 512 * 1024

	// openAIWSWriteBudgetSafetyFactor 是估算值的安全系数。
	openAIWSWriteBudgetSafetyFactor = 3

	// openAIWSWriteBudgetEnv 允许生产不改代码覆盖上限（秒）；
	// 设为 0 表示禁用按字节收紧，回到配置的 write_timeout_seconds。
	//
	// 用环境变量而非 config.yaml：后者需要改上游 config.go 三处
	// （viper 默认值 + 结构体字段 + 校验），而该文件是上游高频改动区。
	openAIWSWriteBudgetEnv = "SUB2API_OPENAI_WS_WRITE_BUDGET_SECONDS"
)

// openAIWSWriteBudgetCap 返回按字节收紧后的写超时上限。
//
// configured 是配置的 write_timeout_seconds 折算值，作为绝对上限：
// 本函数只会收紧、不会放宽，因此不改变任何既有的超时上界语义。
func openAIWSWriteBudgetCap(payloadBytes int, configured time.Duration) time.Duration {
	if configured <= 0 {
		return configured
	}
	if override, ok := openAIWSWriteBudgetOverride(); ok {
		if override <= 0 {
			// 显式禁用：保持配置值。
			return configured
		}
		if override < configured {
			return override
		}
		return configured
	}
	if payloadBytes <= 0 {
		// 未知大小时不做任何假设，交回配置值。
		return configured
	}
	estimated := time.Duration(payloadBytes) * time.Second / openAIWSWriteBudgetBytesPerSecond
	estimated *= openAIWSWriteBudgetSafetyFactor
	if estimated < openAIWSWriteBudgetFloor {
		estimated = openAIWSWriteBudgetFloor
	}
	if estimated > configured {
		return configured
	}
	return estimated
}

// openAIWSWriteBudgetOverride 解析环境变量覆盖值。
//
// 非法值（负数、无法解析）视为未设置：这是降级路径上的旁路保护，
// 配置写错不应让行为比没有该功能时更差。
func openAIWSWriteBudgetOverride() (time.Duration, bool) {
	raw := strings.TrimSpace(os.Getenv(openAIWSWriteBudgetEnv))
	if raw == "" {
		return 0, false
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds < 0 {
		return 0, false
	}
	return time.Duration(seconds) * time.Second, true
}
