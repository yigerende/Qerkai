package service

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

// 上游 502/503 过载重试（二次开发功能，非上游代码）
//
// 需求：OpenAI 上游偶发返回 502/503（"服务过载"）时，上游代码会把该错误直接
// 抛给客户端 —— Codex CLI 侧表现为一次可见的失败。这类错误具备两个特征：
//   1. 请求确定未被处理（上游返回的是网关层错误，不含任何模型输出）；
//   2. 复发概率低，几百毫秒后原账号重试即可成功。
//
// 因此在网关内部做短间隔原地重试，对客户端完全透明。
//
// 设计取舍（为降低与上游的长期合并冲突）：
//   - 判定、配置与观测全部集中在本新增文件与 openai_upstream_5xx_retry_log.go，
//     上游不存在同名文件，冲突代价为零。
//   - 不新造重试机械：复用上游 UpstreamFailoverError 上既有的
//     RetryableOnSameAccount / RequestScopedTransient / SameAccountRetryMax /
//     SameAccountRetryDelay 契约，由 handler 层现成的 sameAccountRetryAllowed
//     循环执行重试。本文件只负责「给错误打上可重试标记」。
//   - 对上游文件的改动是每处 1~6 行的接入点，无逻辑重写。
//
// 关闭开关后的行为保证：
//
//	本文件所有导出/包内入口的第一件事都是 OpenAIUpstream5xxRetryEnabled() 判定，
//	返回 false 时立即原样返回，不修改任何 failoverErr 字段、不写日志、不影响
//	账号降温、不改变重试预算。即开关关闭时链路与上游原始逻辑逐字节一致。

const (
	// openAIUpstream5xxRetryCacheTTL 与 force_openai_upstream_ws 的缓存周期一致。
	openAIUpstream5xxRetryCacheTTL = 60 * time.Second
	// openAIUpstream5xxRetryErrorTTL 读库失败时的短 TTL，便于快速恢复。
	openAIUpstream5xxRetryErrorTTL = 5 * time.Second
	// openAIUpstream5xxRetryDBTimeout 限制 singleflight 内的查询耗时。
	openAIUpstream5xxRetryDBTimeout = 5 * time.Second
	// openAIUpstream5xxRetrySFKey singleflight 的合并键（一次取齐 4 个设置项）。
	openAIUpstream5xxRetrySFKey = "openai_upstream_5xx_retry_config"
)

// 默认值：原账号 2 次、整请求累计 5 次、间隔 500ms。
// N=2 × 500ms 落在「客户端感知不到、又足以跨过上游瞬时抖动」的量级。
const (
	DefaultOpenAIUpstream5xxRetrySameAccount = 2
	DefaultOpenAIUpstream5xxRetryTotal       = 5
	DefaultOpenAIUpstream5xxRetryDelayMS     = 500

	// 上下界仅用于抵挡明显的误配置，与前端 input 的 min/max 保持一致。
	MinOpenAIUpstream5xxRetrySameAccount = 0
	MaxOpenAIUpstream5xxRetrySameAccount = 10
	MinOpenAIUpstream5xxRetryTotal       = 1
	MaxOpenAIUpstream5xxRetryTotal       = 20
	MinOpenAIUpstream5xxRetryDelayMS     = 0
	MaxOpenAIUpstream5xxRetryDelayMS     = 5000
)

// OpenAIUpstream5xxRetryConfig 是本功能的运行时配置快照。
type OpenAIUpstream5xxRetryConfig struct {
	Enabled     bool
	SameAccount int
	Total       int
	Delay       time.Duration
}

type cachedOpenAIUpstream5xxRetryConfig struct {
	config    OpenAIUpstream5xxRetryConfig
	expiresAt int64 // unix nano
}

var (
	openAIUpstream5xxRetryCache    atomic.Value // *cachedOpenAIUpstream5xxRetryConfig
	openAIUpstream5xxRetrySF       singleflight.Group
	openAIUpstream5xxRetrySettings atomic.Pointer[SettingService]
	// openAIUpstream5xxRetryOverride 仅供测试直接指定配置，生产路径始终为空。
	openAIUpstream5xxRetryOverride atomic.Pointer[OpenAIUpstream5xxRetryConfig]
)

// registerOpenAIUpstream5xxRetrySettingService 由 NewSettingService 调用。
//
// 与 registerForceUpstreamWSSettingService 同因：判定发生在没有 context 也没有
// service 引用的调用点（failover 错误构造、账号降温判定），因此采用
// 「包级注册 SettingService + 进程内缓存」而非参数透传。
func registerOpenAIUpstream5xxRetrySettingService(s *SettingService) {
	if s == nil {
		return
	}
	openAIUpstream5xxRetrySettings.Store(s)
}

// refreshOpenAIUpstream5xxRetryCache 在设置写入后立即刷新缓存，使配置即时生效
// （否则最长需等待一个 TTL 周期）。先 Forget 再写入，缩小旧值覆盖新值的竞态窗口。
func refreshOpenAIUpstream5xxRetryCache(settings *SystemSettings) {
	if settings == nil {
		return
	}
	openAIUpstream5xxRetrySF.Forget(openAIUpstream5xxRetrySFKey)
	openAIUpstream5xxRetryCache.Store(&cachedOpenAIUpstream5xxRetryConfig{
		config: OpenAIUpstream5xxRetryConfig{
			Enabled:     settings.OpenAIUpstream5xxRetryEnabled,
			SameAccount: clampOpenAIUpstream5xxRetrySameAccount(settings.OpenAIUpstream5xxRetrySameAccount),
			Total:       clampOpenAIUpstream5xxRetryTotal(settings.OpenAIUpstream5xxRetryTotal),
			Delay:       time.Duration(clampOpenAIUpstream5xxRetryDelayMS(settings.OpenAIUpstream5xxRetryDelayMS)) * time.Millisecond,
		},
		expiresAt: time.Now().Add(openAIUpstream5xxRetryCacheTTL).UnixNano(),
	})
}

func clampOpenAIUpstream5xxRetrySameAccount(value int) int {
	if value < MinOpenAIUpstream5xxRetrySameAccount {
		return MinOpenAIUpstream5xxRetrySameAccount
	}
	if value > MaxOpenAIUpstream5xxRetrySameAccount {
		return MaxOpenAIUpstream5xxRetrySameAccount
	}
	return value
}

func clampOpenAIUpstream5xxRetryTotal(value int) int {
	if value < MinOpenAIUpstream5xxRetryTotal {
		return MinOpenAIUpstream5xxRetryTotal
	}
	if value > MaxOpenAIUpstream5xxRetryTotal {
		return MaxOpenAIUpstream5xxRetryTotal
	}
	return value
}

func clampOpenAIUpstream5xxRetryDelayMS(value int) int {
	if value < MinOpenAIUpstream5xxRetryDelayMS {
		return MinOpenAIUpstream5xxRetryDelayMS
	}
	if value > MaxOpenAIUpstream5xxRetryDelayMS {
		return MaxOpenAIUpstream5xxRetryDelayMS
	}
	return value
}

// OpenAIUpstream5xxRetrySettings 返回当前配置快照。
//
// 热路径上零锁：命中未过期缓存时只做一次 atomic.Load。
// 缺少 SettingService（仅测试或误配可达）时返回关闭态，即保持上游默认行为。
func OpenAIUpstream5xxRetrySettings() OpenAIUpstream5xxRetryConfig {
	if override := openAIUpstream5xxRetryOverride.Load(); override != nil {
		return *override
	}
	if cached, ok := openAIUpstream5xxRetryCache.Load().(*cachedOpenAIUpstream5xxRetryConfig); ok && cached != nil {
		if time.Now().UnixNano() < cached.expiresAt {
			return cached.config
		}
	}
	svc := openAIUpstream5xxRetrySettings.Load()
	if svc == nil || svc.settingRepo == nil {
		return OpenAIUpstream5xxRetryConfig{}
	}
	result, _, _ := openAIUpstream5xxRetrySF.Do(openAIUpstream5xxRetrySFKey, func() (any, error) {
		if cached, ok := openAIUpstream5xxRetryCache.Load().(*cachedOpenAIUpstream5xxRetryConfig); ok && cached != nil {
			if time.Now().UnixNano() < cached.expiresAt {
				return cached.config, nil
			}
		}
		dbCtx, cancel := context.WithTimeout(context.Background(), openAIUpstream5xxRetryDBTimeout)
		defer cancel()

		enabled, enabledErr := openAIUpstream5xxRetryBoolSetting(dbCtx, svc, SettingKeyOpenAIUpstream5xxRetryEnabled)
		config := OpenAIUpstream5xxRetryConfig{
			Enabled: enabled,
			SameAccount: clampOpenAIUpstream5xxRetrySameAccount(openAIUpstream5xxRetryIntSetting(
				dbCtx, svc, SettingKeyOpenAIUpstream5xxRetrySameAccount, DefaultOpenAIUpstream5xxRetrySameAccount)),
			Total: clampOpenAIUpstream5xxRetryTotal(openAIUpstream5xxRetryIntSetting(
				dbCtx, svc, SettingKeyOpenAIUpstream5xxRetryTotal, DefaultOpenAIUpstream5xxRetryTotal)),
		}
		config.Delay = time.Duration(clampOpenAIUpstream5xxRetryDelayMS(openAIUpstream5xxRetryIntSetting(
			dbCtx, svc, SettingKeyOpenAIUpstream5xxRetryDelayMS, DefaultOpenAIUpstream5xxRetryDelayMS))) * time.Millisecond

		ttl := openAIUpstream5xxRetryCacheTTL
		if enabledErr != nil {
			// 读库异常（非「设置不存在」）：按关闭态短 TTL 缓存，便于快速恢复。
			ttl = openAIUpstream5xxRetryErrorTTL
		}
		openAIUpstream5xxRetryCache.Store(&cachedOpenAIUpstream5xxRetryConfig{
			config:    config,
			expiresAt: time.Now().Add(ttl).UnixNano(),
		})
		return config, nil
	})
	if config, ok := result.(OpenAIUpstream5xxRetryConfig); ok {
		return config
	}
	return OpenAIUpstream5xxRetryConfig{}
}

// openAIUpstream5xxRetryBoolSetting 读取布尔设置。
// 返回的 error 仅在「读库真实失败」时非空；「设置不存在」按默认关闭处理。
func openAIUpstream5xxRetryBoolSetting(ctx context.Context, svc *SettingService, key string) (bool, error) {
	raw, err := svc.settingRepo.GetValue(ctx, key)
	if err != nil {
		if errors.Is(err, ErrSettingNotFound) {
			return false, nil
		}
		return false, err
	}
	return strings.EqualFold(strings.TrimSpace(raw), "true"), nil
}

func openAIUpstream5xxRetryIntSetting(ctx context.Context, svc *SettingService, key string, fallback int) int {
	raw, err := svc.settingRepo.GetValue(ctx, key)
	if err != nil {
		return fallback
	}
	value, convErr := strconv.Atoi(strings.TrimSpace(raw))
	if convErr != nil {
		return fallback
	}
	return value
}

// OpenAIUpstream5xxRetryEnabled 报告 502/503 重试是否开启。
//
// 这是所有接入点的第一道判定：返回 false 时整条链路必须与上游原始逻辑一致。
func OpenAIUpstream5xxRetryEnabled() bool {
	config := OpenAIUpstream5xxRetrySettings()
	// SameAccount=0 且 Total 无意义时视为未启用，避免打上可重试标记后
	// 因预算为 0 而白跑一次判定。
	return config.Enabled && config.SameAccount > 0
}

// setOpenAIUpstream5xxRetryForTest 供测试覆盖配置，返回还原函数。
func setOpenAIUpstream5xxRetryForTest(config OpenAIUpstream5xxRetryConfig) func() {
	openAIUpstream5xxRetryOverride.Store(&config)
	return func() { openAIUpstream5xxRetryOverride.Store(nil) }
}

// isOpenAIUpstream5xxRetryStatus 判断状态码是否属于本功能覆盖范围。
//
// 只认 502 与 503：这两者语义明确 —— 上游网关层未能把请求交给模型，
// 请求确定未被处理，原地重试绝对安全。
//
// 刻意不含 500/504：500 可能来自模型侧已开始处理后的内部错误，
// 504 意味着上游已在等待模型响应（重试会造成重复计费与重复副作用）。
// 这两者继续走上游既有的 failover 语义。
func isOpenAIUpstream5xxRetryStatus(statusCode int) bool {
	switch statusCode {
	case http.StatusBadGateway, http.StatusServiceUnavailable:
		return true
	default:
		return false
	}
}

// isOpenAIUpstream5xxRetryAccount 判断账号是否在本功能覆盖范围内。
//
// 仅 OpenAI OAuth 类账号：这是 Codex 链路上会遇到上游 502/503 的账号类型。
// API Key 账号不覆盖 —— 其 5xx 往往来自中转商，语义不可一概而论，
// 且上游对 API Key 账号已有 recordOpenAIAccountModelTransientFailure 的
// 账号+模型级降温策略，不应被本功能绕过。
//
// Spark 影子账号排除：其调度与降温有独立语义（见 markOpenAIOAuth429RateLimited）。
func isOpenAIUpstream5xxRetryAccount(account *Account) bool {
	if account == nil || !account.IsOpenAI() {
		return false
	}
	if account.IsShadow() {
		return false
	}
	return account.IsOpenAIOAuthLike()
}

// IsOpenAIUpstream5xxRetryCandidate 报告「本次失败是否由本功能接管重试」。
//
// 供 handler 层复用同一判据，避免各处重复展开条件。
func IsOpenAIUpstream5xxRetryCandidate(statusCode int, account *Account) bool {
	return OpenAIUpstream5xxRetryEnabled() &&
		isOpenAIUpstream5xxRetryStatus(statusCode) &&
		isOpenAIUpstream5xxRetryAccount(account)
}

// markOpenAIUpstream5xxRetryable 给 502/503 的 failover 错误打上同账号可重试标记。
//
// 由 newOpenAIAccountFailoverErrorWithClassificationHeaders 调用 —— 那是所有
// 账号相关 OpenAI 上游失败（HTTP/SSE、WS 握手、WS 流内终止事件、passthrough）
// 构造 failover 错误的唯一漏斗，一处接入即全链路覆盖。
//
// 三项字段的作用：
//   - RetryableOnSameAccount：让 handler 的 sameAccountRetryAllowed 允许原地重试；
//   - RequestScopedTransient：声明「故障与账号健康无关」，据此
//     TempUnscheduleRetryableError 不会在重试耗尽时临时封禁账号
//     （上游 gateway_service.go 已有该早退）；
//   - SameAccountRetryMax / SameAccountRetryDelay：次数与固定间隔。
//     SameAccountRetryDelay 非零时优先于上游的指数退避，保证间隔可控。
func markOpenAIUpstream5xxRetryable(failoverErr *UpstreamFailoverError, account *Account) {
	if failoverErr == nil {
		return
	}
	if !OpenAIUpstream5xxRetryEnabled() {
		return
	}
	if !isOpenAIUpstream5xxRetryStatus(failoverErr.StatusCode) {
		return
	}
	if !isOpenAIUpstream5xxRetryAccount(account) {
		return
	}
	// 凭证类失败（账号/工作区停用等）已由上游归入 AccountAuth 阶段并要求换号，
	// 重试同一账号必然再次失败。
	if failoverErr.Stage == GatewayFailureStageAccountAuth {
		return
	}
	// 上游已判定为请求级瞬时故障（容量降载 capacity shed）时，重试预算与
	// 客户端文案都已由上游填好。不覆盖，保持既有语义。
	if failoverErr.RequestScopedTransient {
		return
	}
	// 上游已给出更具体的失败归因（如 413 请求体超限）时不接管。
	if strings.TrimSpace(string(failoverErr.Reason)) != "" {
		return
	}
	config := OpenAIUpstream5xxRetrySettings()
	failoverErr.RetryableOnSameAccount = true
	failoverErr.RequestScopedTransient = true
	failoverErr.SameAccountRetryMax = config.SameAccount
	failoverErr.SameAccountRetryDelay = config.Delay
}

// OpenAIUpstream5xxSameAccountRetryLimit 返回本功能要求的同账号重试上限。
//
// 存在必要性：handler 的 effectiveSameAccountRetryLimit 默认取
// account.GetPoolModeRetryCount()，非池模式账号恒为 defaultPoolModeRetryCount(3)，
// 会把管理员配置的 N>3 静默截断到 3。
//
// 返回 0 表示「本功能不接管，沿用上游预算」。判据完全由 status + account + 配置
// 重新导出，因此不需要在 UpstreamFailoverError 上新增标记字段（那会改动上游结构体）。
func OpenAIUpstream5xxSameAccountRetryLimit(failoverErr *UpstreamFailoverError, account *Account) int {
	if failoverErr == nil {
		return 0
	}
	if !IsOpenAIUpstream5xxRetryCandidate(failoverErr.StatusCode, account) {
		return 0
	}
	config := OpenAIUpstream5xxRetrySettings()
	// 只在该错误确实由 markOpenAIUpstream5xxRetryable 标记过时接管：
	// SameAccountRetryMax 与当前配置一致是其充分特征。
	if failoverErr.SameAccountRetryMax != config.SameAccount {
		return 0
	}
	return config.SameAccount
}

// OpenAIUpstream5xxTotalRetryBudgetExhausted 判断本次请求的累计重试预算是否用尽。
//
// 累计口径 = 所有账号的同账号重试次数之和 + 已发生的换号次数。
// 达到 Total 后停止 failover，按正常错误返回客户端 —— 这是「整个请求总共重试
// 多少次」的硬上限，防止在上游大面积过载时把单个请求拖成分钟级等待。
//
// 返回 false 时调用方必须完全沿用上游原有的换号预算判定。
func OpenAIUpstream5xxTotalRetryBudgetExhausted(
	failoverErr *UpstreamFailoverError,
	account *Account,
	sameAccountRetryCount map[int64]int,
	switchCount int,
) bool {
	if failoverErr == nil {
		return false
	}
	if !IsOpenAIUpstream5xxRetryCandidate(failoverErr.StatusCode, account) {
		return false
	}
	config := OpenAIUpstream5xxRetrySettings()
	used := switchCount
	for _, count := range sameAccountRetryCount {
		used += count
	}
	return used >= config.Total
}

// 关于账号降温：本功能无需任何跳过逻辑。
//
// 上游 handleOpenAIAccountUpstreamError 中的账号+模型级瞬时降温分支
// （recordOpenAIAccountModelTransientFailure）显式要求
// account.Type == AccountTypeAPIKey，而本功能的覆盖范围是 OpenAI OAuth /
// SetupToken 账号，两者互不相交；同一函数里的 HandleUpstreamError 对 502/503
// 走 default 分支，其 customErrorCodesEnabled 同样要求 APIKey 类型，
// 因此 OAuth 账号恒得到 shouldDisable=false。
//
// 结论：502/503 不会 park 住 OAuth 账号，同账号重试必然能真正落到该账号上，
// 无需改动上游降温代码。唯一例外是管理员显式配置的 502/503
// 临时不可调度规则（tryTempUnschedulable）—— 那是明确的管理员意图，
// 本功能刻意不覆盖。
