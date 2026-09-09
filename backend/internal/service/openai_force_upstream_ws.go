package service

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/sync/singleflight"
)

// 强制上游 WebSocket（二次开发功能，非上游代码）
//
// 需求：无论客户端使用 HTTP 还是 WS 协议，与 OpenAI 上游之间强制使用 WebSocket。
//
// 上游默认行为：`resolveOpenAIWSDecisionByClientTransport`（openai_client_transport.go）
// 会把 HTTP 入站请求的上游协议强制降级为 http_sse，注释写明
// 「仅允许 WS 入站请求走 WS 上游，避免出现 HTTP -> WS 协议混用」。
//
// 本文件在保留该默认行为的前提下提供一个可在管理后台开关的设置，开启后解除该限制。
//
// 设计取舍（为降低与上游的长期合并冲突）：
//   - 全部后端逻辑集中在本新增文件中，上游不存在同名文件，冲突代价为零。
//   - 对上游 service 层的改动只有两处极小接入点：openai_client_transport.go（90 天 0 次改动）
//     与 openai_ws_protocol_resolver.go 的构造函数（90 天 3 次改动）。
//   - 不改 openai_gateway_forward.go（90 天 82 次）、openai_gateway_handler.go（132 次）、
//     openai_gateway_service.go（91 次）、openai_account_scheduler.go（31 次）等高频文件。
//
// 取值链路：
//
//	settings 表 → 进程内 atomic.Value 缓存（60s TTL）→ resolver 装饰器
//
// 采用「包级注册 SettingService + 进程内缓存」而非把 *SettingService 传入 resolver，
// 原因是 OpenAIWSProtocolResolver.Resolve(account) 的签名既无 context 也无 service，
// 而其构造点 openai_gateway_service.go:550 位于 90 天 91 次改动的热文件中，不宜改动。
// 缓存模式与上游 setting_gateway_runtime.go 的 IsBackendModeEnabled 一致。

// forceUpstreamWSReasonAccountFlag 是覆盖账号级 WS 标记后写入的决策原因，
// 经 openai_gateway_forward.go 落到 gin context 的 openai_ws_transport_reason 与日志，
// 便于在排查时区分「账号本就启用 WS」和「由强制开关覆盖而启用」。
// 该字段仅用于观测，不参与任何行为分支。
const forceUpstreamWSReasonAccountFlag = "force_ws_account_flag_ignored"

const (
	forceUpstreamWSCacheTTL = 60 * time.Second
	// forceUpstreamWSErrorTTL 在读库失败时使用短 TTL，便于快速恢复。
	forceUpstreamWSErrorTTL = 5 * time.Second
	// forceUpstreamWSDBTimeout 限制 singleflight 内的查询耗时，独立于请求 context。
	forceUpstreamWSDBTimeout = 5 * time.Second
)

type cachedForceUpstreamWS struct {
	value     bool
	expiresAt int64 // unix nano
}

var (
	forceUpstreamWSCache    atomic.Value // *cachedForceUpstreamWS
	forceUpstreamWSSF       singleflight.Group
	forceUpstreamWSSettings atomic.Pointer[SettingService]
	// forceUpstreamWSOverride 仅供测试直接指定开关值，生产路径始终为空。
	forceUpstreamWSOverride atomic.Pointer[bool]
)

// registerForceUpstreamWSSettingService 由 NewSettingService 调用，
// 让 resolver 装饰器能在没有 context 与 service 引用的情况下读取设置。
func registerForceUpstreamWSSettingService(s *SettingService) {
	if s == nil {
		return
	}
	forceUpstreamWSSettings.Store(s)
}

// refreshForceUpstreamWSCache 在设置写入后立即刷新缓存，使开关即时生效
// （否则最长需等待一个 TTL 周期）。
// 先使 inflight singleflight 失效再写入新值，缩小旧值覆盖新值的竞态窗口，
// 与上游 setting_update.go 中刷新其他网关设置缓存的做法一致。
func refreshForceUpstreamWSCache(enabled bool) {
	forceUpstreamWSSF.Forget(SettingKeyForceOpenAIUpstreamWS)
	forceUpstreamWSCache.Store(&cachedForceUpstreamWS{
		value:     enabled,
		expiresAt: time.Now().Add(forceUpstreamWSCacheTTL).UnixNano(),
	})
}

// ForceUpstreamWSEnabled 报告强制上游 WS 是否开启。
//
// 热路径上零锁：命中未过期缓存时只做一次 atomic.Load。
// 缺少 SettingService（仅测试或误配可达）时返回 false，即保持上游默认行为。
func ForceUpstreamWSEnabled() bool {
	if override := forceUpstreamWSOverride.Load(); override != nil {
		return *override
	}
	if cached, ok := forceUpstreamWSCache.Load().(*cachedForceUpstreamWS); ok && cached != nil {
		if time.Now().UnixNano() < cached.expiresAt {
			return cached.value
		}
	}
	svc := forceUpstreamWSSettings.Load()
	if svc == nil || svc.settingRepo == nil {
		return false
	}
	result, _, _ := forceUpstreamWSSF.Do(SettingKeyForceOpenAIUpstreamWS, func() (any, error) {
		if cached, ok := forceUpstreamWSCache.Load().(*cachedForceUpstreamWS); ok && cached != nil {
			if time.Now().UnixNano() < cached.expiresAt {
				return cached.value, nil
			}
		}
		dbCtx, cancel := context.WithTimeout(context.Background(), forceUpstreamWSDBTimeout)
		defer cancel()
		raw, err := svc.settingRepo.GetValue(dbCtx, SettingKeyForceOpenAIUpstreamWS)
		if err != nil {
			if errors.Is(err, ErrSettingNotFound) {
				// 设置尚未创建（全新安装或未保存过）：按默认关闭缓存完整 TTL。
				forceUpstreamWSCache.Store(&cachedForceUpstreamWS{
					value:     false,
					expiresAt: time.Now().Add(forceUpstreamWSCacheTTL).UnixNano(),
				})
				return false, nil
			}
			slog.Warn("failed to get force_openai_upstream_ws setting", "error", err)
			forceUpstreamWSCache.Store(&cachedForceUpstreamWS{
				value:     false,
				expiresAt: time.Now().Add(forceUpstreamWSErrorTTL).UnixNano(),
			})
			return false, nil
		}
		enabled := strings.EqualFold(strings.TrimSpace(raw), "true")
		forceUpstreamWSCache.Store(&cachedForceUpstreamWS{
			value:     enabled,
			expiresAt: time.Now().Add(forceUpstreamWSCacheTTL).UnixNano(),
		})
		return enabled, nil
	})
	if value, ok := result.(bool); ok {
		return value
	}
	return false
}

// setForceUpstreamWSForTest 供测试覆盖开关，返回还原函数。
func setForceUpstreamWSForTest(enabled bool) func() {
	forceUpstreamWSOverride.Store(&enabled)
	return func() { forceUpstreamWSOverride.Store(nil) }
}

// forceUpstreamWSKeepDecision 在强制开关开启时，阻止「客户端为 HTTP」导致的上游协议降级。
//
// 由 resolveOpenAIWSDecisionByClientTransport 调用。返回 true 表示保留 decision 原值
// （即允许 HTTP 入站请求使用 WS 上游）。
//
// 注意：本函数只解除「客户端协议」这一道限制。decision 本身仍由
// OpenAIWSProtocolResolver.Resolve 依据全局配置、账号类型、账号 force_http 等条件决定，
// 那些开关继续有效 —— 强制 WS 不是无条件走 WS。
func forceUpstreamWSKeepDecision(decision OpenAIWSProtocolDecision) bool {
	if !isForceUpstreamWSTransport(decision.Transport) {
		// decision 未指向 WS 时无需保留：让上游原逻辑把 reason 记为 client_protocol_http，
		// 保持日志语义清晰（此时降级并非由客户端协议造成）。
		// 先判断 transport 可在开关关闭的常态下省去一次缓存读取。
		if forceUpstreamWSDiagnosticsEnabled() {
			slog.Info("force_upstream_ws.gate",
				"kept", false,
				"incoming_transport", string(decision.Transport),
				"incoming_reason", decision.Reason,
				"note", "decision was not ws_v2 at the client-transport gate",
			)
		}
		return false
	}
	kept := ForceUpstreamWSEnabled()
	if forceUpstreamWSDiagnosticsEnabled() {
		slog.Info("force_upstream_ws.gate",
			"kept", kept,
			"incoming_transport", string(decision.Transport),
			"incoming_reason", decision.Reason,
		)
	}
	return kept
}

// isForceUpstreamWSTransport 判断 transport 是否为可被强制保留的 WS 上游协议。
//
// 只认 WSv2：WSv1（responses_websockets）在上游已停用，命中即返回 400
// （见 openai_gateway_forward.go 中 "OpenAI WSv1 is temporarily unsupported"），
// 保留它只会把请求引向确定的失败。
func isForceUpstreamWSTransport(transport OpenAIUpstreamTransport) bool {
	return transport == OpenAIUpstreamTransportResponsesWebsocketV2
}

// forceUpstreamWSAccountFlagOverride 报告是否应把账号视为「已启用 WSv2」。
//
// 背景：Account.IsOpenAIResponsesWebSocketV2Enabled() 要求
// accounts.extra 中显式写入启用标记，未写入即为 false；而管理后台新建账号时
// WS mode 默认为 off。若不做此覆盖，开关打开后存量账号仍会以
// account_disabled 降级，功能静默失效。
//
// 仅覆盖 OpenAI OAuth 类账号：这类账号在上游能力上均支持 Responses WebSocket。
// API Key 账号不覆盖 —— 其 WS 可用性取决于具体 key 的项目配置，不能一概而论。
func forceUpstreamWSAccountFlagOverride(account *Account) bool {
	if account == nil || !account.IsOpenAI() {
		return false
	}
	// 账号级 force_http 是显式的运维回滚开关，强制模式不得覆盖它。
	if account.IsOpenAIWSForceHTTPEnabled() {
		return false
	}
	return account.IsOpenAIOAuthLike()
}

// newForceUpstreamWSProtocolResolver 用强制逻辑包装 resolver。
//
// 包装在构造函数处而非各调用点，是因为 Resolve() 有 5 处调用（HTTP 转发、
// 账号调度兼容性检查 2 处、WS ingress、response-id 绑定），逐点接入既易漏又会
// 触及多个高频文件。包装 resolver 让全部调用点自动获得一致行为。
//
// 其中账号调度那 2 处尤其关键：HTTP 链路选账号时传入的 requiredTransport 为
// OpenAIUpstreamTransportAny，若 resolver 未被包装，调度器可能选出一个
// Resolve 结果为 http_sse 的账号，随后转发层却判定走 WS，造成决策与账号能力不一致。
func newForceUpstreamWSProtocolResolver(inner OpenAIWSProtocolResolver) OpenAIWSProtocolResolver {
	if inner == nil {
		return nil
	}
	return &forceUpstreamWSProtocolResolver{inner: inner}
}

type forceUpstreamWSProtocolResolver struct {
	inner OpenAIWSProtocolResolver
}

func (r *forceUpstreamWSProtocolResolver) Resolve(account *Account) OpenAIWSProtocolDecision {
	decision := r.inner.Resolve(account)
	if decision.Transport == OpenAIUpstreamTransportResponsesWebsocketV2 {
		return decision
	}
	if forceUpstreamWSDiagnosticsEnabled() {
		accountID := int64(0)
		accountType := ""
		oauthLike := false
		forceHTTP := false
		if account != nil {
			accountID = account.ID
			accountType = account.Type
			oauthLike = account.IsOpenAIOAuthLike()
			forceHTTP = account.IsOpenAIWSForceHTTPEnabled()
		}
		slog.Info("force_upstream_ws.diagnostic",
			"switch_enabled", ForceUpstreamWSEnabled(),
			"inner_transport", string(decision.Transport),
			"inner_reason", decision.Reason,
			"reason_overridable", isForceUpstreamWSAccountDisabledReason(decision.Reason),
			"account_id", accountID,
			"account_type", accountType,
			"account_oauth_like", oauthLike,
			"account_force_http", forceHTTP,
		)
	}
	// 仅在「账号级标记未启用」这一个原因导致降级时才覆盖。
	// 其余降级原因（全局开关关闭、force_http、平台不符、并发无效等）一律尊重。
	if !isForceUpstreamWSAccountDisabledReason(decision.Reason) {
		return decision
	}
	if !forceUpstreamWSAccountFlagOverride(account) {
		return decision
	}
	if !ForceUpstreamWSEnabled() {
		return decision
	}
	return OpenAIWSProtocolDecision{
		Transport: OpenAIUpstreamTransportResponsesWebsocketV2,
		Reason:    forceUpstreamWSReasonAccountFlag,
	}
}

// forceUpstreamWSDiagnosticsEnabled 控制是否输出降级原因诊断日志。
//
// 强制 WS 未按预期生效时（例如账号类型不符、被其他条件先行降级），
// 上游只会静默走 HTTP，链路上没有任何线索。该日志补足这一段可观测性。
// 默认关闭，避免在正常运行时刷屏；排障时设 SUB2API_FORCE_UPSTREAM_WS_DEBUG=1 打开。
func forceUpstreamWSDiagnosticsEnabled() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("SUB2API_FORCE_UPSTREAM_WS_DEBUG")), "1")
}

// isForceUpstreamWSAccountDisabledReason 判断降级原因是否为「账号级 WS 标记未启用」。
//
// 对应 OpenAIWSProtocolResolver.Resolve 中两个分支：
//   - account_disabled：legacy 分支（mode_router_v2_enabled=false，当前默认）下
//     IsOpenAIResponsesWebSocketV2Enabled() 为 false
//   - account_mode_off：mode router v2 分支下账号 mode 为 off 或无法识别
func isForceUpstreamWSAccountDisabledReason(reason string) bool {
	switch strings.TrimSpace(reason) {
	case "account_disabled", "account_mode_off":
		return true
	default:
		return false
	}
}

// 上游协议的可观测性
//
// 需求背景：强制上游 WS 开启后，「本次请求与 OpenAI 之间究竟走了 WS 还是 HTTP」
// 成为运维关心的核心问题，而链路上原本没有任何 INFO 级出口 ——
// 上游只在 openai_gateway_forward.go:218 把它写进 gin context
// （key: openai_ws_transport_decision），且全仓库没有任何生产代码消费它。
//
// 这里把该值接到访问日志，使每条请求日志都带上上游协议。
// 选择日志而非 usage 表，是因为该诉求属于运维观测而非事后统计分析：
// 加 usage 列需 16 个文件 + 一次 migration，且 usage_log_repo_insert.go
// 要求「参数数组 / 8 处 INSERT 列清单 / 占位符 / scan 列顺序」四方对齐，
// 与上游同时加列会造成难以察觉的参数错位。

// OpenAIUpstreamTransportContextKey 是 gin context 中上游协议决策的键。
// 与 openai_gateway_forward.go:218 的字面量保持一致。
const OpenAIUpstreamTransportContextKey = "openai_ws_transport_decision"

// recordOpenAIUpstreamTransport 记录本次请求实际使用的上游协议。
//
// HTTP 入站链路由 openai_gateway_forward.go 自行写入该 key；
// WS 入站链路（ProxyResponsesWebSocketFromClient）不经过 Forward()，
// 需在其自行解析 wsDecision 之后调用本函数补齐，否则 WS 请求的访问日志
// 会缺失上游协议字段。
func recordOpenAIUpstreamTransport(c *gin.Context, decision OpenAIWSProtocolDecision) {
	if c == nil || decision.Transport == OpenAIUpstreamTransportAny {
		return
	}
	c.Set(OpenAIUpstreamTransportContextKey, string(decision.Transport))
}

// GetOpenAIUpstreamTransport 读取本次请求的上游协议，供访问日志中间件使用。
// 返回空字符串表示本次请求未经过 OpenAI 上游协议决策（如非 OpenAI 平台、
// 或在决策之前就已失败），此时调用方应省略该日志字段而非记为 unknown。
func GetOpenAIUpstreamTransport(c *gin.Context) string {
	if c == nil {
		return ""
	}
	raw, ok := c.Get(OpenAIUpstreamTransportContextKey)
	if !ok {
		return ""
	}
	value, _ := raw.(string)
	return strings.TrimSpace(value)
}

// message_too_big 的 WS → HTTP 回退
//
// 背景：上游在命中 WS 后不再回退 HTTP（openai_gateway_forward.go:742 注释
// 「命中 WS 时仅走 WebSocket Mode；不再自动回退 HTTP」）。这在「只有 WS 客户端
// 才走 WS 上游」的前提下是自洽的 —— WS 客户端本就知道自己在用 WS。
//
// 强制上游 WS 开启后前提不再成立：HTTP 客户端可能仅仅因为代理内部选择了 WS 上游，
// 就收到一个 413 message_too_big，而同一请求走 HTTP 本可以正常完成。
// 这是放开闸门后最可能被真实用户踩到的一条。
//
// 仅覆盖 message_too_big，不覆盖 dial_failed 等网络类失败：
// 前者语义明确 —— 帧超出上游 WS 的尺寸上限，请求确定未被处理，回退绝对安全，
// 且换成 HTTP 必然改善（HTTP body 无此限制）。
// 网络类失败若也自动回退，会把「强制 WS」悄悄变成「优先 WS」，
// 掩盖真实的网络问题，应当显式暴露。
//
// 判定形状与切换时机参考 CPA 的 isCodexWebsocketHTTPFallbackError 与
// maybeFallbackPlainHTTPWebsocketBootstrap。

// shouldFallbackToHTTPAfterWSFailure 判断 WS 失败后是否应改用 HTTP 重试本次请求。
//
// 两个条件缺一不可：
//  1. 失败原因为 message_too_big（含 prewarm_ 前缀变体）；
//  2. 尚未向客户端写入任何内容 —— 一旦响应已开始，重试会产生重复输出。
//
// 注意：本函数不检查强制 WS 开关。WS 客户端在开关关闭时也可能命中
// message_too_big，同样不该因协议选择而失败；由调用方按链路语义决定是否启用。
func shouldFallbackToHTTPAfterWSFailure(c *gin.Context, wsErr error) bool {
	if wsErr == nil {
		return false
	}
	if c == nil || c.Writer == nil || c.Writer.Written() {
		return false
	}
	return isOpenAIWSMessageTooBigError(wsErr)
}

// isOpenAIWSMessageTooBigError 判断错误是否为上游 WS 帧超限。
//
// 依赖上游已有的 openAIWSFallbackError 分类（openai_gateway_service.go 中
// message_too_big 已被识别，且归入「不可重试、不给账号降温」一类），
// 不重复实现判定逻辑。
func isOpenAIWSMessageTooBigError(err error) bool {
	if err == nil {
		return false
	}
	var fallbackErr *openAIWSFallbackError
	if !errors.As(err, &fallbackErr) || fallbackErr == nil {
		return false
	}
	reason := strings.TrimPrefix(strings.TrimSpace(fallbackErr.Reason), "prewarm_")
	return reason == "message_too_big"
}

// logOpenAIWSHTTPFallback 记录一次 WS → HTTP 回退，便于观测其发生频率。
// 该事件说明请求体已超出上游 WS 的帧上限，若频繁出现应考虑调整
// gateway.openai_ws.http_bridge_threshold_bytes 或检视客户端请求体积。
func logOpenAIWSHTTPFallback(account *Account, wsErr error) {
	accountID := int64(0)
	if account != nil {
		accountID = account.ID
	}
	slog.Warn("openai_ws.http_fallback",
		"reason", "message_too_big",
		"account_id", accountID,
		"error", wsErr.Error(),
	)
}

// logOpenAIWSPoolLease 记录每次连接池租约的获取情况，用于压测期间观察池行为。
//
// 上游把 conn_pick_ms / queue_wait_ms / conn_reused 写进了 gin context
// （openai_ws_forwarder_v2.go 中的 SetOpsLatencyMs），但没有任何消费方，
// 也没有 HTTP 出口 —— 池的 SnapshotMetrics() 只能在进程内读取。
// 该日志把这三个指标输出到访问日志同级，便于并发压测时统计池的复用率与排队时长。
//
// 由 SUB2API_FORCE_UPSTREAM_WS_DEBUG 控制，默认关闭。
func logOpenAIWSPoolLease(account *Account, lease *openAIWSConnLease) {
	if lease == nil || !forceUpstreamWSDiagnosticsEnabled() {
		return
	}
	accountID := int64(0)
	if account != nil {
		accountID = account.ID
	}
	slog.Info("openai_ws.pool_lease",
		"account_id", accountID,
		"conn_id", lease.ConnID(),
		"reused", lease.Reused(),
		"queue_wait_ms", lease.QueueWaitDuration().Milliseconds(),
		"conn_pick_ms", lease.ConnPickDuration().Milliseconds(),
		"prewarmed", lease.IsPrewarmed(),
	)
}
