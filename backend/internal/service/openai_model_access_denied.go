package service

import (
	"strings"
	"sync"
	"time"

	"github.com/tidwall/gjson"
)

// 账号级模型无权限的换号与短期黑名单。
//
// 二次开发新增文件，上游不存在，不产生合并冲突。
//
// ## 问题
//
// 生产实测（2 小时窗口）：
//
//	2446 条  The model gpt-5.5 does not exist or you do not have access to it.
//	         约 1223 条/小时，占客户端可见错误 43%——当前最大的单一类别
//
// 但 gpt-5.5 本身完全正常：30 天 1,898,640 次请求、12.7 亿 output token。
// 也就是说这不是模型配置问题，是【部分账号没有该模型权限】。
//
// ## 为什么客户端重试无效
//
// 用真实 payload 跑过整条判定链，全部为假：
//
//	openAIStreamErrorEventShouldFailover           false  -> 不换号
//	classifyOpenAIWSErrorEventFromRaw canFallback  false  -> 不回退 HTTP
//	openAIStreamFailedEventRetryableOnSameAccount  false  -> 不同账号重试
//	openAIWSPayloadTransientStatus                     0  -> 零账号级动作
//
// ops 明细里 attempts=0 印证了这一点：请求在第一个账号上就死，不换号；
// 账号也不被标记，仍然全量可调度。客户端重试等于重新抽一次签，
// 而坏账号一直留在池里——这就是「多次尝试后仍是这个错误」的成因。
//
// 且坏账号集合在持续轮换（4 小时内两批完全不同的账号，Codex Pro 组从 47 个
// 涨到 582 个），任何「人工找出坏账号并移除」的方案都会被新流入抵消。
//
// ## 判据的固有困难
//
// OpenAI 把两种情况合成一条消息：模型不存在 OR 该账号无权限。前者是客户端
// 拼错模型名（应快速失败），后者是账号无权限（应换号），消息本身无法区分。
//
// 本实现按后者处理并接受这个取舍：换号预算有上限，且黑名单只针对
// (账号, 模型) 组合而非账号本身，因此拼错模型名的最坏后果是消耗一轮换号
// 预算，不会污染账号在其它模型上的可用性。
//
// ## 处置
//
// A. 放行换号：识别为账号级失败，触发 UpstreamFailoverError 换下一个账号。
// B. 短期黑名单：连续 3 次命中后，该 (账号, 模型) 组合冷却 1 小时，
//    选号阶段直接跳过——后续请求不再重复踩坑，代价随时间摊薄。
//
// 单独用 A 不够：坏账号不被记住，每个请求都要重新试一遍。
// 单独用 B 不够：第一次命中的那个请求仍然失败。两者配合才收敛。
//
// 阈值 3 次而非 1 次：单次失败可能是上游瞬时抖动或模型名拼写问题，直接冷却
// 1 小时会误伤健康账号；连续 3 次才足以判定为稳定的权限缺失。与上游
// openAIAccountModelTransientState 的分级思路一致，但那套仅对
// AccountTypeAPIKey 启用（见 openai_account_runtime_block_fastpath.go），
// 且冷却按秒计（10s/45s）——权限缺失不是瞬态，需要按小时计，因此单独实现。

const (
	// openAIModelAccessDeniedReason 标记该 failover 源于账号模型无权限。
	// handler 侧据此放宽换号预算。
	openAIModelAccessDeniedReason GatewayFailureReason = "openai_model_access_denied"

	// openAIModelAccessDeniedStreakThreshold 是触发冷却所需的连续失败次数。
	openAIModelAccessDeniedStreakThreshold = 3

	// openAIModelAccessDeniedCooldown 是命中阈值后的冷却时长。
	//
	// 按小时计而非秒：模型权限由上游订阅决定，不会在秒级恢复。1 小时同时
	// 兼顾账号升级订阅后的自动恢复——不需要人工介入解除。
	openAIModelAccessDeniedCooldown = time.Hour

	// openAIModelAccessDeniedStreakTTL 限制连败计数的存活时间。
	//
	// 必须显著大于冷却时长：若小于冷却，条目会在冷却期内过期、计数归零，
	// 冷却结束后要重新累积 3 次才再度生效，等于每小时放 3 个请求进坑。
	openAIModelAccessDeniedStreakTTL = 6 * time.Hour

	// openAIModelAccessDeniedMaxEntries 限制 map 规模。
	// 生产 582 账号 × 常用模型数，4096 足够且内存可忽略。
	openAIModelAccessDeniedMaxEntries = 4096

	// openAIModelAccessDeniedMaxSwitches 是该类错误的换号预算。
	//
	// 高于全局 gateway.max_account_switches（默认 10）：冷启动阶段黑名单尚未
	// 建立，需要更多机会找到有权限的账号；黑名单生效后实际很少用满。
	openAIModelAccessDeniedMaxSwitches = 20
)

// isOpenAIModelAccessDeniedError 判断错误负载是否为账号模型无权限。
//
// 判据同时要求错误码与文案特征，避免误伤：只匹配文案不看错误码，会把其它
// invalid_request（如参数错误里恰好出现 "does not exist"）一并卷入，
// 那些换号毫无意义。
func isOpenAIModelAccessDeniedError(payload []byte, message string) bool {
	code, errType, payloadMsg := "", "", ""
	if len(payload) > 0 {
		code = gjson.GetBytes(payload, "error.code").String()
		if code == "" {
			code = gjson.GetBytes(payload, "response.error.code").String()
		}
		errType = gjson.GetBytes(payload, "error.type").String()
		if errType == "" {
			errType = gjson.GetBytes(payload, "response.error.type").String()
		}
		payloadMsg = gjson.GetBytes(payload, "error.message").String()
		if payloadMsg == "" {
			payloadMsg = gjson.GetBytes(payload, "response.error.message").String()
		}
	}
	code = strings.ToLower(strings.TrimSpace(code))
	errType = strings.ToLower(strings.TrimSpace(errType))
	combined := strings.ToLower(strings.TrimSpace(message + " " + payloadMsg))
	if combined == "" {
		return false
	}

	codeMatches := strings.Contains(code, "model_not_found") ||
		strings.Contains(errType, "model_not_found") ||
		strings.Contains(code, "invalid_request") ||
		strings.Contains(errType, "invalid_request")
	if !codeMatches {
		return false
	}
	if strings.Contains(combined, "do not have access") ||
		strings.Contains(combined, "does not have access") {
		return true
	}
	return strings.Contains(combined, "does not exist") && strings.Contains(combined, "model")
}

// openAIModelAccessKey 是 (账号, 模型) 组合键。
type openAIModelAccessKey struct {
	AccountID int64
	Model     string
}

type openAIModelAccessEntry struct {
	failureStreak int
	lastFailure   time.Time
	blockUntil    time.Time
}

// openAIModelAccessDenyList 是进程内的 (账号, 模型) 短期黑名单。
//
// 进程内而非持久化：生产为单机部署，重启后重新学习的代价是几个请求；
// 引入 DB 或 Redis 会给热路径（每次选号都要查）加上网络往返。
type openAIModelAccessDenyList struct {
	mu         sync.Mutex
	entries    map[openAIModelAccessKey]openAIModelAccessEntry
	maxEntries int
}

func newOpenAIModelAccessDenyList(maxEntries int) *openAIModelAccessDenyList {
	if maxEntries <= 0 {
		maxEntries = openAIModelAccessDeniedMaxEntries
	}
	return &openAIModelAccessDenyList{
		entries:    make(map[openAIModelAccessKey]openAIModelAccessEntry),
		maxEntries: maxEntries,
	}
}

func openAIModelAccessNormalizeModel(model string) string {
	model = strings.TrimSpace(model)
	if model == "" || len(model) > openAIModelTransientMaxModelBytes {
		return ""
	}
	return strings.ToLower(model)
}

func openAIModelAccessBuildKey(accountID int64, model string) (openAIModelAccessKey, bool) {
	model = openAIModelAccessNormalizeModel(model)
	if accountID <= 0 || model == "" {
		return openAIModelAccessKey{}, false
	}
	return openAIModelAccessKey{AccountID: accountID, Model: model}, true
}

// recordFailure 记一次失败，返回本次之后的连败数与冷却截止时刻。
func (l *openAIModelAccessDenyList) recordFailure(accountID int64, model string, now time.Time) (int, time.Time) {
	key, ok := openAIModelAccessBuildKey(accountID, model)
	if l == nil || !ok {
		return 0, time.Time{}
	}
	if now.IsZero() {
		now = time.Now()
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.entries == nil {
		l.entries = make(map[openAIModelAccessKey]openAIModelAccessEntry)
	}
	entry, exists := l.entries[key]
	if !exists {
		l.evictOldestLocked()
	}
	// 计数只在超过 TTL 或时钟回拨时重置；成功一次由 recordSuccess 清零。
	if !exists || entry.lastFailure.IsZero() ||
		now.Sub(entry.lastFailure) > openAIModelAccessDeniedStreakTTL ||
		now.Before(entry.lastFailure) {
		entry.failureStreak = 0
		entry.blockUntil = time.Time{}
	}
	entry.failureStreak++
	entry.lastFailure = now
	if entry.failureStreak >= openAIModelAccessDeniedStreakThreshold {
		entry.blockUntil = now.Add(openAIModelAccessDeniedCooldown)
	}
	l.entries[key] = entry
	return entry.failureStreak, entry.blockUntil
}

// recordSuccess 清除该组合的失败记录。
//
// 账号升级订阅后第一次成功即可解除，不必等冷却自然到期。
func (l *openAIModelAccessDenyList) recordSuccess(accountID int64, model string) {
	key, ok := openAIModelAccessBuildKey(accountID, model)
	if l == nil || !ok {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.entries, key)
}

// isBlocked 报告该组合当前是否处于冷却中。
func (l *openAIModelAccessDenyList) isBlocked(accountID int64, model string, now time.Time) bool {
	key, ok := openAIModelAccessBuildKey(accountID, model)
	if l == nil || !ok {
		return false
	}
	if now.IsZero() {
		now = time.Now()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	entry, exists := l.entries[key]
	if !exists || entry.blockUntil.IsZero() {
		return false
	}
	if now.Before(entry.blockUntil) {
		return true
	}
	// 冷却自然到期：清掉 blockUntil 但保留连败计数，让再次失败能立刻重新
	// 触发冷却，而不是又要攒满 3 次。计数本身仍受 TTL 约束。
	entry.blockUntil = time.Time{}
	l.entries[key] = entry
	return false
}

func (l *openAIModelAccessDenyList) size() int {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

// evictOldestLocked 在容量满时淘汰最久未失败的条目。调用方须持锁。
func (l *openAIModelAccessDenyList) evictOldestLocked() {
	if l.maxEntries <= 0 || len(l.entries) < l.maxEntries {
		return
	}
	var oldestKey openAIModelAccessKey
	var oldestAt time.Time
	found := false
	for k, v := range l.entries {
		if !found || v.lastFailure.Before(oldestAt) {
			oldestKey, oldestAt, found = k, v.lastFailure, true
		}
	}
	if found {
		delete(l.entries, oldestKey)
	}
}

// OpenAIModelAccessDeniedMaxSwitches 返回该类错误应使用的换号预算。
//
// handler 侧调用：非该类错误时原样返回 base，不改变任何既有行为。
func OpenAIModelAccessDeniedMaxSwitches(base int, failoverErr *UpstreamFailoverError) int {
	if failoverErr == nil || failoverErr.Reason != openAIModelAccessDeniedReason {
		return base
	}
	if base >= openAIModelAccessDeniedMaxSwitches {
		return base
	}
	return openAIModelAccessDeniedMaxSwitches
}

// openAIModelAccessDenyListInstance 是进程级单例。
//
// 放包级而不是挂到 OpenAIGatewayService 上：后者需要改上游
// openai_gateway_service.go 的结构体定义（字段 + sync.Once + 构造函数三处），
// 那是上游高频改动文件。这份状态本就是进程范围的（生产单机部署），
// 且自带互斥锁与容量上限，包级单例在语义上并不逊色。
var openAIModelAccessDenyListInstance = newOpenAIModelAccessDenyList(openAIModelAccessDeniedMaxEntries)

// resetOpenAIModelAccessDenyListForTest 供测试隔离用。
func resetOpenAIModelAccessDenyListForTest() {
	openAIModelAccessDenyListInstance = newOpenAIModelAccessDenyList(openAIModelAccessDeniedMaxEntries)
}

// recordOpenAIModelAccessDenied 记录一次模型无权限失败。
//
// 返回是否刚刚进入冷却，供调用方决定日志级别。
func recordOpenAIModelAccessDenied(accountID int64, model string) (streak int, blockUntil time.Time) {
	return openAIModelAccessDenyListInstance.recordFailure(accountID, model, time.Now())
}

// clearOpenAIModelAccessDenied 在该组合成功时清除记录。
func clearOpenAIModelAccessDenied(accountID int64, model string) {
	openAIModelAccessDenyListInstance.recordSuccess(accountID, model)
}

// applyOpenAIModelAccessDenied 在识别为模型无权限时记录黑名单并标记错误。
//
// 三件事必须一起做，缺一不可：
//   - 记录 (账号, 模型) 连败，达阈值即冷却，让选号阶段跳过；
//   - 置 Reason，让 handler 放宽换号预算到 20；
//   - 确保 NextAccountRetry，否则 ShouldRetryNextAccount 的语义取决于零值，
//     一旦上游改动零值含义就会静默失效。
//
// canonicalModel 用与选号一致的口径：记录用映射后模型名、查询用客户端原始名
// 会导致黑名单永不命中。
func (s *OpenAIGatewayService) applyOpenAIModelAccessDenied(
	account *Account,
	mappedModel string,
	payload []byte,
	message string,
	failoverErr *UpstreamFailoverError,
) {
	if s == nil || account == nil || failoverErr == nil {
		return
	}
	if !isOpenAIModelAccessDeniedError(payload, message) {
		return
	}
	canonicalModel := canonicalOpenAIAccountSchedulingModel(account, mappedModel)
	streak, blockUntil := recordOpenAIModelAccessDenied(account.ID, canonicalModel)

	failoverErr.Reason = openAIModelAccessDeniedReason
	failoverErr.Scope = GatewayFailureScopeAccount
	failoverErr.NextAccountAction = NextAccountRetry
	// 无权限不是瞬时故障，同账号重试没有意义。
	failoverErr.RetryableOnSameAccount = false
	failoverErr.RequestScopedTransient = false

	blocked := !blockUntil.IsZero() && blockUntil.After(time.Now())
	logOpenAIWSModeInfo(
		"model_access_denied account_id=%d model=%s streak=%d threshold=%d blocked=%v cooldown_until=%s",
		account.ID,
		normalizeOpenAIWSLogValue(canonicalModel),
		streak,
		openAIModelAccessDeniedStreakThreshold,
		blocked,
		normalizeOpenAIWSLogValue(formatOpenAIModelAccessBlockUntil(blockUntil)),
	)
}

func formatOpenAIModelAccessBlockUntil(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Format(time.RFC3339)
}

// isOpenAIModelAccessDeniedBlocked 供选号阶段查询。
//
// 模型口径与上游 isOpenAIAccountModelRuntimeBlocked 保持一致：
// 都经 canonicalOpenAIAccountSchedulingModel 归一，否则记录用映射后的模型名、
// 查询用客户端原始名，两者对不上，黑名单永远不命中。
func (s *OpenAIGatewayService) isOpenAIModelAccessDeniedBlocked(account *Account, requestedModel string) bool {
	if s == nil || account == nil {
		return false
	}
	canonicalModel := canonicalOpenAIAccountSchedulingModel(account, requestedModel)
	return openAIModelAccessDenyListInstance.isBlocked(account.ID, canonicalModel, time.Now())
}
