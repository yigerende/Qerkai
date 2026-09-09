package service

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
)

// 强制上游 WebSocket（二次开发功能）的行为测试。
//
// 本文件是新增文件，上游不存在，因此不会与上游产生合并冲突。
// 上游自带的 openai_client_transport_test.go 与 openai_ws_protocol_resolver_test.go
// 保持原样 —— 开关默认关闭，上游对默认行为的断言必须继续成立。

// forceWSTestConfig 构造一份「全局层面允许 WS」的配置，
// 使测试聚焦于客户端协议门禁与账号级标记这两个维度。
func forceWSTestConfig() *config.Config {
	cfg := &config.Config{}
	cfg.Gateway.OpenAIWS.Enabled = true
	cfg.Gateway.OpenAIWS.OAuthEnabled = true
	cfg.Gateway.OpenAIWS.APIKeyEnabled = true
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	return cfg
}

func forceWSOAuthAccount(wsEnabled bool) *Account {
	account := &Account{
		Platform: PlatformOpenAI,
		Type:     AccountTypeOAuth,
		Extra:    map[string]any{},
	}
	if wsEnabled {
		account.Extra["openai_oauth_responses_websockets_v2_enabled"] = true
	}
	return account
}

// TestForceUpstreamWSDisabledKeepsUpstreamBehavior 是最重要的一条：
// 开关关闭时，HTTP 入站请求必须仍被降级为 http_sse，reason 保持 client_protocol_http。
// 这条守护「二次开发默认不改变上游行为」。
func TestForceUpstreamWSDisabledKeepsUpstreamBehavior(t *testing.T) {
	restore := setForceUpstreamWSForTest(false)
	defer restore()

	wsDecision := OpenAIWSProtocolDecision{
		Transport: OpenAIUpstreamTransportResponsesWebsocketV2,
		Reason:    "ws_v2_enabled",
	}
	got := resolveOpenAIWSDecisionByClientTransport(wsDecision, OpenAIClientTransportHTTP)

	if got.Transport != OpenAIUpstreamTransportHTTPSSE {
		t.Fatalf("expected downgrade to http_sse, got %q", got.Transport)
	}
	if got.Reason != "client_protocol_http" {
		t.Fatalf("expected reason client_protocol_http, got %q", got.Reason)
	}
}

// TestForceUpstreamWSEnabledAllowsHTTPClientToUseWS 验证核心需求：
// 开关开启后，HTTP 入站请求保留 WS 上游决策。
func TestForceUpstreamWSEnabledAllowsHTTPClientToUseWS(t *testing.T) {
	restore := setForceUpstreamWSForTest(true)
	defer restore()

	wsDecision := OpenAIWSProtocolDecision{
		Transport: OpenAIUpstreamTransportResponsesWebsocketV2,
		Reason:    "ws_v2_enabled",
	}
	got := resolveOpenAIWSDecisionByClientTransport(wsDecision, OpenAIClientTransportHTTP)

	if got.Transport != OpenAIUpstreamTransportResponsesWebsocketV2 {
		t.Fatalf("expected ws v2 to be preserved, got %q", got.Transport)
	}
	if got.Reason != "ws_v2_enabled" {
		t.Fatalf("expected original reason to be preserved, got %q", got.Reason)
	}
}

// TestForceUpstreamWSDoesNotPromoteNonWSDecision 验证开关不会把非 WS 决策提升为 WS。
// 全局关闭、账号 force_http 等原因造成的 http_sse 必须保持 http_sse。
func TestForceUpstreamWSDoesNotPromoteNonWSDecision(t *testing.T) {
	restore := setForceUpstreamWSForTest(true)
	defer restore()

	for _, reason := range []string{"global_force_http", "account_force_http", "global_disabled"} {
		httpDecision := OpenAIWSProtocolDecision{
			Transport: OpenAIUpstreamTransportHTTPSSE,
			Reason:    reason,
		}
		got := resolveOpenAIWSDecisionByClientTransport(httpDecision, OpenAIClientTransportHTTP)
		if got.Transport != OpenAIUpstreamTransportHTTPSSE {
			t.Fatalf("reason %q: expected http_sse to stay, got %q", reason, got.Transport)
		}
	}
}

// TestForceUpstreamWSRejectsWSv1 验证只强制 WSv2。
// WSv1 在上游已停用（命中即 400），强制保留它只会把请求导向确定的失败。
func TestForceUpstreamWSRejectsWSv1(t *testing.T) {
	restore := setForceUpstreamWSForTest(true)
	defer restore()

	v1Decision := OpenAIWSProtocolDecision{
		Transport: OpenAIUpstreamTransportResponsesWebsocket,
		Reason:    "ws_v1_enabled",
	}
	got := resolveOpenAIWSDecisionByClientTransport(v1Decision, OpenAIClientTransportHTTP)

	if got.Transport != OpenAIUpstreamTransportHTTPSSE {
		t.Fatalf("expected ws v1 to be downgraded, got %q", got.Transport)
	}
}

// TestForceUpstreamWSKeepsWSClientUnaffected 验证 WS 入站请求的行为不受开关影响
// （开关只解除 HTTP 入站的限制，不改变 WS 入站的既有行为）。
func TestForceUpstreamWSKeepsWSClientUnaffected(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		restore := setForceUpstreamWSForTest(enabled)
		wsDecision := OpenAIWSProtocolDecision{
			Transport: OpenAIUpstreamTransportResponsesWebsocketV2,
			Reason:    "ws_v2_enabled",
		}
		got := resolveOpenAIWSDecisionByClientTransport(wsDecision, OpenAIClientTransportWS)
		if got != wsDecision {
			t.Fatalf("force=%v: expected ws client decision untouched, got %+v", enabled, got)
		}
		restore()
	}
}

// TestForceUpstreamWSResolverOverridesAccountFlag 验证装饰器覆盖账号级 WS 标记。
//
// 背景：OAuth 账号的 extra.openai_oauth_responses_websockets_v2_enabled 默认未写入
// （管理后台新建账号时 WS mode 默认 off），若不覆盖，开关打开后存量账号仍会以
// account_disabled 降级，功能静默失效。
func TestForceUpstreamWSResolverOverridesAccountFlag(t *testing.T) {
	cfg := forceWSTestConfig()
	accountWithoutFlag := forceWSOAuthAccount(false)

	// 开关关闭：账号未显式启用 → 降级 account_disabled（上游默认行为）。
	restoreOff := setForceUpstreamWSForTest(false)
	base := NewOpenAIWSProtocolResolver(cfg).Resolve(accountWithoutFlag)
	restoreOff()
	if base.Transport != OpenAIUpstreamTransportHTTPSSE {
		t.Fatalf("switch off: expected http_sse, got %q", base.Transport)
	}
	if base.Reason != "account_disabled" {
		t.Fatalf("switch off: expected reason account_disabled, got %q", base.Reason)
	}

	// 开关开启：同一账号应被视为已启用 WSv2。
	restoreOn := setForceUpstreamWSForTest(true)
	defer restoreOn()
	forced := NewOpenAIWSProtocolResolver(cfg).Resolve(accountWithoutFlag)
	if forced.Transport != OpenAIUpstreamTransportResponsesWebsocketV2 {
		t.Fatalf("switch on: expected ws v2, got %q (reason %q)", forced.Transport, forced.Reason)
	}
	if forced.Reason != forceUpstreamWSReasonAccountFlag {
		t.Fatalf("switch on: expected reason %q, got %q", forceUpstreamWSReasonAccountFlag, forced.Reason)
	}
}

// TestForceUpstreamWSResolverRespectsAccountForceHTTP 验证账号级 force_http
// 是显式的运维回滚开关，强制模式不得覆盖它。
func TestForceUpstreamWSResolverRespectsAccountForceHTTP(t *testing.T) {
	restore := setForceUpstreamWSForTest(true)
	defer restore()

	account := forceWSOAuthAccount(false)
	account.Extra["openai_ws_force_http"] = true

	got := NewOpenAIWSProtocolResolver(forceWSTestConfig()).Resolve(account)
	if got.Transport != OpenAIUpstreamTransportHTTPSSE {
		t.Fatalf("expected account force_http to be respected, got %q", got.Transport)
	}
	if got.Reason != "account_force_http" {
		t.Fatalf("expected reason account_force_http, got %q", got.Reason)
	}
}

// TestForceUpstreamWSResolverRespectsGlobalForceHTTP 验证全局 force_http
// （紧急回滚开关）优先级高于强制 WS。
func TestForceUpstreamWSResolverRespectsGlobalForceHTTP(t *testing.T) {
	restore := setForceUpstreamWSForTest(true)
	defer restore()

	cfg := forceWSTestConfig()
	cfg.Gateway.OpenAIWS.ForceHTTP = true

	got := NewOpenAIWSProtocolResolver(cfg).Resolve(forceWSOAuthAccount(true))
	if got.Transport != OpenAIUpstreamTransportHTTPSSE {
		t.Fatalf("expected global force_http to win, got %q", got.Transport)
	}
	if got.Reason != "global_force_http" {
		t.Fatalf("expected reason global_force_http, got %q", got.Reason)
	}
}

// TestForceUpstreamWSResolverSkipsAPIKeyAccounts 验证不覆盖 API Key 账号 ——
// 其 WS 可用性取决于具体 key 的项目配置，不能一概而论。
func TestForceUpstreamWSResolverSkipsAPIKeyAccounts(t *testing.T) {
	restore := setForceUpstreamWSForTest(true)
	defer restore()

	account := &Account{
		Platform: PlatformOpenAI,
		Type:     AccountTypeAPIKey,
		Extra:    map[string]any{},
	}

	got := NewOpenAIWSProtocolResolver(forceWSTestConfig()).Resolve(account)
	if got.Transport != OpenAIUpstreamTransportHTTPSSE {
		t.Fatalf("expected api key account not to be forced, got %q", got.Transport)
	}
}

// TestForceUpstreamWSEnabledWithoutSettingService 验证缺少 SettingService 时
// （仅测试或误配可达）安全地返回 false，即保持上游默认行为。
func TestForceUpstreamWSEnabledWithoutSettingService(t *testing.T) {
	prevSvc := forceUpstreamWSSettings.Swap(nil)
	prevCache := forceUpstreamWSCache.Load()
	forceUpstreamWSCache.Store(&cachedForceUpstreamWS{})
	defer func() {
		forceUpstreamWSSettings.Store(prevSvc)
		if cached, ok := prevCache.(*cachedForceUpstreamWS); ok && cached != nil {
			forceUpstreamWSCache.Store(cached)
		} else {
			forceUpstreamWSCache.Store(&cachedForceUpstreamWS{})
		}
	}()

	if ForceUpstreamWSEnabled() {
		t.Fatal("expected false when SettingService is absent")
	}
}

// TestRefreshForceUpstreamWSCache 验证设置写入后立即刷新缓存，开关即时生效。
func TestRefreshForceUpstreamWSCache(t *testing.T) {
	prevCache := forceUpstreamWSCache.Load()
	defer func() {
		if cached, ok := prevCache.(*cachedForceUpstreamWS); ok && cached != nil {
			forceUpstreamWSCache.Store(cached)
		} else {
			forceUpstreamWSCache.Store(&cachedForceUpstreamWS{})
		}
	}()

	refreshForceUpstreamWSCache(true)
	if !ForceUpstreamWSEnabled() {
		t.Fatal("expected enabled right after refresh(true)")
	}

	refreshForceUpstreamWSCache(false)
	if ForceUpstreamWSEnabled() {
		t.Fatal("expected disabled right after refresh(false)")
	}
}

// TestForceUpstreamWSSettingKeyMatchesPersistedKey 守护设置键的一致性：
// 后端读取、写入与前端字段名必须指向同一个 settings 表键。
func TestForceUpstreamWSSettingKeyMatchesPersistedKey(t *testing.T) {
	if SettingKeyForceOpenAIUpstreamWS != "force_openai_upstream_ws" {
		t.Fatalf("setting key drifted: %q", SettingKeyForceOpenAIUpstreamWS)
	}
}

// A5：message_too_big 的 WS → HTTP 回退。
//
// 上游在命中 WS 后不再回退 HTTP。强制上游 WS 开启后，HTTP 客户端可能仅因
// 代理内部选择了 WS 上游就收到 413，而同一请求走 HTTP 本可正常完成。

func newForceWSTestContext(t *testing.T) *gin.Context {
	t.Helper()
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	return c
}

// TestShouldFallbackToHTTPOnMessageTooBig 验证帧超限触发回退。
func TestShouldFallbackToHTTPOnMessageTooBig(t *testing.T) {
	c := newForceWSTestContext(t)
	err := wrapOpenAIWSFallback("message_too_big", errors.New("frame exceeds limit"))

	if !shouldFallbackToHTTPAfterWSFailure(c, err) {
		t.Fatal("expected message_too_big to trigger HTTP fallback")
	}
}

// TestShouldFallbackToHTTPOnPrewarmMessageTooBig 验证 prewarm_ 前缀变体同样识别。
func TestShouldFallbackToHTTPOnPrewarmMessageTooBig(t *testing.T) {
	c := newForceWSTestContext(t)
	err := wrapOpenAIWSFallback("prewarm_message_too_big", errors.New("frame exceeds limit"))

	if !shouldFallbackToHTTPAfterWSFailure(c, err) {
		t.Fatal("expected prewarm_message_too_big to trigger HTTP fallback")
	}
}

// TestShouldNotFallbackOnOtherWSFailures 验证只对帧超限回退。
//
// 尤其是 dial_failed 等网络类失败：自动回退会把「强制 WS」悄悄变成「优先 WS」，
// 掩盖真实的网络问题，应当显式暴露为错误。
func TestShouldNotFallbackOnOtherWSFailures(t *testing.T) {
	reasons := []string{
		"dial_failed",
		"auth_failed",
		"policy_violation",
		"upstream_5xx",
		"acquire_timeout",
		"read_event",
		"previous_response_not_found",
	}
	for _, reason := range reasons {
		t.Run(reason, func(t *testing.T) {
			c := newForceWSTestContext(t)
			err := wrapOpenAIWSFallback(reason, errors.New("boom"))
			if shouldFallbackToHTTPAfterWSFailure(c, err) {
				t.Fatalf("reason %q must not trigger HTTP fallback", reason)
			}
		})
	}
}

// TestShouldNotFallbackAfterResponseWritten 验证已向客户端写入内容后不再回退，
// 否则重试会产生重复输出。这是防重复执行的关键闸门。
func TestShouldNotFallbackAfterResponseWritten(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)

	// 模拟流式已开始：向客户端写出首个事件。
	c.Writer.WriteHeader(http.StatusOK)
	if _, err := c.Writer.Write([]byte("data: {}\n\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	c.Writer.Flush()

	err := wrapOpenAIWSFallback("message_too_big", errors.New("frame exceeds limit"))
	if shouldFallbackToHTTPAfterWSFailure(c, err) {
		t.Fatal("must not fall back after response has been written to the client")
	}
}

// TestShouldNotFallbackOnNilInputs 验证边界输入不 panic 且不回退。
func TestShouldNotFallbackOnNilInputs(t *testing.T) {
	c := newForceWSTestContext(t)
	if shouldFallbackToHTTPAfterWSFailure(c, nil) {
		t.Fatal("nil error must not trigger fallback")
	}
	err := wrapOpenAIWSFallback("message_too_big", errors.New("boom"))
	if shouldFallbackToHTTPAfterWSFailure(nil, err) {
		t.Fatal("nil context must not trigger fallback")
	}
}

// TestIsOpenAIWSMessageTooBigErrorIgnoresPlainErrors 验证非 WS fallback 错误不被误判。
func TestIsOpenAIWSMessageTooBigErrorIgnoresPlainErrors(t *testing.T) {
	if isOpenAIWSMessageTooBigError(errors.New("message_too_big")) {
		t.Fatal("plain error carrying the words must not be classified as ws fallback")
	}
	if isOpenAIWSMessageTooBigError(nil) {
		t.Fatal("nil must not be classified")
	}
}
