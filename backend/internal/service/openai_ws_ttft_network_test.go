package service

import (
	"context"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

// OpenAI WS 网络口径首字（network 模式）测试。
//
// 新增文件，上游不存在，不产生合并冲突。

func networkTTFTOAuthAccount() *Account {
	return &Account{
		Platform: PlatformOpenAI,
		Type:     AccountTypeOAuth,
		Extra:    map[string]any{},
	}
}

func networkTTFTAPIKeyAccount() *Account {
	return &Account{
		Platform: PlatformOpenAI,
		Type:     AccountTypeAPIKey,
		Extra:    map[string]any{},
	}
}

// setTTFTModeForTest 通过网关转发设置缓存注入 TTFT 模式，
// 避免测试依赖数据库。返回还原函数。
func setTTFTModeForTest(t *testing.T, mode string) func() {
	t.Helper()
	prev := gatewayForwardingCache.Load()
	gatewayForwardingCache.Store(&cachedGatewayForwardingSettings{
		openAITTFTMode: mode,
		expiresAt:      0, // 0 表示不过期，见 openAITTFTMode 的读取逻辑
	})
	return func() {
		if cached, ok := prev.(*cachedGatewayForwardingSettings); ok && cached != nil {
			gatewayForwardingCache.Store(cached)
		} else {
			gatewayForwardingCache.Store((*cachedGatewayForwardingSettings)(nil))
		}
	}
}

// TestNetworkTTFTModeNormalization 验证设置值解析：
// network 被识别，非法值回落 semantic（保持上游默认行为）。
func TestNetworkTTFTModeNormalization(t *testing.T) {
	cases := []struct {
		raw  string
		want string
	}{
		{"network", OpenAITTFTModeNetwork},
		{"NETWORK", OpenAITTFTModeNetwork},
		{" network ", OpenAITTFTModeNetwork},
		{"visible", OpenAITTFTModeVisible},
		{"semantic", OpenAITTFTModeSemantic},
		{"", OpenAITTFTModeSemantic},
		{"garbage", OpenAITTFTModeSemantic},
	}
	for _, tc := range cases {
		if got := normalizeOpenAITTFTMode(tc.raw); got != tc.want {
			t.Fatalf("normalizeOpenAITTFTMode(%q) = %q, want %q", tc.raw, got, tc.want)
		}
	}
}

// TestNetworkFirstFrameAcceptsStructuralEvents 是核心用例：
// 网络口径下，上游返回的第一帧即为首字，包括 response.created 这类结构性事件 ——
// 这正是与上游默认口径（要等 .delta）的关键区别。
func TestNetworkFirstFrameAcceptsStructuralEvents(t *testing.T) {
	accepted := []string{
		"response.created",
		"response.in_progress",
		"response.output_item.added",
		"response.output_item.done",
		"response.content_part.added",
		"response.output_text.delta",
		"response.reasoning_summary_text.delta",
		"codex.rate_limits",
	}
	for _, eventType := range accepted {
		if !isOpenAIWSNetworkFirstFrame(eventType) {
			t.Fatalf("network mode must accept %q as first frame", eventType)
		}
	}
}

// TestNetworkFirstFrameRejectsTerminalEvents 验证终止事件不算首字。
//
// 这条保护与口径无关：若把终止事件当首字，则当上游未发出任何中间事件时，
// firstTokenMs 会被填到终止时刻，把「总耗时」误报为「首字延迟」。
func TestNetworkFirstFrameRejectsTerminalEvents(t *testing.T) {
	terminal := []string{
		"response.completed",
		"response.done",
		"response.failed",
	}
	for _, eventType := range terminal {
		if !isOpenAIWSTerminalEvent(eventType) {
			t.Skipf("%q is not treated as terminal upstream; skip", eventType)
		}
		if isOpenAIWSNetworkFirstFrame(eventType) {
			t.Fatalf("terminal event %q must not count as first frame", eventType)
		}
	}
	if isOpenAIWSNetworkFirstFrame("") {
		t.Fatal("empty event type must not count as first frame")
	}
}

// TestNetworkTTFTOnlyAppliesToOAuthAccounts 验证作用范围限定为 OpenAI OAuth。
func TestNetworkTTFTOnlyAppliesToOAuthAccounts(t *testing.T) {
	restore := setTTFTModeForTest(t, OpenAITTFTModeNetwork)
	defer restore()

	svc := &OpenAIGatewayService{cfg: &config.Config{}}
	ctx := context.Background()

	if !shouldUseNetworkTTFT(svc, ctx, networkTTFTOAuthAccount()) {
		t.Fatal("OAuth account must use network TTFT when mode is network")
	}
	if shouldUseNetworkTTFT(svc, ctx, networkTTFTAPIKeyAccount()) {
		t.Fatal("API key account must not use network TTFT")
	}
	if shouldUseNetworkTTFT(svc, ctx, &Account{Platform: PlatformGrok, Type: AccountTypeOAuth}) {
		t.Fatal("non-OpenAI platform must not use network TTFT")
	}
	if shouldUseNetworkTTFT(svc, ctx, nil) {
		t.Fatal("nil account must not use network TTFT")
	}
	if shouldUseNetworkTTFT(nil, ctx, networkTTFTOAuthAccount()) {
		t.Fatal("nil service must not use network TTFT")
	}
}

// TestNetworkTTFTDisabledInOtherModes 验证 semantic / visible 模式下不启用 ——
// 这是「默认不改变上游行为」的守护。
func TestNetworkTTFTDisabledInOtherModes(t *testing.T) {
	svc := &OpenAIGatewayService{cfg: &config.Config{}}
	account := networkTTFTOAuthAccount()

	for _, mode := range []string{OpenAITTFTModeSemantic, OpenAITTFTModeVisible} {
		restore := setTTFTModeForTest(t, mode)
		if shouldUseNetworkTTFT(svc, context.Background(), account) {
			restore()
			t.Fatalf("mode %q must not enable network TTFT", mode)
		}
		restore()
	}
}

// TestFirstTokenEventDispatch 验证判定入口在两种模式下的分派：
// 非 network 模式沿用上游算好的 isTokenEvent，network 模式改用首帧判定。
func TestFirstTokenEventDispatch(t *testing.T) {
	svc := &OpenAIGatewayService{cfg: &config.Config{}}
	account := networkTTFTOAuthAccount()
	ctx := context.Background()

	// semantic 模式：完全跟随传入的 isTokenEvent，不做任何改写。
	restoreSemantic := setTTFTModeForTest(t, OpenAITTFTModeSemantic)
	if isOpenAIWSFirstTokenEvent(svc, ctx, account, "response.created", false) {
		restoreSemantic()
		t.Fatal("semantic mode must defer to isTokenEvent (false)")
	}
	if !isOpenAIWSFirstTokenEvent(svc, ctx, account, "response.output_text.delta", true) {
		restoreSemantic()
		t.Fatal("semantic mode must defer to isTokenEvent (true)")
	}
	restoreSemantic()

	// network 模式：response.created 即便 isTokenEvent=false 也算首字。
	restoreNetwork := setTTFTModeForTest(t, OpenAITTFTModeNetwork)
	defer restoreNetwork()
	if !isOpenAIWSFirstTokenEvent(svc, ctx, account, "response.created", false) {
		t.Fatal("network mode must accept response.created regardless of isTokenEvent")
	}
	if isOpenAIWSFirstTokenEvent(svc, ctx, account, "response.completed", false) {
		t.Fatal("network mode must still reject terminal events")
	}
	// API Key 账号在 network 模式下仍走上游判定。
	if isOpenAIWSFirstTokenEvent(svc, ctx, networkTTFTAPIKeyAccount(), "response.created", false) {
		t.Fatal("api key account must keep upstream judgement even in network mode")
	}
}
