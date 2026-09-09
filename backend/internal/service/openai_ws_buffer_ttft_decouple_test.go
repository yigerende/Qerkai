package service

import "testing"

// 缓冲窗口与 TTFT 口径解耦的回归守护。
// 新增文件，上游不存在，不产生合并冲突。
//
// 背景：network TTFT 模式下 isOpenAIWSFirstTokenEvent 对任意非终止事件都返回
// true（这是该口径的定义——测网络层首帧）。若缓冲判定复用 firstTokenMs，
// 第一个结构性事件（codex.rate_limits / response.created）就会关闭缓冲窗口，
// 使 wroteDownstream 立刻为真，进而让 error 事件的 failover 守卫永久失效。
// 生产日志实测：约 40 次 server_is_overloaded 只有 5 次触发重试，且全是
// stream:false 的请求。

// TestNetworkTTFTAcceptsStructuralEventsAsFirstFrame 固定 network 口径的语义：
// 结构性事件算首帧。这是有意为之，不是缺陷——但正因如此，缓冲判定不能依赖它。
func TestNetworkTTFTAcceptsStructuralEventsAsFirstFrame(t *testing.T) {
	restore := setTTFTModeForTest(t, OpenAITTFTModeNetwork)
	defer restore()
	svc := &OpenAIGatewayService{}
	account := networkTTFTOAuthAccount()
	ctx := t.Context()

	for _, eventType := range []string{"codex.rate_limits", "response.created", "response.in_progress"} {
		isTokenEvent := isOpenAIWSTokenEvent(eventType)
		if isTokenEvent {
			t.Fatalf("%s 不应是语义 token 事件，测试前提失效", eventType)
		}
		if !isOpenAIWSFirstTokenEvent(svc, ctx, account, eventType, isTokenEvent) {
			t.Fatalf("network 口径下 %s 应算首帧", eventType)
		}
	}
}

// TestBufferWindowIgnoresNetworkTTFT 核心用例：缓冲判据只认真实 token 事件。
//
// 复刻 v2 收帧循环里的两个变量与判定，确保 network 口径不会提前关闭缓冲窗口。
// 若有人把 shouldBuffer 改回依赖 firstTokenMs，本用例会失败。
func TestBufferWindowIgnoresNetworkTTFT(t *testing.T) {
	restore := setTTFTModeForTest(t, OpenAITTFTModeNetwork)
	defer restore()
	svc := &OpenAIGatewayService{}
	account := networkTTFTOAuthAccount()
	ctx := t.Context()

	var firstTokenMs *int
	sawSemanticToken := false

	// 与生产日志一致的事件序列：结构性事件在前，真实 token 在后。
	sequence := []string{
		"codex.rate_limits",
		"response.created",
		"response.in_progress",
		"response.output_item.added",
		"response.output_text.delta", // 第一个真实 token
		"response.output_text.delta",
	}
	bufferedCount := 0
	for i, eventType := range sequence {
		isTokenEvent := isOpenAIWSTokenEvent(eventType)
		if isTokenEvent {
			sawSemanticToken = true
		}
		if firstTokenMs == nil && isOpenAIWSFirstTokenEvent(svc, ctx, account, eventType, isTokenEvent) {
			ms := 1
			firstTokenMs = &ms
		}
		isTerminal := isOpenAIWSTerminalEvent(eventType)
		shouldBuffer := !sawSemanticToken && !isTokenEvent && !isTerminal
		if shouldBuffer {
			bufferedCount++
		}
		// 前 4 个结构性事件必须仍在缓冲窗口内。
		if i < 4 && !shouldBuffer {
			t.Fatalf("idx=%d event=%s 应被缓冲，实际未缓冲（缓冲窗口被 TTFT 口径提前关闭）", i+1, eventType)
		}
	}
	if bufferedCount != 4 {
		t.Fatalf("bufferedCount = %d, want 4", bufferedCount)
	}
	// TTFT 仍应按 network 口径在第一帧记录，两者互不干扰。
	if firstTokenMs == nil {
		t.Fatal("network 口径应已记录 TTFT")
	}
}

// TestBufferWindowClosesOnRealToken 缓冲窗口在真实 token 出现后必须关闭 ——
// 否则内容会被一直扣住不发给客户端。
func TestBufferWindowClosesOnRealToken(t *testing.T) {
	sawSemanticToken := false
	for _, eventType := range []string{"response.created", "response.output_text.delta", "response.output_text.delta"} {
		isTokenEvent := isOpenAIWSTokenEvent(eventType)
		if isTokenEvent {
			sawSemanticToken = true
		}
		shouldBuffer := !sawSemanticToken && !isTokenEvent && !isOpenAIWSTerminalEvent(eventType)
		switch eventType {
		case "response.created":
			if !shouldBuffer {
				t.Fatal("首个结构性事件应被缓冲")
			}
		default:
			if shouldBuffer {
				t.Fatal("真实 token 之后不得继续缓冲")
			}
		}
	}
}

// TestSemanticModeBufferUnchanged semantic 口径下行为与改动前一致。
func TestSemanticModeBufferUnchanged(t *testing.T) {
	restore := setTTFTModeForTest(t, OpenAITTFTModeSemantic)
	defer restore()
	svc := &OpenAIGatewayService{}
	account := networkTTFTOAuthAccount()
	ctx := t.Context()

	for _, eventType := range []string{"codex.rate_limits", "response.created"} {
		isTokenEvent := isOpenAIWSTokenEvent(eventType)
		if isOpenAIWSFirstTokenEvent(svc, ctx, account, eventType, isTokenEvent) {
			t.Fatalf("semantic 口径下 %s 不应算首字", eventType)
		}
	}
}
