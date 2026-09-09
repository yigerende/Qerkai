package service

import (
	"context"
	"strings"
)

// OpenAI WS 上游的网络口径首字（二次开发功能，非上游代码）
//
// 需求：让 OpenAI OAuth 账号在 WS 上游链路上的 first_token_ms 与 CPA 对齐 ——
// CPA 取「上游返回的第一个字节/帧」，不做语义过滤
// （HTTP 侧 usage_helpers.go 的 Read 读到首个非零字节即 mark；
// WS 侧 codex_websockets_stream.go 在帧路由归属校验后立即 mark，不看 type）。
//
// 上游默认口径（isOpenAIWSTokenEvent）要求事件是「真正的 token」：
// 排除 response.created / in_progress / output_item.added / output_item.done，
// 实际要等到第一个 .delta。这段间隔是模型从「决定 reasoning」到「吐出首个
// reasoning token」，可达数百毫秒至数秒，且随 reasoning effort 增长。
//
// 两种口径各有价值，因此不替换而是新增第三种模式，由既有的
// openai_ttft_mode 设置切换（semantic / visible / network），默认仍为 semantic。
// 复用该设置而非新建开关，是因为 TTFT 口径本就是单选语义 ——
// 独立开关会与 semantic/visible 产生「同时开启听谁的」的冲突。
//
// 作用范围严格限定（按需求）：
//   - 仅 OpenAI OAuth 类账号；API Key 账号与其他平台不受影响
//   - 仅 WS 上游链路；HTTP 链路的 semantic/visible 判定完全不变
//   - 仅改判定终点，不改计时起点 —— 起点仍是 Forward() 入口，
//     故 network 模式下的数值仍包含预处理与连接池排队，
//     与 CPA「握手后起算」并非逐段可比；该模式回答的是
//     「上游何时开始回话」，而非「网络往返有多快」。

// shouldUseNetworkTTFT 报告本次请求是否应采用网络口径的首字判定。
//
// 三个条件全部满足才启用：模式为 network、账号为 OpenAI OAuth 类、service 可用。
// 任一不满足即回落上游默认行为，保证该功能默认不改变任何既有语义。
func shouldUseNetworkTTFT(s *OpenAIGatewayService, ctx context.Context, account *Account) bool {
	if s == nil || account == nil {
		return false
	}
	// 仅 OpenAI OAuth 类账号：API Key 账号的上游行为取决于具体 key 的项目配置，
	// 不纳入本次对齐范围。
	if !account.IsOpenAIOAuthLike() {
		return false
	}
	return s.openAITTFTMode(ctx) == OpenAITTFTModeNetwork
}

// isOpenAIWSNetworkFirstFrame 判断事件是否可作为网络口径的首字。
//
// 与上游 isOpenAIWSTokenEvent 的区别：不再要求事件承载 token，
// response.created / in_progress / output_item.* 等结构性事件同样计入 ——
// 它们本就是上游返回的第一批帧，正是「网络层首字节」要测的东西。
//
// 唯一的例外是终止事件。上游在 isOpenAIWSTokenEvent 处写明了原因：
// 若把终止事件当作首字，则当上游没有发出任何中间事件时，
// firstTokenMs 会被填到终止时刻，等于把「总耗时」误报为「首字延迟」。
// 该保护与口径无关，两种模式都应保留。
func isOpenAIWSNetworkFirstFrame(eventType string) bool {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return false
	}
	return !isOpenAIWSTerminalEvent(eventType)
}

// isOpenAIWSFirstTokenEvent 是 WS 上游链路首字判定的统一入口。
//
// isTokenEvent 由调用方传入（上游已算好的 isOpenAIWSTokenEvent 结果），
// 避免在热路径上重复判定。
func isOpenAIWSFirstTokenEvent(
	s *OpenAIGatewayService,
	ctx context.Context,
	account *Account,
	eventType string,
	isTokenEvent bool,
) bool {
	if !shouldUseNetworkTTFT(s, ctx, account) {
		return isTokenEvent
	}
	return isOpenAIWSNetworkFirstFrame(eventType)
}
