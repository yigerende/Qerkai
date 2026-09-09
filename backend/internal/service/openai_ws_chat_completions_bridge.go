package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// Chat Completions 入站 + WebSocket 上游的协议桥接。
//
// 二次开发新增文件，上游不存在，不产生合并冲突。
//
// 背景：/v1/chat/completions 与 /v1/responses 在发上游那一刻已经收敛——
// CC 链路先把 ChatCompletionsRequest 转成 Responses 形态，再调用与 Responses
// 链路完全相同的 buildUpstreamRequest。也就是说上游侧本来就是 Responses 协议，
// 与 WS 转发器处理的协议天然一致，出站方向无需任何转换。
//
// 唯一的差异在回程：forwardOpenAIWSV2 的 emitStreamMessage 把上游帧原样包成
// `data: <原帧>` 直写客户端。这对 /v1/responses 是正确的（下游要的就是
// Responses 事件），但 CC 客户端只认 chat.completion.chunk，收到 Responses
// 事件会解析失败。
//
// 本文件提供一个下游写出拦截器：CC 入站走 WS 时，把上游 Responses 事件经
// apicompat 状态机转换成 CC chunk 再写出。转换逻辑完全复用 CC HTTP 链路
// 用的那一套（ResponsesEventToChatChunks / ChatChunkToSSE /
// FinalizeResponsesChatStream），不另写一份，避免与上游 apicompat 的演进分叉。

// openAIWSChatBridgeContextKey 在 gin.Context 中存放桥接器。
const openAIWSChatBridgeContextKey = "openai_ws_chat_completions_bridge"

// openAIWSChatBridge 把上游 Responses 事件转成 Chat Completions SSE。
//
// 它只负责「写什么给客户端」，不介入连接管理、会话粘性、计费与重试——
// 那些仍由 forwardOpenAIWSV2 原有逻辑处理。
type openAIWSChatBridge struct {
	state *apicompat.ResponsesEventToChatState
	// clientStream 是客户端请求的形态。注意它与上游形态无关：
	// apicompat.ChatCompletionsToResponses 恒定把上游置为 Stream:true
	// （"upstream always streams"），CC 非流式一律由网关缓冲后自行聚合。
	// 因此 clientStream=false 时 WS 走的仍是流式收帧，只是不向下游写 chunk，
	// 改为在终止事件到达后由 TransformFinalResponse 一次性输出 JSON。
	clientStream bool
	// terminalResponse 缓存终止事件里的 response 对象（非流式用）。
	terminalResponse []byte
	// outputCollector 聚合增量事件。上游 response.completed 的 output 常为空
	// （内容只在 delta 事件里），直接拿它转 CC 会丢正文。v2 自己的 collector
	// 只在 reqStream=false 时启用，而 CC 的上游恒为流式，那条通路用不上，
	// 因此桥接器自己持有一个，复用同一套聚合实现。
	//
	// 流式同样启用（原先仅 !stream）：反向情形也存在——正文只出现在
	// response.output_item.done 或终止事件的 output 里、完全没有 .delta。
	// 此时 ResponsesEventToChatChunks 对这两类事件都不产出 chunk
	// （output_item.done 无 case；resToChatHandleCompleted 不读 Response.Output），
	// 客户端只会拿到 role + finish + usage，正文彻底丢失。
	// 生产实测：CC 流式请求 200 / 5.1s / 输入 270 token / 输出为空、首字为空。
	outputCollector *openAIWSOutputCollector
	// originalModel 回给客户端的模型名（客户端请求的那个，不是上游映射后的）。
	originalModel string
	// clientOutputStarted 标记是否已向客户端写出过语义内容。
	clientOutputStarted bool
	// doneSent 防止 [DONE] 重复写出。
	doneSent bool
}

// newOpenAIWSChatBridge 构造桥接器。
func newOpenAIWSChatBridge(originalModel string, stream bool) *openAIWSChatBridge {
	state := apicompat.NewResponsesEventToChatState()
	state.Model = originalModel
	// 与 CC HTTP 链路保持一致：网关作为计费链路一环，强制透出 usage，
	// 不绑定客户端是否显式请求，避免级联代理计费为 0。
	state.IncludeUsage = true
	return &openAIWSChatBridge{
		state:         state,
		clientStream:  stream,
		originalModel: originalModel,
		// 两种形态都启用：非流式要靠它拼出响应体，流式要靠它在上游没发
		// 任何 .delta 时补出正文。详见 outputCollector 字段注释。
		outputCollector: newOpenAIWSOutputCollector(true),
	}
}

// bindOpenAIWSChatBridge 把桥接器绑定到请求上下文。
func bindOpenAIWSChatBridge(c *gin.Context, bridge *openAIWSChatBridge) {
	if c == nil || bridge == nil {
		return
	}
	c.Set(openAIWSChatBridgeContextKey, bridge)
}

// openAIWSChatBridgeFromContext 取出桥接器；未绑定时返回 nil。
//
// forwardOpenAIWSV2 通过它判断是否需要改写下游输出：返回 nil 时保持
// 上游原有的原样透传行为，Responses 入站完全不受影响。
func openAIWSChatBridgeFromContext(c *gin.Context) *openAIWSChatBridge {
	if c == nil {
		return nil
	}
	raw, ok := c.Get(openAIWSChatBridgeContextKey)
	if !ok {
		return nil
	}
	bridge, _ := raw.(*openAIWSChatBridge)
	return bridge
}

// TransformStreamFrame 把一条上游 WS 帧转成要写给客户端的 SSE 字节。
//
// 返回 nil 表示该帧不产生下游输出（例如纯结构性事件），调用方应跳过写出
// 而不是写一个空帧。
func (b *openAIWSChatBridge) TransformStreamFrame(message []byte) []byte {
	if b == nil || len(message) == 0 {
		return nil
	}
	// 两种客户端形态都要喂聚合器：增量事件带正文，终止事件带 response 与 usage，
	// 而正文也可能只出现在 output_item.done / 终止事件的 output 里。
	eventType := strings.TrimSpace(gjson.GetBytes(message, "type").String())
	b.outputCollector.Observe(eventType, message)
	b.captureTerminalResponse(eventType, message)
	if !b.clientStream {
		// 客户端要非流式：帧已被消费，但不向下游写任何 chunk。
		return nil
	}
	var event apicompat.ResponsesStreamEvent
	if err := json.Unmarshal(message, &event); err != nil {
		// 解析失败不能把原始 Responses 事件泄漏给 CC 客户端——它读不懂。
		// 静默跳过，终止事件的 usage 仍由 v2 自己的解析链路负责。
		return nil
	}
	chunks := apicompat.ResponsesEventToChatChunks(&event, b.state)
	// 终止事件必须在写出 finish_reason 之前补正文：一旦 finish chunk 出去，
	// 客户端就认定本轮结束，之后的 content chunk 会被丢弃或视为协议错误。
	recovered := ""
	if isOpenAIWSChatRecoverableTerminal(eventType) {
		recovered = b.recoveredStreamContentSSE()
	}
	if len(chunks) == 0 && recovered == "" {
		return nil
	}
	var buf strings.Builder
	buf.WriteString(recovered)
	for _, chunk := range chunks {
		sse, err := apicompat.ChatChunkToSSE(chunk)
		if err != nil {
			continue
		}
		buf.WriteString(sse)
	}
	if buf.Len() == 0 {
		return nil
	}
	b.clientOutputStarted = true
	return []byte(buf.String())
}

// isOpenAIWSChatRecoverableTerminal 报告该终止事件是否值得尝试补正文。
//
// 只覆盖成功语义的终止事件。response.failed 不在其中：它代表本轮没有产出，
// 补正文既无内容可补，也会掩盖真实错误。
func isOpenAIWSChatRecoverableTerminal(eventType string) bool {
	switch eventType {
	case "response.completed", "response.done", "response.incomplete":
		return true
	}
	return false
}

// recoveredStreamContentSSE 在流式路径从未发出任何正文 chunk 时，用聚合到的
// 内容补出一个 content chunk 的 SSE 字节；无需补时返回空串。
//
// 判据用 state.SawText 而不是 clientOutputStarted：后者在 response.created 的
// role chunk 写出后就为真，无法区分「发过正文」与「只发过 role」。
//
// 提取正文复用 apicompat.ResponsesToChatCompletions——它就是 CC 非流式路径在
// 用的那套 output→content 展开（output_text 拼接、tool_calls、reasoning 归位），
// 不另写一份，避免与上游 apicompat 的演进分叉。
func (b *openAIWSChatBridge) recoveredStreamContentSSE() string {
	if b == nil || !b.clientStream || b.state == nil || b.state.SawText {
		return ""
	}
	base := b.terminalResponse
	if len(base) == 0 {
		base = []byte(`{}`)
	}
	patched := b.outputCollector.PatchFinalResponse(base)
	var responsesResp apicompat.ResponsesResponse
	if err := json.Unmarshal(patched, &responsesResp); err != nil {
		return ""
	}
	chatResp := apicompat.ResponsesToChatCompletions(&responsesResp, b.originalModel)
	if chatResp == nil || len(chatResp.Choices) == 0 {
		return ""
	}
	raw := chatResp.Choices[0].Message.Content
	if len(raw) == 0 {
		return ""
	}
	var text string
	if err := json.Unmarshal(raw, &text); err != nil || text == "" {
		return ""
	}
	delta := apicompat.ChatDelta{Content: &text}
	// response.created 缺失时 role 还没发过，与正文并入同一个 delta 补上，
	// 否则客户端会收到一个没有 role 的 assistant 消息。
	if !b.state.SentRole {
		b.state.SentRole = true
		delta.Role = "assistant"
	}
	sse, err := apicompat.ChatChunkToSSE(apicompat.ChatCompletionsChunk{
		ID:          b.state.ID,
		Object:      "chat.completion.chunk",
		Created:     b.state.Created,
		Model:       b.state.Model,
		ServiceTier: b.state.ServiceTier,
		Choices: []apicompat.ChatChunkChoice{{
			Index: 0,
			Delta: delta,
		}},
	})
	if err != nil {
		return ""
	}
	// 置位 SawText，避免 FinalizeStream 再补一次。
	b.state.SawText = true
	return sse
}

// captureTerminalResponse 从终止事件里抽出 response 对象，供非流式转换使用。
//
// v2 只在 reqStream=false 时才填充自己的 finalResponse，而 CC 链路的上游恒为
// 流式（见 clientStream 字段注释），因此非流式响应体必须由桥接器自己攒。
func (b *openAIWSChatBridge) captureTerminalResponse(eventType string, message []byte) {
	if b == nil || len(message) == 0 {
		return
	}
	switch eventType {
	case "response.completed", "response.done", "response.incomplete":
	default:
		return
	}
	response := gjson.GetBytes(message, "response")
	if !response.Exists() || response.Type != gjson.JSON {
		return
	}
	b.terminalResponse = []byte(response.Raw)
}

// FinalizeStream 产出流式收尾字节（finish chunk + [DONE]）。
func (b *openAIWSChatBridge) FinalizeStream() []byte {
	if b == nil || !b.clientStream || b.doneSent {
		return nil
	}
	b.doneSent = true
	var buf strings.Builder
	// 上游没发终止事件就断流时（TransformStreamFrame 的补正文分支不会触发），
	// 这里是最后一次机会——同样必须在 finish chunk 之前。
	// state.Finalized 为真说明终止事件已处理过，recoveredStreamContentSSE
	// 会因 SawText 已置位而返回空串，不会重复补。
	buf.WriteString(b.recoveredStreamContentSSE())
	for _, chunk := range apicompat.FinalizeResponsesChatStream(b.state) {
		sse, err := apicompat.ChatChunkToSSE(chunk)
		if err != nil {
			continue
		}
		buf.WriteString(sse)
	}
	buf.WriteString("data: [DONE]\n\n")
	return []byte(buf.String())
}

// TransformFinalResponse 把非流式的 Responses 终态响应体转成 CC 响应体。
func (b *openAIWSChatBridge) TransformFinalResponse(finalResponse []byte) []byte {
	if b == nil || b.clientStream {
		return finalResponse
	}
	if len(finalResponse) == 0 {
		// v2 在上游流式时不填 finalResponse，改用桥接器自己缓存的终止 response。
		finalResponse = b.terminalResponse
	}
	if len(finalResponse) == 0 {
		return finalResponse
	}
	// 终止事件 output 为空时，用聚合到的增量内容回填，否则正文会丢。
	finalResponse = b.outputCollector.PatchFinalResponse(finalResponse)
	var responsesResp apicompat.ResponsesResponse
	if err := json.Unmarshal(finalResponse, &responsesResp); err != nil {
		return finalResponse
	}
	chatResp := apicompat.ResponsesToChatCompletions(&responsesResp, b.originalModel)
	if chatResp == nil {
		return finalResponse
	}
	encoded, err := json.Marshal(chatResp)
	if err != nil {
		return finalResponse
	}
	return encoded
}

// TransformErrorResponse 把非流式错误体转成 CC 错误形态。
func (b *openAIWSChatBridge) TransformErrorResponse(statusCode int, errType, message string) any {
	return gin.H{
		"error": gin.H{
			"type":    errType,
			"message": message,
		},
	}
}

// WriteStreamError 以 CC 形态写出流内错误，并补 [DONE] 让客户端正常收尾。
func (b *openAIWSChatBridge) WriteStreamError(c *gin.Context, code, message string) {
	if b == nil || c == nil || c.Writer == nil || !b.clientStream {
		return
	}
	if _, err := fmt.Fprint(c.Writer, buildChatStreamErrorSSE(code, message)); err != nil {
		return
	}
	if !b.doneSent {
		b.doneSent = true
		_, _ = fmt.Fprint(c.Writer, "data: [DONE]\n\n")
	}
	if fl, ok := c.Writer.(http.Flusher); ok {
		fl.Flush()
	}
}

// shouldRouteChatCompletionsViaWS 判断 CC 入站请求是否应改走 WS 上游。
//
// 判据完全复用 Responses 链路那一套：协议解析器（含强制上游 WS 装饰器）说
// 该账号走 WS，才走 WS。这样两个端点的开关行为天然一致，不引入第二套判定。
//
// 明确排除的情况：
//   - 非 OpenAI 平台（Grok / 国产供应商各有独立转发链路）
//   - 走 CC 直转的账号（上游端点就是 /v1/chat/completions，不是 Responses）
//   - Responses 形状的 body（Cursor 兼容路径，字段集与标准 CC 不同）
func (s *OpenAIGatewayService) shouldRouteChatCompletionsViaWS(c *gin.Context, account *Account) bool {
	if s == nil || account == nil || c == nil {
		return false
	}
	if account.Platform != PlatformOpenAI {
		return false
	}
	if shouldForwardOpenAIResponsesViaRawChatCompletions(account) {
		return false
	}
	resolver := s.getOpenAIWSProtocolResolver()
	if resolver == nil {
		return false
	}
	return resolver.Resolve(account).Transport == OpenAIUpstreamTransportResponsesWebsocketV2
}

// forwardChatCompletionsViaWS 让 CC 入站请求复用 Responses 的 WS 转发链路。
//
// 复用而非另写一条：出站方向 responsesBody 已是 Responses 形态，与 Forward
// 期望的入参一致；WS 的连接池、会话粘性、重连恢复、lineage、重试预算等
// 逻辑全部原样生效，不需要在 CC 侧复制一份（那些逻辑在 Forward 里约 200 行，
// 且是上游高频改动区）。
//
// 回程差异由绑定在 context 上的桥接器承担：forwardOpenAIWSV2 写下游时会取出
// 它并把 Responses 事件转成 CC chunk。
func (s *OpenAIGatewayService) forwardChatCompletionsViaWS(
	ctx context.Context,
	c *gin.Context,
	account *Account,
	responsesBody []byte,
	originalModel string,
	clientStream bool,
) (*OpenAIForwardResult, error) {
	bindOpenAIWSChatBridge(c, newOpenAIWSChatBridge(originalModel, clientStream))
	result, err := s.Forward(ctx, c, account, responsesBody)
	if result != nil {
		// 回给客户端与计费口径都用客户端请求的模型名，而不是上游映射后的。
		result.Model = originalModel
	}
	return result, err
}

// WantsBufferedResponse 表示客户端要的是非流式 JSON。
//
// 因为上游恒为流式，v2 的 reqStream 恒为 true，这个方法是 v2 判断
// 「该写 SSE 收尾还是写一次性 JSON」的唯一依据。
func (b *openAIWSChatBridge) WantsBufferedResponse() bool {
	return b != nil && !b.clientStream
}
