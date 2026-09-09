package service

import (
	"net/http"

	"github.com/Wei-Shaw/sub2api/internal/util/responseheaders"
	"github.com/gin-gonic/gin"
)

// SSE 响应头的延迟提交。
//
// 二次开发新增文件，上游不存在，不产生合并冲突。
//
// 问题：forwardOpenAIWSV2 在进入 reqStream 分支时就无条件设置 SSE 响应头，
// 但此时一个字节都还没写出。若随后在写出任何事件之前失败（容量降载、握手
// 失败、读错误），或者根本不打算写 SSE（CC 非流式客户端），响应体就会与
// 声明的 Content-Type 矛盾：
//
//	Content-Type: text/event-stream      ← reqStream 分支设的
//	body: {"error":{"type":"upstream_error","message":"..."}}
//
// gin 的 render.writeContentType 只在 Content-Type 为空时才写入
// （render.go: `if val := header["Content-Type"]; len(val) == 0`），
// 所以后续的 c.JSON / c.Data 覆盖不掉这个头。
//
// 下游按 SSE 逐行扫描一个裸 JSON 体，取不到任何 `data:` 行，最终对空串做
// json.Unmarshal —— 生产环境级联代理侧大量出现的
// "unexpected end of JSON input" 就是这么来的：真实错误原因被完全吞掉，
// 客户端只看到一个解析失败。
//
// 这个缺陷在上游一直存在，但被掩盖过一段时间：network 首字口径曾让
// shouldBuffer 恒为 false（见 openai_ws_forwarder_v2.go 的 sawSemanticToken
// 注释），首个结构性事件立即写出，于是「设了头但没写字节」的窗口几乎不存在。
// 缓冲窗口恢复后该窗口重新变宽，缺陷随之显现。
//
// 修法不是在每个错误出口后面补一次清理（出口有八处以上，任何新增出口都会
// 漏掉），而是回到成因：不要在还没打算发 SSE 时就声明 SSE。本文件把这组头
// 的写入推迟到第一次真正向下游写字节的那一刻。
//
// 注意 http.Flusher 的能力探测不依赖响应头，因此仍留在原处（提前失败比
// 写到一半才发现不支持流式要好）。

// openAIWSSSEHeaderGate 把 SSE 响应头的写入推迟到首次真正写出下游字节时。
//
// 零值不可用，必须经 newOpenAIWSSSEHeaderGate 构造：stream=false 时整体为
// no-op，调用方无需自己判断形态。
type openAIWSSSEHeaderGate struct {
	c      *gin.Context
	filter *responseheaders.CompiledHeaderFilter
	stream bool
	sent   bool
}

func newOpenAIWSSSEHeaderGate(
	c *gin.Context,
	filter *responseheaders.CompiledHeaderFilter,
	stream bool,
) *openAIWSSSEHeaderGate {
	return &openAIWSSSEHeaderGate{c: c, filter: filter, stream: stream}
}

// ensure 在首次调用时写出 SSE 响应头，之后为空操作。
//
// 必须在每一处真正写下游字节之前调用。漏调用的后果是响应缺少
// Content-Type: text/event-stream，客户端会把 SSE 流当普通响应体处理——
// 与本文件要修的问题方向相反但同样致命，因此 v2 侧的调用点应贴着写操作放。
func (g *openAIWSSSEHeaderGate) ensure() {
	if g == nil || !g.stream || g.sent || g.c == nil {
		return
	}
	g.sent = true
	if g.filter != nil {
		responseheaders.WriteFilteredHeaders(g.c.Writer.Header(), http.Header{}, g.filter)
	}
	g.c.Header("Content-Type", "text/event-stream")
	g.c.Header("Cache-Control", "no-cache")
	g.c.Header("Connection", "keep-alive")
	g.c.Header("X-Accel-Buffering", "no")
}

// Sent 报告 SSE 头是否已写出，供需要区分「已进入 SSE 语义」的分支使用。
func (g *openAIWSSSEHeaderGate) Sent() bool {
	return g != nil && g.sent
}
