package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/tidwall/gjson"
)

// CC 入站 + WS 上游的端到端验证。
// 新增文件，上游不存在，不产生合并冲突。

func newCCWSTestService(t *testing.T, wsURL string) (*OpenAIGatewayService, *Account) {
	t.Helper()
	cfg := &config.Config{}
	cfg.Security.URLAllowlist.Enabled = false
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Gateway.OpenAIWS.Enabled = true
	cfg.Gateway.OpenAIWS.OAuthEnabled = true
	cfg.Gateway.OpenAIWS.APIKeyEnabled = true
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 2
	cfg.Gateway.OpenAIWS.QueueLimitPerConn = 8
	cfg.Gateway.OpenAIWS.DialTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 30
	cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 10

	svc := &OpenAIGatewayService{
		cfg: cfg,
		httpUpstream: &httpUpstreamRecorder{resp: &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"usage":{"input_tokens":1,"output_tokens":1}}`)),
		}},
		cache:            &stubGatewayCache{},
		openaiWSResolver: NewOpenAIWSProtocolResolver(cfg),
		toolCorrector:    NewCodexToolCorrector(),
	}
	account := &Account{
		ID: 42, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive,
		Schedulable: true, Concurrency: 2,
		Credentials: map[string]any{"api_key": "sk-test", "base_url": wsURL},
		Extra:       map[string]any{"responses_websockets_v2_enabled": true},
	}
	return svc, account
}

func newCCWSTestContext() (*gin.Context, *httptest.ResponseRecorder) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	gid := int64(7)
	c.Set("api_key", &APIKey{GroupID: &gid})
	return c, rec
}

// mock 上游：按 Responses 协议推事件。
func newResponsesWSServer(t *testing.T, frames []string) *httptest.Server {
	t.Helper()
	up := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var req map[string]any
		if conn.ReadJSON(&req) != nil {
			return
		}
		for _, f := range frames {
			if err := conn.WriteMessage(websocket.TextMessage, []byte(f)); err != nil {
				return
			}
		}
	}))
}

// TestCCOverWSStreamingProducesChatChunks 核心用例：
// CC 入站走 WS 上游，客户端必须收到 chat.completion.chunk，而不是 Responses 事件。
func TestCCOverWSStreamingProducesChatChunks(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ws := newResponsesWSServer(t, []string{
		`{"type":"response.created","response":{"id":"resp_cc_1","model":"gpt-5.1"}}`,
		`{"type":"response.output_text.delta","delta":"Hello","output_index":0,"content_index":0,"item_id":"msg_1"}`,
		`{"type":"response.output_text.delta","delta":" world","output_index":0,"content_index":0,"item_id":"msg_1"}`,
		`{"type":"response.completed","response":{"id":"resp_cc_1","model":"gpt-5.1","status":"completed","usage":{"input_tokens":11,"output_tokens":5}}}`,
	})
	defer ws.Close()

	svc, account := newCCWSTestService(t, ws.URL)
	c, rec := newCCWSTestContext()

	body := []byte(`{"model":"gpt-5.1","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	result, err := svc.ForwardAsChatCompletions(context.Background(), c, account, body, "", "")
	if err != nil {
		t.Fatalf("forward failed: %v", err)
	}
	out := rec.Body.String()
	t.Logf("客户端收到:\n%s", out)

	// 必须是 CC 格式
	if !strings.Contains(out, `"object":"chat.completion.chunk"`) {
		t.Fatalf("客户端未收到 chat.completion.chunk:\n%s", out)
	}
	// 绝不能泄漏 Responses 事件
	for _, leak := range []string{"response.output_text.delta", "response.created", "response.completed"} {
		if strings.Contains(out, leak) {
			t.Fatalf("Responses 事件泄漏给 CC 客户端 (%s):\n%s", leak, out)
		}
	}
	// 内容完整
	if !strings.Contains(out, "Hello") || !strings.Contains(out, "world") {
		t.Fatalf("内容不完整:\n%s", out)
	}
	// 必须有 [DONE] 收尾
	if !strings.Contains(out, "data: [DONE]") {
		t.Fatalf("缺少 [DONE] 收尾:\n%s", out)
	}
	// 模型名回显客户端请求的那个
	if result == nil || result.Model != "gpt-5.1" {
		t.Fatalf("result.Model = %v, want gpt-5.1", result)
	}
	// usage 必须记账
	if result.Usage.InputTokens != 11 || result.Usage.OutputTokens != 5 {
		t.Fatalf("usage 未正确记录: in=%d out=%d", result.Usage.InputTokens, result.Usage.OutputTokens)
	}
	if !result.OpenAIWSMode {
		t.Fatal("result.OpenAIWSMode 应为 true")
	}
}

// TestCCOverWSNonStreamingProducesChatCompletion 非流式：必须是 chat.completion 对象。
func TestCCOverWSNonStreamingProducesChatCompletion(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ws := newResponsesWSServer(t, []string{
		`{"type":"response.created","response":{"id":"resp_cc_2","model":"gpt-5.1"}}`,
		`{"type":"response.output_text.delta","delta":"Answer","output_index":0,"content_index":0,"item_id":"msg_1"}`,
		`{"type":"response.completed","response":{"id":"resp_cc_2","model":"gpt-5.1","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Answer"}]}],"usage":{"input_tokens":8,"output_tokens":3}}}`,
	})
	defer ws.Close()

	svc, account := newCCWSTestService(t, ws.URL)
	c, rec := newCCWSTestContext()

	body := []byte(`{"model":"gpt-5.1","stream":false,"messages":[{"role":"user","content":"q"}]}`)
	result, err := svc.ForwardAsChatCompletions(context.Background(), c, account, body, "", "")
	if err != nil {
		t.Fatalf("forward failed: %v", err)
	}
	out := rec.Body.String()
	t.Logf("客户端收到:\n%s", out)

	if got := gjson.Get(out, "object").String(); got != "chat.completion" {
		t.Fatalf("object = %q, want chat.completion\n%s", got, out)
	}
	if got := gjson.Get(out, "choices.0.message.content").String(); !strings.Contains(got, "Answer") {
		t.Fatalf("content = %q, want to contain Answer\n%s", got, out)
	}
	if result == nil || result.Usage.InputTokens != 8 {
		t.Fatalf("usage 未记录: %+v", result)
	}
}

// TestResponsesInboundUnaffectedByBridge 回归守护：
// Responses 入站没有绑定桥接器，必须保持原样透传。
func TestResponsesInboundUnaffectedByBridge(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ws := newResponsesWSServer(t, []string{
		`{"type":"response.created","response":{"id":"resp_r1","model":"gpt-5.1"}}`,
		`{"type":"response.output_text.delta","delta":"raw","output_index":0,"content_index":0,"item_id":"m1"}`,
		`{"type":"response.completed","response":{"id":"resp_r1","model":"gpt-5.1","usage":{"input_tokens":2,"output_tokens":1}}}`,
	})
	defer ws.Close()

	svc, account := newCCWSTestService(t, ws.URL)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/openai/v1/responses", nil)
	gid := int64(7)
	c.Set("api_key", &APIKey{GroupID: &gid})

	body := []byte(`{"model":"gpt-5.1","stream":true,"input":[{"type":"input_text","text":"hi"}]}`)
	if _, err := svc.Forward(context.Background(), c, account, body); err != nil {
		t.Fatalf("forward failed: %v", err)
	}
	out := rec.Body.String()
	// Responses 入站必须原样透传 Responses 事件
	if !strings.Contains(out, "response.output_text.delta") {
		t.Fatalf("Responses 入站的事件被改写了:\n%s", out)
	}
	if strings.Contains(out, "chat.completion.chunk") {
		t.Fatalf("Responses 入站不应产生 CC chunk:\n%s", out)
	}
}

// ---------------------------------------------------------------------------
// 正文只在终止事件 / output_item.done 里时的流式兜底
//
// 生产实测症状：CC 流式请求 status 200、5.1s、输入 270 token，但输出为空、
// 首字为空。上游整轮没发任何 .delta，正文只在 output_item.done 与终止事件的
// output 里，而这两类事件在 ResponsesEventToChatChunks 里都不产出 chunk，
// 客户端只拿到 role + finish + usage。
// ---------------------------------------------------------------------------

func ccChunkContents(t *testing.T, body string) string {
	t.Helper()
	var sb strings.Builder
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		payload := strings.TrimPrefix(line, "data: ")
		if payload == "[DONE]" {
			continue
		}
		sb.WriteString(gjson.Get(payload, "choices.0.delta.content").String())
	}
	return sb.String()
}

// 正文只在终止事件的 output 里：必须补出 content chunk。
func TestCCOverWSStreamingRecoversContentFromTerminalOutput(t *testing.T) {
	b := newOpenAIWSChatBridge("gpt-5.6-sol", true)

	b.TransformStreamFrame([]byte(`{"type":"response.created","response":{"id":"resp_1","model":"gpt-5.6-sol"}}`))
	b.TransformStreamFrame([]byte(`{"type":"response.in_progress"}`))
	terminal := b.TransformStreamFrame([]byte(`{"type":"response.completed","response":{"id":"resp_1","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"HELLO"}]}],"usage":{"input_tokens":270,"output_tokens":2}}}`))

	got := ccChunkContents(t, string(terminal))
	if got != "HELLO" {
		t.Fatalf("补正文失败: content=%q\nframe=%s", got, terminal)
	}
	// finish_reason 必须在正文之后，否则客户端已认定本轮结束。
	body := string(terminal)
	if ci, fi := strings.Index(body, "HELLO"), strings.Index(body, "finish_reason\":\"stop"); ci < 0 || fi < 0 || ci > fi {
		t.Fatalf("正文必须先于 finish_reason 写出: content_at=%d finish_at=%d\n%s", ci, fi, body)
	}
}

// 正文只在 output_item.done 里（终止事件 output 为空）：同样要补出。
func TestCCOverWSStreamingRecoversContentFromOutputItemDone(t *testing.T) {
	b := newOpenAIWSChatBridge("gpt-5.6-sol", true)

	b.TransformStreamFrame([]byte(`{"type":"response.created","response":{"id":"resp_2","model":"gpt-5.6-sol"}}`))
	b.TransformStreamFrame([]byte(`{"type":"response.output_item.done","output_index":0,"item":{"id":"msg_1","type":"message","role":"assistant","content":[{"type":"output_text","text":"WORLD"}]}}`))
	terminal := b.TransformStreamFrame([]byte(`{"type":"response.completed","response":{"id":"resp_2","status":"completed","output":[],"usage":{"input_tokens":270,"output_tokens":2}}}`))

	if got := ccChunkContents(t, string(terminal)); got != "WORLD" {
		t.Fatalf("output_item.done 的正文未补出: content=%q\nframe=%s", got, terminal)
	}
}

// 上游正常发 .delta 时不得重复补正文。
func TestCCOverWSStreamingNoDuplicateWhenDeltasPresent(t *testing.T) {
	b := newOpenAIWSChatBridge("gpt-5.6-sol", true)

	b.TransformStreamFrame([]byte(`{"type":"response.created","response":{"id":"resp_3","model":"gpt-5.6-sol"}}`))
	d1 := b.TransformStreamFrame([]byte(`{"type":"response.output_text.delta","delta":"HEL"}`))
	d2 := b.TransformStreamFrame([]byte(`{"type":"response.output_text.delta","delta":"LO"}`))
	terminal := b.TransformStreamFrame([]byte(`{"type":"response.completed","response":{"id":"resp_3","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"HELLO"}]}]}}`))
	tail := b.FinalizeStream()

	all := string(d1) + string(d2) + string(terminal) + string(tail)
	if got := ccChunkContents(t, all); got != "HELLO" {
		t.Fatalf("正文重复或丢失: content=%q（期望 HELLO）\n%s", got, all)
	}
}

// 断流（无终止事件）时，FinalizeStream 是最后一次补正文的机会。
func TestCCOverWSStreamingRecoversOnFinalizeWithoutTerminal(t *testing.T) {
	b := newOpenAIWSChatBridge("gpt-5.6-sol", true)

	b.TransformStreamFrame([]byte(`{"type":"response.created","response":{"id":"resp_4","model":"gpt-5.6-sol"}}`))
	b.TransformStreamFrame([]byte(`{"type":"response.output_item.done","output_index":0,"item":{"id":"msg_2","type":"message","role":"assistant","content":[{"type":"output_text","text":"TAIL"}]}}`))
	tail := b.FinalizeStream()

	if got := ccChunkContents(t, string(tail)); got != "TAIL" {
		t.Fatalf("断流兜底失败: content=%q\n%s", got, tail)
	}
	if !strings.Contains(string(tail), "data: [DONE]") {
		t.Fatalf("收尾必须仍带 [DONE]:\n%s", tail)
	}
}

// response.failed 不补正文：本轮没有产出，补了会掩盖真实错误。
func TestCCOverWSStreamingSkipsRecoveryOnFailed(t *testing.T) {
	b := newOpenAIWSChatBridge("gpt-5.6-sol", true)

	b.TransformStreamFrame([]byte(`{"type":"response.created","response":{"id":"resp_5","model":"gpt-5.6-sol"}}`))
	failed := b.TransformStreamFrame([]byte(`{"type":"response.failed","response":{"id":"resp_5","status":"failed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"LEAK"}]}]}}`))

	if strings.Contains(string(failed), "LEAK") {
		t.Fatalf("response.failed 不应补正文:\n%s", failed)
	}
}

// 真的没有正文（纯工具调用）时不得凭空造出 content chunk。
func TestCCOverWSStreamingNoContentChunkWhenToolCallOnly(t *testing.T) {
	b := newOpenAIWSChatBridge("gpt-5.6-sol", true)

	b.TransformStreamFrame([]byte(`{"type":"response.created","response":{"id":"resp_6","model":"gpt-5.6-sol"}}`))
	b.TransformStreamFrame([]byte(`{"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","call_id":"call_1","name":"do_thing"}}`))
	b.TransformStreamFrame([]byte(`{"type":"response.function_call_arguments.delta","output_index":0,"delta":"{}"}`))
	terminal := b.TransformStreamFrame([]byte(`{"type":"response.completed","response":{"id":"resp_6","status":"completed","output":[{"type":"function_call","call_id":"call_1","name":"do_thing","arguments":"{}"}]}}`))

	if got := ccChunkContents(t, string(terminal)); got != "" {
		t.Fatalf("纯工具调用不应有 content: content=%q\n%s", got, terminal)
	}
	if !strings.Contains(string(terminal), "tool_calls") {
		t.Fatalf("工具调用的 finish_reason 应为 tool_calls:\n%s", terminal)
	}
}

// 补出的 chunk 必须带 role（response.created 缺失时不能给出无 role 的消息）。
func TestCCOverWSStreamingRecoveredChunkCarriesRole(t *testing.T) {
	b := newOpenAIWSChatBridge("gpt-5.6-sol", true)

	// 没有 response.created，直接终止事件。
	terminal := b.TransformStreamFrame([]byte(`{"type":"response.completed","response":{"id":"resp_7","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"SOLO"}]}]}}`))

	body := string(terminal)
	if got := ccChunkContents(t, body); got != "SOLO" {
		t.Fatalf("content=%q\n%s", got, body)
	}
	if !strings.Contains(body, `"role":"assistant"`) {
		t.Fatalf("补出的首个 chunk 必须带 role:\n%s", body)
	}
}

// 非流式路径行为不变：仍由 TransformFinalResponse 一次性输出。
func TestCCOverWSNonStreamingUnaffectedByRecovery(t *testing.T) {
	b := newOpenAIWSChatBridge("gpt-5.6-sol", false)

	if out := b.TransformStreamFrame([]byte(`{"type":"response.created","response":{"id":"resp_8"}}`)); out != nil {
		t.Fatalf("非流式不应向下游写 chunk: %s", out)
	}
	b.TransformStreamFrame([]byte(`{"type":"response.output_item.done","output_index":0,"item":{"id":"m","type":"message","role":"assistant","content":[{"type":"output_text","text":"NS"}]}}`))
	b.TransformStreamFrame([]byte(`{"type":"response.completed","response":{"id":"resp_8","status":"completed","output":[]}}`))

	final := b.TransformFinalResponse(nil)
	if got := gjson.GetBytes(final, "choices.0.message.content").String(); got != "NS" {
		t.Fatalf("非流式正文丢失: %s", final)
	}
}
