package service

import (
	"encoding/json"
	"testing"

	"github.com/tidwall/gjson"
)

// 非流式 output 聚合测试。
//
// 事件序列取自真实上游（gpt-5.5，输入 "say exactly: HELLO"）的实测抓取：
//
//	response.output_item.added
//	response.output_item.done   item_type=reasoning
//	response.output_item.added
//	response.content_part.added
//	response.output_text.delta  delta='HEL'
//	response.output_text.delta  delta='LO'
//	response.output_text.done   text='HELLO'
//	response.content_part.done
//	response.output_item.done   item_type=message
//	response.completed          ← response.output 为空数组
func feedOpenAIWSEvents(c *openAIWSOutputCollector, events []string) {
	for _, raw := range events {
		c.Observe(gjson.Get(raw, "type").String(), []byte(raw))
	}
}

// outputTexts 提取 output 中所有 message item 的可见文本。
func outputTexts(t *testing.T, finalResponse []byte) []string {
	t.Helper()
	var texts []string
	for _, item := range gjson.GetBytes(finalResponse, "output").Array() {
		if item.Get("type").String() != "message" {
			continue
		}
		for _, part := range item.Get("content").Array() {
			if part.Get("type").String() == "output_text" {
				texts = append(texts, part.Get("text").String())
			}
		}
	}
	return texts
}

// TestOutputCollectorBackfillsEmptyOutput 是核心用例：
// 复现实测缺陷 —— 终结事件 output 为空，内容只在增量事件里。
func TestOutputCollectorBackfillsEmptyOutput(t *testing.T) {
	collector := newOpenAIWSOutputCollector(true)
	feedOpenAIWSEvents(collector, []string{
		`{"type":"response.output_item.done","output_index":0,"item":{"id":"rs_1","type":"reasoning","summary":[]}}`,
		`{"type":"response.output_text.delta","output_index":1,"item_id":"msg_1","content_index":0,"delta":"HEL"}`,
		`{"type":"response.output_text.delta","output_index":1,"item_id":"msg_1","content_index":0,"delta":"LO"}`,
		`{"type":"response.output_text.done","output_index":1,"item_id":"msg_1","content_index":0,"text":"HELLO"}`,
		`{"type":"response.output_item.done","output_index":1,"item":{"id":"msg_1","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"HELLO"}]}}`,
	})

	patched := collector.PatchFinalResponse([]byte(`{"id":"resp_1","object":"response","status":"completed","output":[]}`))

	if got := outputTexts(t, patched); len(got) != 1 || got[0] != "HELLO" {
		t.Fatalf("expected output text [HELLO], got %v (payload=%s)", got, patched)
	}
	// reasoning item 也应保留，顺序按 output_index。
	items := gjson.GetBytes(patched, "output").Array()
	if len(items) != 2 {
		t.Fatalf("expected 2 output items, got %d: %s", len(items), patched)
	}
	if items[0].Get("type").String() != "reasoning" {
		t.Fatalf("expected reasoning first, got %q", items[0].Get("type").String())
	}
	if !json.Valid(patched) {
		t.Fatal("patched response must remain valid JSON")
	}
}

// TestOutputCollectorTextOnlyFallback 验证只有文本增量、无 output_item.done 时
// 也能重建出 message item（上游未发送 item done 的场景）。
func TestOutputCollectorTextOnlyFallback(t *testing.T) {
	collector := newOpenAIWSOutputCollector(true)
	feedOpenAIWSEvents(collector, []string{
		`{"type":"response.output_text.delta","output_index":0,"item_id":"msg_1","content_index":0,"delta":"AB"}`,
		`{"type":"response.output_text.delta","output_index":0,"item_id":"msg_1","content_index":0,"delta":"CD"}`,
	})

	patched := collector.PatchFinalResponse([]byte(`{"status":"completed","output":[]}`))
	if got := outputTexts(t, patched); len(got) != 1 || got[0] != "ABCD" {
		t.Fatalf("expected [ABCD], got %v", got)
	}
}

// TestOutputCollectorDoneOverridesDelta 验证 done 事件的完整文本覆盖 delta 累积，
// 避免出现 "HELHELLO" 这类重复拼接。
func TestOutputCollectorDoneOverridesDelta(t *testing.T) {
	collector := newOpenAIWSOutputCollector(true)
	feedOpenAIWSEvents(collector, []string{
		`{"type":"response.output_text.delta","output_index":0,"item_id":"m1","content_index":0,"delta":"HEL"}`,
		`{"type":"response.output_text.done","output_index":0,"item_id":"m1","content_index":0,"text":"HELLO"}`,
	})

	patched := collector.PatchFinalResponse([]byte(`{"output":[]}`))
	if got := outputTexts(t, patched); len(got) != 1 || got[0] != "HELLO" {
		t.Fatalf("expected [HELLO] (done must replace deltas), got %v", got)
	}
}

// TestOutputCollectorPreservesNonEmptyOutput 验证终结事件已有可见文本时不改动 ——
// 这是对上游默认行为的回归保护。
func TestOutputCollectorPreservesNonEmptyOutput(t *testing.T) {
	collector := newOpenAIWSOutputCollector(true)
	feedOpenAIWSEvents(collector, []string{
		`{"type":"response.output_text.done","output_index":0,"item_id":"m1","content_index":0,"text":"FROM_EVENTS"}`,
	})

	original := []byte(`{"output":[{"id":"m1","type":"message","role":"assistant","content":[{"type":"output_text","text":"FROM_COMPLETED"}]}]}`)
	patched := collector.PatchFinalResponse(original)

	if got := outputTexts(t, patched); len(got) != 1 || got[0] != "FROM_COMPLETED" {
		t.Fatalf("must not overwrite existing visible output, got %v", got)
	}
}

// TestOutputCollectorFillsEmptyMessageItem 验证终结事件里 message item 存在但
// content 为空时，用增量文本补齐而非追加重复条目。
func TestOutputCollectorFillsEmptyMessageItem(t *testing.T) {
	collector := newOpenAIWSOutputCollector(true)
	feedOpenAIWSEvents(collector, []string{
		`{"type":"response.output_text.done","output_index":0,"item_id":"m1","content_index":0,"text":"RECOVERED"}`,
	})

	patched := collector.PatchFinalResponse([]byte(`{"output":[{"id":"m1","type":"message","role":"assistant","content":[]}]}`))

	texts := outputTexts(t, patched)
	if len(texts) != 1 || texts[0] != "RECOVERED" {
		t.Fatalf("expected [RECOVERED], got %v (payload=%s)", texts, patched)
	}
	// 同一 item id 不应产生两条。
	if n := len(gjson.GetBytes(patched, "output").Array()); n != 1 {
		t.Fatalf("expected 1 deduped item, got %d: %s", n, patched)
	}
}

// TestOutputCollectorMultipleContentParts 验证多个 content part 按索引顺序拼接。
func TestOutputCollectorMultipleContentParts(t *testing.T) {
	collector := newOpenAIWSOutputCollector(true)
	feedOpenAIWSEvents(collector, []string{
		`{"type":"response.output_text.delta","output_index":0,"item_id":"m1","content_index":1,"delta":"SECOND"}`,
		`{"type":"response.output_text.delta","output_index":0,"item_id":"m1","content_index":0,"delta":"FIRST"}`,
	})

	patched := collector.PatchFinalResponse([]byte(`{"output":[]}`))
	got := outputTexts(t, patched)
	if len(got) != 2 || got[0] != "FIRST" || got[1] != "SECOND" {
		t.Fatalf("expected content parts ordered [FIRST SECOND], got %v", got)
	}
}

// TestOutputCollectorDisabledForStreaming 验证流式路径不收集、不改写。
// 流式响应由事件流本身承载内容，聚合既无必要也不应引入开销。
func TestOutputCollectorDisabledForStreaming(t *testing.T) {
	collector := newOpenAIWSOutputCollector(false)
	feedOpenAIWSEvents(collector, []string{
		`{"type":"response.output_text.done","output_index":0,"item_id":"m1","content_index":0,"text":"IGNORED"}`,
	})

	original := []byte(`{"output":[]}`)
	if patched := collector.PatchFinalResponse(original); string(patched) != string(original) {
		t.Fatalf("streaming path must not patch: %s", patched)
	}
}

// TestOutputCollectorNilAndEmptyInputs 验证边界输入不 panic。
func TestOutputCollectorNilAndEmptyInputs(t *testing.T) {
	var nilCollector *openAIWSOutputCollector
	nilCollector.Observe("response.output_text.done", []byte(`{"text":"x"}`))
	if got := nilCollector.PatchFinalResponse([]byte(`{"output":[]}`)); string(got) != `{"output":[]}` {
		t.Fatalf("nil collector must be a no-op, got %s", got)
	}

	collector := newOpenAIWSOutputCollector(true)
	collector.Observe("response.output_text.done", nil)
	collector.Observe("", []byte(`{}`))
	if got := collector.PatchFinalResponse(nil); got != nil {
		t.Fatalf("nil response must stay nil, got %s", got)
	}
	// 无任何收集时不应改写。
	original := []byte(`{"output":[]}`)
	if got := collector.PatchFinalResponse(original); string(got) != string(original) {
		t.Fatalf("no collected content must leave response untouched, got %s", got)
	}
}

// TestOutputCollectorKeepsFunctionCallItems 验证工具调用等非 message item 被保留。
func TestOutputCollectorKeepsFunctionCallItems(t *testing.T) {
	collector := newOpenAIWSOutputCollector(true)
	feedOpenAIWSEvents(collector, []string{
		`{"type":"response.output_item.done","output_index":0,"item":{"id":"fc_1","type":"function_call","call_id":"call_1","name":"do_it","arguments":"{}"}}`,
	})

	patched := collector.PatchFinalResponse([]byte(`{"output":[]}`))
	items := gjson.GetBytes(patched, "output").Array()
	if len(items) != 1 || items[0].Get("type").String() != "function_call" {
		t.Fatalf("expected function_call item preserved, got %s", patched)
	}
}
