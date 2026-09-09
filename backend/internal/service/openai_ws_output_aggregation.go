package service

import (
	"bytes"
	"sort"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// 非流式响应的 output 聚合（二次开发功能，非上游代码）
//
// 问题：上游 WS 的 response.completed 事件其 response.output 可能为空数组 ——
// 模型产出只存在于增量事件（response.output_item.done / response.output_text.delta|done）中。
// 而上游 openai_ws_forwarder_v2.go 的非流式分支直接照搬终结事件的 response 对象：
//
//	finalResponse = []byte(responseField.Raw)
//
// 于是非流式客户端拿到 output: [] —— 回答完全丢失，但 usage 显示确有产出。
//
// 实测（gpt-5.5，输入 "say exactly: HELLO"）：
//   - stream=true：response.output_text.delta 逐字返回 HEL + LO，正常
//   - stream=false：events=13 token_events=3，但 response.output 为空
//
// 该缺陷在上游一直存在，只是从未被触发 —— WS ingress 的客户端（Codex CLI）
// 全部使用流式，非流式分支从无真实流量。强制上游 WS 开启后，
// HTTP 非流式请求首次成为这条路径的主流量，缺陷随即暴露。
//
// 实现移植自 CPA_dev/CLIProxyAPI-Pro 的 codex_executor_terminal.go
// （patchCodexCompletedOutputWithText 及其辅助函数），该实现已在生产验证。
// 移植时保留其全部边界处理：按 output_index 排序、item_id/content_index 分组、
// done 事件覆盖 delta 累积、空 message item 检测、按 id/call_id 去重。
//
// 注意：不强制向上游写 stream=true。CPA 无条件强制，而 sub2api 刻意保留客户端原值
// （buildOpenAIWSCreatePayload 注释：「保留 stream 字段（与 Codex CLI 一致）」）——
// 这是客户端指纹对齐的一部分。实测证明透传 stream=false 时上游同样发送增量事件，
// 强制改写没有收益，反而偏离 CLI 对齐。

// openAIWSOutputCollector 在事件循环中收集可用于重建 response.output 的信息。
// 零值可用；仅在非流式请求中启用收集以避免流式路径的额外开销。
type openAIWSOutputCollector struct {
	enabled bool

	// itemsByIndex 保存 response.output_item.done 的 item，按 output_index 索引。
	itemsByIndex map[int64][]byte
	// itemsFallback 保存缺少 output_index 的 item，按到达顺序追加。
	itemsFallback [][]byte

	text openAIWSOutputTextAccumulator
}

func newOpenAIWSOutputCollector(enabled bool) *openAIWSOutputCollector {
	return &openAIWSOutputCollector{enabled: enabled}
}

// Observe 处理一个上游事件。非收集模式下立即返回，成本仅一次布尔判断。
func (c *openAIWSOutputCollector) Observe(eventType string, message []byte) {
	if c == nil || !c.enabled || len(message) == 0 {
		return
	}
	switch eventType {
	case "response.output_item.done":
		c.collectOutputItemDone(message)
	case "response.output_text.delta", "response.output_text.done":
		c.text.collect(eventType, message)
	}
}

func (c *openAIWSOutputCollector) collectOutputItemDone(message []byte) {
	item := gjson.GetBytes(message, "item")
	if !item.Exists() || item.Type != gjson.JSON {
		return
	}
	raw := []byte(item.Raw)
	if index := gjson.GetBytes(message, "output_index"); index.Exists() {
		if c.itemsByIndex == nil {
			c.itemsByIndex = make(map[int64][]byte)
		}
		c.itemsByIndex[index.Int()] = raw
		return
	}
	c.itemsFallback = append(c.itemsFallback, raw)
}

// PatchFinalResponse 在终结事件的 response 对象缺少可见输出时补齐 output。
//
// finalResponse 是 response.completed 事件中的 response 对象（而非整个事件），
// 因此这里操作的 JSON 路径是 "output" 而非 CPA 的 "response.output"。
func (c *openAIWSOutputCollector) PatchFinalResponse(finalResponse []byte) []byte {
	if c == nil || !c.enabled || len(finalResponse) == 0 {
		return finalResponse
	}

	textItems := c.text.outputItems()
	doneItems := c.collectedItems()
	// done items 齐全但没有可见文本时（如仅有 reasoning item），用文本项补齐。
	if len(doneItems) > 0 && !openAIWSItemsHaveVisibleText(doneItems) && len(textItems) > 0 {
		doneItems = openAIWSMergeItems(doneItems, textItems)
	}
	sourceItems := doneItems
	if len(sourceItems) == 0 {
		sourceItems = textItems
	}
	if len(sourceItems) == 0 {
		return finalResponse
	}

	existing := gjson.GetBytes(finalResponse, "output")
	if !existing.Exists() || !existing.IsArray() || len(existing.Array()) == 0 {
		return openAIWSSetOutput(finalResponse, sourceItems)
	}

	hydrated := c.hydrateItemIDs(finalResponse, existing.Array())
	existingItems := openAIWSOutputArrayItems(gjson.GetBytes(hydrated, "output"))
	if openAIWSItemsHaveVisibleText(existingItems) {
		return hydrated
	}
	needsBackfill := false
	for _, item := range existingItems {
		if openAIWSMessageItemEmpty(item) {
			needsBackfill = true
			break
		}
	}
	if !needsBackfill {
		return hydrated
	}
	merged := openAIWSMergeItems(existingItems, sourceItems)
	if len(merged) == 0 || openAIWSItemsEqual(existingItems, merged) {
		return hydrated
	}
	return openAIWSSetOutput(hydrated, merged)
}

// hydrateItemIDs 为终结事件中缺少 id 的 output item 补上 done 事件里的 id。
func (c *openAIWSOutputCollector) hydrateItemIDs(finalResponse []byte, items []gjson.Result) []byte {
	patched := finalResponse
	for index, item := range items {
		id := gjson.GetBytes([]byte(item.Raw), "id")
		if id.Exists() && id.Type != gjson.Null &&
			(id.Type != gjson.String || strings.TrimSpace(id.String()) != "") {
			continue
		}
		done, ok := c.itemsByIndex[int64(index)]
		if !ok {
			continue
		}
		doneID := gjson.GetBytes(done, "id")
		if doneID.Type != gjson.String || strings.TrimSpace(doneID.String()) == "" {
			continue
		}
		updated, err := sjson.SetRawBytes(patched, "output."+strconv.Itoa(index)+".id", []byte(doneID.Raw))
		if err != nil {
			continue
		}
		patched = updated
	}
	return patched
}

// collectedItems 返回按 output_index 排序的 done items，缺索引者追加在后。
func (c *openAIWSOutputCollector) collectedItems() [][]byte {
	indexes := make([]int64, 0, len(c.itemsByIndex))
	for index := range c.itemsByIndex {
		indexes = append(indexes, index)
	}
	sort.Slice(indexes, func(i, j int) bool { return indexes[i] < indexes[j] })
	items := make([][]byte, 0, len(c.itemsByIndex)+len(c.itemsFallback))
	for _, index := range indexes {
		items = append(items, c.itemsByIndex[index])
	}
	return append(items, c.itemsFallback...)
}

// openAIWSOutputTextAccumulator 按 output_index / item_id 分组累积 output_text。
type openAIWSOutputTextAccumulator struct {
	byOutput map[int64]*openAIWSOutputTextItem
	byItem   map[string]*openAIWSOutputTextItem
	order    []string
	fallback *openAIWSOutputTextItem
}

type openAIWSOutputTextItem struct {
	id       string
	parts    map[int64]*strings.Builder
	partKeys []int64
}

func (a *openAIWSOutputTextAccumulator) collect(eventType string, message []byte) {
	path := "delta"
	if eventType == "response.output_text.done" {
		path = "text"
	}
	text := gjson.GetBytes(message, path).String()
	// done 事件偶尔把最终文本放在 delta 字段。
	if text == "" && eventType == "response.output_text.done" {
		text = gjson.GetBytes(message, "delta").String()
	}
	if text == "" {
		return
	}
	item := a.itemForEvent(message)
	if item == nil {
		return
	}
	contentIndex := int64(0)
	if result := gjson.GetBytes(message, "content_index"); result.Exists() {
		contentIndex = result.Int()
	}
	if item.parts == nil {
		item.parts = make(map[int64]*strings.Builder)
	}
	part := item.parts[contentIndex]
	if part == nil {
		part = &strings.Builder{}
		item.parts[contentIndex] = part
		item.partKeys = append(item.partKeys, contentIndex)
	}
	// done 携带完整文本，直接覆盖此前累积的 delta，避免重复。
	if eventType == "response.output_text.done" {
		part.Reset()
	}
	part.WriteString(text)
}

func (a *openAIWSOutputTextAccumulator) itemForEvent(message []byte) *openAIWSOutputTextItem {
	itemID := strings.TrimSpace(gjson.GetBytes(message, "item_id").String())
	if outputIndex := gjson.GetBytes(message, "output_index"); outputIndex.Exists() {
		if a.byOutput == nil {
			a.byOutput = make(map[int64]*openAIWSOutputTextItem)
		}
		index := outputIndex.Int()
		item := a.byOutput[index]
		if item == nil {
			item = &openAIWSOutputTextItem{}
			a.byOutput[index] = item
		}
		if item.id == "" {
			item.id = itemID
		}
		return item
	}
	if itemID != "" {
		if a.byItem == nil {
			a.byItem = make(map[string]*openAIWSOutputTextItem)
		}
		item := a.byItem[itemID]
		if item == nil {
			item = &openAIWSOutputTextItem{id: itemID}
			a.byItem[itemID] = item
			a.order = append(a.order, itemID)
		}
		return item
	}
	if a.fallback == nil {
		a.fallback = &openAIWSOutputTextItem{}
	}
	return a.fallback
}

func (a *openAIWSOutputTextAccumulator) outputItems() [][]byte {
	items := make([][]byte, 0, len(a.byOutput)+len(a.byItem)+1)
	indexes := make([]int64, 0, len(a.byOutput))
	for index := range a.byOutput {
		indexes = append(indexes, index)
	}
	sort.Slice(indexes, func(i, j int) bool { return indexes[i] < indexes[j] })
	for _, index := range indexes {
		if raw := buildOpenAIWSMessageItem(a.byOutput[index]); len(raw) > 0 {
			items = append(items, raw)
		}
	}
	for _, itemID := range a.order {
		if raw := buildOpenAIWSMessageItem(a.byItem[itemID]); len(raw) > 0 {
			items = append(items, raw)
		}
	}
	if raw := buildOpenAIWSMessageItem(a.fallback); len(raw) > 0 {
		items = append(items, raw)
	}
	return items
}

// buildOpenAIWSMessageItem 把累积的文本片段组装成一个 Responses message item。
func buildOpenAIWSMessageItem(item *openAIWSOutputTextItem) []byte {
	if item == nil || len(item.parts) == 0 {
		return nil
	}
	partKeys := append([]int64(nil), item.partKeys...)
	sort.Slice(partKeys, func(i, j int) bool { return partKeys[i] < partKeys[j] })

	var buf bytes.Buffer
	buf.WriteByte('{')
	if item.id != "" {
		buf.WriteString(`"id":`)
		buf.WriteString(strconv.Quote(item.id))
		buf.WriteByte(',')
	}
	buf.WriteString(`"type":"message","role":"assistant","status":"completed","content":[`)
	wrote := false
	for _, key := range partKeys {
		part := item.parts[key]
		if part == nil || part.String() == "" {
			continue
		}
		if wrote {
			buf.WriteByte(',')
		}
		buf.WriteString(`{"type":"output_text","text":`)
		buf.WriteString(strconv.Quote(part.String()))
		buf.WriteByte('}')
		wrote = true
	}
	if !wrote {
		return nil
	}
	buf.WriteString(`]}`)
	return buf.Bytes()
}

func openAIWSSetOutput(finalResponse []byte, items [][]byte) []byte {
	if len(items) == 0 {
		return finalResponse
	}
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i, item := range items {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.Write(item)
	}
	buf.WriteByte(']')
	patched, err := sjson.SetRawBytes(finalResponse, "output", buf.Bytes())
	if err != nil {
		return finalResponse
	}
	return patched
}

// openAIWSMergeItems 合并已有与补齐的 item，丢弃空 message 并按 id/call_id 去重。
func openAIWSMergeItems(existing, backfill [][]byte) [][]byte {
	items := make([][]byte, 0, len(existing)+len(backfill))
	seen := make(map[string]struct{}, len(existing)+len(backfill))
	appendItem := func(item []byte) {
		if len(bytes.TrimSpace(item)) == 0 || !openAIWSItemHasContent(item) {
			return
		}
		key := openAIWSItemDedupKey(item)
		if key != "" {
			if _, ok := seen[key]; ok {
				return
			}
			seen[key] = struct{}{}
		}
		items = append(items, item)
	}
	for _, item := range existing {
		if !openAIWSMessageItemEmpty(item) {
			appendItem(item)
		}
	}
	for _, item := range backfill {
		appendItem(item)
	}
	return items
}

func openAIWSOutputArrayItems(result gjson.Result) [][]byte {
	if !result.Exists() || !result.IsArray() {
		return nil
	}
	items := make([][]byte, 0, len(result.Array()))
	for _, item := range result.Array() {
		if item.Type == gjson.JSON && strings.TrimSpace(item.Raw) != "" {
			items = append(items, []byte(item.Raw))
		}
	}
	return items
}

func openAIWSItemsEqual(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(bytes.TrimSpace(a[i]), bytes.TrimSpace(b[i])) {
			return false
		}
	}
	return true
}

func openAIWSItemsHaveVisibleText(items [][]byte) bool {
	for _, item := range items {
		if openAIWSMessageItemHasVisibleText(gjson.ParseBytes(item)) {
			return true
		}
	}
	return false
}

// openAIWSItemHasContent 判断 item 是否值得保留：
// 非 message 类型（reasoning、function_call 等）一律保留，message 需有可见内容。
func openAIWSItemHasContent(item []byte) bool {
	result := gjson.ParseBytes(item)
	if !result.Exists() || result.Type != gjson.JSON {
		return false
	}
	if strings.TrimSpace(result.Get("type").String()) != "message" {
		return true
	}
	return !openAIWSMessageItemEmptyResult(result)
}

func openAIWSMessageItemEmpty(item []byte) bool {
	return openAIWSMessageItemEmptyResult(gjson.ParseBytes(item))
}

func openAIWSMessageItemEmptyResult(item gjson.Result) bool {
	if strings.TrimSpace(item.Get("type").String()) != "message" {
		return false
	}
	if openAIWSMessageItemHasVisibleText(item) {
		return false
	}
	content := item.Get("content")
	if !content.Exists() || content.Type == gjson.Null {
		return true
	}
	if content.Type == gjson.String {
		return strings.TrimSpace(content.String()) == ""
	}
	if !content.IsArray() {
		raw := strings.TrimSpace(content.Raw)
		return raw == "" || raw == "null"
	}
	if len(content.Array()) == 0 {
		return true
	}
	for _, part := range content.Array() {
		if openAIWSContentPartHasVisibleText(part) {
			return false
		}
		// 非文本类 part（如图片）也算有内容，不应判定为空。
		partType := strings.TrimSpace(part.Get("type").String())
		if partType != "" && partType != "output_text" && partType != "input_text" &&
			partType != "text" && partType != "refusal" {
			if raw := strings.TrimSpace(part.Raw); raw != "" && raw != "{}" && raw != "null" {
				return false
			}
		}
	}
	return true
}

func openAIWSMessageItemHasVisibleText(item gjson.Result) bool {
	if item.Type != gjson.JSON {
		return false
	}
	itemType := strings.TrimSpace(item.Get("type").String())
	if itemType != "" && itemType != "message" && itemType != "output_text" {
		return false
	}
	if strings.TrimSpace(item.Get("text").String()) != "" ||
		strings.TrimSpace(item.Get("output_text").String()) != "" ||
		strings.TrimSpace(item.Get("refusal").String()) != "" {
		return true
	}
	content := item.Get("content")
	if content.Type == gjson.String {
		return strings.TrimSpace(content.String()) != ""
	}
	for _, part := range content.Array() {
		if openAIWSContentPartHasVisibleText(part) {
			return true
		}
	}
	return false
}

func openAIWSContentPartHasVisibleText(part gjson.Result) bool {
	if part.Type == gjson.String {
		return strings.TrimSpace(part.String()) != ""
	}
	if part.Type != gjson.JSON {
		return false
	}
	return strings.TrimSpace(part.Get("text").String()) != "" ||
		strings.TrimSpace(part.Get("output_text").String()) != "" ||
		strings.TrimSpace(part.Get("refusal").String()) != ""
}

func openAIWSItemDedupKey(item []byte) string {
	result := gjson.ParseBytes(item)
	itemType := strings.TrimSpace(result.Get("type").String())
	if id := strings.TrimSpace(result.Get("id").String()); id != "" {
		return itemType + ":id:" + id
	}
	if callID := strings.TrimSpace(result.Get("call_id").String()); callID != "" {
		return itemType + ":call:" + callID
	}
	return ""
}
