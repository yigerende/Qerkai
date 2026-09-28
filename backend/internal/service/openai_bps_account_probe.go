package service

import (
	"bufio"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/basispoints"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

func (s *AccountTestService) testBPSAccount(c *gin.Context, account *Account, model, prompt, mode string, opts AccountTestOptions) error {
	if !account.IsOpenAIOAuth() {
		return s.sendErrorAndEnd(c, "BPS 仅支持 OpenAI OAuth 账号")
	}
	cfg := s.settingService.bpsSettings(c.Request.Context())
	if cfg == nil || !cfg.settings.Enabled {
		return s.sendErrorAndEnd(c, "请先开启 BPS；BPS 与强制 WS、WS 优化调度互斥")
	}
	if mode != "" && mode != "default" {
		return s.sendErrorAndEnd(c, "BPS 不支持 compact 测试")
	}
	if model == "" {
		model = cfg.settings.Models[0]
	}
	if _, ok := cfg.models[model]; !ok || !account.IsModelSupported(model) {
		return s.sendErrorAndEnd(c, "模型不在 BPS 配置或账号允许范围内")
	}
	if s.bpsGateway == nil {
		return s.sendErrorAndEnd(c, "BPS executor is unavailable")
	}
	if strings.TrimSpace(prompt) == "" {
		prompt = "hi"
	}
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("X-Accel-Buffering", "no")
	start := time.Now()
	s.sendEvent(c, TestEvent{Type: "test_start", Model: model})
	s.sendEvent(c, TestEvent{Type: "status", Text: "端点：" + cfg.settings.ResponsesURL})
	body := map[string]any{"model": model, "stream": true, "input": prompt}
	if opts.ReasoningEffort != "" {
		body["reasoning"] = map[string]string{"effort": opts.ReasoningEffort}
	}
	raw, _ := json.Marshal(body)
	token, _, err := s.bpsGateway.GetAccessToken(c.Request.Context(), account)
	if err != nil {
		return s.sendErrorAndEnd(c, err.Error())
	}
	options := basispoints.StreamOptions{ForwardNotifications: s.bpsGateway.openAIStreamTTFTMode(c.Request.Context(), account) == OpenAITTFTModeNetwork}
	if opts.qualityModel != nil {
		// Observe the upstream declaration before downstream display policies or
		// stream reconstruction, including conflicts in notification metadata.
		options.ObserveEvent = func(event, data string) { opts.qualityModel.ObserveOpenAI([]byte(data), event) }
		options.ResetAttempt = func() { *opts.qualityModel = upstreamResponseModelObserver{} }
	}
	resp, err := openBPSResponse(c.Request.Context(), cfg.settings, account, token, raw, true, "admin-test", s.httpUpstream, false, options)
	if err != nil {
		return s.sendErrorAndEnd(c, err.Error())
	}
	defer resp.Body.Close()
	transport := "HTTP/SSE"
	if resp.Header.Get("X-Qerkai-BPS-Transport") == "ws" {
		transport = "WebSocket"
	}
	s.sendEvent(c, TestEvent{Type: "status", Text: "实际传输：" + transport})
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64<<10), cfg.settings.MaxResponseBytes)
	completed := false
	for scanner.Scan() {
		data, ok := extractOpenAISSEDataLine(scanner.Text())
		if !ok || data == "[DONE]" {
			continue
		}
		event := gjson.Parse(data)
		switch event.Get("type").String() {
		case "response.output_text.delta":
			s.sendEvent(c, TestEvent{Type: "content", Text: event.Get("delta").String()})
		case "response.completed":
			completed = true
			r := event.Get("response")
			s.sendEvent(c, TestEvent{Type: "status", Text: fmt.Sprintf("返回模型：%s；输入：%d；输出：%d；缓存：%d；耗时：%d ms", r.Get("model").String(), r.Get("usage.input_tokens").Int(), r.Get("usage.output_tokens").Int(), r.Get("usage.input_tokens_details.cached_tokens").Int(), time.Since(start).Milliseconds())})
		case "response.incomplete":
			return s.sendErrorAndEnd(c, "BPS 响应未完成："+event.Get("response.incomplete_details").Raw)
		case "error", "response.failed":
			return s.sendErrorAndEnd(c, extractOpenAISSEErrorMessage([]byte(data)))
		}
	}
	if err := scanner.Err(); err != nil {
		return s.sendErrorAndEnd(c, err.Error())
	}
	if !completed {
		return s.sendErrorAndEnd(c, "BPS 响应缺少完成事件")
	}
	s.sendEvent(c, TestEvent{Type: "test_complete", Success: true})
	return nil
}
