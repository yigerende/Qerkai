package service

import (
	"bytes"
	"context"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const downstreamModelObserverContextKey = "openai_downstream_model_observer"

// One writer belongs to one forwarding attempt/WS turn, never to a pooled
// connection. It only touches declared response model fields, not answer text,
// upstream request bodies, the raw-model observer, or billing inputs.
type openAIDownstreamModelWriter struct {
	align     bool
	sent      string
	requested string
	upstream  *upstreamResponseModelObserver
	observed  upstreamResponseModelObserver
}

func (s *OpenAIGatewayService) newDownstreamModelWriter(c *gin.Context, account *Account, requested, sent string) *openAIDownstreamModelWriter {
	w := &openAIDownstreamModelWriter{sent: upstreamSentModel(requested, sent), requested: strings.TrimSpace(requested), upstream: upstreamResponseModelObserverFromContext(c)}
	if c != nil {
		c.Set(downstreamModelObserverContextKey, w)
	}
	if s == nil || account == nil || !account.IsOpenAI() || c == nil || w.sent == "" {
		return w
	}
	ctx := context.Background()
	if c.Request != nil {
		ctx = c.Request.Context()
	}
	w.align = s.settingService.openAIDownstreamModelAlignment(ctx).matches(getOpenAIGroupIDFromContext(c), w.sent)
	return w
}

func (w *openAIDownstreamModelWriter) JSON(payload []byte, eventType string) []byte {
	if w == nil || !bytes.Contains(payload, []byte(`"model"`)) {
		return payload
	}
	if w.align {
		// No recursive string replacement: a tool schema, answer, or error
		// message may legitimately contain a different model name.
		for _, path := range []string{"model", "response.model", "message.model"} {
			value := gjson.GetBytes(payload, path)
			if value.Type != gjson.String || value.Str == w.alignedModel(value.Str) {
				continue
			}
			if !gjson.ValidBytes(payload) {
				return payload
			}
			if rewritten, err := sjson.SetBytes(payload, path, w.sent); err == nil {
				payload = rewritten
			}
		}
	}
	w.observed.Observe(firstValidTrimmedGJSONString(payload, "response.model", "model", "message.model"), isUpstreamResponseModelTerminalEvent(eventType))
	return payload
}

func (w *openAIDownstreamModelWriter) alignedModel(model string) string {
	returned := model
	// Existing protocol/model mapping may already have restored the client
	// alias. Only a real upstream mismatch permits overriding that alias.
	if w != nil && model == w.requested && w.upstream != nil {
		returned = w.upstream.Model()
	}
	return w.alignedModelFromUpstream(model, returned)
}

func (w *openAIDownstreamModelWriter) alignedModelFromUpstream(model, returned string) string {
	if w == nil || !w.align || strings.TrimSpace(model) == "" || strings.TrimSpace(returned) == "" || upstreamModelsMatchForAudit(w.sent, strings.TrimSpace(returned)) {
		return model
	}
	return w.sent
}

func (w *openAIDownstreamModelWriter) SSELine(line string) string {
	if w == nil || !strings.Contains(line, `"model"`) {
		return line
	}
	data, ok := extractOpenAISSEDataLine(line)
	if !ok {
		return line
	}
	payload := []byte(data)
	out := w.JSON(payload, gjson.GetBytes(payload, "type").String())
	if bytes.Equal(payload, out) {
		return line
	}
	return line[:len(line)-len(data)] + string(out)
}

func (w *openAIDownstreamModelWriter) Body(payload []byte) []byte {
	if w == nil || !bytes.Contains(payload, []byte(`"model"`)) {
		return payload
	}
	if !bodyHasSSEFraming(payload) {
		return w.JSON(payload, gjson.GetBytes(payload, "type").String())
	}
	lines := strings.Split(string(payload), "\n")
	changed := false
	for i, line := range lines {
		lines[i] = w.SSELine(line)
		changed = changed || lines[i] != line
	}
	if !changed {
		return payload
	}
	return []byte(strings.Join(lines, "\n"))
}

func (w *openAIDownstreamModelWriter) Model() string {
	if w == nil {
		return ""
	}
	return w.observed.Model()
}

func observedDownstreamModel(c *gin.Context) string {
	if c == nil {
		return ""
	}
	value, _ := c.Get(downstreamModelObserverContextKey)
	w, _ := value.(*openAIDownstreamModelWriter)
	return w.Model()
}

func resetDownstreamModelObservation(c *gin.Context) {
	if c != nil {
		c.Set(downstreamModelObserverContextKey, (*openAIDownstreamModelWriter)(nil))
	}
}

func snapshotDownstreamModel(c *gin.Context, result *OpenAIForwardResult) {
	if result != nil && result.DownstreamModel == "" {
		result.DownstreamModel = observedDownstreamModel(c)
	}
}

// Protocol converters already choose a client-facing model. Record that exact
// value instead of inferring it later from the upstream declaration.
func observeConvertedDownstreamModel(c *gin.Context, model string) {
	if c == nil || strings.TrimSpace(model) == "" {
		return
	}
	value, _ := c.Get(downstreamModelObserverContextKey)
	w, _ := value.(*openAIDownstreamModelWriter)
	if w == nil {
		w = &openAIDownstreamModelWriter{}
		c.Set(downstreamModelObserverContextKey, w)
	}
	w.observed.Observe(model, true)
}

func convertedDownstreamModel(c *gin.Context, model string) string {
	if c == nil {
		return model
	}
	value, _ := c.Get(downstreamModelObserverContextKey)
	w, _ := value.(*openAIDownstreamModelWriter)
	return w.alignedModelFromUpstream(model, observedUpstreamResponseModel(c))
}

func rewriteConvertedDownstreamSSE(c *gin.Context, sse string) string {
	if c == nil {
		return sse
	}
	value, _ := c.Get(downstreamModelObserverContextKey)
	w, _ := value.(*openAIDownstreamModelWriter)
	if w == nil {
		w = &openAIDownstreamModelWriter{}
		c.Set(downstreamModelObserverContextKey, w)
	}
	w.upstream = upstreamResponseModelObserverFromContext(c)
	return string(w.Body([]byte(sse)))
}
