package service

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/gin-gonic/gin"
)

// Reuse the format converters, not the Codex retry/refusal readers: the BPS
// executor has already validated the batch and owns its commit/error boundary.
func (s *OpenAIGatewayService) deliverBPSCompatResponse(c *gin.Context, account *Account, resp *http.Response, model, upstream, format string, stream bool, start time.Time) (*OpenAIForwardResult, error) {
	if !stream {
		raw, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		var response apicompat.ResponsesResponse
		if err := json.Unmarshal(raw, &response); err != nil {
			return nil, err
		}
		observer := upstreamResponseModelObserverFromContext(c)
		observer.Observe(response.Model, true)
		observer.ObserveServiceTier(response.ServiceTier, true)
		s.newDownstreamModelWriter(c, account, model, upstream)
		MarkResponseCommitted(c)
		if format == "chat" {
			converted := apicompat.ResponsesToChatCompletions(&response, model)
			converted.Model = convertedDownstreamModel(c, converted.Model)
			c.JSON(http.StatusOK, converted)
		} else {
			converted := apicompat.ResponsesToAnthropic(&response, model)
			converted.Model = convertedDownstreamModel(c, converted.Model)
			c.JSON(http.StatusOK, converted)
		}
		return &OpenAIForwardResult{Model: model, BillingModel: upstream, UpstreamModel: upstream,
			UpstreamResponseModel: observer.Model(), DownstreamModel: observedDownstreamModel(c),
			UpstreamResponseServiceTier: observedUpstreamResponseServiceTier(c),
			ResponseID:                  response.ID, RequestID: resp.Header.Get("x-request-id"), UpstreamHeaders: resp.Header,
			Usage: copyOpenAIUsageFromResponsesUsage(response.Usage), Duration: time.Since(start),
			UpstreamEndpoint: openAIBPSEndpoint, UpstreamTerminalEvent: "response." + response.Status}, nil
	}
	chat := apicompat.NewResponsesEventToChatState()
	chat.Model, chat.IncludeUsage = model, true
	anthropic := apicompat.NewResponsesEventToAnthropicState()
	anthropic.Model = model
	write := func(frame string) error {
		c.Header("Content-Type", "text/event-stream")
		c.Header("Cache-Control", "no-cache")
		c.Header("X-Accel-Buffering", "no")
		MarkResponseCommitted(c)
		_, err := io.WriteString(c.Writer, rewriteConvertedDownstreamSSE(c, frame))
		if err == nil {
			c.Writer.Flush()
		}
		return err
	}
	writeChat := func(chunks []apicompat.ChatCompletionsChunk) error {
		for _, chunk := range chunks {
			frame, err := apicompat.ChatChunkToSSE(chunk)
			if err != nil {
				return err
			}
			if err := write(frame); err != nil {
				return err
			}
		}
		return nil
	}
	writeAnthropic := func(events []apicompat.AnthropicStreamEvent) error {
		for _, event := range events {
			frame, err := apicompat.ResponsesAnthropicEventToSSE(event)
			if err != nil {
				return err
			}
			if err := write(frame); err != nil {
				return err
			}
		}
		return nil
	}
	return s.deliverBPSResponse(c, account, resp, model, upstream, true, start, func(raw []byte) error {
		var event apicompat.ResponsesStreamEvent
		if err := json.Unmarshal(raw, &event); err != nil {
			return err
		}
		if event.Type == "error" || event.Type == "response.failed" {
			message := extractOpenAISSEErrorMessage(raw)
			if format == "anthropic" {
				return write(buildAnthropicStreamErrorSSE("api_error", message))
			}
			body, _ := json.Marshal(gin.H{"error": gin.H{"type": "upstream_error", "message": message}})
			return write("data: " + string(body) + "\n\n")
		}
		terminal := event.Type == "response.completed" || event.Type == "response.incomplete"
		if format == "chat" {
			if err := writeChat(apicompat.ResponsesEventToChatChunks(&event, chat)); err != nil {
				return err
			}
			if terminal {
				if err := writeChat(apicompat.FinalizeResponsesChatStream(chat)); err != nil {
					return err
				}
				return write("data: [DONE]\n\n")
			}
			return nil
		}
		if err := writeAnthropic(apicompat.ResponsesEventToAnthropicEvents(&event, anthropic)); err != nil {
			return err
		}
		if terminal {
			return writeAnthropic(apicompat.FinalizeResponsesAnthropicStream(anthropic))
		}
		return nil
	})
}
