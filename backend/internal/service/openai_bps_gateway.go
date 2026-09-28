package service

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/Wei-Shaw/sub2api/internal/pkg/basispoints"
	"github.com/Wei-Shaw/sub2api/internal/pkg/claude"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

var openAIBPSEngine basispoints.Engine

const openAIBPSEndpoint = "/basispoints/api/responses"

// BPSIngressEnabled is used before account selection. The scheduler still
// restricts the BPS bridge to exact OpenAI OAuth accounts.
func (s *OpenAIGatewayService) BPSIngressEnabled(ctx context.Context, c *gin.Context, model string) bool {
	if s == nil || s.settingService == nil {
		return false
	}
	cfg := s.settingService.bpsSettings(ctx)
	if !cfg.groupMatches(getOpenAIGroupIDFromContext(c)) {
		return false
	}
	_, ok := cfg.models[model]
	return ok
}

func (s *OpenAIGatewayService) bpsRoute(ctx context.Context, c *gin.Context, account *Account, body []byte, mapped string) *cachedOpenAIBPS {
	if account == nil || !account.IsOpenAIOAuth() || s.settingService == nil {
		return nil
	}
	cfg := s.settingService.bpsSettings(ctx)
	if cfg == nil || !cfg.settings.Enabled {
		return nil
	}
	model := gjson.GetBytes(body, "model").String()
	if mapped != "" {
		model = mapped
	}
	if !cfg.matches(account, getOpenAIGroupIDFromContext(c), model) {
		return nil
	}
	return cfg
}

func openBPSResponse(ctx context.Context, cfg OpenAIBPSSettings, account *Account, token string, body []byte, stream bool, scope string, upstream HTTPUpstream, compact bool, options ...basispoints.StreamOptions) (*http.Response, error) {
	if account == nil || !account.IsOpenAIOAuth() {
		return nil, &basispoints.APIError{Status: 400, Kind: "invalid_account_type", Message: "BPS requires an OpenAI OAuth account"}
	}
	if upstream == nil {
		return nil, errors.New("BPS HTTP transport is unavailable")
	}
	proxy := ""
	if account.Proxy != nil {
		proxy = account.Proxy.URL()
	}
	request := basispoints.ExecutorRequest{Payload: body, SourceFormat: "codex", Stream: stream, StorageJSON: basispoints.Credential(token, account.GetChatGPTAccountID()), CacheScope: fmt.Sprintf("%s/%d/%s", scope, account.ID, gjson.GetBytes(body, "model").String())}
	wsProxy := proxy
	if wsProxy == "" {
		// Match Qerkai's HTTP transport: an account without a proxy connects directly.
		wsProxy = "direct"
	}
	request.AuthAttributes = map[string]string{
		// BPS transport is controlled only by BPS settings, independently of
		// the account's Codex WS mode and forced-HTTP setting.
		"websockets":            strconv.FormatBool(cfg.UpstreamTransport == "auto"),
		"basispoints_proxy_url": wsProxy,
	}
	if compact {
		request.Alt = "responses/compact"
	}
	return openAIBPSEngine.Open(ctx, cfg.Config, request, func(req *http.Request) (*http.Response, error) {
		return upstream.Do(req, proxy, account.ID, account.Concurrency)
	}, options...)
}

func bpsError(err error) (int, string) {
	var api *basispoints.APIError
	if errors.As(err, &api) {
		return api.Status, api.Kind
	}
	return http.StatusBadGateway, "upstream_error"
}

func (s *OpenAIGatewayService) recordBPSError(c *gin.Context, account *Account, err error) {
	status, code := bpsError(err)
	setOpsUpstreamError(c, status, err.Error(), "")
	appendOpsUpstreamError(c, OpsUpstreamErrorEvent{AccountID: account.ID, AccountName: account.Name, Platform: account.Platform, ProxyID: opsUpstreamProxyID(account), ProxyName: opsUpstreamProxyName(account), UpstreamStatusCode: status, Kind: "http_error", Message: err.Error()})
	// Local contract validation (400/422) is never a credential failure.
	if (status == 401 || status == 429) && s.rateLimitService != nil {
		payload, _ := json.Marshal(map[string]any{"error": map[string]string{"code": code, "message": err.Error()}})
		s.handleFailoverSideEffects(c.Request.Context(), &http.Response{StatusCode: status, Header: make(http.Header)}, account, payload, "")
	}
}

func (s *OpenAIGatewayService) writeBPSError(c *gin.Context, account *Account, err error, format string) {
	s.recordBPSError(c, account, err)
	status, code := bpsError(err)
	MarkResponseCommitted(c)
	if format == "anthropic" {
		writeAnthropicError(c, status, code, err.Error())
		return
	}
	c.JSON(status, gin.H{"error": gin.H{"type": code, "code": code, "message": err.Error()}})
}

func (s *OpenAIGatewayService) forwardBPS(ctx context.Context, c *gin.Context, account *Account, body []byte, cfg *cachedOpenAIBPS, format, mapped, cacheKey string) (*OpenAIForwardResult, error) {
	start := time.Now()
	beginUpstreamResponseModelObservation(c)
	resetDownstreamModelObservation(c)
	resetOpenAIStateUsage(c)
	SetActualOpenAIUpstreamEndpoint(c, openAIBPSEndpoint)
	original := gjson.GetBytes(body, "model").String()
	model := original
	if mapped != "" {
		model = mapped
	}
	clientStream := gjson.GetBytes(body, "stream").Bool()
	restriction := s.detectCodexClientRestriction(c, account, body)
	if restriction.Enabled && !restriction.Matched {
		err := &basispoints.APIError{Status: 403, Kind: "forbidden_error", Message: CodexClientRestrictionMessage(restriction)}
		s.writeBPSError(c, account, err, format)
		return nil, err
	}
	converted, err := bpsConvertRequest(body, format, model, cacheKey, c.GetHeader("anthropic-beta"))
	if err != nil {
		s.writeBPSError(c, account, err, format)
		return nil, err
	}
	stream := clientStream
	token, _, err := s.GetAccessToken(ctx, account)
	if err != nil {
		s.writeBPSError(c, account, err, format)
		return nil, err
	}
	timing := &bpsFirstToken{start: start, mode: s.openAIStreamTTFTMode(ctx, account)}
	resp, err := openBPSResponse(ctx, cfg.settings, account, token, converted, stream, fmt.Sprintf("key:%d", getAPIKeyIDFromContext(c)), s.httpUpstream, isOpenAIResponsesCompactPath(c), timing.options())
	if err != nil {
		s.writeBPSError(c, account, err, format)
		return nil, err
	}
	defer resp.Body.Close()
	actual, _ := cfg.settings.Config.UpstreamModelFor(model)
	SetOpsUpstreamModel(c, actual)
	var result *OpenAIForwardResult
	if format == "chat" || format == "anthropic" {
		result, err = s.deliverBPSCompatResponse(c, account, resp, original, actual, format, clientStream, start)
	} else {
		result, err = s.deliverBPSResponse(c, account, resp, original, actual, clientStream, start, nil)
	}
	if result != nil {
		result.FirstTokenMs = timing.milliseconds()
	}
	if err != nil {
		// Compatibility readers must not turn a BPS contract error into a Codex retry.
		if !c.Writer.Written() {
			s.writeBPSError(c, account, err, format)
		}
		return result, fmt.Errorf("BPS response: %s", err)
	}
	if result != nil {
		result.UpstreamEndpoint = openAIBPSEndpoint
		result.StateInjected = false
		// Preserve HTTP ingress billing identity; usage type reads the BPS transport metadata.
		result.OpenAIWSMode = false
		result.OpenAIUpstream5xxRetryCount = 0
		result.ReasoningEffort = extractOpenAIReasoningEffortFromBody(converted, actual, model, original)
	}
	return result, nil
}

func bpsConvertRequest(body []byte, format, model, cacheKey, beta string) ([]byte, error) {
	var out []byte
	var err error
	switch format {
	case "chat":
		if gjson.GetBytes(body, "input").Exists() && !gjson.GetBytes(body, "messages").Exists() {
			out = body
			break
		}
		var req apicompat.ChatCompletionsRequest
		if err = json.Unmarshal(body, &req); err != nil {
			break
		}
		var converted *apicompat.ResponsesRequest
		converted, err = apicompat.ChatCompletionsToResponses(&req)
		if err == nil {
			out, err = json.Marshal(converted)
		}
	case "anthropic":
		var req apicompat.AnthropicRequest
		if err = json.Unmarshal(body, &req); err != nil {
			break
		}
		var converted *apicompat.ResponsesRequest
		converted, err = apicompat.AnthropicToResponses(&req)
		if err == nil {
			if containsBetaToken(beta, claude.BetaFastMode) {
				converted.ServiceTier = "priority"
			}
			out, err = json.Marshal(converted)
		}
	default:
		out = body
	}
	if err != nil {
		return nil, &basispoints.APIError{Status: 400, Kind: "invalid_request", Message: err.Error()}
	}
	// Preserve explicit Responses extensions through compatibility conversion.
	// In particular, unsupported contracts must reach the core and be rejected,
	// not silently disappear in a typed Chat/Messages conversion.
	if format != "responses" {
		for _, field := range []string{"prompt_cache_key", "metadata", "context_management", "previous_response_id", "service_tier", "text"} {
			if value := gjson.GetBytes(body, field); value.Exists() {
				out, err = sjson.SetRawBytes(out, field, []byte(value.Raw))
				if err != nil {
					return nil, err
				}
			}
		}
	}
	if format == "anthropic" && containsBetaToken(beta, claude.BetaFastMode) {
		out, err = sjson.SetBytes(out, "service_tier", "priority")
		if err != nil {
			return nil, err
		}
	}
	out, err = sjson.SetBytes(out, "model", model)
	if err == nil && cacheKey != "" && !gjson.GetBytes(out, "prompt_cache_key").Exists() {
		out, err = sjson.SetBytes(out, "prompt_cache_key", cacheKey)
	}
	return out, err
}

// deliverBPSResponse forwards the core's validated wire events verbatim, except
// for the existing optional downstream-model display policy. No Codex retry or
// notification buffering is applied to this independent executor.
func (s *OpenAIGatewayService) deliverBPSResponse(c *gin.Context, account *Account, resp *http.Response, model, upstream string, stream bool, start time.Time, wsWrite func([]byte) error) (*OpenAIForwardResult, error) {
	observer := upstreamResponseModelObserverFromContext(c)
	if observer == nil {
		observer = beginUpstreamResponseModelObservation(c)
	}
	display := s.newDownstreamModelWriter(c, account, model, upstream)
	result := &OpenAIForwardResult{Model: model, BillingModel: upstream, UpstreamModel: upstream, UpstreamEndpoint: openAIBPSEndpoint, Stream: stream, RequestID: resp.Header.Get("x-request-id"), UpstreamHeaders: resp.Header}
	defer func() {
		result.Duration = time.Since(start)
		result.UpstreamResponseModel = observer.Model()
		result.UpstreamResponseModelConflict = observer.Conflict()
		result.DownstreamModel = display.Model()
	}()
	observe := func(raw []byte, terminal bool) {
		kind := gjson.GetBytes(raw, "type").String()
		observer.ObserveOpenAI(raw, kind)
		response := gjson.ParseBytes(raw)
		if response.Get("response").Exists() {
			response = response.Get("response")
		}
		if id := response.Get("id").String(); id != "" {
			result.ResponseID = id
		}
		if terminal {
			usage := response.Get("usage")
			result.Usage = OpenAIUsage{InputTokens: int(usage.Get("input_tokens").Int()), OutputTokens: int(usage.Get("output_tokens").Int()), CacheReadInputTokens: int(usage.Get("input_tokens_details.cached_tokens").Int())}
			result.UpstreamTerminalEvent = kind
		}
	}
	if !stream {
		raw, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		observe(raw, true)
		MarkResponseCommitted(c)
		c.Data(http.StatusOK, "application/json", display.JSON(raw, "response.completed"))
		return result, nil
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64<<10), 128<<20)
	var failure error
	terminal := false
	for scanner.Scan() {
		line := scanner.Text()
		data, ok := extractOpenAISSEDataLine(line)
		if ok && data != "[DONE]" {
			raw := []byte(data)
			kind := gjson.GetBytes(raw, "type").String()
			last := kind == "response.completed" || kind == "response.incomplete"
			observe(raw, last)
			terminal = terminal || last
			if kind == "error" || kind == "response.failed" {
				failure = errors.New(extractOpenAISSEErrorMessage(raw))
				if failure.Error() == "" {
					failure = errors.New("BPS stream failed")
				}
			}
			if wsWrite != nil {
				if err := wsWrite(display.JSON(raw, kind)); err != nil {
					result.ClientDisconnect = true
					return result, err
				}
				continue
			}
		}
		if wsWrite != nil {
			continue
		}
		c.Header("Content-Type", "text/event-stream")
		c.Header("Cache-Control", "no-cache")
		c.Header("X-Accel-Buffering", "no")
		MarkResponseCommitted(c)
		if _, err := io.WriteString(c.Writer, display.SSELine(line)+"\n"); err != nil {
			result.ClientDisconnect = true
			return result, err
		}
		if line == "" {
			c.Writer.Flush()
		}
	}
	if err := scanner.Err(); err != nil {
		return result, err
	}
	if failure != nil {
		setOpsUpstreamError(c, 502, failure.Error(), "")
		return result, failure
	}
	if !terminal {
		return result, errors.New("BPS stream ended without a completed or incomplete event")
	}
	return result, nil
}
