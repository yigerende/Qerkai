package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

func (s *OpenAIGatewayService) forwardOpenAIWSV2(
	ctx context.Context,
	c *gin.Context,
	account *Account,
	reqBody map[string]any,
	clientPromptCacheKey string,
	token string,
	decision OpenAIWSProtocolDecision,
	isCodexCLI bool,
	reqStream bool,
	originalModel string,
	mappedModel string,
	startTime time.Time,
	attempt int,
	lastFailureReason string,
	agentTaskRecoveryTried *bool,
) (*OpenAIForwardResult, error) {
	if s == nil || account == nil {
		return nil, wrapOpenAIWSFallback("invalid_state", errors.New("service or account is nil"))
	}
	responseModelObserver := &upstreamResponseModelObserver{}

	wsURL, err := s.buildOpenAIResponsesWSURL(account)
	if err != nil {
		return nil, wrapOpenAIWSFallback("build_ws_url", err)
	}
	wsHost := "-"
	wsPath := "-"
	if parsed, parseErr := url.Parse(wsURL); parseErr == nil && parsed != nil {
		if h := strings.TrimSpace(parsed.Host); h != "" {
			wsHost = normalizeOpenAIWSLogValue(h)
		}
		if p := strings.TrimSpace(parsed.Path); p != "" {
			wsPath = normalizeOpenAIWSLogValue(p)
		}
	}
	logOpenAIWSModeDebug(
		"dial_target account_id=%d account_type=%s ws_host=%s ws_path=%s",
		account.ID,
		account.Type,
		wsHost,
		wsPath,
	)

	payload := s.buildOpenAIWSCreatePayload(reqBody, account)
	payloadStrategy, removedKeys := applyOpenAIWSRetryPayloadStrategy(payload, attempt)
	turnState := ""
	turnMetadata := ""
	if c != nil && c.Request != nil {
		turnState = strings.TrimSpace(c.GetHeader(openAIWSTurnStateHeader))
		turnMetadata = strings.TrimSpace(c.GetHeader(openAIWSTurnMetadataHeader))
	}
	setOpenAIWSTurnMetadata(payload, turnMetadata)
	applyStagedCodexFingerprintClientMetadata(c, account, payload)
	previousResponseID := openAIWSPayloadString(payload, "previous_response_id")
	previousResponseIDKind := ClassifyOpenAIPreviousResponseIDKind(previousResponseID)
	promptCacheKey := strings.TrimSpace(clientPromptCacheKey)
	if promptCacheKey == "" {
		// Fingerprint convergence may inject a default key when the client did
		// not send one; retain that fallback without replacing an explicit raw key.
		promptCacheKey = openAIWSPayloadString(payload, "prompt_cache_key")
	}
	_, hasTools := payload["tools"]
	debugEnabled := isOpenAIWSModeDebugEnabled()
	payloadBytes := -1
	resolvePayloadBytes := func() int {
		if payloadBytes >= 0 {
			return payloadBytes
		}
		payloadBytes = len(payloadAsJSONBytes(payload))
		return payloadBytes
	}
	streamValue := "-"
	if raw, ok := payload["stream"]; ok {
		streamValue = normalizeOpenAIWSLogValue(strings.TrimSpace(fmt.Sprintf("%v", raw)))
	}
	payloadEventType := openAIWSPayloadString(payload, "type")
	if payloadEventType == "" {
		payloadEventType = "response.create"
	}
	if s.shouldEmitOpenAIWSPayloadSchema(attempt) {
		logOpenAIWSModeInfo(
			"[debug] payload_schema account_id=%d attempt=%d event=%s payload_keys=%s payload_bytes=%d payload_key_sizes=%s input_summary=%s stream=%s payload_strategy=%s removed_keys=%s has_previous_response_id=%v has_prompt_cache_key=%v has_tools=%v",
			account.ID,
			attempt,
			payloadEventType,
			normalizeOpenAIWSLogValue(strings.Join(sortedKeys(payload), ",")),
			resolvePayloadBytes(),
			normalizeOpenAIWSLogValue(summarizeOpenAIWSPayloadKeySizes(payload, openAIWSPayloadKeySizeTopN)),
			normalizeOpenAIWSLogValue(summarizeOpenAIWSInput(payload["input"])),
			streamValue,
			normalizeOpenAIWSLogValue(payloadStrategy),
			normalizeOpenAIWSLogValue(strings.Join(removedKeys, ",")),
			previousResponseID != "",
			promptCacheKey != "",
			hasTools,
		)
	}

	stateStore := s.getOpenAIWSStateStore()
	groupID := getOpenAIGroupIDFromContext(c)
	sessionHash := s.GenerateSessionHash(c, nil)
	if sessionHash == "" {
		var legacySessionHash string
		sessionHash, legacySessionHash = openAIWSSessionHashesFromID(promptCacheKey)
		attachOpenAILegacySessionHashToGin(c, legacySessionHash)
	}
	if turnState == "" && stateStore != nil && sessionHash != "" {
		if savedTurnState, ok := stateStore.GetSessionTurnState(groupID, sessionHash); ok {
			turnState = savedTurnState
		}
	}
	preferredConnID := ""
	if stateStore != nil && previousResponseID != "" {
		if connID, ok := stateStore.GetResponseConn(previousResponseID); ok {
			preferredConnID = connID
		}
	}
	storeDisabled := s.isOpenAIWSStoreDisabledInRequest(reqBody, account)
	if stateStore != nil && storeDisabled && previousResponseID == "" && sessionHash != "" {
		if connID, ok := stateStore.GetSessionConn(groupID, sessionHash); ok {
			preferredConnID = connID
		}
	}
	storeDisabledConnMode := s.openAIWSStoreDisabledConnMode()
	forceNewConnByPolicy := shouldForceNewConnOnStoreDisabled(storeDisabledConnMode, lastFailureReason)
	forceNewConn := forceNewConnByPolicy && storeDisabled && previousResponseID == "" && sessionHash != "" && preferredConnID == ""
	poolOptimized := OpenAIWSPoolOptimizationActive() && account != nil && account.Type == AccountTypeOAuth && sessionHash != ""
	if poolOptimized {
		forceNewConn = false
	}
	wsHeaders, sessionResolution, buildHdrErr := s.buildOpenAIWSHeaders(
		ctx,
		c,
		account,
		token,
		decision,
		isCodexCLI,
		turnState,
		turnMetadata,
		promptCacheKey,
		openAIWSPayloadString(payload, "model"),
		openAIWSPayloadString(payload, "service_tier"),
	)
	if buildHdrErr != nil {
		return nil, fmt.Errorf("build ws headers: %w", buildHdrErr)
	}
	logOpenAIWSModeDebug(
		"acquire_start account_id=%d account_type=%s transport=%s preferred_conn_id=%s has_previous_response_id=%v session_hash=%s has_turn_state=%v turn_state_len=%d has_turn_metadata=%v turn_metadata_len=%d store_disabled=%v store_disabled_conn_mode=%s retry_last_reason=%s force_new_conn=%v header_user_agent=%s header_openai_beta=%s header_originator=%s header_accept_language=%s header_session_id=%s header_conversation_id=%s session_id_source=%s conversation_id_source=%s has_prompt_cache_key=%v has_chatgpt_account_id=%v has_authorization=%v has_session_id=%v has_conversation_id=%v proxy_enabled=%v",
		account.ID,
		account.Type,
		normalizeOpenAIWSLogValue(string(decision.Transport)),
		truncateOpenAIWSLogValue(preferredConnID, openAIWSIDValueMaxLen),
		previousResponseID != "",
		truncateOpenAIWSLogValue(sessionHash, 12),
		turnState != "",
		len(turnState),
		turnMetadata != "",
		len(turnMetadata),
		storeDisabled,
		normalizeOpenAIWSLogValue(storeDisabledConnMode),
		truncateOpenAIWSLogValue(lastFailureReason, openAIWSLogValueMaxLen),
		forceNewConn,
		openAIWSHeaderValueForLog(wsHeaders, "user-agent"),
		openAIWSHeaderValueForLog(wsHeaders, "openai-beta"),
		openAIWSHeaderValueForLog(wsHeaders, "originator"),
		openAIWSHeaderValueForLog(wsHeaders, "accept-language"),
		openAIWSHeaderValueForLog(wsHeaders, "session_id"),
		openAIWSHeaderValueForLog(wsHeaders, "conversation_id"),
		normalizeOpenAIWSLogValue(sessionResolution.SessionSource),
		normalizeOpenAIWSLogValue(sessionResolution.ConversationSource),
		promptCacheKey != "",
		hasOpenAIWSHeader(wsHeaders, "chatgpt-account-id"),
		hasOpenAIWSHeader(wsHeaders, "authorization"),
		hasOpenAIWSHeader(wsHeaders, "session_id"),
		hasOpenAIWSHeader(wsHeaders, "conversation_id"),
		account.ProxyID != nil && account.Proxy != nil,
	)

	acquireCtx, acquireCancel := context.WithTimeout(ctx, s.openAIWSAcquireTimeout())
	defer acquireCancel()

	lease, err := s.getOpenAIWSConnPool().Acquire(acquireCtx, openAIWSAcquireRequest{
		Account: account,
		WSURL:   wsURL,
		Headers: wsHeaders,
		HeadersFactory: func(factoryCtx context.Context, headers http.Header) (http.Header, error) {
			return s.refreshOpenAIAgentIdentityHeaders(factoryCtx, account, headers)
		},
		PreferredConnID: preferredConnID,
		ForceNewConn:    forceNewConn,
		PoolOptimized:   poolOptimized,
		SessionHash:     sessionHash,
		ProxyURL: func() string {
			if account.ProxyID != nil && account.Proxy != nil {
				return account.Proxy.URL()
			}
			return ""
		}(),
	})
	if err != nil {
		var agentDialErr *openAIWSDialError
		if s.isAgentIdentityAccount(ctx, account) && errors.As(err, &agentDialErr) && isAgentIdentityTaskInvalidWSDialError(agentDialErr) && agentTaskRecoveryTried != nil && !*agentTaskRecoveryTried {
			*agentTaskRecoveryTried = true
			if recoveryErr := s.recoverAgentIdentityTask(ctx, account, account.GetCredential("task_id")); recoveryErr != nil {
				return nil, fmt.Errorf("agent identity task recovery failed: %w", recoveryErr)
			}
			return nil, &agentIdentityTaskRecoveredError{}
		}
		s.handleOpenAIWSDialTransientFailure(ctx, account, mappedModel, err)
		// 二次开发：握手的账号级错误（401/402/403/429/529）走与 HTTP 一致的
		// 账号处理，并转成可 failover 的错误让请求切到其他账号，而不是直接失败。
		// Cloudflare 边缘限速的 403 已在判定中排除（由 dial limiter 退避重试）。
		// 详见 openai_ws_dial_auth_failure.go。
		if dialErr, accountErrStatus, ok := openAIWSDialAccountErrorStatus(err); ok {
			s.handleOpenAIWSDialAccountError(ctx, account, mappedModel, accountErrStatus, dialErr)
			if c == nil || c.Writer == nil || !c.Writer.Written() {
				return nil, wrapOpenAIWSDialAccountErrorFailover(accountErrStatus, dialErr)
			}
		} else if edgeErr, edgeOK := isOpenAIWSDialEdgeRateLimitedError(err); edgeOK {
			// 二次开发：边缘限速的 403 换账号但不标记原账号——该账号只是握手
			// 过密，凭证健康（实测账号 A 被限时账号 B 正常）。dial limiter 已在
			// 此前退避重试过，走到这里说明限速窗口未过，换号是唯一有效动作。
			if c == nil || c.Writer == nil || !c.Writer.Written() {
				logOpenAIWSModeInfo(
					"dial_edge_rate_limited_failover account_id=%d status=%d",
					account.ID, edgeErr.StatusCode,
				)
				return nil, wrapOpenAIWSEdgeRateLimitedFailover(edgeErr)
			}
		}
		dialStatus, dialClass, dialCloseStatus, dialCloseReason, dialRespServer, dialRespVia, dialRespCFRay, dialRespReqID := summarizeOpenAIWSDialError(err)
		logOpenAIWSModeInfo(
			"acquire_fail account_id=%d account_type=%s transport=%s reason=%s dial_status=%d dial_class=%s dial_close_status=%s dial_close_reason=%s dial_resp_server=%s dial_resp_via=%s dial_resp_cf_ray=%s dial_resp_x_request_id=%s cause=%s preferred_conn_id=%s force_new_conn=%v ws_host=%s ws_path=%s proxy_enabled=%v",
			account.ID,
			account.Type,
			normalizeOpenAIWSLogValue(string(decision.Transport)),
			normalizeOpenAIWSLogValue(classifyOpenAIWSAcquireError(err)),
			dialStatus,
			dialClass,
			dialCloseStatus,
			truncateOpenAIWSLogValue(dialCloseReason, openAIWSHeaderValueMaxLen),
			dialRespServer,
			dialRespVia,
			dialRespCFRay,
			dialRespReqID,
			truncateOpenAIWSLogValue(err.Error(), openAIWSLogValueMaxLen),
			truncateOpenAIWSLogValue(preferredConnID, openAIWSIDValueMaxLen),
			forceNewConn,
			wsHost,
			wsPath,
			account.ProxyID != nil && account.Proxy != nil,
		)
		var dialErr *openAIWSDialError
		if errors.As(err, &dialErr) && dialErr != nil && dialErr.StatusCode == http.StatusTooManyRequests {
			s.persistOpenAIWSRateLimitSignal(ctx, account, dialErr.ResponseHeaders, nil, "rate_limit_exceeded", "rate_limit_error", strings.TrimSpace(err.Error()), mappedModel)
		}
		return nil, wrapOpenAIWSFallback(classifyOpenAIWSAcquireError(err), err)
	}
	// cleanExit 标记正常终端事件退出，此时上游不会再发送帧，连接可安全归还复用。
	// 所有异常路径（读写错误、error 事件等）已在各自分支中提前调用 MarkBroken，
	// 因此 defer 中只需处理正常退出时不 MarkBroken 即可。
	cleanExit := false
	defer func() {
		if !cleanExit {
			lease.MarkBroken()
		}
		lease.Release()
	}()
	connID := strings.TrimSpace(lease.ConnID())
	logOpenAIWSModeDebug(
		"connected account_id=%d account_type=%s transport=%s conn_id=%s conn_reused=%v conn_pick_ms=%d queue_wait_ms=%d has_previous_response_id=%v",
		account.ID,
		account.Type,
		normalizeOpenAIWSLogValue(string(decision.Transport)),
		connID,
		lease.Reused(),
		lease.ConnPickDuration().Milliseconds(),
		lease.QueueWaitDuration().Milliseconds(),
		previousResponseID != "",
	)
	if previousResponseID != "" {
		logOpenAIWSModeInfo(
			"continuation_probe account_id=%d account_type=%s conn_id=%s previous_response_id=%s previous_response_id_kind=%s preferred_conn_id=%s conn_reused=%v store_disabled=%v session_hash=%s header_session_id=%s header_conversation_id=%s session_id_source=%s conversation_id_source=%s has_turn_state=%v turn_state_len=%d has_prompt_cache_key=%v",
			account.ID,
			account.Type,
			truncateOpenAIWSLogValue(connID, openAIWSIDValueMaxLen),
			truncateOpenAIWSLogValue(previousResponseID, openAIWSIDValueMaxLen),
			normalizeOpenAIWSLogValue(previousResponseIDKind),
			truncateOpenAIWSLogValue(preferredConnID, openAIWSIDValueMaxLen),
			lease.Reused(),
			storeDisabled,
			truncateOpenAIWSLogValue(sessionHash, 12),
			openAIWSHeaderValueForLog(wsHeaders, "session_id"),
			openAIWSHeaderValueForLog(wsHeaders, "conversation_id"),
			normalizeOpenAIWSLogValue(sessionResolution.SessionSource),
			normalizeOpenAIWSLogValue(sessionResolution.ConversationSource),
			turnState != "",
			len(turnState),
			promptCacheKey != "",
		)
	}
	logOpenAIWSPoolLease(account, lease)
	if c != nil {
		SetOpsLatencyMs(c, OpsOpenAIWSConnPickMsKey, lease.ConnPickDuration().Milliseconds())
		SetOpsLatencyMs(c, OpsOpenAIWSQueueWaitMsKey, lease.QueueWaitDuration().Milliseconds())
		c.Set(OpsOpenAIWSConnReusedKey, lease.Reused())
		if connID != "" {
			c.Set(OpsOpenAIWSConnIDKey, connID)
		}
	}

	handshakeTurnState := strings.TrimSpace(lease.HandshakeHeader(openAIWSTurnStateHeader))
	logOpenAIWSModeDebug(
		"handshake account_id=%d conn_id=%s has_turn_state=%v turn_state_len=%d",
		account.ID,
		connID,
		handshakeTurnState != "",
		len(handshakeTurnState),
	)
	if handshakeTurnState != "" {
		if stateStore != nil && sessionHash != "" {
			stateStore.BindSessionTurnState(groupID, sessionHash, handshakeTurnState, s.openAIWSSessionStickyTTL())
		}
		if c != nil {
			c.Header(http.CanonicalHeaderKey(openAIWSTurnStateHeader), handshakeTurnState)
		}
	}

	if err := s.performOpenAIWSGeneratePrewarm(
		ctx,
		lease,
		decision,
		payload,
		previousResponseID,
		reqBody,
		account,
		stateStore,
		groupID,
	); err != nil {
		return nil, err
	}

	// 二次开发：写超时按 payload 大小收紧（只收紧不放宽）。统一的 120s 上限
	// 会让一次大 payload 写占满两分钟，把 5s 的重连预算彻底吃光，导致最需要
	// 重连的请求反而没有重连保护。详见 openai_ws_write_budget.go。
	if err := lease.WriteJSONWithContextTimeout(
		ctx, payload, openAIWSWriteBudgetCap(resolvePayloadBytes(), s.openAIWSWriteTimeout()),
	); err != nil {
		lease.MarkBroken()
		logOpenAIWSModeInfo(
			"write_request_fail account_id=%d conn_id=%s cause=%s payload_bytes=%d",
			account.ID,
			connID,
			truncateOpenAIWSLogValue(err.Error(), openAIWSLogValueMaxLen),
			resolvePayloadBytes(),
		)
		return nil, wrapOpenAIWSFallback("write_request", err)
	}
	if debugEnabled {
		logOpenAIWSModeDebug(
			"write_request_sent account_id=%d conn_id=%s stream=%v payload_bytes=%d previous_response_id=%s",
			account.ID,
			connID,
			reqStream,
			resolvePayloadBytes(),
			truncateOpenAIWSLogValue(previousResponseID, openAIWSIDValueMaxLen),
		)
	}

	usage := &OpenAIUsage{}
	imageCounter := newOpenAIImageOutputCounter()
	var firstTokenMs *int
	// 二次开发：缓冲窗口必须由「是否出过真实 token 事件」决定，不能复用
	// firstTokenMs——network TTFT 口径下第一个结构性事件（如 codex.rate_limits）
	// 就会置位 firstTokenMs，把上游设计的「首字前缓冲」窗口压缩到零，
	// 导致 wroteDownstream 立刻为真、error 事件再也无法安全 failover。
	// TTFT 是观测口径，缓冲是容错机制，两者必须正交。
	sawSemanticToken := false
	responseID := ""
	var finalResponse []byte
	// 二次开发：非流式时收集增量事件，用于在终结事件 output 为空时回填。
	// 详见 openai_ws_output_aggregation.go。
	outputCollector := newOpenAIWSOutputCollector(!reqStream)
	wroteDownstream := false
	needModelReplace := originalModel != mappedModel
	var mappedModelBytes []byte
	if needModelReplace && mappedModel != "" {
		mappedModelBytes = []byte(mappedModel)
	}
	bufferedStreamEvents := make([][]byte, 0, 4)
	eventCount := 0
	tokenEventCount := 0
	terminalEventCount := 0
	bufferedEventCount := 0
	flushedBufferedEventCount := 0
	firstEventType := ""
	lastEventType := ""
	upstreamTerminalEvent := ""

	// 二次开发：SSE 头改为延迟提交，不在此处无条件写出。
	// 「设了 text/event-stream 但一个字节都没写」的窗口会让后续的 JSON 错误体
	// 带着 SSE 头发给客户端（gin 覆盖不掉已存在的 Content-Type），下游解析必然
	// 失败。详见 openai_ws_sse_headers.go。
	sseHeaders := newOpenAIWSSSEHeaderGate(c, s.responseHeaderFilter, reqStream)
	var flusher http.Flusher
	if reqStream {
		// 能力探测不依赖响应头，仍保持在写出之前：提前失败优于写到一半才发现。
		f, ok := c.Writer.(http.Flusher)
		if !ok {
			lease.MarkBroken()
			return nil, wrapOpenAIWSFallback("streaming_not_supported", errors.New("streaming not supported"))
		}
		flusher = f
	}

	clientDisconnected := false
	flushBatchSize := s.openAIWSEventFlushBatchSize()
	flushInterval := s.openAIWSEventFlushInterval()
	pendingFlushEvents := 0
	lastFlushAt := time.Now()
	flushStreamWriter := func(force bool) {
		if clientDisconnected || flusher == nil || pendingFlushEvents <= 0 {
			return
		}
		if !force && flushBatchSize > 1 && pendingFlushEvents < flushBatchSize {
			if flushInterval <= 0 || time.Since(lastFlushAt) < flushInterval {
				return
			}
		}
		flusher.Flush()
		pendingFlushEvents = 0
		lastFlushAt = time.Now()
	}
	// 二次开发：CC 入站走 WS 时，上游 Responses 事件需转成 chat.completion.chunk
	// 才能被客户端解析。未绑定桥接器时为 nil，行为与上游完全一致。
	// 详见 openai_ws_chat_completions_bridge.go。
	chatBridge := openAIWSChatBridgeFromContext(c)
	emitStreamMessage := func(message []byte, forceFlush bool) {
		if clientDisconnected {
			return
		}
		var frame []byte
		if chatBridge != nil {
			converted := chatBridge.TransformStreamFrame(message)
			if len(converted) == 0 {
				// 该事件不产生 CC 输出（纯结构性事件），跳过写出。
				return
			}
			frame = converted
		} else {
			frame = make([]byte, 0, len(message)+8)
			frame = append(frame, "data: "...)
			frame = append(frame, message...)
			frame = append(frame, '\n', '\n')
		}
		// 二次开发：SSE 头贴着首次写出提交，见 openai_ws_sse_headers.go。
		sseHeaders.ensure()
		_, wErr := c.Writer.Write(frame)
		if wErr == nil {
			wroteDownstream = true
			pendingFlushEvents++
			flushStreamWriter(forceFlush)
			return
		}
		clientDisconnected = true
		logger.LegacyPrintf("service.openai_gateway", "[OpenAI WS Mode] client disconnected, continue draining upstream: account=%d", account.ID)
	}
	flushBufferedStreamEvents := func(reason string) {
		if len(bufferedStreamEvents) == 0 {
			return
		}
		flushed := len(bufferedStreamEvents)
		for _, buffered := range bufferedStreamEvents {
			emitStreamMessage(buffered, false)
		}
		bufferedStreamEvents = bufferedStreamEvents[:0]
		flushStreamWriter(true)
		flushedBufferedEventCount += flushed
		if debugEnabled {
			logOpenAIWSModeDebug(
				"buffer_flush account_id=%d conn_id=%s reason=%s flushed=%d total_flushed=%d client_disconnected=%v",
				account.ID,
				connID,
				truncateOpenAIWSLogValue(reason, openAIWSLogValueMaxLen),
				flushed,
				flushedBufferedEventCount,
				clientDisconnected,
			)
		}
	}

	readTimeout := s.openAIWSReadTimeout()
	var pendingJSONDocuments [][]byte

	for {
		var message []byte
		var readErr error
		if len(pendingJSONDocuments) > 0 {
			message = pendingJSONDocuments[0]
			pendingJSONDocuments = pendingJSONDocuments[1:]
		} else {
			message, readErr = lease.ReadMessageWithContextTimeout(ctx, readTimeout)
			if readErr == nil {
				if documents, repaired := splitOpenAIConcatenatedJSONDocuments(message); repaired {
					logOpenAIWSModeInfo(
						"concatenated_json_repaired account_id=%d conn_id=%s documents=%d bytes=%d",
						account.ID,
						truncateOpenAIWSLogValue(connID, openAIWSIDValueMaxLen),
						len(documents),
						len(message),
					)
					message = documents[0]
					pendingJSONDocuments = append(pendingJSONDocuments, documents[1:]...)
				}
			}
		}
		if readErr == nil && !json.Valid(message) {
			eventType, _, _ := parseOpenAIWSEventEnvelope(message)
			if eventType == "" {
				eventType = "unknown"
			}
			lease.MarkBroken()
			logOpenAIWSModeInfo(
				"invalid_event_json account_id=%d conn_id=%s event_type=%s bytes=%d wrote_downstream=%v",
				account.ID,
				truncateOpenAIWSLogValue(connID, openAIWSIDValueMaxLen),
				truncateOpenAIWSLogValue(eventType, openAIWSLogValueMaxLen),
				len(message),
				wroteDownstream,
			)
			if !wroteDownstream {
				return nil, wrapOpenAIWSFallback("invalid_event_json", errors.New("upstream websocket returned malformed Responses event JSON"))
			}
			return nil, errors.New("upstream websocket returned malformed Responses event JSON after downstream output")
		}
		if readErr != nil {
			lease.MarkBroken()
			closeStatus, closeReason := summarizeOpenAIWSReadCloseError(readErr)
			logOpenAIWSModeInfo(
				"read_fail account_id=%d conn_id=%s wrote_downstream=%v close_status=%s close_reason=%s cause=%s events=%d token_events=%d terminal_events=%d buffered_pending=%d buffered_flushed=%d first_event=%s last_event=%s",
				account.ID,
				connID,
				wroteDownstream,
				closeStatus,
				closeReason,
				truncateOpenAIWSLogValue(readErr.Error(), openAIWSLogValueMaxLen),
				eventCount,
				tokenEventCount,
				terminalEventCount,
				len(bufferedStreamEvents),
				flushedBufferedEventCount,
				truncateOpenAIWSLogValue(firstEventType, openAIWSLogValueMaxLen),
				truncateOpenAIWSLogValue(lastEventType, openAIWSLogValueMaxLen),
			)
			if !wroteDownstream {
				// 二次开发：复用的池连接一个事件都没收到就断，是它在池中空闲期间
				// 被上游 keepalive 打死，不是本次请求的上游故障。单独标记，让重连
				// 不计入重试预算。详见 openai_ws_stale_conn.go。
				if openAIWSReadFailureIsStalePooledConn(lease.Reused(), eventCount, wroteDownstream) &&
					classifyOpenAIWSReadFallbackReason(readErr) == "read_event" {
					return nil, wrapOpenAIWSStaleConnFallback(readErr)
				}
				return nil, wrapOpenAIWSFallback(classifyOpenAIWSReadFallbackReason(readErr), readErr)
			}
			if clientDisconnected {
				break
			}
			setOpsUpstreamError(c, 0, sanitizeUpstreamErrorMessage(readErr.Error()), "")
			return nil, fmt.Errorf("openai ws read event: %w", readErr)
		}
		if normalized, changed := normalizeCompletedImageGenerationStatus(message); changed {
			message = normalized
		}

		eventType, eventResponseID, responseField := parseOpenAIWSEventEnvelope(message)
		if eventType == "" {
			continue
		}
		responseModelObserver.ObserveOpenAI(message, eventType)
		outputCollector.Observe(eventType, message)
		eventCount++
		if firstEventType == "" {
			firstEventType = eventType
		}
		lastEventType = eventType

		if responseID == "" && eventResponseID != "" {
			responseID = eventResponseID
		}

		isTokenEvent := isOpenAIWSTokenEvent(eventType)
		if isTokenEvent {
			tokenEventCount++
		}
		isTerminalEvent := isOpenAIWSTerminalEvent(eventType)
		if isTerminalEvent {
			terminalEventCount++
		}
		// 二次开发：network 模式下改用网络口径首帧判定（仅 OpenAI OAuth）。
		// 详见 openai_ws_ttft_network.go。
		if isTokenEvent {
			sawSemanticToken = true
		}
		if firstTokenMs == nil && isOpenAIWSFirstTokenEvent(s, ctx, account, eventType, isTokenEvent) {
			ms := int(time.Since(startTime).Milliseconds())
			firstTokenMs = &ms
		}
		if debugEnabled && shouldLogOpenAIWSEvent(eventCount, eventType) {
			logOpenAIWSModeDebug(
				"event_received account_id=%d conn_id=%s idx=%d type=%s bytes=%d token=%v terminal=%v buffered_pending=%d",
				account.ID,
				connID,
				eventCount,
				truncateOpenAIWSLogValue(eventType, openAIWSLogValueMaxLen),
				len(message),
				isTokenEvent,
				isTerminalEvent,
				len(bufferedStreamEvents),
			)
		}

		if !clientDisconnected {
			if needModelReplace && len(mappedModelBytes) > 0 && openAIWSEventMayContainModel(eventType) && bytes.Contains(message, mappedModelBytes) {
				message = replaceOpenAIWSMessageModel(message, mappedModel, originalModel)
			}
			if openAIWSEventMayContainToolCalls(eventType) && openAIWSMessageLikelyContainsToolCalls(message) {
				if corrected, changed := s.toolCorrector.CorrectToolCallsInSSEBytes(message); changed {
					message = corrected
				}
			}
			message = restoreCodexToolNamesFromContext(c, message)
		}
		if openAIWSMessageShouldParseUsage(eventType, message) {
			parseOpenAIWSResponseUsageFromCompletedEvent(message, usage)
		}
		imageCounter.AddSSEData(message)

		if eventType == "error" || eventType == "response.failed" {
			markOpenAICyberPolicyEvent(c, message, http.StatusOK, usage)
		}

		if eventType == "error" {
			s.handleOpenAIWSErrorEventTransientFailure(ctx, account, mappedModel, lease.HandshakeHeaders(), message)
			errCodeRaw, errTypeRaw, errMsgRaw := parseOpenAIWSErrorEventFields(message)
			s.persistOpenAIWSRateLimitSignal(ctx, account, lease.HandshakeHeaders(), message, errCodeRaw, errTypeRaw, errMsgRaw, mappedModel)
			errMsg := strings.TrimSpace(errMsgRaw)
			if errMsg == "" {
				errMsg = "Upstream websocket error"
			}
			fallbackReason, canFallback := classifyOpenAIWSErrorEventFromRaw(errCodeRaw, errTypeRaw, errMsgRaw)
			errCode, errType, errMessage := summarizeOpenAIWSErrorEventFieldsFromRaw(errCodeRaw, errTypeRaw, errMsgRaw)
			logOpenAIWSModeInfo(
				"error_event account_id=%d conn_id=%s idx=%d fallback_reason=%s can_fallback=%v err_code=%s err_type=%s err_message=%s",
				account.ID,
				connID,
				eventCount,
				truncateOpenAIWSLogValue(fallbackReason, openAIWSLogValueMaxLen),
				canFallback,
				errCode,
				errType,
				errMessage,
			)
			if fallbackReason == "previous_response_not_found" {
				logOpenAIWSModeInfo(
					"previous_response_not_found_diag account_id=%d account_type=%s conn_id=%s previous_response_id=%s previous_response_id_kind=%s response_id=%s event_idx=%d req_stream=%v store_disabled=%v conn_reused=%v session_hash=%s header_session_id=%s header_conversation_id=%s session_id_source=%s conversation_id_source=%s has_turn_state=%v turn_state_len=%d has_prompt_cache_key=%v err_code=%s err_type=%s err_message=%s",
					account.ID,
					account.Type,
					connID,
					truncateOpenAIWSLogValue(previousResponseID, openAIWSIDValueMaxLen),
					normalizeOpenAIWSLogValue(previousResponseIDKind),
					truncateOpenAIWSLogValue(responseID, openAIWSIDValueMaxLen),
					eventCount,
					reqStream,
					storeDisabled,
					lease.Reused(),
					truncateOpenAIWSLogValue(sessionHash, 12),
					openAIWSHeaderValueForLog(wsHeaders, "session_id"),
					openAIWSHeaderValueForLog(wsHeaders, "conversation_id"),
					normalizeOpenAIWSLogValue(sessionResolution.SessionSource),
					normalizeOpenAIWSLogValue(sessionResolution.ConversationSource),
					turnState != "",
					len(turnState),
					promptCacheKey != "",
					errCode,
					errType,
					errMessage,
				)
			}
			// error 事件后连接不再可复用，避免回池后污染下一请求。
			lease.MarkBroken()
			// 二次开发：容量降载等瞬态 error 事件走 HTTP SSE 同一套 failover 判定
			// ——先同账号退避重试（保住 prompt 缓存亲和），再换账号。必须放在
			// canFallback 之前：后者只做同账号 WS 重连，上游整体降载时重连大概率
			// 仍然降载。详见 openai_ws_error_event_failover.go。
			if shouldFailoverOpenAIWSErrorEvent(message, errMsg, wroteDownstream) {
				s.handleOpenAIStreamTerminalAccountSideEffects(
					c, account, message, errMsg, lease.HandshakeHeaders(), mappedModel,
				)
				return nil, s.newOpenAIWSErrorEventFailover(
					c, account, responseID, message, errMsg, mappedModel, lease.HandshakeHeaders(),
				)
			}
			if !wroteDownstream && canFallback {
				return nil, wrapOpenAIWSFallback(fallbackReason, errors.New(errMsg))
			}
			statusCode := openAIWSErrorHTTPStatusFromRaw(errCodeRaw, errTypeRaw)
			setOpsUpstreamError(c, statusCode, errMsg, "")
			if reqStream && !clientDisconnected {
				flushBufferedStreamEvents("error_event")
				// 二次开发：CC 入站的流内错误要用 CC 形态 + [DONE] 收尾，
				// 直接透传 Responses error 事件客户端读不懂。
				if chatBridge != nil {
					sseHeaders.ensure()
					chatBridge.WriteStreamError(c, errCodeRaw, errMsg)
					wroteDownstream = true
				} else {
					emitStreamMessage(message, true)
				}
			}
			if !reqStream {
				c.JSON(statusCode, gin.H{
					"error": gin.H{
						"type":    "upstream_error",
						"message": errMsg,
					},
				})
			}
			// 二次开发：把上游错误原文随 error 带给 handler，避免兜底文案覆盖。
			// 详见 openai_ws_client_error.go。
			return nil, wrapOpenAIWSClientVisibleError(errMsg, errCodeRaw, statusCode,
				fmt.Errorf("openai ws error event: %s", errMsg))
		}

		if reqStream {
			// 在首个 token 前先缓冲事件（如 response.created），
			// 以便上游早期断连时仍可安全回退到 HTTP，不给下游发送半截流。
			// 二次开发：判据由 firstTokenMs 换成 sawSemanticToken，见其声明处注释。
			shouldBuffer := !sawSemanticToken && !isTokenEvent && !isTerminalEvent
			if shouldBuffer {
				buffered := make([]byte, len(message))
				copy(buffered, message)
				bufferedStreamEvents = append(bufferedStreamEvents, buffered)
				bufferedEventCount++
				if debugEnabled && shouldLogOpenAIWSBufferedEvent(bufferedEventCount) {
					logOpenAIWSModeDebug(
						"buffer_enqueue account_id=%d conn_id=%s idx=%d event_idx=%d event_type=%s buffer_size=%d",
						account.ID,
						connID,
						bufferedEventCount,
						eventCount,
						truncateOpenAIWSLogValue(eventType, openAIWSLogValueMaxLen),
						len(bufferedStreamEvents),
					)
				}
			} else {
				flushBufferedStreamEvents(eventType)
				emitStreamMessage(message, isTerminalEvent)
			}
		} else {
			if responseField.Exists() && responseField.Type == gjson.JSON {
				finalResponse = []byte(responseField.Raw)
			}
		}

		if isTerminalEvent {
			if !clientDisconnected {
				markOpenAIWSClientVisibleFailure(c, eventType, message)
			}
			upstreamTerminalEvent = s.handleOpenAIWSTerminalTransientFailure(ctx, account, mappedModel, lease.HandshakeHeaders(), message)
			// A terminal event must be the final JSON document in its WS message.
			// Ignore any tail for the completed client turn, but never reuse the
			// ambiguous upstream connection for another request.
			cleanExit = len(pendingJSONDocuments) == 0
			break
		}
	}

	if !reqStream {
		if len(finalResponse) == 0 {
			logOpenAIWSModeInfo(
				"missing_final_response account_id=%d conn_id=%s events=%d token_events=%d terminal_events=%d wrote_downstream=%v",
				account.ID,
				connID,
				eventCount,
				tokenEventCount,
				terminalEventCount,
				wroteDownstream,
			)
			if !wroteDownstream {
				return nil, wrapOpenAIWSFallback("missing_final_response", errors.New("no terminal response payload"))
			}
			return nil, errors.New("ws finished without final response")
		}

		// 二次开发：上游 response.completed 的 output 可能为空（内容只在增量事件里），
		// 回填后再做后续改写，确保 model 替换与工具修正作用在完整载荷上。
		finalResponse = outputCollector.PatchFinalResponse(finalResponse)
		if needModelReplace {
			finalResponse = s.replaceModelInResponseBody(finalResponse, mappedModel, originalModel)
		}
		finalResponse = s.correctToolCallsInResponseBody(finalResponse)
		populateOpenAIUsageFromResponseJSON(finalResponse, usage)
		if responseID == "" {
			responseID = strings.TrimSpace(gjson.GetBytes(finalResponse, "id").String())
		}

		// 二次开发：CC 入站时把 Responses 终态响应转成 chat.completion 形态。
		if chatBridge != nil {
			c.Data(http.StatusOK, "application/json", chatBridge.TransformFinalResponse(finalResponse))
		} else {
			c.Data(http.StatusOK, "application/json", finalResponse)
		}
	} else {
		// 二次开发：CC 入站的收尾分两种。
		// apicompat.ChatCompletionsToResponses 恒定把上游置为 stream:true，
		// 所以 CC 的非流式客户端也会走到这个 reqStream=true 分支——此时不能只
		// 补 [DONE]，而要把攒下的终止 response 一次性转成 chat.completion JSON。
		if chatBridge != nil && !clientDisconnected {
			if chatBridge.WantsBufferedResponse() {
				c.Data(http.StatusOK, "application/json", chatBridge.TransformFinalResponse(nil))
			} else if tail := chatBridge.FinalizeStream(); len(tail) > 0 {
				sseHeaders.ensure()
				if _, wErr := c.Writer.Write(tail); wErr != nil {
					clientDisconnected = true
				}
			}
		}
		flushStreamWriter(true)
	}

	if responseID != "" && stateStore != nil {
		ttl := s.openAIWSResponseStickyTTL()
		logOpenAIWSBindResponseAccountWarn(groupID, account.ID, responseID, stateStore.BindResponseAccount(ctx, groupID, responseID, account.ID, ttl))
		stateStore.BindResponseConn(responseID, lease.ConnID(), ttl)
	}
	if stateStore != nil && storeDisabled && sessionHash != "" {
		stateStore.BindSessionConn(groupID, sessionHash, lease.ConnID(), s.openAIWSSessionStickyTTL())
	}
	firstTokenMsValue := -1
	if firstTokenMs != nil {
		firstTokenMsValue = *firstTokenMs
	}
	logOpenAIWSModeDebug(
		"completed account_id=%d conn_id=%s response_id=%s stream=%v duration_ms=%d events=%d token_events=%d terminal_events=%d buffered_events=%d buffered_flushed=%d first_event=%s last_event=%s first_token_ms=%d wrote_downstream=%v client_disconnected=%v",
		account.ID,
		connID,
		truncateOpenAIWSLogValue(strings.TrimSpace(responseID), openAIWSIDValueMaxLen),
		reqStream,
		time.Since(startTime).Milliseconds(),
		eventCount,
		tokenEventCount,
		terminalEventCount,
		bufferedEventCount,
		flushedBufferedEventCount,
		truncateOpenAIWSLogValue(firstEventType, openAIWSLogValueMaxLen),
		truncateOpenAIWSLogValue(lastEventType, openAIWSLogValueMaxLen),
		firstTokenMsValue,
		wroteDownstream,
		clientDisconnected,
	)

	return &OpenAIForwardResult{
		RequestID:                     responseID,
		Usage:                         *usage,
		Model:                         originalModel,
		UpstreamModel:                 mappedModel,
		UpstreamResponseModel:         responseModelObserver.Model(),
		UpstreamResponseModelConflict: responseModelObserver.Conflict(),
		UpstreamResponseServiceTier:   responseModelObserver.ServiceTier(),
		ImageCount:                    imageCounter.Count(),
		ImageOutputSizes:              imageCounter.Sizes(),
		ServiceTier:                   resolvedOpenAIUpstreamServiceTierFromObserver(responseModelObserver, extractOpenAIServiceTier(reqBody)),
		ReasoningEffort:               extractOpenAIReasoningEffort(reqBody, mappedModel, originalModel),
		RequestedReasoningEffort:      CanonicalRequestedReasoningEffortFromReqBody(reqBody, originalModel, mappedModel),
		Stream:                        reqStream,
		OpenAIWSMode:                  true,
		UpstreamTerminalEvent:         upstreamTerminalEvent,
		ResponseHeaders:               lease.HandshakeHeaders(),
		Duration:                      time.Since(startTime),
		FirstTokenMs:                  firstTokenMs,
	}, nil
}

// ProxyResponsesWebSocketFromClient 处理客户端入站 WebSocket（OpenAI Responses WS Mode）并转发到上游。
// 当前实现按“单请求 -> 终止事件 -> 下一请求”的顺序代理，适配 Codex CLI 的 turn 模式。
// stripCodexSparkImageGenerationToolFromRawPayload removes the image_generation
// tool from a raw /responses payload when the upstream model is gpt-5.3-codex-spark.
// Spark rejects that tool upstream with HTTP 400 (invalid_request_error, param=tools);
// Codex clients advertise it by default. Returns the (possibly unchanged) payload,
// whether it changed, and any JSON decode error.
func stripCodexSparkImageGenerationToolFromRawPayload(payload []byte, model string) ([]byte, bool, error) {
	if !isCodexSparkModel(model) {
		return payload, false, nil
	}
	return stripOpenAIImageGenerationToolsFromRawPayload(payload)
}
