package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// The downstream connection carries standard Responses events. The upstream
// transport follows BPS settings, with the same executor as HTTP ingress.
func (s *OpenAIGatewayService) proxyBPSWebSocket(ctx context.Context, c *gin.Context, conn *coderws.Conn, account *Account, first []byte, hooks *OpenAIWSIngressHooks, cfg *cachedOpenAIBPS) (returnErr error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	messages := make(chan openAIWSClientReadResult, 1)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			kind, payload, err := conn.Read(ctx)
			if err != nil {
				cancel()
				return
			}
			select {
			case messages <- openAIWSClientReadResult{messageType: kind, payload: payload}:
			case <-ctx.Done():
				return
			}
		}
	}()
	defer func() {
		var closeErr *OpenAIWSClientCloseError
		if errors.As(returnErr, &closeErr) {
			_ = conn.Close(closeErr.statusCode, closeErr.reason)
		}
		cancel()
		_ = conn.CloseNow()
		<-readerDone
	}()
	write := func(raw []byte) error {
		writeCtx, done := newOpenAIWSDownstreamWriteContext(ctx, hooks, 30*time.Second)
		defer done()
		return conn.Write(writeCtx, coderws.MessageText, raw)
	}
	payload := first
	previousModel := gjson.GetBytes(first, "model").String()
	if hooks != nil && hooks.InitialRequestModel != "" {
		previousModel = hooks.InitialRequestModel
	}
	for turn := 1; ; turn++ {
		started := time.Now()
		if turn == 1 && hooks != nil && !hooks.InitialTurnStartedAt.IsZero() {
			started = hooks.InitialTurnStartedAt
		}
		if !gjson.ValidBytes(payload) {
			return NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, "invalid JSON", nil)
		}
		kind := gjson.GetBytes(payload, "type").String()
		if kind != "" && kind != "response.create" {
			return NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, "BPS accepts response.create with full input history", nil)
		}
		model := gjson.GetBytes(payload, "model").String()
		if turn == 1 && hooks != nil && hooks.InitialRequestModel != "" {
			model = hooks.InitialRequestModel
		}
		if model == "" {
			model = previousModel
		}
		previousModel = model
		original := model
		if hooks != nil {
			if hooks.TurnStarted != nil {
				hooks.TurnStarted(turn, started)
			}
			if turn > 1 && hooks.BeforeRequest != nil {
				if err := hooks.BeforeRequest(turn, payload, original); err != nil {
					return err
				}
			}
			if hooks.MapRequestModel != nil {
				var err error
				model, err = hooks.MapRequestModel(turn, original)
				if err != nil {
					return err
				}
			}
		}
		current := s.settingService.bpsSettings(ctx)
		if !current.matches(account, getOpenAIGroupIDFromContext(c), model) {
			return NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, "BPS route changed; start a new session", nil)
		}
		cfg = current
		if hooks != nil {
			if hooks.BeforeTurn != nil {
				if err := hooks.BeforeTurn(turn); err != nil {
					return err
				}
			}
			if hooks.RequestStarted != nil {
				hooks.RequestStarted(turn)
			}
		}
		var err error
		payload, err = sjson.SetBytes(payload, "model", model)
		if err != nil {
			return err
		}
		payload, err = applyOpenAIWSReasoningEffortPolicy(payload, hooks)
		if err != nil {
			return err
		}
		restriction := s.detectCodexClientRestriction(c, account, payload)
		if restriction.Enabled && !restriction.Matched {
			return NewOpenAIWSClientCloseError(coderws.StatusPolicyViolation, CodexClientRestrictionMessage(restriction), nil)
		}
		beginUpstreamResponseModelObservation(c)
		resetOpenAIStateUsage(c)
		resetDownstreamModelObservation(c)
		SetActualOpenAIUpstreamEndpoint(c, openAIBPSEndpoint)
		token, _, err := s.GetAccessToken(ctx, account)
		timing := &bpsFirstToken{start: started, mode: s.openAIStreamTTFTMode(ctx, account)}
		var result *OpenAIForwardResult
		if err == nil {
			resp, openErr := openBPSResponse(ctx, cfg.settings, account, token, payload, true, fmt.Sprintf("key:%d", getAPIKeyIDFromContext(c)), s.httpUpstream, false, timing.options())
			err = openErr
			if err == nil {
				upstream, _ := cfg.settings.Config.UpstreamModelFor(model)
				result, err = s.deliverBPSResponse(c, account, resp, original, upstream, true, started, write)
				_ = resp.Body.Close()
			} else {
				s.recordBPSError(c, account, err)
				status, _ := bpsError(err)
				_ = write(buildOpenAIWSHTTPBridgeErrorEvent(status, err.Error()))
			}
		} else {
			s.recordBPSError(c, account, err)
			status, _ := bpsError(err)
			_ = write(buildOpenAIWSHTTPBridgeErrorEvent(status, err.Error()))
		}
		if result != nil {
			result.FirstTokenMs = timing.milliseconds()
			// Preserve per-turn billing identity even when the BPS upstream uses HTTP.
			result.OpenAIWSMode = true
			result.StateInjected = false
		}
		if hooks != nil && hooks.AfterTurn != nil {
			hooks.AfterTurn(turn, result, err)
		}
		if err != nil {
			return errors.New("BPS websocket turn: " + err.Error())
		}
		idle := s.openAIWSIngressInterTurnIdleTimeout()
		var timer *time.Timer
		var idleC <-chan time.Time
		if idle > 0 {
			timer = time.NewTimer(idle)
			idleC = timer.C
		}
		select {
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return nil
		case <-idleC:
			return NewOpenAIWSClientCloseError(coderws.StatusNormalClosure, "idle timeout", nil)
		case next := <-messages:
			if timer != nil {
				timer.Stop()
			}
			if next.messageType != coderws.MessageText {
				return NewOpenAIWSClientCloseError(coderws.StatusUnsupportedData, "expected JSON text", nil)
			}
			payload = next.payload
		}
	}
}
