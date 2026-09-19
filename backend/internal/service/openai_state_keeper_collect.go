package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// Use Qerkai's Chat Completions compatibility entry point in-process. Keeping
// the chosen account explicit avoids the public API's account scheduling and
// billing; the normal converter and OAuth request builder still do the work.
const openAIStateKeeperCollectionPath = "/v1/chat/completions"
const openAIStateKeeperCollectionURL = chatgptCodexURL

type openAIStateProbeWriter struct{ header http.Header }

func (w *openAIStateProbeWriter) Header() http.Header         { return w.header }
func (w *openAIStateProbeWriter) WriteHeader(int)             {}
func (w *openAIStateProbeWriter) Write(b []byte) (int, error) { return len(b), nil }
func (w *openAIStateProbeWriter) Flush()                      {}

func (s *OpenAIStateKeeperService) collect(ctx context.Context, q OpenAIStateKeeperSettings, id int64) openAIStateProbeResult {
	ctx = WithHTTPUpstreamProfile(ctx, HTTPUpstreamProfileOpenAIStateCollection)
	failure := func(message string) openAIStateProbeResult {
		return openAIStateProbeResult{result: "failed", message: message, proxyFailure: true}
	}
	account, err := s.accounts.GetByID(ctx, id)
	if err != nil || account == nil {
		return failure("账号不可用，请检查 OAuth 凭据和账号状态")
	}
	s.syncAccountAvailability([]*Account{account})
	s.mu.RLock()
	entry := s.entryLocked(id, q.Model)
	unavailable := entry == nil || entry.scopeLoading || entry.row.AccountUnavailable || !s.config.Load().includesCollectionAccount(account)
	s.mu.RUnlock()
	if unavailable || ctx.Err() != nil {
		return failure("账号不可用，已停止采集")
	}
	proxy, err := s.proxies.GetByID(ctx, q.ProxyID)
	if err != nil || proxy == nil || !proxy.IsActive() || proxy.IsExpired(time.Now()) {
		return failure("采集代理不可用；未回退到直连")
	}
	body, _ := json.Marshal(map[string]any{"model": q.Model, "messages": []any{map[string]any{"role": "user", "content": "hi"}}})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, openAIStateKeeperCollectionPath, bytes.NewReader(body))
	if err != nil {
		return failure("无法构造采集请求，请检查账号认证信息")
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	applyOpenAICodexProbeHeaders(request.Header)
	c, _ := gin.CreateTestContext(&openAIStateProbeWriter{header: make(http.Header)})
	c.Request = request
	c.Set(openAIStateProbeContextKey, true)
	// Override the proxy only on this probe's account copy. Never change the
	// stored account or let a public scheduler switch to another credential.
	probeAccount := *account
	probeAccount.ProxyID = &proxy.ID
	probeAccount.Proxy = proxy
	probeAccount.Concurrency = max(1, min(q.AccountConcurrency, q.Concurrency, q.MaxAttempts))
	var out openAIStateProbeResult
	_, err = s.gateway.forwardAsChatCompletions(ctx, c, &probeAccount, body, "", "", false, &out)
	if err != nil {
		if ctx.Err() != nil {
			return failure("采集已取消或超过 45 秒超时")
		}
		return failure("本地 Chat Completions 采集失败，请检查账号权限、认证及代理连接")
	}
	if out.result == "" {
		return failure("本地 Chat Completions 未取得原始上游响应，未保存状态")
	}
	out.credentialStamp = stateKeeperCredentialStamp(account)
	return out
}

func parseOpenAIStateProbeResponse(response *http.Response) openAIStateProbeResult {
	value := response.Header.Get(openAICodexTurnStateHeader)
	out := openAIStateProbeResult{status: response.StatusCode, result: "not_observed", hasCodexTurnState: value != "", turnStateLength: len(value)}
	if response.StatusCode == http.StatusTooManyRequests {
		out.retryAfter = keeperRetryAfter(response.Header.Get("Retry-After"), time.Now())
	}
	out.proxyFailure = response.StatusCode == http.StatusProxyAuthRequired || response.StatusCode == http.StatusForbidden || response.StatusCode >= 500
	// Header acquisition completes independently of the model's response body.
	if response.StatusCode == http.StatusOK && validCollectedState(value) {
		out.result = "collected"
		out.value = value
		out.message = "已取得 HTTP 200 与 x-codex-turn-state 响应头"
		return out
	}
	if response.StatusCode >= 400 {
		out.result = "upstream_error"
		out.message = fmt.Sprintf("采集上游返回 HTTP %d，未保存状态", response.StatusCode)
		body, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		code := gjson.GetBytes(body, "error.code").String()
		if code == "" {
			code = gjson.GetBytes(body, "error.type").String()
		}
		out.accountUnavailable = response.StatusCode == http.StatusUnauthorized
		switch code {
		case "account_deactivated", "account_disabled", "account_suspended", "user_deactivated", "token_revoked", "invalid_api_key", "invalid_token", "token_expired":
			out.accountUnavailable = true
		}
		// Only classify known errors. Upstream error text can contain credentials.
		switch code {
		case "billing_not_active":
			out.permanentFailure = true
			out.message = fmt.Sprintf("Chat Completions 返回 HTTP %d：billing_not_active（API 计费未启用），未保存状态", response.StatusCode)
		case "insufficient_quota":
			out.permanentFailure = true
			out.message = fmt.Sprintf("Chat Completions 返回 HTTP %d：insufficient_quota（API 额度不足），未保存状态", response.StatusCode)
		case "invalid_api_key":
			out.message = fmt.Sprintf("Chat Completions 返回 HTTP %d：invalid_api_key（当前凭据不被采集接口接受），未保存状态", response.StatusCode)
		case "model_not_found":
			out.permanentFailure = true
			out.message = fmt.Sprintf("Chat Completions 返回 HTTP %d：model_not_found（当前凭据无法使用所选模型），未保存状态", response.StatusCode)
		case "unsupported_model", "model_not_available", "invalid_model", "permission_denied":
			out.permanentFailure = true
			out.message = fmt.Sprintf("采集上游返回 HTTP %d：%s，请检查模型及权限", response.StatusCode, code)
		}
		return out
	}
	// Bound both read size and lifetime. Never persist response bodies, tokens,
	// or proxy credentials in the status API/log; only report classified results.
	scanner := bufio.NewScanner(io.LimitReader(response.Body, 64<<10))
	scanner.Buffer(make([]byte, 4096), 64<<10)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if bytes.HasPrefix(line, []byte("data:")) {
			line = bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
		}
		if gjson.ValidBytes(line) {
			headers := gjson.GetBytes(line, "headers")
			headers.ForEach(func(k, v gjson.Result) bool {
				if strings.EqualFold(k.String(), openAICodexTurnStateHeader) {
					out.hasCodexTurnState = true
				}
				return true
			})
			kind := gjson.GetBytes(line, "type").String()
			if kind == "error" || kind == "response.failed" {
				out.result = "upstream_error"
			}
			if kind == "response.completed" || kind == "response.done" || kind == "response.failed" {
				break
			}
		}
	}
	switch {
	case out.result == "upstream_error":
		out.message = "上游在响应流中返回错误，未取得有效 Turn-State 响应头"
	case scanner.Err() != nil:
		out.result = "failed"
		out.message = "采集响应读取中断或超出限制，未取得有效 Turn-State 响应头"
	case out.hasCodexTurnState:
		out.message = "未同时取得 HTTP 200 与有效 x-codex-turn-state 响应头，未保存状态"
	default:
		out.message = fmt.Sprintf("返回 HTTP %d，未取得有效 x-codex-turn-state 响应头", response.StatusCode)
	}
	return out
}
