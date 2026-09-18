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
	failure := func(message string) openAIStateProbeResult {
		return openAIStateProbeResult{result: "failed", message: message}
	}
	account, err := s.accounts.GetByID(ctx, id)
	if err != nil || !stateKeeperAccountEligible(account) {
		return failure("账号不可用，请检查 OAuth 凭据和账号状态")
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
	probeAccount.Concurrency = 1
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
	out := openAIStateProbeResult{status: response.StatusCode, result: "not_observed", hasCodexTurnState: response.Header.Get(openAICodexTurnStateHeader) != ""}
	value := strings.TrimSpace(response.Header.Get(openAICollectedStateHeader))
	if response.StatusCode == 292 && validCollectedState(value) {
		out.result = "collected"
		out.value = value
		out.message = "已取得 HTTP 292 与 current_turn_state；缓存时长按配置计算"
		return out
	}
	if response.StatusCode >= 400 {
		out.result = "upstream_error"
		out.message = fmt.Sprintf("采集上游返回 HTTP %d，未取得可注入状态", response.StatusCode)
		body, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		code := gjson.GetBytes(body, "error.code").String()
		if code == "" {
			code = gjson.GetBytes(body, "error.type").String()
		}
		// Only classify known errors. Upstream error text can contain credentials.
		switch code {
		case "billing_not_active":
			out.message = fmt.Sprintf("Chat Completions 返回 HTTP %d：billing_not_active（API 计费未启用），未取得 292 State", response.StatusCode)
		case "insufficient_quota":
			out.message = fmt.Sprintf("Chat Completions 返回 HTTP %d：insufficient_quota（API 额度不足），未取得 292 State", response.StatusCode)
		case "invalid_api_key":
			out.message = fmt.Sprintf("Chat Completions 返回 HTTP %d：invalid_api_key（当前凭据不被采集接口接受），未取得 292 State", response.StatusCode)
		case "model_not_found":
			out.message = fmt.Sprintf("Chat Completions 返回 HTTP %d：model_not_found（当前凭据无法使用所选模型），未取得 292 State", response.StatusCode)
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
		out.message = "上游在响应流中返回错误，未取得可注入状态"
	case scanner.Err() != nil:
		out.result = "failed"
		out.message = "采集响应读取中断或超出限制，未取得可注入状态"
	case out.hasCodexTurnState:
		out.message = "仅发现普通 Codex 回合状态，未取得文章所述的 292 状态，未用于自动注入"
	default:
		out.message = fmt.Sprintf("返回 HTTP %d，未同时取得 292 与 current_turn_state 响应头", response.StatusCode)
	}
	return out
}
