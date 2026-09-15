//go:build unit

package handler

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

const wsBusinessProcessingMessage = "An error occurred while processing your request. You can retry your request, or contact us through our help center at help.openai.com if the error persists. Please include the request ID request-test in your message."
const wsBusinessOverloadMessage = "Our servers are currently overloaded. Please try again later."

type wsBusinessAttempt struct{ account, body, turnState string }
type wsBusinessObservation struct {
	count, intendedStatus, upstreamErrors int
	requestError                          bool
}
type wsBusinessUsageRepo struct {
	service.UsageLogRepository
	mu   sync.Mutex
	logs []*service.UsageLog
}

func (r *wsBusinessUsageRepo) Create(_ context.Context, value *service.UsageLog) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logs = append(r.logs, value)
	return true, nil
}

type wsBusinessFixture struct {
	t                 *testing.T
	server            *httptest.Server
	mu                sync.Mutex
	attempts          map[string][]wsBusinessAttempt
	observations      map[string]wsBusinessObservation
	usage             *wsBusinessUsageRepo
	firstFrameSeen    chan struct{}
	releaseOutput     chan struct{}
	outputStarted     chan struct{}
	active            atomic.Int64
	peak              atomic.Int64
	handshakes        atomic.Int64
	handshakeFailures int64
	sameAccount       int
	failureSent       chan struct{}
}

func newWSBusinessFixture(t *testing.T, retry service.OpenAIUpstream5xxRetryConfig, accountCount int, handshakeFailures int64) *wsBusinessFixture {
	t.Helper()
	gin.SetMode(gin.TestMode)
	f := &wsBusinessFixture{t: t, attempts: make(map[string][]wsBusinessAttempt), observations: make(map[string]wsBusinessObservation), usage: &wsBusinessUsageRepo{}, handshakeFailures: handshakeFailures, sameAccount: retry.SameAccount}
	upstream := httptest.NewServer(http.HandlerFunc(f.serveWS))
	t.Cleanup(upstream.Close)
	accounts := make([]service.Account, accountCount)
	for i := range accounts {
		accounts[i] = service.Account{ID: int64(8100 + i), Name: fmt.Sprintf("simulated-oauth-%d", i), Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth,
			Status: service.StatusActive, Schedulable: true, Priority: i, Credentials: map[string]any{"access_token": fmt.Sprintf("fake-local-token-%d", i)}}
	}
	h := newOpenAIResponsesFailoverTestHandler(t, nil, accounts...)
	t.Cleanup(service.ConfigureOpenAIWSBusinessRetryForTest(h.gatewayService, "ws"+strings.TrimPrefix(upstream.URL, "http"), retry, f.usage))
	router := gin.New()
	router.Use(gin.Recovery())
	router.POST("/v1/responses", func(c *gin.Context) {
		active := f.active.Add(1)
		defer f.active.Add(-1)
		for old := f.peak.Load(); active > old; old = f.peak.Load() {
			if f.peak.CompareAndSwap(old, active) {
				break
			}
		}
		groupID := int64(3131)
		c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{ID: 99, GroupID: &groupID, Group: &service.Group{ID: groupID, Platform: service.PlatformOpenAI, RateMultiplier: 1}, User: &service.User{ID: 100}})
		c.Set(string(middleware2.ContextKeyUser), middleware2.AuthSubject{UserID: 100})
		originalWriter := c.Writer
		capture := acquireOpsCaptureWriter(originalWriter)
		capture.setContext(c)
		c.Writer = capture
		defer func() { c.Writer = originalWriter; releaseOpsCaptureWriter(capture) }()
		h.Responses(c)
		capture.finalizeCapture()
		service.FlushOpenAIUpstream5xxRetryTracker(c, c.Writer.Status())
		obs := wsBusinessObservation{count: service.OpenAIUpstream5xxRetryCount(c)}
		parsed := parseOpsErrorResponse(capture.capturedBytes())
		if terminal, ok := capture.capturedTerminalError(); ok && !parsed.StreamFailure {
			parsed = terminal
		}
		obs.requestError = c.Writer.Status() >= 400 || parsed.StreamFailure || len(service.GetOpsStreamErrors(c)) > 0
		if streamErr, ok := service.GetOpsStreamError(c); ok {
			obs.intendedStatus = streamErr.IntendedStatus
		}
		if events, ok := c.Get(service.OpsUpstreamErrorsKey); ok {
			if values, ok := events.([]*service.OpsUpstreamErrorEvent); ok {
				obs.upstreamErrors = len(values)
			}
		}
		f.mu.Lock()
		f.observations[c.GetHeader("X-Test-Case")] = obs
		f.mu.Unlock()
	})
	f.server = httptest.NewServer(router)
	t.Cleanup(f.server.Close)
	return f
}

func (f *wsBusinessFixture) serveWS(w http.ResponseWriter, req *http.Request) {
	if f.handshakes.Add(1) <= f.handshakeFailures {
		http.Error(w, "bad gateway", 502)
		return
	}
	w.Header().Set("X-Codex-Turn-State", "handshake_"+strings.TrimPrefix(req.Header.Get("Authorization"), "Bearer "))
	conn, err := websocket.Accept(w, req, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	defer conn.CloseNow()
	ctx := req.Context()
	write := func(raw string) bool { return conn.Write(ctx, websocket.MessageText, []byte(raw)) == nil }
	for {
		_, raw, err := conn.Read(ctx)
		if err != nil {
			return
		}
		key := gjson.GetBytes(raw, "input.0.content.0.text").String()
		if key == "" {
			f.t.Errorf("missing input text in mock WS request: %s", raw)
			return
		}
		f.mu.Lock()
		f.attempts[key] = append(f.attempts[key], wsBusinessAttempt{account: req.Header.Get("Authorization"), body: string(raw), turnState: req.Header.Get("X-Codex-Turn-State")})
		attempt := len(f.attempts[key])
		f.mu.Unlock()
		id := fmt.Sprintf("resp_%s_%d", strings.ReplaceAll(key, "/", "_"), attempt)
		if !write(`{"type":"codex.rate_limits","rate_limits":{"allowed":true,"limit_reached":false}}`) {
			return
		}
		if strings.HasPrefix(key, "firstframe/") && attempt == 1 {
			select {
			case <-f.firstFrameSeen:
			case <-time.After(5 * time.Second):
				f.t.Error("CPA first notification was buffered")
				return
			case <-ctx.Done():
				return
			}
		}
		if !write(fmt.Sprintf(`{"type":"codex.response.metadata","headers":{"x-codex-turn-state":"state_%s"}}`, id)) {
			return
		}
		if !write(fmt.Sprintf(`{"type":"response.created","sequence_number":0,"response":{"id":%q,"object":"response","status":"in_progress","output":[],"error":null}}`, id)) {
			return
		}
		if !write(fmt.Sprintf(`{"type":"response.in_progress","sequence_number":1,"response":{"id":%q,"status":"in_progress","output":[],"error":null}}`, id)) {
			return
		}
		mode := strings.Split(key, "/")[0]
		failures := 4 // configured three same-account retries, then switch
		switch mode {
		case "success", "handshake", "late", "tool", "reasoning", "unknown", "unrelated":
			failures = 0
		case "one", "one503", "firstframe", "cancel", "slow", "lateafter":
			failures = 1
		case "exhaust":
			failures = 100
		}
		late := mode == "late" || mode == "tool" || mode == "reasoning" || mode == "unknown" || mode == "lateafter"
		if attempt > failures {
			boundary := fmt.Sprintf(`{"type":"response.output_text.delta","sequence_number":2,"response_id":%q,"delta":%q}`, id, "answer:"+key)
			switch mode {
			case "tool":
				boundary = `{"type":"response.output_item.added","sequence_number":2,"item":{"id":"fc_1","type":"function_call","call_id":"call_1","name":"test_tool","arguments":""}}`
			case "reasoning":
				boundary = `{"type":"response.reasoning_summary_text.delta","sequence_number":2,"delta":"thinking"}`
			case "unknown":
				boundary = `{"type":"provider.new_output","sequence_number":2,"content":"visible"}`
			case "unrelated":
				boundary = ""
			}
			if boundary != "" && !write(boundary) {
				return
			}
			if mode == "slow" {
				close(f.outputStarted)
				select {
				case <-f.releaseOutput:
				case <-ctx.Done():
					return
				}
			}
		}
		if attempt <= failures || late || mode == "unrelated" {
			status, message := 502, wsBusinessProcessingMessage
			if attempt%2 == 0 || mode == "one503" {
				status, message = 503, wsBusinessOverloadMessage
			}
			if mode == "unrelated" {
				status, message = 503, "different upstream server failure"
			}
			// Exercise both WS envelopes without relying on an HTTP error status.
			failure := fmt.Sprintf(`{"type":"error","sequence_number":3,"error":{"type":"server_error","code":"server_error","status_code":%d,"message":%q}}`, status, message)
			if attempt%2 == 0 || mode == "unrelated" || mode == "one503" {
				failure = fmt.Sprintf(`{"type":"response.failed","sequence_number":3,"response":{"id":%q,"status":"failed","output":[],"error":{"type":"server_error","code":"server_error","status_code":%d,"message":%q}}}`, id, status, message)
			}
			if !write(failure) {
				return
			}
			if mode == "cancel" && attempt == 1 {
				close(f.failureSent)
			}
			continue
		}
		if !write(fmt.Sprintf(`{"type":"response.completed","sequence_number":3,"response":{"id":%q,"object":"response","model":"gpt-5.1","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":%q}]}],"usage":{"input_tokens":3,"output_tokens":2}}}`, id, "answer:"+key)) {
			return
		}
	}
}

func (f *wsBusinessFixture) request(ctx context.Context, key string, extraHeaders ...http.Header) (*http.Response, error) {
	body, _ := json.Marshal(map[string]any{"model": "gpt-5.1", "stream": true, "input": []any{map[string]any{"role": "user", "content": []any{map[string]any{"type": "input_text", "text": key}}}}, "instructions": "Keep input unchanged.", "reasoning": map[string]any{"effort": "low"}, "tools": []any{map[string]any{"type": "function", "name": "test_tool", "parameters": map[string]any{"type": "object", "properties": map[string]any{}}}}, "tool_choice": "auto"})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, f.server.URL+"/v1/responses", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "codex_cli_rs/0.114.0")
	req.Header.Set("session_id", key)
	req.Header.Set("X-Test-Case", key)
	for _, headers := range extraHeaders {
		for name, values := range headers {
			req.Header[name] = values
		}
	}
	return f.server.Client().Do(req)
}

func readWSBusinessSSE(resp *http.Response) ([]gjson.Result, error) {
	defer resp.Body.Close()
	scanner := bufio.NewScanner(resp.Body)
	var events []gjson.Result
	for scanner.Scan() {
		if line := scanner.Text(); strings.HasPrefix(line, "data: ") {
			events = append(events, gjson.Parse(strings.TrimPrefix(line, "data: ")))
		}
	}
	return events, scanner.Err()
}

func (f *wsBusinessFixture) verify(t *testing.T, key string, events []gjson.Result, retries int, success bool) {
	t.Helper()
	f.mu.Lock()
	attempts := append([]wsBusinessAttempt(nil), f.attempts[key]...)
	obs := f.observations[key]
	f.mu.Unlock()
	require.Len(t, attempts, retries+1, key)
	require.Equal(t, retries, obs.count, key)
	require.NotEmpty(t, events, key)
	created, delta, terminal := 0, 0, 0
	responseID := ""
	lastSeq := int64(-1)
	for _, event := range events {
		switch event.Get("type").String() {
		case "response.created":
			created++
			responseID = event.Get("response.id").String()
		case "response.output_text.delta":
			delta++
			require.Equal(t, "answer:"+key, event.Get("delta").String())
		case "response.completed", "response.failed", "error":
			terminal++
		}
		if seq := event.Get("sequence_number"); seq.Exists() {
			require.Greater(t, seq.Int(), lastSeq, key)
			lastSeq = seq.Int()
		}
		if id := event.Get("response.id").String(); id != "" {
			require.Equal(t, responseID, id, key)
		}
		if id := event.Get("response_id").String(); id != "" {
			require.Equal(t, responseID, id, key)
		}
	}
	require.Equal(t, 1, created, key)
	require.Equal(t, 1, terminal, key)
	if success {
		require.Equal(t, "response.completed", events[len(events)-1].Get("type").String(), key)
		require.Equal(t, 1, delta, key)
		require.Zero(t, obs.intendedStatus, key)
		require.False(t, obs.requestError, "recovered requests must not enter request errors")
	} else {
		require.NotEqual(t, "response.completed", events[len(events)-1].Get("type").String(), key)
		require.True(t, obs.requestError, "visible failures must enter request errors")
	}
	for i, attempt := range attempts {
		for _, field := range []string{"model", "input", "instructions", "tools", "reasoning", "tool_choice", "parallel_tool_calls", "include"} {
			require.JSONEq(t, fmt.Sprintf(`{"value":%s}`, jsonValue(gjson.Get(attempts[0].body, field))), fmt.Sprintf(`{"value":%s}`, jsonValue(gjson.Get(attempt.body, field))), fmt.Sprintf("%s attempt=%d field=%s", key, i, field))
		}
		if i > 0 && i <= f.sameAccount {
			require.Equal(t, attempts[0].account, attempt.account, key)
		}
		if i == f.sameAccount+1 {
			require.NotEqual(t, attempts[0].account, attempt.account, key)
			require.Empty(t, attempt.turnState, "switching accounts must not forward the prior account's turn-state")
		}
	}
}

func TestOpenAIWSBusinessRetryCancelAndNoAvailableAccount(t *testing.T) {
	t.Run("cancel_during_delay", func(t *testing.T) {
		service.ClearOpenAIUpstream5xxRetryLog()
		t.Cleanup(service.ClearOpenAIUpstream5xxRetryLog)
		f := newWSBusinessFixture(t, service.OpenAIUpstream5xxRetryConfig{Enabled: true, SameAccount: 3, Total: 5, Delay: 500 * time.Millisecond}, 3, 0)
		f.failureSent = make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		resp, err := f.request(ctx, "cancel/1")
		require.NoError(t, err)
		select {
		case <-f.failureSent:
		case <-time.After(5 * time.Second):
			t.Fatal("upstream did not send failure")
		}
		cancel()
		resp.Body.Close()
		require.Eventually(t, func() bool { f.mu.Lock(); defer f.mu.Unlock(); _, ok := f.observations["cancel/1"]; return ok }, 5*time.Second, 10*time.Millisecond)
		f.mu.Lock()
		defer f.mu.Unlock()
		require.Len(t, f.attempts["cancel/1"], 1)
		require.Zero(t, f.observations["cancel/1"].count)
		require.Zero(t, service.GetOpenAIUpstream5xxRetryLog(service.OpenAIUpstream5xxRetryLogFilter{}).Stats.Intercepted)
	})
	t.Run("no_available_account", func(t *testing.T) {
		service.ClearOpenAIUpstream5xxRetryLog()
		t.Cleanup(service.ClearOpenAIUpstream5xxRetryLog)
		f := newWSBusinessFixture(t, service.OpenAIUpstream5xxRetryConfig{Enabled: true, SameAccount: 1, Total: 5}, 1, 0)
		resp, err := f.request(context.Background(), "exhaust/only")
		require.NoError(t, err)
		events, err := readWSBusinessSSE(resp)
		require.NoError(t, err)
		f.verify(t, "exhaust/only", events, 1, false)
		page := service.GetOpenAIUpstream5xxRetryLog(service.OpenAIUpstream5xxRetryLogFilter{Event: service.OpenAIUpstream5xxRetryEventExhausted})
		require.Len(t, page.Entries, 1)
		require.Equal(t, "no_available_account", page.Entries[0].StopReason)
		require.Equal(t, 503, page.Entries[0].StatusCode)
	})
	t.Run("zero_same_account", func(t *testing.T) {
		f := newWSBusinessFixture(t, service.OpenAIUpstream5xxRetryConfig{Enabled: true, SameAccount: 0, Total: 2}, 2, 0)
		resp, err := f.request(context.Background(), "one503/switch")
		require.NoError(t, err)
		events, err := readWSBusinessSSE(resp)
		require.NoError(t, err)
		f.verify(t, "one503/switch", events, 1, true)
		// The next turn may echo either the initial HTTP header or the final
		// metadata blob. The issuing account must be checked for each value.
		var finalState string
		for _, event := range events {
			if event.Get("type").String() == "codex.response.metadata" {
				finalState = event.Get("headers.x-codex-turn-state").String()
			}
		}
		require.NotEmpty(t, finalState)
		resp, err = f.request(context.Background(), "success/next", http.Header{"Session_id": []string{"one503/switch"}, "X-Codex-Turn-State": []string{finalState}})
		require.NoError(t, err)
		events, err = readWSBusinessSSE(resp)
		require.NoError(t, err)
		f.verify(t, "success/next", events, 0, true)
		f.mu.Lock()
		defer f.mu.Unlock()
		require.Empty(t, f.attempts["success/next"][0].turnState)
	})
	t.Run("time_budget", func(t *testing.T) {
		t.Setenv("SUB2API_OPENAI_CAPACITY_SHED_BUDGET_SECONDS", "1")
		service.ClearOpenAIUpstream5xxRetryLog()
		t.Cleanup(service.ClearOpenAIUpstream5xxRetryLog)
		f := newWSBusinessFixture(t, service.OpenAIUpstream5xxRetryConfig{Enabled: true, SameAccount: 3, Total: 5, Delay: 600 * time.Millisecond}, 2, 0)
		resp, err := f.request(context.Background(), "exhaust/time")
		require.NoError(t, err)
		events, err := readWSBusinessSSE(resp)
		require.NoError(t, err)
		f.verify(t, "exhaust/time", events, 2, false)
		page := service.GetOpenAIUpstream5xxRetryLog(service.OpenAIUpstream5xxRetryLogFilter{Event: service.OpenAIUpstream5xxRetryEventExhausted})
		require.Len(t, page.Entries, 1)
		require.Equal(t, "retry_time_exhausted", page.Entries[0].StopReason)
	})
}

func jsonValue(value gjson.Result) string {
	if !value.Exists() {
		return "null"
	}
	return value.Raw
}

func TestOpenAIWSBusinessRetryHTTPIntegration(t *testing.T) {
	service.ClearOpenAIUpstream5xxRetryLog()
	t.Cleanup(service.ClearOpenAIUpstream5xxRetryLog)
	f := newWSBusinessFixture(t, service.OpenAIUpstream5xxRetryConfig{Enabled: true, SameAccount: 3, Total: 5, Delay: time.Millisecond}, 5, 0)
	for _, tc := range []struct {
		key     string
		retries int
		success bool
	}{{"recover/1", 4, true}, {"exhaust/1", 5, false}, {"success/1", 0, true}, {"late/1", 0, false}, {"tool/1", 0, false}, {"reasoning/1", 0, false}, {"unknown/1", 0, false}, {"unrelated/1", 0, false}, {"lateafter/1", 1, false}} {
		t.Run(tc.key, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			resp, err := f.request(ctx, tc.key)
			require.NoError(t, err)
			events, err := readWSBusinessSSE(resp)
			require.NoError(t, err)
			f.verify(t, tc.key, events, tc.retries, tc.success)
		})
	}
	f.usage.mu.Lock()
	defer f.usage.mu.Unlock()
	found := false
	for _, entry := range f.usage.logs {
		if strings.Contains(entry.RequestID, "recover_1") {
			found = true
			require.NotNil(t, entry.OpenAIUpstream5xxRetryCount)
			require.Equal(t, 4, *entry.OpenAIUpstream5xxRetryCount)
		}
	}
	require.True(t, found, "recovered response must persist its retry count into the usage repository")
	page := service.GetOpenAIUpstream5xxRetryLog(service.OpenAIUpstream5xxRetryLogFilter{})
	require.Equal(t, 10, page.Stats.Intercepted)
	require.Equal(t, 1, page.Stats.Succeeded)
	require.Equal(t, 2, page.Stats.Exhausted)
}

func TestOpenAIWSBusinessRetryCPAFirstFrameAndLatency(t *testing.T) {
	service.ClearOpenAIUpstream5xxRetryLog()
	t.Cleanup(service.ClearOpenAIUpstream5xxRetryLog)
	f := newWSBusinessFixture(t, service.OpenAIUpstream5xxRetryConfig{Enabled: true, SameAccount: 3, Total: 5, Delay: time.Millisecond}, 3, 0)
	f.firstFrameSeen = make(chan struct{})
	resp, err := f.request(context.Background(), "firstframe/1")
	require.NoError(t, err)
	reader := bufio.NewReader(resp.Body)
	line, err := reader.ReadString('\n')
	require.NoError(t, err)
	require.Contains(t, line, `"type":"codex.rate_limits"`)
	close(f.firstFrameSeen)
	_, err = io.ReadAll(reader)
	require.NoError(t, err)
	resp.Body.Close()
	f.outputStarted = make(chan struct{})
	f.releaseOutput = make(chan struct{})
	resp, err = f.request(context.Background(), "slow/1")
	require.NoError(t, err)
	<-f.outputStarted
	time.Sleep(250 * time.Millisecond)
	close(f.releaseOutput)
	events, err := readWSBusinessSSE(resp)
	require.NoError(t, err)
	f.verify(t, "slow/1", events, 1, true)
	page := service.GetOpenAIUpstream5xxRetryLog(service.OpenAIUpstream5xxRetryLogFilter{Event: service.OpenAIUpstream5xxRetryEventSucceeded})
	require.Len(t, page.Entries, 2)
	require.Less(t, page.Entries[0].ExtraLatencyMS, int64(200), "answer generation after recovery must not inflate retry latency")
}

func TestOpenAIWSBusinessRetryOffAndHandshake(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			service.ClearOpenAIUpstream5xxRetryLog()
			t.Cleanup(service.ClearOpenAIUpstream5xxRetryLog)
			f := newWSBusinessFixture(t, service.OpenAIUpstream5xxRetryConfig{Enabled: enabled, SameAccount: 3, Total: 5, Delay: time.Millisecond}, 3, 2)
			resp, err := f.request(context.Background(), "handshake/1")
			require.NoError(t, err)
			events, err := readWSBusinessSSE(resp)
			require.NoError(t, err)
			f.verify(t, "handshake/1", events, 0, true)
			require.GreaterOrEqual(t, f.handshakes.Load(), int64(3))
			if !enabled {
				resp, err = f.request(context.Background(), "one/off")
				require.NoError(t, err)
				events, err = readWSBusinessSSE(resp)
				require.NoError(t, err)
				require.NotEmpty(t, events)
				require.NotEqual(t, "response.completed", events[len(events)-1].Get("type").String())
				f.mu.Lock()
				require.Len(t, f.attempts["one/off"], 1)
				require.Zero(t, f.observations["one/off"].count)
				f.mu.Unlock()
			}
			require.Zero(t, service.GetOpenAIUpstream5xxRetryLog(service.OpenAIUpstream5xxRetryLogFilter{}).Stats.Intercepted)
		})
	}
}

func TestOpenAIWSBusinessRetryConcurrentSimulation(t *testing.T) {
	if testing.Short() {
		t.Skip("real HTTP/SSE and WS concurrency simulation")
	}
	for _, concurrency := range []int{50, 200} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
			service.ClearOpenAIUpstream5xxRetryLog()
			t.Cleanup(service.ClearOpenAIUpstream5xxRetryLog)
			f := newWSBusinessFixture(t, service.OpenAIUpstream5xxRetryConfig{Enabled: true, SameAccount: 3, Total: 5, Delay: time.Millisecond}, 5, 0)
			const total = 500
			jobs := make(chan int)
			var wg sync.WaitGroup
			started := time.Now()
			for worker := 0; worker < concurrency; worker++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for n := range jobs {
						key := fmt.Sprintf("load/%04d", n)
						ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
						resp, err := f.request(ctx, key)
						if err != nil {
							t.Errorf("%s: %v", key, err)
							cancel()
							continue
						}
						events, err := readWSBusinessSSE(resp)
						cancel()
						if err != nil {
							t.Errorf("%s: %v", key, err)
							continue
						}
						f.verify(t, key, events, 4, true)
					}
				}()
			}
			for i := 0; i < total; i++ {
				jobs <- i
			}
			close(jobs)
			wg.Wait()
			require.GreaterOrEqual(t, f.peak.Load(), int64(concurrency/2), "simulation must exercise concurrent in-flight requests")
			f.usage.mu.Lock()
			defer f.usage.mu.Unlock()
			require.Len(t, f.usage.logs, total)
			for _, entry := range f.usage.logs {
				require.NotNil(t, entry.OpenAIUpstream5xxRetryCount)
				require.Equal(t, 4, *entry.OpenAIUpstream5xxRetryCount)
			}
			page := service.GetOpenAIUpstream5xxRetryLog(service.OpenAIUpstream5xxRetryLogFilter{})
			require.Equal(t, total*4, page.Stats.Intercepted)
			require.Equal(t, total, page.Stats.Succeeded)
			require.Zero(t, page.Stats.Exhausted)
			t.Logf("requests=%d concurrency=%d peak=%d upstream_attempts=%d business_retries=%d elapsed=%s", total, concurrency, f.peak.Load(), total*5, total*4, time.Since(started))
		})
	}
}
