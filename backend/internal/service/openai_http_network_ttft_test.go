package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestHTTPNetworkTTFTIndependentOfForceWS(t *testing.T) {
	defer setTTFTModeForTest(t, OpenAITTFTModeNetwork)()
	for _, forced := range []bool{false, true} {
		for _, passthrough := range []bool{false, true} {
			for _, timeout := range []int{0, 5} {
				t.Run(fmt.Sprintf("forced=%t/passthrough=%t/timeout=%d", forced, passthrough, timeout), func(t *testing.T) {
					defer setForceUpstreamWSForTest(forced)()
					reader, writer := io.Pipe()
					defer reader.Close()
					firstFlush := make(chan struct{})
					var once sync.Once
					firstFrameOnly := false
					done := make(chan struct{})
					go func() {
						defer close(done)
						defer writer.Close()
						_, _ = io.WriteString(writer, "event: response.created\ndata: {\"response\":{\"id\":\"resp_network\"}}\n\n")
						select {
						case <-firstFlush:
						case <-time.After(2 * time.Second):
						}
						time.Sleep(250 * time.Millisecond)
						_, _ = io.WriteString(writer, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n")
						_, _ = io.WriteString(writer, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_network\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n")
					}()
					first, body, err := runHTTPNetworkTTFT(t, passthrough, timeout, reader, networkTTFTOAuthAccount(), func(output string) {
						once.Do(func() {
							firstFrameOnly = strings.Contains(output, "response.created") && !strings.Contains(output, "hello") && strings.HasSuffix(output, "\n\n")
							close(firstFlush)
						})
					})
					require.NoError(t, err)
					require.True(t, firstFrameOnly, "created must be flushed before upstream sends an answer")
					require.NotNil(t, first)
					require.Less(t, *first, 200, "created should be timed before delayed output")
					require.Contains(t, body, "response.created")
					require.Contains(t, body, "hello")
					<-done
				})
			}
		}
	}
}

func TestHTTPNetworkTTFTCommitsNotificationsBeforeFailure(t *testing.T) {
	defer setTTFTModeForTest(t, OpenAITTFTModeNetwork)()
	defer setForceUpstreamWSForTest(false)()
	for _, passthrough := range []bool{false, true} {
		body := "data: {\"type\":\"response.created\"}\n\n" +
			"data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_error\",\"message\":\"upstream overloaded\"}}}\n\n"
		first, output, err := runHTTPNetworkTTFT(t, passthrough, 5, io.NopCloser(strings.NewReader(body)), networkTTFTOAuthAccount())
		var failover *UpstreamFailoverError
		require.Error(t, err)
		require.NotErrorAs(t, err, &failover, "a committed notification prevents replay of the HTTP response")
		require.NotNil(t, first)
		require.Contains(t, output, "response.created")
		require.Contains(t, output, "response.failed")
	}
	reader, writer := io.Pipe()
	defer reader.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer writer.Close()
		_, _ = io.WriteString(writer, "data: {\"type\":\"response.created\"}\n\n")
		time.Sleep(1100 * time.Millisecond)
		_, _ = io.WriteString(writer, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n")
		_, _ = io.WriteString(writer, "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n")
	}()
	first, output, err := runHTTPNetworkTTFT(t, false, 1, reader, networkTTFTOAuthAccount())
	require.NoError(t, err, "a forwarded first frame satisfies the first-output deadline")
	require.NotNil(t, first)
	require.Contains(t, output, "response.created")
	require.Contains(t, output, "hello")
	<-done
}

func TestHTTPNetworkTTFTTerminalOnlyAndAccountScope(t *testing.T) {
	defer setTTFTModeForTest(t, OpenAITTFTModeNetwork)()
	for _, passthrough := range []bool{false, true} {
		body := "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n"
		first, _, err := runHTTPNetworkTTFT(t, passthrough, 0, io.NopCloser(strings.NewReader(body)), networkTTFTOAuthAccount())
		require.NoError(t, err)
		require.Nil(t, first, "terminal-only streams have no network first frame")
	}
	svc := &OpenAIGatewayService{}
	for _, account := range []*Account{nil, networkTTFTAPIKeyAccount(), {Platform: PlatformGrok, Type: AccountTypeOAuth}} {
		require.Equal(t, OpenAITTFTModeSemantic, svc.openAIStreamTTFTMode(context.Background(), account))
	}
	require.Equal(t, OpenAITTFTModeNetwork, svc.openAIStreamTTFTMode(context.Background(), &Account{Platform: PlatformOpenAI, Type: AccountTypeSetupToken}))
	for _, mode := range []string{OpenAITTFTModeSemantic, OpenAITTFTModeVisible} {
		restore := setTTFTModeForTest(t, mode)
		require.Equal(t, mode, svc.openAIStreamTTFTMode(context.Background(), networkTTFTOAuthAccount()))
		restore()
	}
	for _, data := range []string{"", "[DONE]", "invalid", `{"type":"response.failed"}`, `{"type":"response.incomplete"}`} {
		require.False(t, openAIStreamDataStartsTTFT(data, "", true, OpenAITTFTModeNetwork))
	}
}

type networkTTFTRecorder struct {
	*httptest.ResponseRecorder
	onFlush func(string)
}

func (r *networkTTFTRecorder) Flush() {
	r.ResponseRecorder.Flush()
	if r.onFlush != nil {
		r.onFlush(r.Body.String())
	}
}

func runHTTPNetworkTTFT(t *testing.T, passthrough bool, timeout int, body io.ReadCloser, account *Account, onFlush ...func(string)) (*int, string, error) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{
		MaxLineSize: defaultMaxLineSize, OpenAIFirstOutputTimeoutSeconds: timeout,
	}}}
	recorder := &networkTTFTRecorder{ResponseRecorder: httptest.NewRecorder()}
	if len(onFlush) > 0 {
		recorder.onFlush = onFlush[0]
	}
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: body}
	if passthrough {
		result, err := svc.handleStreamingResponsePassthrough(context.Background(), resp, c, account, time.Now(), "test-model", "test-model")
		require.NotNil(t, result)
		return result.firstTokenMs, recorder.Body.String(), err
	}
	result, err := svc.handleStreamingResponse(context.Background(), resp, c, account, time.Now(), "test-model", "test-model")
	require.NotNil(t, result)
	return result.firstTokenMs, recorder.Body.String(), err
}
