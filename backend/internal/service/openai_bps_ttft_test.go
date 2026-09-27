//go:build unit

package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestBPSFirstTokenAllModesAndRetryReset(t *testing.T) {
	for _, mode := range []string{OpenAITTFTModeNetwork, OpenAITTFTModeSemantic, OpenAITTFTModeVisible} {
		t.Run(mode, func(t *testing.T) {
			timing := &bpsFirstToken{start: time.Now().Add(-time.Second), mode: mode}
			opts := timing.options()
			require.Equal(t, mode == OpenAITTFTModeNetwork, opts.ForwardNotifications)
			opts.ObserveEvent("response.created", `{"type":"response.created","response":{"id":"resp"}}`)
			require.Equal(t, mode == OpenAITTFTModeNetwork, timing.milliseconds() != nil)
			opts.ObserveEvent("response.output_text.delta", `{"type":"response.output_text.delta","delta":""}`)
			require.Equal(t, mode != OpenAITTFTModeVisible, timing.milliseconds() != nil)
			opts.ObserveEvent("response.output_text.delta", `{"type":"response.output_text.delta","delta":"hello"}`)
			require.NotNil(t, timing.milliseconds())
			first := *timing.milliseconds()
			opts.ObserveEvent("response.completed", `{"type":"response.completed"}`)
			require.Equal(t, first, *timing.milliseconds())
			opts.ResetAttempt()
			require.Nil(t, timing.milliseconds())
		})
	}
}

func TestBPSNetworkTTFTDoesNotUseReconstructedTerminalFrames(t *testing.T) {
	restore := setTTFTModeForTest(t, OpenAITTFTModeNetwork)
	defer restore()
	gatewayForwardingCache.Store(&cachedGatewayForwardingSettings{openAITTFTMode: OpenAITTFTModeNetwork, expiresAt: time.Now().Add(time.Hour).UnixNano()})
	gateway, account := bpsTestGateway(t)
	gateway.httpUpstream = bpsFixtureHTTP(bpsTextFixture)
	c, rec := downstreamTestContext(11)
	result, err := gateway.Forward(context.Background(), c, account, []byte(`{"model":"gpt-6-astra","stream":true,"input":"hi"}`))
	require.NoError(t, err, rec.Body.String())
	require.Contains(t, rec.Body.String(), "response.created")
	require.Nil(t, result.FirstTokenMs, "synthetic notification is not an upstream network frame")
}

func TestBPSNetworkNotificationFlushedBeforeUpstreamCompletion(t *testing.T) {
	restore := setTTFTModeForTest(t, OpenAITTFTModeNetwork)
	defer restore()
	gatewayForwardingCache.Store(&cachedGatewayForwardingSettings{openAITTFTMode: OpenAITTFTModeNetwork, expiresAt: time.Now().Add(time.Hour).UnixNano()})
	gateway, account := bpsTestGateway(t)
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	release := make(chan struct{})
	go func() {
		_, _ = io.WriteString(writer, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_bps\",\"model\":\"gpt-6-astra\",\"status\":\"in_progress\",\"output\":[]}}\n\n")
		select {
		case <-release:
		case <-ctx.Done():
			return
		}
		_, _ = io.WriteString(writer, "data: {\"type\":\"response.completed\",\"response\":"+bpsTextFixture+"}\n\n")
		_ = writer.Close()
	}()
	gateway.httpUpstream = &keeperHTTPStub{do: func(*http.Request, string, int64, int) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: reader}, nil
	}}
	flushed := make(chan string, 20)
	recorder := &networkTTFTRecorder{ResponseRecorder: httptest.NewRecorder(), onFlush: func(s string) { flushed <- s }}
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil).WithContext(ctx)
	done := make(chan error, 1)
	var result *OpenAIForwardResult
	go func() {
		var err error
		result, err = gateway.forwardBPS(ctx, c, account, []byte(`{"model":"gpt-6-astra","stream":true,"input":"hi"}`), gateway.settingService.bpsSettings(ctx), "responses", "", "")
		done <- err
	}()
	select {
	case output := <-flushed:
		require.Contains(t, output, `"type":"response.created"`)
		require.NotContains(t, output, "hello")
	case <-ctx.Done():
		t.Fatal("network notification waited for answer")
	}
	close(release)
	require.NoError(t, <-done)
	require.NotNil(t, result.FirstTokenMs)
	require.Equal(t, 1, strings.Count(recorder.Body.String(), `"type":"response.created"`))
	require.Contains(t, recorder.Body.String(), "hello")
}
