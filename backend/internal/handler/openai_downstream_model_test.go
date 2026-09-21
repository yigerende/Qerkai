package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// Real client/upstream sockets exercise per-turn policy isolation and usage.
func TestDownstreamModelNativeWS(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, mode := range []string{service.OpenAIWSIngressModePassthrough, service.OpenAIWSIngressModeCtxPool} {
		for _, scope := range []struct {
			name    string
			enabled bool
			groups  []int64
			align   bool
		}{
			{"off", false, nil, false},
			{"all", true, nil, true},
			{"selected", true, []int64{4301}, true},
			{"other-group", true, []int64{99}, false},
		} {
			t.Run(mode+"/"+scope.name, func(t *testing.T) {
				var dials atomic.Int32
				requests := make(chan string, 8)
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					conn, err := coderws.Accept(w, r, nil)
					if err != nil {
						return
					}
					dials.Add(1)
					defer conn.CloseNow()
					ctx, cancel := context.WithTimeout(r.Context(), 12*time.Second)
					defer cancel()
					for turn := 1; ; turn++ {
						_, payload, err := conn.Read(ctx)
						if err != nil {
							return
						}
						requests <- gjson.GetBytes(payload, "model").String()
						modelField := `,"model":"gpt-5.6-luna"`
						if turn == 4 {
							modelField = ""
						}
						created := fmt.Sprintf(`{"type":"response.created","response":{"id":"resp_alignment_%d","status":"in_progress"%s}}`, turn, modelField)
						completed := fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp_alignment_%d","status":"completed"%s,"output":[],"usage":{"input_tokens":2,"output_tokens":3}}}`, turn, modelField)
						for _, event := range []string{created, `{"type":"response.output_text.delta","delta":"original gpt-5.6-luna answer"}`, completed} {
							if conn.Write(ctx, coderws.MessageText, []byte(event)) != nil {
								return
							}
						}
					}
				}))
				t.Cleanup(upstream.Close)
				policy, err := json.Marshal(service.OpenAIDownstreamModelAlignmentSettings{Enabled: scope.enabled, GroupIDs: scope.groups, Models: []string{"gpt-6-astra"}})
				require.NoError(t, err)
				harness := newOpenAIWSPassthroughHandlerHarnessWithSettings(t, upstream.URL, map[string]string{
					service.SettingKeyOpenAIDownstreamModelAlignment: string(policy),
				}, mode)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				for turn, model := range []string{"gpt-6-astra", "gpt-5.6-terra", "gpt-6-astra", "gpt-6-astra"} {
					payload := fmt.Sprintf(`{"type":"response.create","model":%q,"input":"original prompt","stream":true}`, model)
					require.NoError(t, harness.clientConn.Write(ctx, coderws.MessageText, []byte(payload)))
					want := "gpt-5.6-luna"
					if scope.align && model == "gpt-6-astra" {
						want = model
					}
					if turn == 3 {
						want = ""
					}
					for {
						_, event, err := harness.clientConn.Read(ctx)
						require.NoError(t, err)
						switch gjson.GetBytes(event, "type").String() {
						case "response.output_text.delta":
							require.Equal(t, "original gpt-5.6-luna answer", gjson.GetBytes(event, "delta").String())
						case "response.created", "response.completed":
							require.Equal(t, want, gjson.GetBytes(event, "response.model").String())
						}
						if gjson.GetBytes(event, "type").String() == "response.completed" {
							break
						}
					}
					select {
					case sent := <-requests:
						require.Equal(t, model, sent, "the upstream request must stay unchanged")
					case <-ctx.Done():
						t.Fatal("upstream request was not captured")
					}
					select {
					case row := <-harness.usageLogs:
						if turn == 3 {
							require.Nil(t, row.DownstreamModel)
							require.Nil(t, row.UpstreamResponseModel)
						} else {
							require.NotNil(t, row.DownstreamModel)
							require.Equal(t, want, *row.DownstreamModel)
							require.Equal(t, "gpt-5.6-luna", *row.UpstreamResponseModel)
							require.True(t, *row.UpstreamModelMismatch)
						}
						require.Equal(t, 2, row.InputTokens)
						require.Equal(t, 3, row.OutputTokens)
					case <-ctx.Done():
						t.Fatal("usage row was not recorded")
					}
				}
				require.EqualValues(t, 1, dials.Load(), "all turns should reuse their upstream connection")
				require.NoError(t, harness.clientConn.CloseNow())
			})
		}
	}
}
