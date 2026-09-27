package basispoints

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestNativeNetworkNotificationBeforeAnswer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	release := make(chan struct{})
	go func() {
		_, _ = writer.Write(streamFixtureEvents(map[string]any{"type": "response.created", "response": map[string]any{"id": "resp_incremental", "model": "gpt-6-astra", "status": "in_progress", "output": []any{}}}))
		select {
		case <-release:
		case <-ctx.Done():
			return
		}
		// A complete tool response is validated only after notification delivery.
		_, _ = writer.Write(streamFixtureEvents(map[string]any{"type": "response.completed", "response": incrementalTerminal("hello")}))
		_ = writer.Close()
	}()
	var engine Engine
	resp, err := engine.Open(ctx, DefaultConfig(), nativeFixtureRequest([]byte(`{"model":"gpt-6-astra-basispoints","input":"hello"}`), true), func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: reader}, nil
	}, StreamOptions{ForwardNotifications: true})
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	scanner := bufio.NewScanner(resp.Body)
	var output strings.Builder
	for scanner.Scan() {
		line := scanner.Text()
		output.WriteString(line + "\n")
		if strings.HasPrefix(line, "data:") {
			if !strings.Contains(line, "response.created") {
				t.Fatalf("unexpected first event: %s", line)
			}
			break
		}
	}
	if output.Len() == 0 {
		t.Fatal("notification blocked waiting for answer")
	}
	close(release)
	for scanner.Scan() {
		output.WriteString(scanner.Text() + "\n")
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if strings.Count(output.String(), `"type":"response.created"`) != 1 || !strings.Contains(output.String(), `"type":"response.completed"`) {
		t.Fatalf("missing completion or duplicate notification: %s", output.String())
	}
}

func TestNativeNetworkNotificationKeepsToolValidationAndFailureBoundary(t *testing.T) {
	for _, bad := range []bool{false, true} {
		t.Run(map[bool]string{true: "invalid", false: "valid"}[bad], func(t *testing.T) {
			tool := "exec_command"
			if bad {
				tool = "unknown_tool"
			}
			call := rawRelayNative("call_network", tool, `{"cmd":"echo hi"}`, []any{tool})
			response := map[string]any{"id": "resp_network", "status": "completed", "output": []any{call}}
			prefix := streamFixtureEvents(map[string]any{"type": "response.created", "response": map[string]any{"id": "resp_network", "status": "in_progress", "output": []any{}}})
			raw := append(prefix, streamFixtureEvents(map[string]any{"type": "response.completed", "response": response})...)
			var calls atomic.Int32
			var engine Engine
			resp, err := engine.Open(context.Background(), DefaultConfig(), nativeFixtureRequest([]byte(`{"model":"gpt-6-astra-basispoints","input":"hello","tools":[{"type":"function","name":"exec_command","parameters":{"type":"object"}}]}`), true), func(*http.Request) (*http.Response, error) {
				calls.Add(1)
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(bytes.NewReader(raw))}, nil
			}, StreamOptions{ForwardNotifications: true})
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 1 {
				t.Fatal("retried a committed stream")
			}
			if strings.Contains(string(body), "run_officejs") || strings.Contains(string(body), "unknown_tool\"}") {
				t.Fatalf("unvalidated relay leaked: %s", body)
			}
			if bad {
				if !strings.Contains(string(body), `"type":"error"`) || strings.Contains(string(body), `"type":"response.completed"`) {
					t.Fatalf("bad tool marked successful: %s", body)
				}
			} else if !strings.Contains(string(body), `"name":"exec_command"`) || !strings.Contains(string(body), `"type":"response.completed"`) {
				t.Fatalf("tool missing: %s", body)
			}
		})
	}
}
