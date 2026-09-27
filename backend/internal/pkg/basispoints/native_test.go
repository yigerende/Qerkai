package basispoints

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func nativeFixtureRequest(body []byte, stream bool) ExecutorRequest {
	return ExecutorRequest{Payload: body, SourceFormat: "codex", Stream: stream, StorageJSON: Credential("fixture-token", "fixture-account"), CacheScope: "tenant-1/account-1"}
}

func TestNativeAttachmentConcurrentCacheAndCredentialIsolation(t *testing.T) {
	dataURL, _ := testImageDataURL(t)
	source := jsonBytes(map[string]any{"model": DefaultModelID, "input": []any{map[string]any{"role": "user", "content": []any{map[string]any{"type": "input_image", "image_url": dataURL, "detail": "high"}}}}})
	var engine Engine
	var uploads atomic.Int32
	do := func(req *http.Request) (*http.Response, error) {
		body := jsonBytes(incrementalTerminal("image received"))
		if strings.HasSuffix(req.URL.Path, "/attachments") {
			uploads.Add(1)
			body = []byte(`{"openai_file_id":"file_fixture"}`)
		} else {
			raw, _ := io.ReadAll(req.Body)
			if !bytes.Contains(raw, []byte(`"file_id":"file_fixture"`)) || bytes.Contains(raw, []byte("data:image")) {
				return nil, fmt.Errorf("attachment was not uploaded: %s", raw)
			}
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(bytes.NewReader(body))}, nil
	}
	var wg sync.WaitGroup
	errorsC := make(chan error, 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := engine.Open(context.Background(), DefaultConfig(), nativeFixtureRequest(source, false), do)
			if err == nil {
				_, err = io.ReadAll(resp.Body)
				resp.Body.Close()
			}
			if err != nil {
				errorsC <- err
			}
		}()
	}
	wg.Wait()
	close(errorsC)
	for err := range errorsC {
		t.Error(err)
	}
	if uploads.Load() != 1 {
		t.Fatalf("same image uploaded %d times", uploads.Load())
	}
	request := nativeFixtureRequest(source, false)
	request.StorageJSON = Credential("different-token", "different-account")
	resp, err := engine.Open(context.Background(), DefaultConfig(), request, do)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if uploads.Load() != 2 {
		t.Fatal("image cache leaked across credentials")
	}
}

func TestNativeAttachmentWaiterCancellation(t *testing.T) {
	var cache attachmentCache
	key := sha256.Sum256([]byte("same-image"))
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		cache.getOrUpload(key, func() (string, error) { close(started); <-release; return "file", nil })
	}()
	defer func() { close(release); <-done }()
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := cache.getOrUploadContext(ctx, key, func() (string, error) { t.Error("duplicate upload"); return "", nil })
	if err != context.Canceled {
		t.Fatalf("waiter did not cancel: %v", err)
	}
}

func TestNativeExecutorMatchesCPARequestAndResponse(t *testing.T) {
	source := []byte(`{"model":"gpt-6-astra-basispoints","input":"hello","prompt_cache_key":"session","reasoning":{"effort":"ultra"}}`)
	terminal := incrementalTerminal("  hello\n")
	var cpaBody []byte
	svc := NewService()
	svc.SetHost(func(method string, payload any, out any) error {
		if method != "host.http.do" {
			t.Fatalf("unexpected callback %s", method)
		}
		cpaBody = payload.(map[string]any)["body"].([]byte)
		*out.(*upstreamResponse) = upstreamResponse{StatusCode: 200, Headers: http.Header{"Content-Type": {"application/json"}}, Body: jsonBytes(terminal)}
		return nil
	})
	request := nativeFixtureRequest(source, false)
	result, err := svc.Handle("executor.execute", jsonBytes(request))
	if err != nil {
		t.Fatal(err)
	}
	var engine Engine
	resp, err := engine.Open(context.Background(), DefaultConfig(), request, func(req *http.Request) (*http.Response, error) {
		raw, _ := io.ReadAll(req.Body)
		var want, got any
		_ = json.Unmarshal(cpaBody, &want)
		_ = json.Unmarshal(raw, &got)
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("request differs from CPA core: %s", raw)
		}
		if req.Method != "POST" || req.Header.Get("X-OpenAI-Internal-Basispoints-Client-Product") != "basispoints-excel-plugin" || req.Header.Get("Authorization") != "Bearer fixture-token" || req.Header.Get("ChatGPT-Account-ID") != "fixture-account" {
			t.Fatal("incorrect BPS transport contract")
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(bytes.NewReader(jsonBytes(terminal)))}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, result.(map[string]any)["Payload"].([]byte)) {
		t.Fatalf("response differs: %s", got)
	}
}

func TestNativeIncrementalDeliveryAndCancellation(t *testing.T) {
	release := make(chan struct{})
	canceled := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write(incrementalPrefix("hello"))
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
			close(canceled)
			return
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	cfg := DefaultConfig()
	cfg.ResponsesURL = server.URL + "/responses"
	var engine Engine
	resp, err := engine.Open(context.Background(), cfg, nativeFixtureRequest([]byte(`{"model":"gpt-6-astra-basispoints","input":"hello"}`), true), server.Client().Do)
	if err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(resp.Body)
	found := false
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), "response.output_text.delta") {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("text was not delivered before upstream completion")
	}
	_ = resp.Body.Close()
	select {
	case <-canceled:
	case <-time.After(3 * time.Second):
		t.Fatal("client cancellation did not release upstream")
	}
}

func TestNativeToolsRoundTripAndTenantIsolation(t *testing.T) {
	patch := "*** Begin Patch\n*** Add File: hello.txt\n+hello \"world\"\n*** End Patch\n"
	call := rawRelayNative("same-call-id", "apply_patch", patch, []any{"apply_patch"})
	source := map[string]any{"model": DefaultModelID, "prompt_cache_key": "session", "input": "edit file", "tools": []any{map[string]any{"type": "custom", "name": "apply_patch"}}}
	var engine Engine
	request := nativeFixtureRequest(jsonBytes(source), false)
	resp, err := engine.Open(context.Background(), DefaultConfig(), request, func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(bytes.NewReader(jsonBytes(map[string]any{"id": "resp_tool", "status": "completed", "output": []any{call}})))}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	decoded, _ := rawObject(raw)
	restored := objectValue(decoded["output"].([]any)[0])
	if restored["input"] != patch || restored["name"] != "apply_patch" {
		t.Fatal("patch changed")
	}
	// The client owns tool execution; exercise a real file round trip in a temp workspace.
	path := filepath.Join(t.TempDir(), "hello.txt")
	if err := os.WriteFile(path, []byte("hello \"world\"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	source["input"] = []any{messageItem("user", "edit file"), restored, map[string]any{"type": "custom_tool_call_output", "call_id": "same-call-id", "output": string(content)}}
	request.Payload = jsonBytes(source)
	parsed, _ := executorSource(request)
	prepared, err := prepareResponsesBody(parsed, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	input := prepared["input"].([]any)
	matched := false
	for _, item := range input {
		if objectValue(item)["name"] == transportName {
			matched = reflect.DeepEqual(objectValue(item), call)
		}
	}
	if !matched {
		t.Fatal("native tool identity was not replayed")
	}
	request.CacheScope = "tenant-2/account-1"
	other, _ := executorSource(request)
	if rememberedNativeCall("same-call-id", stringValue(other[nativeScopeField])) != nil {
		t.Fatal("tool cache leaked across tenants")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
}

func TestNativeRetryBeforeCommitAndFailureAfterCommit(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprint(committed), func(t *testing.T) {
			var attempts atomic.Int32
			var engine Engine
			bad := rawRelayNative("bad-call", "unknown_tool", `{}`, []any{"unknown_tool"})
			source := []byte(`{"model":"gpt-6-astra-basispoints","input":"hello","tools":[{"type":"function","name":"exec_command","parameters":{"type":"object"}}]}`)
			resp, err := engine.Open(context.Background(), DefaultConfig(), nativeFixtureRequest(source, true), func(req *http.Request) (*http.Response, error) {
				n := attempts.Add(1)
				var raw []byte
				if committed {
					raw = append(incrementalPrefix("hello"), streamFixtureEvents(map[string]any{"type": "response.completed", "response": incrementalTerminal("hello", bad)})...)
				} else if n == 1 {
					raw = streamFixtureEvents(map[string]any{"type": "response.completed", "response": map[string]any{"id": "r", "status": "completed", "output": []any{bad}}})
				} else {
					raw = streamFixtureEvents(map[string]any{"type": "response.completed", "response": incrementalTerminal("fixed")})
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(bytes.NewReader(raw))}, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			raw, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if committed {
				if attempts.Load() != 1 || !bytes.Contains(raw, []byte(`"type":"error"`)) || bytes.Contains(raw, []byte(`"type":"response.completed"`)) {
					t.Fatalf("post-commit retry or false success: %s", raw)
				}
			} else if attempts.Load() != 2 || !bytes.Contains(raw, []byte("fixed")) {
				t.Fatal("pre-commit correction was not preserved")
			}
		})
	}
}

func TestNativeConcurrentStreams(t *testing.T) {
	for _, concurrency := range []int{100, 500} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
			var engine Engine
			start := make(chan struct{})
			errorsC := make(chan error, concurrency)
			var wg sync.WaitGroup
			began := time.Now()
			for i := 0; i < concurrency; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					text := fmt.Sprintf("account-%d", i)
					r := nativeFixtureRequest(jsonBytes(map[string]any{"model": DefaultModelID, "input": text}), true)
					r.CacheScope = text
					resp, err := engine.Open(context.Background(), DefaultConfig(), r, func(req *http.Request) (*http.Response, error) {
						raw := append(incrementalPrefix(text), streamFixtureEvents(map[string]any{"type": "response.completed", "response": incrementalTerminal(text)})...)
						return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(bytes.NewReader(raw))}, nil
					})
					if err != nil {
						errorsC <- err
						return
					}
					defer resp.Body.Close()
					raw, err := io.ReadAll(resp.Body)
					if err != nil {
						errorsC <- err
						return
					}
					if !bytes.Contains(raw, []byte(text)) || !bytes.Contains(raw, []byte(`"type":"response.completed"`)) {
						errorsC <- fmt.Errorf("missing content or terminal for %d", i)
					}
				}(i)
			}
			close(start)
			wg.Wait()
			close(errorsC)
			for err := range errorsC {
				t.Error(err)
			}
			t.Logf("%d concurrent streams completed in %s", concurrency, time.Since(began))
		})
	}
}
