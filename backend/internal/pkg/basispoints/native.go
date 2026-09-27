package basispoints

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Engine owns only bounded caches. Credentials and configuration belong to each request.
type Engine struct{ attachments attachmentCache }

type HTTPDo func(*http.Request) (*http.Response, error)

// StreamOptions are supplied by the host, never by the client request body.
// Event observation happens on real upstream events, before delivery buffering.
type StreamOptions struct {
	ForwardNotifications bool
	ObserveEvent         func(event, data string)
	ResetAttempt         func()
}

func DefaultConfig() Config { c := defaultConfig(); c.DataDir = ""; return c }
func NormalizeConfig(c Config) (Config, error) {
	c = c.clone()
	c.DataDir = ""
	err := c.normalize()
	return c, err
}
func (c Config) UpstreamModelFor(model string) (string, bool) { return c.resolveUpstreamModel(model) }

// Open preserves the upstream core's pre-commit validation, correction retry and
// incremental stream delivery. The returned Body must always be closed.
func (e *Engine) Open(ctx context.Context, cfg Config, request ExecutorRequest, do HTTPDo, options ...StreamOptions) (*http.Response, error) {
	cfg, err := NormalizeConfig(cfg)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	host := &nativeHTTPHost{ctx: ctx, do: do, cfg: cfg, streams: make(map[string]*nativeHTTPStream)}
	s := &Service{cfg: cfg, sharedAttachments: &e.attachments, requestContext: ctx, host: host.call}
	body, cred, err := s.prepareRequest(request)
	if err != nil {
		cancel()
		host.closeAll()
		return nil, err
	}
	if !request.Stream {
		defer cancel()
		defer host.closeAll()
		payload, _, headers, err := s.executeResponse(request, body, cred)
		if err != nil {
			return nil, err
		}
		headers.Set("X-Qerkai-BPS-Attempts", strconv.Itoa(host.attemptCount()))
		return &http.Response{StatusCode: http.StatusOK, Header: headers, Body: io.NopCloser(bytes.NewReader(payload))}, nil
	}
	reader, writer := io.Pipe()
	ready := make(chan error, 1)
	run := &runningStream{service: s}
	delivery := newStreamDelivery("", func() { ready <- nil }, func(frame []byte) error {
		_, err := writer.Write(frame)
		return err
	})
	if len(options) > 0 {
		delivery.options = options[0]
	}
	stop := context.AfterFunc(ctx, func() { _ = reader.CloseWithError(ctx.Err()); host.closeAll() })
	go func() {
		defer cancel()
		defer stop()
		defer host.closeAll()
		defer writer.Close()
		response, err := s.readStreamingResponse(request, body, cred, run, delivery)
		if err == nil {
			err = delivery.finish(response)
		}
		if !delivery.committed {
			ready <- err
			return
		}
		if err != nil && !delivery.disconnected {
			_ = delivery.fail(err)
		}
	}()
	if err := <-ready; err != nil {
		cancel()
		_ = reader.Close()
		return nil, err
	}
	headers := host.responseHeaders()
	for _, name := range []string{"Content-Length", "Content-Encoding", "Transfer-Encoding", "ETag"} {
		headers.Del(name)
	}
	headers.Set("Content-Type", "text/event-stream")
	headers.Set("Cache-Control", "no-cache")
	return &http.Response{StatusCode: http.StatusOK, Header: headers, Body: &nativeBody{ReadCloser: reader, cancel: cancel}}, nil
}

type nativeBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *nativeBody) Close() error { b.cancel(); return b.ReadCloser.Close() }

type nativeHTTPStream struct {
	body   io.ReadCloser
	cancel context.CancelFunc
}
type nativeHTTPHost struct {
	ctx      context.Context
	do       HTTPDo
	cfg      Config
	mu       sync.Mutex
	streams  map[string]*nativeHTTPStream
	headers  http.Header
	attempts int
	nextID   int
}

func (h *nativeHTTPHost) attemptCount() int { h.mu.Lock(); defer h.mu.Unlock(); return h.attempts }
func (h *nativeHTTPHost) responseHeaders() http.Header {
	h.mu.Lock()
	defer h.mu.Unlock()
	result := h.headers.Clone()
	if result == nil {
		result = make(http.Header)
	}
	result.Set("X-Qerkai-BPS-Attempts", strconv.Itoa(h.attempts))
	return result
}
func (h *nativeHTTPHost) closeAll() {
	h.mu.Lock()
	streams := h.streams
	h.streams = make(map[string]*nativeHTTPStream)
	h.mu.Unlock()
	for _, stream := range streams {
		stream.cancel()
		_ = stream.body.Close()
	}
}
func (h *nativeHTTPHost) call(method string, payload any, out any) error {
	p, ok := payload.(map[string]any)
	if !ok {
		return fmt.Errorf("invalid BPS host payload")
	}
	switch method {
	case "host.http.do", "host.http.do_stream":
		ctx, cancel := context.WithTimeout(h.ctx, time.Duration(h.cfg.TimeoutSeconds)*time.Second)
		body, _ := p["body"].([]byte)
		req, err := http.NewRequestWithContext(ctx, stringValue(p["method"]), stringValue(p["url"]), bytes.NewReader(body))
		if err != nil {
			cancel()
			return err
		}
		if headers, ok := p["headers"].(http.Header); ok {
			for name, values := range headers {
				for _, value := range values {
					req.Header.Add(name, value)
				}
			}
		}
		h.mu.Lock()
		if req.URL.String() == h.cfg.ResponsesURL {
			h.attempts++
		}
		h.mu.Unlock()
		resp, err := h.do(req)
		if err != nil {
			cancel()
			return err
		}
		if resp == nil || resp.Body == nil {
			cancel()
			return errors.New("empty BPS HTTP response")
		}
		h.mu.Lock()
		h.headers = resp.Header.Clone()
		h.mu.Unlock()
		if method == "host.http.do" {
			defer cancel()
			defer resp.Body.Close()
			raw, err := io.ReadAll(io.LimitReader(resp.Body, int64(h.cfg.MaxResponseBytes)+1))
			if err != nil {
				return err
			}
			if len(raw) > h.cfg.MaxResponseBytes {
				return fail(502, "upstream_response_too_large", "Basis Points response exceeds configured limit")
			}
			*out.(*upstreamResponse) = upstreamResponse{StatusCode: resp.StatusCode, Headers: resp.Header, Body: raw}
			return nil
		}
		h.mu.Lock()
		h.nextID++
		id := strconv.Itoa(h.nextID)
		h.streams[id] = &nativeHTTPStream{body: resp.Body, cancel: cancel}
		h.mu.Unlock()
		*out.(*upstreamStream) = upstreamStream{StatusCode: resp.StatusCode, Headers: resp.Header, StreamID: id}
		return nil
	case "host.http.stream_read":
		id := stringValue(p["stream_id"])
		h.mu.Lock()
		stream := h.streams[id]
		h.mu.Unlock()
		if stream == nil {
			return errors.New("BPS response stream closed")
		}
		buffer := make([]byte, 32<<10)
		n, err := stream.body.Read(buffer)
		chunk := streamChunk{Payload: buffer[:n], Done: errors.Is(err, io.EOF)}
		if err != nil && !chunk.Done {
			chunk.Error = safeError(err)
		}
		*out.(*streamChunk) = chunk
		return nil
	case "host.http.stream_close":
		id := stringValue(p["stream_id"])
		h.mu.Lock()
		stream := h.streams[id]
		delete(h.streams, id)
		h.mu.Unlock()
		if stream != nil {
			stream.cancel()
			return stream.body.Close()
		}
		return nil
	default:
		return fmt.Errorf("unsupported native BPS host callback: %s", method)
	}
}

// Catalog invokes the same metadata contract as the CPA model interceptor.
func Catalog(cfg Config, body []byte) ([]byte, error) {
	cfg, err := NormalizeConfig(cfg)
	if err != nil {
		return nil, err
	}
	s := &Service{cfg: cfg}
	result, err := s.interceptModelCatalog(jsonBytes(catalogInterceptRequest{SourceFormat: "openai", StatusCode: 200, Body: body}))
	if err != nil {
		return nil, err
	}
	if raw, ok := result.(map[string]any)["Body"].([]byte); ok {
		return raw, nil
	}
	return body, nil
}

// Credential is built from Qerkai's refreshed OAuth access token, never a second token store.
func Credential(token, accountID string) []byte {
	raw, _ := json.Marshal(map[string]any{"access_token": token, "account_id": accountID, "type": "codex"})
	return raw
}
