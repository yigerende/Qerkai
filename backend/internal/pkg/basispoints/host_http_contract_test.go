package basispoints

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"testing"
)

// CPA callHostHTTPDo 直接序列化 pluginapi.HTTPResponse，其字段为 PascalCase。
// 不用 upstreamResponse 编造返回值，以便捕捉插件自身 JSON 标签错误。
func TestHostHTTPResponseJSONContract(t *testing.T) {
	for _, status := range []int{200, 201, 204, 400, 401, 413, 422, 429, 500, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			data := jsonBytes(map[string]any{"openai_file_id": "file-contract-test"})
			headers := http.Header{"Content-Type": {"application/json"}}
			raw := jsonBytes(map[string]any{"StatusCode": status, "Headers": headers, "Body": data})
			var got upstreamResponse
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatal(err)
			}
			if got.StatusCode != status || string(got.Body) != string(data) || !reflect.DeepEqual(got.Headers, headers) {
				t.Fatalf("host HTTP response fields lost: status=%d, want=%d", got.StatusCode, status)
			}
		})
	}
}

// CPA 的流式回调使用独立 RPC 结构，仍然是 snake_case，不能跟着改名。
func TestHostHTTPStreamJSONContract(t *testing.T) {
	raw := jsonBytes(map[string]any{"status_code": 201, "headers": http.Header{"Content-Type": {"text/event-stream"}}, "stream_id": "host-stream"})
	var stream upstreamStream
	if err := json.Unmarshal(raw, &stream); err != nil {
		t.Fatal(err)
	}
	if stream.StatusCode != 201 || stream.StreamID != "host-stream" || stream.Headers.Get("Content-Type") != "text/event-stream" {
		t.Fatal("stream response fields lost")
	}
	var chunk streamChunk
	if err := json.Unmarshal(jsonBytes(map[string]any{"payload": []byte("chunk"), "error": "interrupted", "done": true}), &chunk); err != nil {
		t.Fatal(err)
	}
	if string(chunk.Payload) != "chunk" || chunk.Error != "interrupted" || !chunk.Done {
		t.Fatal("stream read fields lost")
	}
}
