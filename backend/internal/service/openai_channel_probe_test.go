package service

import "testing"

func TestIsSub2APIChannelProbeRequest(t *testing.T) {
	tests := []struct {
		name string
		body string
		want bool
	}{
		{
			name: "chat completions probe",
			body: `{"model":"gpt-5.4","messages":[{"role":"user","content":"Calculate and respond with ONLY the number, nothing else.\n\nQ: 3 + 5 = ?\nA: 8\n\nQ: 12 - 7 = ?\nA: 5\n\nQ: 7 - 2 = ?\nA:"}],"max_tokens":50,"stream":false}`,
			want: true,
		},
		{
			name: "responses probe",
			body: `{"model":"gpt-5.4","instructions":"You are a channel health-check endpoint. Answer the arithmetic challenge exactly and briefly.","input":"Calculate and respond with ONLY the number, nothing else.\n\nQ: 3 + 5 = ?\nA: 8\n\nQ: 12 - 7 = ?\nA: 5\n\nQ: 7 - 2 = ?\nA:","max_output_tokens":50,"stream":false}`,
			want: true,
		},
		{
			name: "streaming probe shape is excluded",
			body: `{"model":"gpt-5.4","messages":[{"role":"user","content":"Calculate and respond with ONLY the number, nothing else.\n\nQ: 3 + 5 = ?\nA: 8\n\nQ: 12 - 7 = ?\nA: 5\n\nQ: 7 - 2 = ?\nA:"}],"max_tokens":50,"stream":true}`,
			want: false,
		},
		{
			name: "ordinary nonstream request is excluded",
			body: `{"model":"gpt-5.4","messages":[{"role":"user","content":"Calculate and respond with ONLY the number, nothing else."}],"max_tokens":50,"stream":false}`,
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsSub2APIChannelProbeRequest([]byte(tt.body)); got != tt.want {
				t.Fatalf("IsSub2APIChannelProbeRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}
