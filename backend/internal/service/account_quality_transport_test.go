//go:build unit

package service

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"io"
	"net/http"
	"testing"
)

type qualityAccountRepo struct {
	AccountRepository
	account *Account
}

func (r *qualityAccountRepo) GetByID(context.Context, int64) (*Account, error) { return r.account, nil }
func TestAccountQualityReusesSingleAccountTest(t *testing.T) {
	q := DefaultAccountQualitySettings()
	question := q.Questions[0]
	for _, tc := range []struct {
		name, body string
		code       int
		wantErr    bool
	}{{"answer", "data: {\"type\":\"response.output_text.delta\",\"delta\":\"21\"}\n\ndata: {\"type\":\"response.completed\"}\n\n", 200, false}, {"empty", "data: {\"type\":\"response.completed\"}\n\n", 200, true}, {"rate-limit", "rate limited", 429, true}} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := &queuedHTTPUpstream{responses: []*http.Response{newJSONResponse(tc.code, tc.body)}}
			account := &Account{ID: 71, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Concurrency: 1, Credentials: map[string]any{"access_token": "test-token"}}
			svc := &AccountQualityService{tests: &AccountTestService{accountRepo: &qualityAccountRepo{account: account}, httpUpstream: upstream}}
			answer, _, err := svc.testAnswer(context.Background(), 71, q, question)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, "21", answer)
			}
			require.Len(t, upstream.requests, 1)
			body, err := io.ReadAll(upstream.requests[0].Body)
			require.NoError(t, err)
			require.Equal(t, question.Prompt, gjson.GetBytes(body, "input.0.content.0.text").String())
			require.Equal(t, q.ReasoningEffort, gjson.GetBytes(body, "reasoning.effort").String())
			require.Empty(t, upstream.requests[0].Header.Get("X-Quality-Global-Proxy"))
		})
	}
}
