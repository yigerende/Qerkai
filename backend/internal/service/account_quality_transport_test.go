//go:build unit

package service

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"io"
	"net/http"
	"strings"
	"testing"
)

type qualityAccountRepo struct {
	AccountRepository
	account *Account
}

func TestAccountQualityInjectsMatchingStateWithoutChangingPromptOrTransport(t *testing.T) {
	defer setForceUpstreamWSForTest(false)()
	for _, tc := range []struct {
		name                                           string
		enabled, matchingGroup, matchingModel, refresh bool
	}{
		{"enabled", true, true, true, true},
		{"disabled", false, true, true, true},
		{"other-group", true, false, true, true},
		{"other-model", true, true, false, true},
		{"response-refresh-disabled", true, true, true, false},
		{"no-state", true, true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keeper, _, account := keeperTestService(t)
			account.GroupIDs = []int64{11}
			settings := keeper.config.Load().OpenAIStateKeeperSettings
			settings.InjectionEnabled, settings.DegradedStateLengths = tc.enabled, []int{356}
			settings.ResponseRefreshEnabled = tc.refresh
			if !tc.matchingGroup {
				account.GroupIDs = []int64{12}
			}
			require.NoError(t, keeper.Save(context.Background(), settings))
			if tc.name == "no-state" {
				keeper.entryLocked(account.ID).value = ""
			}
			q := DefaultAccountQualitySettings()
			q.Model = settings.Model
			if !tc.matchingModel {
				q.Model = "other-model"
			}
			question := q.Questions[0]
			response := newJSONResponse(200, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"21\"}\n\ndata: {\"type\":\"response.completed\"}\n\n")
			response.Header.Set(openAICodexTurnStateHeader, strings.Repeat("d", 356))
			upstream := &queuedHTTPUpstream{responses: []*http.Response{response}}
			svc := &AccountQualityService{tests: &AccountTestService{accountRepo: &qualityAccountRepo{account: account}, httpUpstream: upstream}}
			svc.stateKeeper.Store(keeper)
			answer, _, err := svc.testAnswer(context.Background(), account.ID, q, question)
			require.NoError(t, err)
			require.Equal(t, "21", answer)
			require.Len(t, upstream.requests, 1)
			want := ""
			if tc.enabled && tc.matchingGroup && tc.matchingModel && tc.name != "no-state" {
				want = "collected-secret"
			}
			require.Equal(t, want, upstream.requests[0].Header.Get(openAICodexTurnStateHeader))
			require.Equal(t, "https://chatgpt.com/backend-api/codex/responses", upstream.requests[0].URL.String())
			require.Empty(t, upstream.requests[0].Header.Get("Upgrade"))
			body, err := io.ReadAll(upstream.requests[0].Body)
			require.NoError(t, err)
			require.Equal(t, question.Prompt, gjson.GetBytes(body, "input.0.content.0.text").String())
			keeperDrainObservations(keeper)
			require.Equal(t, tc.enabled && tc.matchingGroup && tc.matchingModel && tc.refresh, len(keeper.queue) == 1)
			if want != "" {
				require.Equal(t, "quality_http", keeper.Recent([]int64{account.ID})[0].Injections[0].Source)
			}
		})
	}
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
