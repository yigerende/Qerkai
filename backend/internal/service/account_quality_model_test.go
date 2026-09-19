//go:build unit

package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type qualityModelAuditStub struct {
	UsageLogRepository
	logs []ModelAuditLog
	err  error
}

type qualityModelAccountRepo struct {
	qualityAccountRepo
	errors int
}

func (r *qualityModelAccountRepo) SetError(context.Context, int64, string) error {
	r.errors++
	return nil
}

func (r *qualityModelAuditStub) LatestModelAudit(_ context.Context, input ModelAuditInput) ([]ModelAuditResult, error) {
	return []ModelAuditResult{{AccountID: input.Accounts[0].AccountID, Logs: r.logs}}, r.err
}

func qualityModelTestLogs(id int64, model string, count int, at time.Time) []ModelAuditLog {
	logs := make([]ModelAuditLog, count)
	mismatch := true
	for i := range logs {
		logs[i] = ModelAuditLog{ID: int64(i + 1), AccountID: id, CreatedAt: at, RequestStartedAt: &at, SentModel: model, ResponseModel: "wrong-model", Mismatch: &mismatch}
	}
	return logs
}

func TestAccountQualityModelUsesThreeLogsOrOneHi(t *testing.T) {
	defer setForceUpstreamWSForTest(false)()
	for _, count := range []int{0, 1, 2, 3} {
		for _, injection := range []bool{false, true} {
			t.Run(fmt.Sprintf("logs=%d/injection=%t", count, injection), func(t *testing.T) {
				keeper, _, account := keeperTestService(t)
				account.GroupIDs = []int64{11}
				cfg := keeper.config.Load().OpenAIStateKeeperSettings
				cfg.InjectionEnabled = injection
				require.NoError(t, keeper.Save(context.Background(), cfg))
				q := DefaultAccountQualitySettings()
				q.Model, q.ModelAuditModel = "question-model", cfg.Model
				q.UpdatedAt = time.Now().Add(-time.Hour)
				response := fmt.Sprintf("data: {\"type\":\"response.completed\",\"response\":{\"model\":%q}}\n\n", q.ModelAuditModel)
				u := &queuedHTTPUpstream{responses: []*http.Response{newJSONResponse(200, response)}}
				s := &AccountQualityService{usage: &UsageService{usageRepo: &qualityModelAuditStub{logs: qualityModelTestLogs(account.ID, q.ModelAuditModel, count, time.Now())}}, tests: &AccountTestService{accountRepo: &qualityAccountRepo{account: account}, httpUpstream: u}}
				s.stateKeeper.Store(keeper)
				v := AccountQualityResult{AccountID: account.ID}
				s.checkQualityModel(context.Background(), q, &v)
				if count == 3 {
					require.Empty(t, u.requests)
					require.Equal(t, 3, v.Model.Failures)
					s.checkQualityModel(context.Background(), q, &v)
					require.Empty(t, u.requests, "rechecking three old logs must not trigger a probe")
					require.Equal(t, 3, v.Model.Failures, "old logs cannot count twice")
					return
				}
				require.Len(t, u.requests, 1)
				require.Equal(t, 1, v.Model.Successes)
				require.Zero(t, v.Model.Failures, "partial logs are replaced by one fresh probe")
				require.Zero(t, v.Model.LatestID, "direct samples do not invent usage log IDs")
				require.Equal(t, q.ModelAuditModel, v.Model.ResponseModel)
				body, err := io.ReadAll(u.requests[0].Body)
				require.NoError(t, err)
				require.Equal(t, "hi", gjson.GetBytes(body, "input.0.content.0.text").String())
				require.Equal(t, q.ModelAuditModel, gjson.GetBytes(body, "model").String())
				wantState := ""
				if injection {
					wantState = "collected-secret"
				}
				require.Equal(t, wantState, u.requests[0].Header.Get(openAICodexTurnStateHeader))
			})
		}
	}
}

func TestAccountQualityModelHiVerdictsAndFailures(t *testing.T) {
	defer setForceUpstreamWSForTest(false)()
	for _, tc := range []struct {
		name, body, status string
		code               int
		queryErr           error
	}{
		{"matching", "data: {\"type\":\"response.completed\",\"response\":{\"model\":\"gpt-6-astra\"}}\n\n", "normal", 200, nil},
		{"mismatch", "data: {\"type\":\"response.completed\",\"response\":{\"model\":\"other-model\"}}\n\n", "degraded", 200, nil},
		{"variant", "data: {\"type\":\"response.completed\",\"response\":{\"model\":\"gpt-6-astra-2026-09-19\"}}\n\n", "variant", 200, nil},
		{"conflict", "data: {\"type\":\"response.created\",\"response\":{\"model\":\"wrong-model\"}}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"model\":\"gpt-6-astra\"}}\n\n", "degraded", 200, nil},
		{"missing-model", "data: {\"type\":\"response.completed\"}\n\n", "error", 200, nil},
		{"incomplete", "data: {\"type\":\"response.created\",\"response\":{\"model\":\"gpt-6-astra\"}}\n\n", "error", 200, nil},
		{"unauthorized", "unauthorized", "error", 401, nil},
		{"rate-limited", "rate limited", "error", 429, nil},
		{"query-failed", "", "error", 200, errors.New("log query unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := DefaultAccountQualitySettings()
			q.UpdatedAt = time.Now().Add(-time.Hour)
			account := keeperTestAccount(1)
			repo := &qualityModelAccountRepo{qualityAccountRepo: qualityAccountRepo{account: account}}
			u := &queuedHTTPUpstream{responses: []*http.Response{newJSONResponse(tc.code, tc.body), newJSONResponse(tc.code, tc.body)}}
			s := &AccountQualityService{usage: &UsageService{usageRepo: &qualityModelAuditStub{err: tc.queryErr}}, tests: &AccountTestService{accountRepo: repo, httpUpstream: u}}
			collected := time.Now().Add(-time.Minute)
			v := AccountQualityResult{AccountID: 1, Model: QualityModelResult{StateCollectedAt: &collected, StateValidationPending: true}}
			s.checkQualityModel(context.Background(), q, &v)
			require.True(t, v.Model.StateValidationPending, "one probe cannot bypass consecutive thresholds")
			s.checkQualityModel(context.Background(), q, &v)
			require.Equal(t, tc.status, v.Model.Status)
			if tc.status == "error" {
				require.NotEmpty(t, v.Model.Error)
				require.True(t, v.Model.StateValidationPending)
				require.Zero(t, v.Model.Successes+v.Model.Failures)
				require.Equal(t, v.Model.CheckedAt.Add(time.Duration(q.RetrySeconds)*time.Second), *v.Model.NextAt)
			} else {
				require.False(t, v.Model.StateValidationPending)
				require.Equal(t, tc.status == "degraded", v.Model.Degraded)
				require.Equal(t, 2, v.Model.Successes+v.Model.Failures)
			}
			if tc.queryErr != nil {
				require.Empty(t, u.requests, "a query error is not evidence of missing logs")
			} else {
				require.Len(t, u.requests, 2)
			}
			if tc.code == 401 {
				require.Equal(t, 2, repo.errors)
			} else {
				require.Zero(t, repo.errors)
			}
		})
	}
}

func TestAccountQualityModelInvalidLogsTriggerHi(t *testing.T) {
	defer setForceUpstreamWSForTest(false)()
	for _, mode := range []string{"before-state", "no-start-time", "old-config", "other-model", "no-response-model", "no-verdict", "other-account"} {
		t.Run(mode, func(t *testing.T) {
			q := DefaultAccountQualitySettings()
			q.UpdatedAt = time.Now().Add(-time.Hour)
			collected := time.Now().Add(-time.Minute)
			logs := qualityModelTestLogs(1, q.ModelAuditModel, 3, time.Now())
			switch mode {
			case "before-state":
				before := collected.Add(-time.Second)
				logs[0].RequestStartedAt = &before
			case "no-start-time":
				logs[0].RequestStartedAt = nil
			case "old-config":
				logs[0].CreatedAt = q.UpdatedAt.Add(-time.Second)
			case "other-model":
				logs[0].SentModel = "other-model"
			case "no-response-model":
				logs[0].ResponseModel = ""
			case "no-verdict":
				logs[0].Mismatch = nil
			case "other-account":
				logs[0].AccountID = 2
			}
			u := &queuedHTTPUpstream{responses: []*http.Response{newJSONResponse(200, "data: {\"type\":\"response.completed\",\"response\":{\"model\":\"gpt-6-astra\"}}\n\n")}}
			s := &AccountQualityService{usage: &UsageService{usageRepo: &qualityModelAuditStub{logs: logs}}, tests: &AccountTestService{accountRepo: &qualityAccountRepo{account: keeperTestAccount(1)}, httpUpstream: u}}
			v := AccountQualityResult{AccountID: 1, Model: QualityModelResult{StateCollectedAt: &collected, StateValidationPending: true}}
			s.checkQualityModel(context.Background(), q, &v)
			require.Len(t, u.requests, 1)
			require.Equal(t, "state_pending", v.Model.Status)
			require.Equal(t, 1, v.Model.Successes)
			require.Zero(t, v.Model.Failures)
		})
	}
}

func TestAccountQualityModelHiPersistsAndRejectsNewerState(t *testing.T) {
	defer setForceUpstreamWSForTest(false)()
	for _, refreshDuringRequest := range []bool{false, true} {
		t.Run(fmt.Sprint(refreshDuringRequest), func(t *testing.T) {
			db := qualitySchedulingDB(t)
			_, keeper, q, _ := qualityCollectedStateFixture(t)
			u := &qualityRecoveryHTTP{do: func(*http.Request) (*http.Response, error) {
				if refreshDuringRequest {
					keeper.run(openAIStateKeeperJob{accountID: 1, revision: keeper.config.Load().Revision})
				}
				return newJSONResponse(200, "data: {\"type\":\"response.completed\",\"response\":{\"model\":\"gpt-6-astra\"}}\n\n"), nil
			}}
			account := keeperTestAccount(1)
			account.GroupIDs = []int64{11}
			s := &AccountQualityService{db: db, settings: &SettingService{settingRepo: &qualityPGSettings{db: db}}, usage: &UsageService{usageRepo: &qualityModelAuditStub{}}, tests: &AccountTestService{accountRepo: &qualityAccountRepo{account: account}, httpUpstream: u}}
			s.stateKeeper.Store(keeper)
			q.RecoveryLimit = 1
			q, err := s.SaveSettings(context.Background(), q)
			require.NoError(t, err)
			v := AccountQualityResult{AccountID: 1, Revision: q.Revision}
			require.NoError(t, s.probe(context.Background(), q, v, 1))
			var raw []byte
			require.NoError(t, db.QueryRow(`SELECT payload FROM account_quality_states WHERE account_id=1`).Scan(&raw))
			require.NoError(t, json.Unmarshal(raw, &v))
			if refreshDuringRequest {
				require.Equal(t, "state_pending", v.Model.Status)
				require.Zero(t, v.Model.Successes)
			} else {
				require.Equal(t, "normal", v.Model.Status)
				require.Equal(t, 1, v.Model.Successes)
			}
			history, err := s.History(context.Background(), 1, 10)
			require.NoError(t, err)
			require.Len(t, history, 1)
			require.Equal(t, "model", history[0].DetectionKind)
		})
	}
}
