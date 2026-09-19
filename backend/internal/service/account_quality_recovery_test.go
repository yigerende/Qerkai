//go:build unit

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type qualityRecoveryHTTP struct {
	HTTPUpstream
	calls atomic.Int32
	do    func(*http.Request) (*http.Response, error)
}

func (u *qualityRecoveryHTTP) DoWithTLS(req *http.Request, _ string, _ int64, _ int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	u.calls.Add(1)
	return u.do(req)
}

func qualityRecoveryFixture(t *testing.T) (*AccountQualityService, *OpenAIStateKeeperService, *Account, *qualityRecoveryHTTP, AccountQualitySettings) {
	t.Helper()
	db := qualitySchedulingDB(t)
	keeper, _, a := keeperTestService(t)
	a.GroupIDs, a.Schedulable = []int64{11}, true
	keeper.files = keeperTestFileStore(t)
	keeper.run(openAIStateKeeperJob{accountID: a.ID, revision: keeper.config.Load().Revision})
	u := &qualityRecoveryHTTP{}
	u.do = func(req *http.Request) (*http.Response, error) {
		require.Equal(t, "collected-secret", req.Header.Get(openAICodexTurnStateHeader))
		body, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		model := gjson.GetBytes(body, "model").String()
		payload := `data: {"type":"response.output_text.delta","delta":"21"}` + "\n\n" +
			`data: {"type":"response.completed","response":{"model":"` + model + `"}}` + "\n\n"
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(payload))}, nil
	}
	svc := &AccountQualityService{db: db, settings: &SettingService{settingRepo: &qualityPGSettings{db: db}},
		tests: &AccountTestService{accountRepo: &qualityAccountRepo{account: a}, httpUpstream: u}, recoveryWake: make(chan struct{}, 1)}
	svc.stateKeeper.Store(keeper)
	keeper.quality = svc
	q := DefaultAccountQualitySettings()
	q.Enabled, q.ModelAuditEnabled, q.PauseOnDegradation = true, true, true
	q.DegradationConditions, q.Mode = []string{"question", "model"}, "content"
	q.Questions = q.Questions[:1]
	q, err := svc.SaveSettings(context.Background(), q)
	require.NoError(t, err)
	now := time.Now().UTC()
	v := AccountQualityResult{AccountID: a.ID, Revision: q.Revision,
		Question: QualityQuestionResult{QualityVerdict: QualityVerdict{Status: "degraded", Degraded: true, CheckedAt: &now}},
	}
	require.NoError(t, svc.saveResult(context.Background(), q, v, 0))
	require.True(t, readQualityRecovery(t, svc).Scheduling.Paused)
	return svc, keeper, a, u, q
}

func readQualityRecovery(t *testing.T, s *AccountQualityService) AccountQualityResult {
	t.Helper()
	var raw []byte
	require.NoError(t, s.db.QueryRow(`SELECT payload FROM account_quality_states WHERE account_id=1`).Scan(&raw))
	var v AccountQualityResult
	require.NoError(t, json.Unmarshal(raw, &v))
	return v
}

func TestAccountQualityRecoveryWithoutUserLogsAndCollectionWake(t *testing.T) {
	defer setForceUpstreamWSForTest(false)()
	s, keeper, a, u, q := qualityRecoveryFixture(t)
	ctx := context.Background()
	require.NoError(t, s.runQualityRecovery(ctx))
	v := readQualityRecovery(t, s)
	require.True(t, v.Scheduling.Paused)
	require.Equal(t, 1, v.Scheduling.Successes)
	require.Equal(t, q.ModelAuditModel, v.Model.ResponseModel)
	require.EqualValues(t, 1, u.calls.Load(), "matching models share a single answer probe")
	require.NoError(t, s.runQualityRecovery(ctx))
	require.EqualValues(t, 1, u.calls.Load(), "periodic checks respect the retry interval")
	keeper.run(openAIStateKeeperJob{accountID: a.ID, revision: keeper.config.Load().Revision})
	require.NoError(t, s.runQualityRecovery(ctx))
	v = readQualityRecovery(t, s)
	require.Equal(t, 1, v.Scheduling.Successes, "a new State resets previous recovery evidence")
	require.True(t, v.Scheduling.Paused)
	// The persisted pause and streak survive a new service instance.
	restarted := &AccountQualityService{db: s.db, settings: s.settings, tests: s.tests, recoveryWake: make(chan struct{}, 1)}
	restarted.stateKeeper.Store(keeper)
	restarted.notifyQualityCollection(a.ID)
	require.NoError(t, restarted.runQualityRecovery(ctx))
	v = readQualityRecovery(t, s)
	require.False(t, v.Scheduling.Paused)
	require.Equal(t, q.RecoveryLimit, v.Scheduling.Successes)
	require.Equal(t, "normal", v.Overall.Status)
	history, err := s.History(ctx, a.ID, 20)
	require.NoError(t, err)
	require.Equal(t, "recovery", history[0].DetectionKind)
}

func TestAccountQualityRecoveryFailuresCannotResume(t *testing.T) {
	for _, mode := range []string{"wrong-answer", "wrong-model", "missing-model", "transport", "401-account", "new-state-in-flight", "disabled-in-flight"} {
		t.Run(mode, func(t *testing.T) {
			s, keeper, a, u, q := qualityRecoveryFixture(t)
			original := u.do
			u.do = func(req *http.Request) (*http.Response, error) {
				if mode == "transport" {
					return nil, context.DeadlineExceeded
				}
				if mode == "new-state-in-flight" {
					keeper.run(openAIStateKeeperJob{accountID: a.ID, revision: keeper.config.Load().Revision})
				}
				if mode == "disabled-in-flight" {
					q.PauseOnDegradation = false
					_, err := s.SaveSettings(context.Background(), q)
					require.NoError(t, err)
				}
				resp, err := original(req)
				require.NoError(t, err)
				body, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				if mode == "wrong-answer" {
					body = []byte(strings.ReplaceAll(string(body), `"21"`, `"20"`))
				}
				if mode == "wrong-model" {
					body = []byte(strings.ReplaceAll(string(body), q.ModelAuditModel, "different-model"))
				}
				if mode == "missing-model" {
					body = []byte(strings.ReplaceAll(string(body), q.ModelAuditModel, ""))
				}
				resp.Body = io.NopCloser(strings.NewReader(string(body)))
				return resp, nil
			}
			if mode == "401-account" {
				a.Status = StatusError
			}
			require.NoError(t, s.runQualityRecovery(context.Background()))
			v := readQualityRecovery(t, s)
			require.True(t, v.Scheduling.Paused)
			require.Zero(t, v.Scheduling.Successes)
			if mode == "401-account" {
				require.Zero(t, u.calls.Load())
				require.NotEmpty(t, v.Scheduling.Error)
				require.NotNil(t, v.Scheduling.NextAt)
			}
		})
	}
}

func TestAccountQualityRecoveryDifferentModelsAndInjectionOff(t *testing.T) {
	s, keeper, a, u, q := qualityRecoveryFixture(t)
	q.ModelAuditModel = "gpt-5.5"
	var err error
	q, err = s.SaveSettings(context.Background(), q)
	require.NoError(t, err)
	settings := keeper.config.Load().OpenAIStateKeeperSettings
	settings.InjectionEnabled = false
	require.NoError(t, keeper.Save(context.Background(), settings))
	models := []string{}
	u.do = func(req *http.Request) (*http.Response, error) {
		require.Empty(t, req.Header.Get(openAICodexTurnStateHeader))
		body, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		model := gjson.GetBytes(body, "model").String()
		models = append(models, model)
		return newJSONResponse(200, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"21\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"model\":\""+model+"\"}}\n\n"), nil
	}
	for i := 0; i < q.RecoveryLimit; i++ {
		s.notifyQualityCollection(a.ID)
		require.NoError(t, s.runQualityRecovery(context.Background()))
	}
	require.Equal(t, []string{q.Model, q.ModelAuditModel, q.Model, q.ModelAuditModel}, models)
	require.False(t, readQualityRecovery(t, s).Scheduling.Paused)
}

func TestAccountQualityPauseIsSeparateFromManualAndCredentialState(t *testing.T) {
	for _, schedulable := range []bool{true, false} {
		for _, status := range []string{StatusActive, StatusError, StatusDisabled} {
			a := keeperTestAccount(1)
			a.Schedulable, a.Status = schedulable, status
			a.Extra = map[string]any{QualitySchedulingPausedExtraKey: true}
			require.False(t, a.IsSchedulable())
			a.Extra[QualitySchedulingPausedExtraKey] = false
			require.Equal(t, schedulable && status == StatusActive, a.IsSchedulable())
		}
	}
}

func TestAccountQualityPauseBlocksFreshAndStickyHTTPAndWSSelection(t *testing.T) {
	for _, advanced := range []string{"false", "true"} {
		for _, transport := range []OpenAIUpstreamTransport{OpenAIUpstreamTransportAny, OpenAIUpstreamTransportResponsesWebsocketV2} {
			for _, sticky := range []string{"", "session", "response"} {
				t.Run(fmt.Sprintf("%s/%s/%s", advanced, transport, sticky), func(t *testing.T) {
					account := *keeperTestAccount(1)
					account.Schedulable, account.GroupIDs = true, []int64{11}
					account.Extra = map[string]any{"responses_websockets_v2_enabled": true, QualitySchedulingPausedExtraKey: true}
					cache := &schedulerTestGatewayCache{sessionBindings: map[string]int64{"openai:paused-session": 1}}
					gateway := &OpenAIGatewayService{accountRepo: schedulerTestOpenAIAccountRepo{accounts: []Account{account}}, cache: cache,
						cfg: newSchedulerTestOpenAIWSV2Config(), rateLimitService: newOpenAIAdvancedSchedulerRateLimitService(advanced),
						concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{})}
					group := int64(11)
					require.NoError(t, gateway.getOpenAIWSStateStore().BindResponseAccount(context.Background(), group, "resp_paused", 1, time.Hour))
					session, previous := "", ""
					if sticky == "session" {
						session = "paused-session"
					} else if sticky == "response" {
						previous = "resp_paused"
					}
					selection, _, err := gateway.SelectAccountWithScheduler(context.Background(), &group, previous, session, "gpt-6-astra", nil, transport, false)
					require.Error(t, err)
					require.Nil(t, selection)
					account.Extra[QualitySchedulingPausedExtraKey] = false
					selection, _, err = gateway.SelectAccountWithScheduler(context.Background(), &group, "", "", "gpt-6-astra", nil, transport, false)
					require.NoError(t, err)
					require.EqualValues(t, 1, selection.Account.ID)
					if selection.ReleaseFunc != nil {
						selection.ReleaseFunc()
					}
				})
			}
		}
	}
}

type qualityRecoveryManyAccounts struct{ AccountRepository }

func (qualityRecoveryManyAccounts) GetByID(_ context.Context, id int64) (*Account, error) {
	return keeperTestAccount(id), nil
}

func TestAccountQualityRecoveryHundredAccountsBounded(t *testing.T) {
	db := qualitySchedulingDB(t)
	u := &qualityRecoveryHTTP{}
	var active, maximum atomic.Int32
	u.do = func(req *http.Request) (*http.Response, error) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := maximum.Load(); old < n; old = maximum.Load() {
			if maximum.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		return newJSONResponse(200, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"21\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"model\":\"gpt-6-astra\"}}\n\n"), nil
	}
	s := &AccountQualityService{db: db, settings: &SettingService{settingRepo: &qualityPGSettings{db: db}},
		tests: &AccountTestService{accountRepo: qualityRecoveryManyAccounts{}, httpUpstream: u}, recoveryWake: make(chan struct{}, 1)}
	q := DefaultAccountQualitySettings()
	q.Enabled, q.ModelAuditEnabled, q.PauseOnDegradation, q.Mode, q.Concurrency = true, true, true, "content", 4
	q.Questions = q.Questions[:1]
	q, err := s.SaveSettings(context.Background(), q)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO accounts(id,platform) SELECT id,'openai' FROM generate_series(63,100) id`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO account_quality_states(account_id,revision,version,payload)
 SELECT id,$1,'',jsonb_build_object('account_id',id,'revision',$1::text,'scheduling',jsonb_build_object('paused',true)) FROM accounts`, q.Revision)
	require.NoError(t, err)
	for i := 0; i < q.RecoveryLimit; i++ {
		for id := int64(1); id <= 100; id++ {
			s.notifyQualityCollection(id)
		}
		require.NoError(t, s.runQualityRecovery(context.Background()))
	}
	var paused int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM account_quality_states WHERE payload->'scheduling'->>'paused'='true'`).Scan(&paused))
	require.Zero(t, paused)
	require.EqualValues(t, 200, u.calls.Load())
	require.Greater(t, maximum.Load(), int32(1))
	require.LessOrEqual(t, maximum.Load(), int32(q.Concurrency))
}
