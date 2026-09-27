//go:build unit

package service

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type qualityBPSHTTP struct {
	HTTPUpstream
	do func(*http.Request) (*http.Response, error)
}

func (u *qualityBPSHTTP) Do(r *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	return u.do(r)
}
func (u *qualityBPSHTTP) DoWithTLS(r *http.Request, _ string, _ int64, _ int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return u.do(r)
}

type qualityBPSAudit struct {
	UsageLogRepository
	input ModelAuditInput
	logs  []ModelAuditLog
}

func (r *qualityBPSAudit) LatestModelAudit(_ context.Context, input ModelAuditInput) ([]ModelAuditResult, error) {
	r.input = input
	return []ModelAuditResult{{AccountID: input.Accounts[0].AccountID, Logs: r.logs}}, nil
}

func TestAccountQualityBPSRoutingAndRealModelObservation(t *testing.T) {
	defer setForceUpstreamWSForTest(false)()
	for _, scenario := range []string{"enabled", "disabled", "other-group", "other-model", "all-groups", "alias", "mismatch"} {
		t.Run(scenario, func(t *testing.T) {
			gateway, account := bpsTestGateway(t)
			account.GroupIDs = []int64{11}
			q := DefaultAccountQualitySettings()
			cfg := gateway.settingService.bpsSettings(context.Background()).settings
			switch scenario {
			case "disabled":
				cfg.Enabled = false
			case "other-group":
				account.GroupIDs = []int64{12}
			case "other-model":
				q.Model = "gpt-5.6-sol"
			case "all-groups":
				cfg.GroupIDs = nil
				account.GroupIDs = nil
			case "alias":
				q.Model = "astra-bps"
				cfg.Models = []string{q.Model}
				cfg.ModelMappings = map[string]string{q.Model: "gpt-6-astra"}
			}
			gateway.settingService.bpsSettingsCache.Store(newCachedOpenAIBPS(cfg, time.Hour))
			wantBPS := scenario != "disabled" && scenario != "other-group" && scenario != "other-model"
			calls := 0
			returned := q.Model
			if wantBPS {
				returned = "gpt-6-astra"
			}
			if scenario == "mismatch" {
				returned = "gpt-5.6-luna"
			}
			u := &qualityBPSHTTP{do: func(r *http.Request) (*http.Response, error) {
				calls++
				assert.Equal(t, wantBPS, r.URL.Host == "bps.openai.com")
				raw, err := io.ReadAll(r.Body)
				if !assert.NoError(t, err) {
					return nil, err
				}
				assert.Contains(t, string(raw), "17 plus 4")
				effortField := "reasoning.effort"
				if wantBPS {
					effortField = "reasoning_effort"
				}
				assert.Equal(t, q.ReasoningEffort, gjson.GetBytes(raw, effortField).String())
				body := strings.ReplaceAll(bpsTextFixture, `  hello\n`, "21")
				body = strings.ReplaceAll(body, "gpt-6-astra", returned)
				if !wantBPS {
					body = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"21\"}\n\ndata: {\"type\":\"response.completed\",\"response\":" + body + "}\n\n"
				}
				return newJSONResponse(200, body), nil
			}}
			s := &AccountQualityService{tests: &AccountTestService{accountRepo: &qualityAccountRepo{account: account}, httpUpstream: u, bpsGateway: gateway, settingService: gateway.settingService}}
			observer := &upstreamResponseModelObserver{}
			answer, _, err := s.testAnswer(context.Background(), account.ID, q, QualityQuestion{Prompt: "What is 17 plus 4?"}, observer)
			require.NoError(t, err)
			require.Equal(t, "21", answer)
			require.Equal(t, returned, observer.Model())
			require.Equal(t, 1, calls)
		})
	}
}

func TestAccountQualityBPSAuditScopeAndNoLogsProbe(t *testing.T) {
	for _, logged := range []bool{false, true} {
		t.Run(map[bool]string{false: "probe", true: "logs"}[logged], func(t *testing.T) {
			gateway, account := bpsTestGateway(t)
			account.GroupIDs = []int64{11}
			q := DefaultAccountQualitySettings()
			q.UpdatedAt = time.Now().Add(-time.Hour)
			q.RecoveryLimit = 1
			audit := &qualityBPSAudit{}
			if logged {
				audit.logs = qualityModelTestLogs(account.ID, q.ModelAuditModel, 1, time.Now())
				*audit.logs[0].Mismatch = false
				audit.logs[0].ResponseModel = q.ModelAuditModel
			}
			calls := 0
			u := &qualityBPSHTTP{do: func(r *http.Request) (*http.Response, error) {
				calls++
				assert.Equal(t, "bps.openai.com", r.URL.Host)
				return newJSONResponse(200, bpsTextFixture), nil
			}}
			s := &AccountQualityService{usage: &UsageService{usageRepo: audit}, tests: &AccountTestService{accountRepo: &qualityAccountRepo{account: account}, httpUpstream: u, bpsGateway: gateway, settingService: gateway.settingService}}
			v := AccountQualityResult{AccountID: account.ID, Model: QualityModelResult{QualityVerdict: QualityVerdict{Degraded: true, Failures: 8}, LatestID: 500}}
			s.checkQualityModel(context.Background(), q, &v)
			require.True(t, audit.input.OnlyBPS)
			require.False(t, audit.input.ExcludeBPS)
			require.Equal(t, "normal", v.Model.Status)
			require.False(t, v.Model.Degraded)
			require.Zero(t, v.Model.Failures)
			require.Equal(t, openAIBPSEndpoint, v.Model.UpstreamEndpoint)
			require.Equal(t, map[bool]int{true: 0, false: 1}[logged], calls)
			cfg := gateway.settingService.bpsSettings(context.Background()).settings
			cfg.Enabled = false
			gateway.settingService.bpsSettingsCache.Store(newCachedOpenAIBPS(cfg, time.Hour))
			audit.logs = qualityModelTestLogs(account.ID, q.ModelAuditModel, 1, time.Now())
			s.checkQualityModel(context.Background(), q, &v)
			require.False(t, audit.input.OnlyBPS)
			require.True(t, audit.input.ExcludeBPS)
			require.Equal(t, "", v.Model.UpstreamEndpoint)
		})
	}
}

func TestAccountQualityBPSRecoveryUsesConfiguredEndpoint(t *testing.T) {
	s, _, account, _, q := qualityRecoveryFixture(t)
	gateway, _ := bpsTestGateway(t)
	s.tests.bpsGateway, s.tests.settingService = gateway, gateway.settingService
	calls := 0
	s.tests.httpUpstream = &qualityBPSHTTP{do: func(r *http.Request) (*http.Response, error) {
		calls++
		assert.Equal(t, "bps.openai.com", r.URL.Host)
		assert.Empty(t, r.Header.Get(openAICodexTurnStateHeader))
		return newJSONResponse(200, strings.ReplaceAll(bpsTextFixture, `  hello\n`, "21")), nil
	}}
	for n := 0; n < q.RecoveryLimit; n++ {
		s.notifyQualityCollection(account.ID)
		require.NoError(t, s.runQualityRecovery(context.Background()))
	}
	v := readQualityRecovery(t, s)
	require.Equal(t, q.RecoveryLimit, calls)
	require.False(t, v.Scheduling.Paused)
	require.Equal(t, "normal", v.Overall.Status)
	require.Equal(t, openAIBPSEndpoint, v.Model.UpstreamEndpoint)
}
