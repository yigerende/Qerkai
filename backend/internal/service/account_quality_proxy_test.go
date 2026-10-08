//go:build unit

package service

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/stretchr/testify/require"
)

type qualityProxyUpstream struct {
	proxies []string
	err     error
}

func (u *qualityProxyUpstream) Do(req *http.Request, proxy string, _ int64, _ int) (*http.Response, error) {
	u.proxies = append(u.proxies, proxy)
	if u.err != nil {
		return nil, u.err
	}
	if req.URL.Host == "bps.openai.com" {
		return newJSONResponse(200, strings.ReplaceAll(bpsTextFixture, `  hello\n`, "21")), nil
	}
	return newJSONResponse(200, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"21\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"model\":\"gpt-6-astra\"}}\n\n"), nil
}

func (u *qualityProxyUpstream) DoWithTLS(req *http.Request, proxy string, id int64, concurrency int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return u.Do(req, proxy, id, concurrency)
}

func qualityProxyFixture(t *testing.T, bps bool) (*AccountQualityService, *Account, *qualityProxyUpstream) {
	t.Helper()
	gateway, account := bpsTestGateway(t)
	account.GroupIDs = []int64{11}
	account.ProxyID = new(int64(41))
	account.Proxy = &Proxy{ID: 41, Protocol: "http", Host: "proxy.example", Port: 8080, Username: "test-user", Password: "test-password"}
	cfg := gateway.settingService.bpsSettings(context.Background()).settings
	cfg.Enabled, cfg.UpstreamTransport = bps, "http"
	gateway.settingService.bpsSettingsCache.Store(newCachedOpenAIBPS(cfg, time.Hour))
	u := &qualityProxyUpstream{}
	s := &AccountQualityService{tests: &AccountTestService{
		accountRepo: &qualityAccountRepo{account: account}, httpUpstream: u,
		bpsGateway: gateway, settingService: gateway.settingService,
	}}
	return s, account, u
}

func TestAccountQualityRequestsHonorAccountProxy(t *testing.T) {
	for _, route := range []string{"original", "bps"} {
		for _, scenario := range []string{"http", "https", "socks5", "socks5h", "no-proxy", "missing-proxy", "wrong-proxy", "connection-error"} {
			t.Run(route+"/"+scenario, func(t *testing.T) {
				s, account, upstream := qualityProxyFixture(t, route == "bps")
				want := account.Proxy.URL()
				switch scenario {
				case "https", "socks5", "socks5h":
					account.Proxy.Protocol = scenario
					want = account.Proxy.URL()
				case "no-proxy":
					account.ProxyID, account.Proxy, want = nil, nil, ""
				case "missing-proxy":
					account.Proxy = nil
				case "wrong-proxy":
					account.Proxy.ID++
				case "connection-error":
					upstream.err = errors.New("proxy connection refused")
				}
				q := DefaultAccountQualitySettings()
				answer, _, err := s.testAnswer(context.Background(), account.ID, q, q.Questions[0])
				switch scenario {
				case "missing-proxy", "wrong-proxy":
					require.ErrorContains(t, err, "代理")
					require.Empty(t, upstream.proxies, "an unresolved account proxy must never turn into a direct request")
				case "connection-error":
					require.ErrorContains(t, err, "proxy connection refused")
					require.Equal(t, []string{want}, upstream.proxies, "a failed proxy must not be retried directly")
				default:
					require.NoError(t, err)
					require.Equal(t, "21", answer)
					require.Equal(t, []string{want}, upstream.proxies)
				}
			})
		}
	}
}

func TestAccountQualityModelProbeHonorsAccountProxy(t *testing.T) {
	for _, route := range []string{"original", "bps"} {
		for _, scenario := range []string{"no-logs", "has-logs", "missing-proxy"} {
			t.Run(route+"/"+scenario, func(t *testing.T) {
				s, account, upstream := qualityProxyFixture(t, route == "bps")
				q := DefaultAccountQualitySettings()
				q.RecoveryLimit = 1
				q.UpdatedAt = time.Now().Add(-time.Hour)
				audit := &qualityBPSAudit{}
				if scenario == "has-logs" {
					audit.logs = qualityModelTestLogs(account.ID, q.ModelAuditModel, 1, time.Now())
					*audit.logs[0].Mismatch = false
					audit.logs[0].ResponseModel = q.ModelAuditModel
				} else if scenario == "missing-proxy" {
					account.Proxy = nil
				}
				s.usage = &UsageService{usageRepo: audit}
				result := AccountQualityResult{AccountID: account.ID}
				s.checkQualityModel(context.Background(), q, &result)
				if scenario == "missing-proxy" {
					require.Equal(t, "error", result.Model.Status)
					require.Contains(t, result.Model.Error, "代理")
					require.Empty(t, upstream.proxies)
					return
				}
				require.Equal(t, "normal", result.Model.Status)
				if scenario == "has-logs" {
					require.Empty(t, upstream.proxies, "existing log evidence must not send another request")
				} else {
					require.Equal(t, []string{account.Proxy.URL()}, upstream.proxies)
				}
			})
		}
	}
}

func TestAccountQualityReadsUpdatedAccountProxy(t *testing.T) {
	s, account, upstream := qualityProxyFixture(t, false)
	q := DefaultAccountQualitySettings()
	original := account.Proxy.URL()
	_, _, err := s.testAnswer(context.Background(), account.ID, q, q.Questions[0])
	require.NoError(t, err)
	// The next detection must load the account again, including a changed proxy.
	updated := *account
	updated.ProxyID = new(int64(42))
	updated.Proxy = &Proxy{ID: 42, Protocol: "socks5", Host: "new-proxy.example", Port: 1080}
	s.tests.accountRepo.(*qualityAccountRepo).account = &updated
	_, _, err = s.testAnswer(context.Background(), account.ID, q, q.Questions[0])
	require.NoError(t, err)
	require.Equal(t, []string{original, updated.Proxy.URL()}, upstream.proxies)
}

func TestAccountQualityRecoveryHonorsAccountProxy(t *testing.T) {
	for _, route := range []string{"original", "bps"} {
		t.Run(route, func(t *testing.T) {
			s, _, account, _, q := qualityRecoveryFixture(t)
			quality, configured, upstream := qualityProxyFixture(t, route == "bps")
			account.ProxyID, account.Proxy = configured.ProxyID, configured.Proxy
			quality.tests.accountRepo = &qualityAccountRepo{account: account}
			s.tests = quality.tests
			var want []string
			for range q.RecoveryLimit {
				s.notifyQualityCollection(account.ID)
				require.NoError(t, s.runQualityRecovery(context.Background()))
				want = append(want, account.Proxy.URL())
			}
			require.Equal(t, want, upstream.proxies)
			require.False(t, readQualityRecovery(t, s).Scheduling.Paused)
		})
	}
}
