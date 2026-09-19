package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func keeperReauthFixture(t *testing.T) (*OpenAIStateKeeperService, *Account) {
	t.Helper()
	s, _, a := keeperTestService(t)
	a.Credentials["chatgpt_user_id"] = "user-one"
	a.GroupIDs = []int64{11}
	s.files = keeperTestFileStore(t)
	q := s.Snapshot().Settings
	q.Models = []string{q.Model, "second-model"}
	require.NoError(t, s.Save(context.Background(), q))
	require.NoError(t, s.Schedule(nil))
	for len(s.queue) > 0 {
		s.run(<-s.queue)
	}
	return s, a
}

func TestStateKeeperReauthPolicyDefaultsForOldSettings(t *testing.T) {
	q := DefaultOpenAIStateKeeperSettings()
	require.NoError(t, json.Unmarshal([]byte(`{"injection_enabled":true}`), &q))
	require.True(t, q.SuspendOldStateOnReauth)
	require.NoError(t, json.Unmarshal([]byte(`{"suspend_old_state_on_reauth":false}`), &q))
	require.False(t, q.SuspendOldStateOnReauth)
}

func TestStateKeeperReauthPolicyControlsHTTPWSAndQuality(t *testing.T) {
	for _, suspend := range []bool{true, false} {
		for _, inject := range []bool{true, false} {
			t.Run(fmt.Sprintf("suspend=%t/inject=%t", suspend, inject), func(t *testing.T) {
				s, a := keeperReauthFixture(t)
				q := s.Snapshot().Settings
				q.SuspendOldStateOnReauth, q.InjectionEnabled = suspend, inject
				require.NoError(t, s.Save(context.Background(), q))
				a.Credentials["access_token"] = "reauthorized-token"
				// Injection must apply the policy immediately, before the next poll.
				expected := "native-state"
				if inject && !suspend {
					expected = "collected-secret"
				}
				for _, model := range q.Models {
					body := []byte(fmt.Sprintf(`{"model":%q,"stream":true}`, model))
					req := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
					req.Header.Set(openAICodexTurnStateHeader, "native-state")
					out := s.gateway.prepareCollectedStateHTTP(keeperTestContext(11), a, body, req)
					require.Equal(t, expected, out.Header.Get(openAICodexTurnStateHeader))
					if !inject {
						require.Same(t, req, out)
					}
					headers := http.Header{"X-Codex-Turn-State": {"native-state"}}
					ticket := s.gateway.prepareCollectedStateWS(keeperTestContext(11), a, model, headers)
					require.Equal(t, expected, headers.Get(openAICodexTurnStateHeader))
					require.Equal(t, inject && !suspend, ticket.poolVersion() != "")
					qualityHeaders := http.Header{"X-Codex-Turn-State": {"native-state"}}
					s.prepareQualityState(a, model, qualityHeaders)
					require.Equal(t, expected, qualityHeaders.Get(openAICodexTurnStateHeader))
				}
				require.Empty(t, s.queue, "business requests never start or wait for collection")
			})
		}
	}
}

func TestStateKeeperReauthCollectsAllModelsWithoutPriorPauseAndDeduplicates(t *testing.T) {
	s, a := keeperReauthFixture(t)
	a.Status = StatusError
	require.NoError(t, s.SyncSelection(context.Background()))
	require.False(t, s.Snapshot().Rows[0].Paused, "account status alone did not pause an idle collector")
	a.Credentials["access_token"], a.Status = "external-api-relogin", StatusActive
	require.NoError(t, s.SyncSelection(context.Background()))
	next := *s.Snapshot().Rows[0].NextRetryAt
	for range 3 {
		require.NoError(t, s.SyncSelection(context.Background()))
		require.NoError(t, s.Save(context.Background(), s.Snapshot().Settings))
		require.Equal(t, next, *s.Snapshot().Rows[0].NextRetryAt)
		s.scheduleDue(time.Now().Add(time.Second))
	}
	require.Len(t, s.queue, 2)
	for _, row := range s.Snapshot().Rows[0].Models {
		require.Equal(t, "credentials_updated", row.RoundSource)
		require.False(t, row.AccountUnavailable)
	}
	for len(s.queue) > 0 {
		s.run(<-s.queue)
	}
	require.NoError(t, s.SyncSelection(context.Background()))
	s.scheduleDue(time.Now().Add(time.Hour))
	require.Empty(t, s.queue)
	for _, model := range s.Snapshot().Settings.Models {
		r, err := s.files.load(a.ID, s.Snapshot().Settings.forModel(model))
		require.NoError(t, err)
		require.Equal(t, stateKeeperCredentialStamp(a), r.CredentialStamp)
		require.Equal(t, "collected-secret", s.valueFor(keeperTestContext(11), a, model))
	}
}

func TestStateKeeperReauthLateStateOr401CannotOverwriteNewCredentials(t *testing.T) {
	for _, status := range []int{200, 401} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			s, a := keeperReauthFixture(t)
			model := s.Snapshot().Settings.Model
			stamp := stateKeeperCredentialStamp(a)
			started, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
			s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
				close(started)
				<-release
				return openAIStateProbeResult{status: status, result: "collected", value: "late-old-state", credentialStamp: stamp}
			}
			go func() {
				s.run(openAIStateKeeperJob{accountID: a.ID, model: model, revision: s.config.Load().Revision, source: "manual"})
				close(finished)
			}()
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("collection did not start")
			}
			a.Credentials["access_token"] = "new-token-during-old-request"
			// Do not poll here: result publication must notice the repository update.
			close(release)
			select {
			case <-finished:
			case <-time.After(5 * time.Second):
				t.Fatal("old round did not drain")
			}
			e := s.entryLocked(a.ID, model)
			require.Equal(t, "collected-secret", e.value)
			require.False(t, e.row.AccountUnavailable)
			require.True(t, e.row.AutoRetryPending)
			require.Equal(t, "credentials_updated", e.row.RoundSource)
			r, err := s.files.load(a.ID, s.Snapshot().Settings.forModel(model))
			require.NoError(t, err)
			require.Equal(t, "collected-secret", r.Value)
			s.probe = func(context.Context, OpenAIStateKeeperSettings, int64) openAIStateProbeResult {
				return openAIStateProbeResult{status: 200, result: "collected", value: "fresh-state", credentialStamp: stateKeeperCredentialStamp(a)}
			}
			s.scheduleDue(time.Now().Add(time.Second))
			require.Len(t, s.queue, 2)
			for len(s.queue) > 0 {
				s.run(<-s.queue)
			}
			require.Equal(t, "fresh-state", s.valueFor(keeperTestContext(11), a, model))
		})
	}
}

func TestStateKeeperReauthPersistsIntentAndOldStatePolicyAcrossRestart(t *testing.T) {
	for _, suspend := range []bool{true, false} {
		t.Run(fmt.Sprint(suspend), func(t *testing.T) {
			s, a := keeperReauthFixture(t)
			q := s.Snapshot().Settings
			q.SuspendOldStateOnReauth = suspend
			require.NoError(t, s.Save(context.Background(), q))
			a.Credentials["access_token"] = "changed-while-stopped"
			restart := func() *OpenAIStateKeeperService {
				r := newOpenAIStateKeeper(s.settings, s.accounts, s.proxies, s.gateway)
				t.Cleanup(r.Stop)
				r.files = s.files
				r.reload(context.Background())
				require.NoError(t, r.SyncSelection(context.Background()))
				r.restoreStateFiles(context.Background())
				return r
			}
			r := restart()
			for _, model := range q.Models {
				e := r.entryLocked(a.ID, model)
				require.True(t, e.row.AutoRetryPending)
				require.Equal(t, !suspend, r.valueFor(keeperTestContext(11), a, model) != "")
				require.NoError(t, r.persistRuntime(a.ID, model))
			}
			again := restart()
			again.scheduleDue(time.Now().Add(time.Second))
			require.Len(t, again.queue, 2)
			require.NoError(t, again.SyncSelection(context.Background()))
			again.scheduleDue(time.Now().Add(time.Second))
			require.Len(t, again.queue, 2)
		})
	}
}

func TestStateKeeperReauthCannotBorrowStateAcrossOwners(t *testing.T) {
	for _, change := range []string{"team", "user", "missing-identity"} {
		t.Run(change, func(t *testing.T) {
			s, a := keeperReauthFixture(t)
			q := s.Snapshot().Settings
			q.SuspendOldStateOnReauth = false
			require.NoError(t, s.Save(context.Background(), q))
			a.Credentials["access_token"] = "different-login"
			switch change {
			case "team":
				a.Credentials["chatgpt_account_id"] = "other-team"
			case "user":
				a.Credentials["chatgpt_user_id"] = "other-user-same-team"
			case "missing-identity":
				delete(a.Credentials, "chatgpt_user_id")
			}
			require.Empty(t, s.valueFor(keeperTestContext(11), a, q.Model))
		})
	}
}

func TestStateKeeperReauthRespectsBudgetsPausesAndDisabledCollection(t *testing.T) {
	for _, mode := range []string{"429", "five-minute", "ten-minute", "hourly", "file-error", "disabled", "unavailable"} {
		t.Run(mode, func(t *testing.T) {
			s, a := keeperReauthFixture(t)
			q := s.Snapshot().Settings
			q.AccountFiveMinuteLimit, q.AccountTenMinuteLimit = 5, 10
			q.Enabled = mode != "disabled"
			require.NoError(t, s.Save(context.Background(), q))
			now := time.Now().UTC()
			limit := s.collectionLimitLocked(a.ID)
			switch mode {
			case "429":
				limit.CooldownUntil = now.Add(time.Minute)
			case "five-minute":
				limit.FiveMinute = openAIStateBudgetWindow{StartedAt: now, Requests: 5}
			case "ten-minute":
				limit.TenMinute = openAIStateBudgetWindow{StartedAt: now, Requests: 10}
			case "hourly":
				limit.WindowStartedAt, limit.WindowRequests = now, q.AccountHourlyLimit
			case "file-error":
				for _, e := range s.rows {
					e.row.Paused, e.row.PauseReason = true, "采集轮次文件无法读取，等待人工重试"
				}
			case "unavailable":
				a.Status = StatusError
			}
			a.Credentials["access_token"] = "updated-token"
			require.NoError(t, s.SyncSelection(context.Background()))
			s.scheduleDue(now.Add(time.Second))
			require.Empty(t, s.queue)
			if mode == "disabled" {
				q.Enabled = true
				require.NoError(t, s.Save(context.Background(), q))
				s.scheduleDue(now.Add(time.Second))
				require.Len(t, s.queue, 2)
			}
		})
	}
}
