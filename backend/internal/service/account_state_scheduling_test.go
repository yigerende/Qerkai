//go:build unit

package service

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func enableStateSchedulingForTest(s *OpenAIStateKeeperService) {
	q := *s.config.Load()
	q.RequireValidState, q.SchedulingStateModel, q.SchedulingStateMinutes = true, q.Model, 55
	s.config.Store(&q)
}

func TestStateKeeperSchedulingValidity(t *testing.T) {
	for _, mode := range []string{"valid", "boundary", "before-boundary", "future", "missing", "other-model", "failed-refresh", "reauth", "reuse", "identity", "disabled-injection", "length", "scope", "401", "apikey", "off"} {
		t.Run(mode, func(t *testing.T) {
			s, a := keeperReauthFixture(t)
			enableStateSchedulingForTest(s)
			cfg := s.config.Load().OpenAIStateKeeperSettings
			e := s.entryLocked(a.ID, cfg.SchedulingStateModel)
			now := time.Now().UTC()
			collected := now.Add(-45 * time.Minute)
			e.row.CollectedAt = &collected
			want := "valid"
			switch mode {
			case "boundary":
				collected, want = now.Add(-55*time.Minute), "expired"
			case "before-boundary":
				collected = now.Add(-55*time.Minute + time.Nanosecond)
			case "future":
				collected, want = now.Add(time.Minute), "expired"
			case "missing":
				e.row.StateFileSaved, want = false, "missing"
			case "other-model":
				cfg.SchedulingStateModel, want = "not-collected", "missing"
			case "failed-refresh":
				e.row.Paused, e.row.Message = true, "collection failed"
			case "reauth", "reuse", "identity":
				a.Credentials["access_token"] = "new-token"
				cfg.SuspendOldStateOnReauth = mode == "reauth"
				if mode != "reuse" {
					want = "unavailable"
				}
				if mode == "identity" {
					a.Credentials["chatgpt_user_id"] = "other-user"
				}
			case "disabled-injection":
				cfg.InjectionEnabled, want = false, "unavailable"
			case "length":
				cfg.AllowedStateLengths, want = []int{332}, "unavailable"
			case "scope":
				cfg.GroupIDs, want = []int64{99}, "unavailable"
			case "401":
				a.Status, want = StatusError, "unavailable"
			case "apikey":
				a.Type = AccountTypeAPIKey
			case "off":
				cfg.RequireValidState = false
			}
			updated := *s.config.Load()
			updated.OpenAIStateKeeperSettings = cfg
			updated.groups = make(map[int64]bool)
			for _, id := range cfg.GroupIDs {
				updated.groups[id] = true
			}
			s.config.Store(&updated)
			v := s.schedulingValidity(a, now)
			if mode == "off" || mode == "apikey" {
				require.Nil(t, v)
				return
			}
			require.Equal(t, want, v.Status)
			if want == "valid" {
				require.Equal(t, collected.Add(55*time.Minute), *v.ExpiresAt)
				require.NotEmpty(t, v.Version)
			}
		})
	}
}

func TestStateKeeperSchedulingSettingsDefaultsAndDependencies(t *testing.T) {
	q := DefaultOpenAIStateKeeperSettings()
	require.NoError(t, json.Unmarshal([]byte(`{"enabled":false}`), &q))
	require.False(t, q.RequireValidState)
	require.Equal(t, "gpt-6-astra", q.SchedulingStateModel)
	require.Equal(t, 55, q.SchedulingStateMinutes)
	q.RequireValidState = true
	require.Error(t, q.Validate())
	p := DefaultAccountQualitySettings()
	require.Error(t, validateStateSchedulingQuality(p))
	p.Enabled, p.ModelAuditEnabled = true, true
	require.NoError(t, validateStateSchedulingQuality(p))
	p.AllGroups = false
	require.Error(t, validateStateSchedulingQuality(p))
}

func TestAccountQualityStateSchedulingRejectsDisablingRequiredDetection(t *testing.T) {
	s, keeper, _, _, q := qualityRecoveryFixture(t)
	keeper.settings = s.settings.settingRepo
	settings := keeper.config.Load().OpenAIStateKeeperSettings
	settings.RequireValidState = true
	require.NoError(t, keeper.Save(context.Background(), settings))
	for _, mode := range []string{"disabled", "no-model", "partial-groups"} {
		changed := q
		switch mode {
		case "disabled":
			changed.Enabled = false
		case "no-model":
			changed.ModelAuditEnabled, changed.DegradationConditions = false, []string{"question"}
		case "partial-groups":
			changed.AllGroups, changed.GroupIDs = false, []int64{11}
		}
		_, err := s.SaveSettings(context.Background(), changed)
		require.Error(t, err, mode)
	}
	settings.RequireValidState = false
	require.NoError(t, keeper.Save(context.Background(), settings))
	q.Enabled = false
	_, err := s.SaveSettings(context.Background(), q)
	require.NoError(t, err)
}

func TestAccountQualityStateRequiredPausesAndNeedsFullRecovery(t *testing.T) {
	s, keeper, a, u, q := qualityRecoveryFixture(t)
	q.PauseOnDegradation = false
	var err error
	q, err = s.SaveSettings(context.Background(), q)
	require.NoError(t, err)
	enableStateSchedulingForTest(keeper)
	_, err = s.db.Exec(`DELETE FROM account_quality_states WHERE account_id=1`)
	require.NoError(t, err)
	e := keeper.entryLocked(a.ID, q.ModelAuditModel)
	expired := time.Now().Add(-time.Hour)
	e.row.CollectedAt = &expired
	v := AccountQualityResult{AccountID: 1, Revision: q.Revision}
	require.NoError(t, s.probe(context.Background(), q, v, 1))
	v = readQualityRecovery(t, s)
	require.True(t, v.Scheduling.Paused)
	require.True(t, v.Scheduling.StateRequired)
	require.False(t, v.Scheduling.QualityPaused)
	require.Zero(t, u.calls.Load(), "invalid State is rejected without an upstream probe")
	require.NoError(t, s.runQualityRecovery(context.Background()))
	require.Zero(t, u.calls.Load())
	keeper.run(openAIStateKeeperJob{accountID: a.ID, revision: keeper.config.Load().Revision})
	require.True(t, readQualityRecovery(t, s).Scheduling.Paused, "collection alone cannot resume")
	require.NoError(t, s.runQualityRecovery(context.Background()))
	v = readQualityRecovery(t, s)
	require.True(t, v.Scheduling.Paused)
	require.Equal(t, 1, v.Scheduling.Successes)
	s.notifyQualityCollection(a.ID)
	require.NoError(t, s.runQualityRecovery(context.Background()))
	v = readQualityRecovery(t, s)
	require.False(t, v.Scheduling.Paused)
	require.False(t, v.Scheduling.StateRequired)
	require.Equal(t, "normal", v.Overall.Status)
}

func TestAccountQualityStateRecoveryFencesChangesDuringProbe(t *testing.T) {
	for _, mode := range []string{"expired", "credentials", "required-model", "new-state", "bad-answer"} {
		t.Run(mode, func(t *testing.T) {
			s, keeper, a, u, q := qualityRecoveryFixture(t)
			enableStateSchedulingForTest(keeper)
			original := u.do
			u.do = func(req *http.Request) (*http.Response, error) {
				resp, err := original(req)
				cfg := keeper.config.Load().OpenAIStateKeeperSettings
				switch mode {
				case "expired":
					expired := time.Now().Add(-time.Hour)
					keeper.entryLocked(a.ID, cfg.Model).row.CollectedAt = &expired
				case "credentials":
					a.Credentials["access_token"] = "reauth-during-probe"
				case "required-model":
					updated := *keeper.config.Load()
					updated.SchedulingStateModel = "other-model"
					keeper.config.Store(&updated)
				case "new-state":
					keeper.run(openAIStateKeeperJob{accountID: a.ID, revision: cfg.Revision})
				case "bad-answer":
					return newJSONResponse(200, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"wrong\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"model\":\"gpt-6-astra\"}}\n\n"), nil
				}
				return resp, err
			}
			require.NoError(t, s.runQualityRecovery(context.Background()))
			v := readQualityRecovery(t, s)
			require.True(t, v.Scheduling.Paused)
			require.Zero(t, v.Scheduling.Successes)
			require.NotNil(t, v.Scheduling.NextAt)
			require.WithinDuration(t, time.Now().Add(time.Duration(q.RetrySeconds)*time.Second), *v.Scheduling.NextAt, 2*time.Second)
		})
	}
}

func TestAccountQualitySettingsSavePreservesAllDeadlines(t *testing.T) {
	s, keeper, a, u, q := qualityRecoveryFixture(t)
	ctx := context.Background()
	require.NoError(t, s.runQualityRecovery(ctx))
	v := readQualityRecovery(t, s)
	require.Equal(t, 1, v.Scheduling.Successes)
	deadline := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	_, err := s.db.Exec(`UPDATE account_quality_states SET question_next_at=$1,model_next_at=$1,next_at=$1 WHERE account_id=1`, deadline)
	require.NoError(t, err)
	unchanged, err := s.SaveSettings(ctx, q)
	require.NoError(t, err)
	require.Equal(t, q.Revision, unchanged.Revision)
	require.Equal(t, 1, readQualityRecovery(t, s).Scheduling.Successes)
	q.IntervalSeconds++
	q, err = s.SaveSettings(ctx, q)
	require.NoError(t, err)
	keeper.syncQuality(ctx)
	require.NoError(t, s.runQualityRecovery(ctx))
	require.EqualValues(t, 1, u.calls.Load(), "saving neither probes nor bypasses recovery interval")
	var question, model time.Time
	require.NoError(t, s.db.QueryRow(`SELECT question_next_at,model_next_at FROM account_quality_states WHERE account_id=1`).Scan(&question, &model))
	require.True(t, deadline.Equal(question))
	require.True(t, deadline.Equal(model))
	require.Equal(t, v.Scheduling.NextAt, readQualityRecovery(t, s).Scheduling.NextAt)
	_, err = s.db.Exec(`UPDATE accounts SET deleted_at=NOW() WHERE id<>1;
 UPDATE account_quality_states SET payload=payload-'scheduling' WHERE account_id=1`)
	require.NoError(t, err)
	require.NoError(t, s.runDue(ctx, 0))
	require.NoError(t, s.runDue(ctx, 1))
	require.EqualValues(t, 1, u.calls.Load(), "ordinary deadlines also survive a settings revision")
	require.NoError(t, s.Schedule(ctx, []int64{a.ID}))
	require.NoError(t, s.runDue(ctx, 0))
	require.EqualValues(t, 2, u.calls.Load(), "explicit detection is still immediate")
}
