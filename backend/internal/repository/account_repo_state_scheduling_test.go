package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func stateSchedulingRepository(t *testing.T) (*sql.DB, *accountRepository) {
	t.Helper()
	db, repo := qualityRepositoryDB(t)
	_, err := db.Exec(`CREATE SEQUENCE test_account_ids START 10;
 ALTER TABLE accounts ALTER COLUMN id SET DEFAULT nextval('test_account_ids');
 ALTER TABLE accounts ADD COLUMN name TEXT NOT NULL DEFAULT 'test', ADD COLUMN notes TEXT,
 ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), ADD COLUMN concurrency INT NOT NULL DEFAULT 1,
 ADD COLUMN priority INT NOT NULL DEFAULT 1, ADD COLUMN error_message TEXT NOT NULL DEFAULT '',
 ADD COLUMN quota_dimension TEXT NOT NULL DEFAULT 'account', ADD COLUMN parent_account_id BIGINT,
 ADD COLUMN rate_multiplier NUMERIC NOT NULL DEFAULT 1, ADD COLUMN load_factor INT,
 ADD COLUMN proxy_id BIGINT, ADD COLUMN proxy_fallback_origin_id BIGINT, ADD COLUMN last_used_at TIMESTAMPTZ,
 ADD COLUMN rate_limited_at TIMESTAMPTZ, ADD COLUMN rate_limit_reset_at TIMESTAMPTZ,
 ADD COLUMN overload_until TIMESTAMPTZ, ADD COLUMN temp_unschedulable_until TIMESTAMPTZ,
 ADD COLUMN temp_unschedulable_reason TEXT, ADD COLUMN session_window_start TIMESTAMPTZ,
 ADD COLUMN session_window_end TIMESTAMPTZ, ADD COLUMN session_window_status TEXT;
 CREATE TABLE account_groups(account_id BIGINT,group_id BIGINT,priority INT,PRIMARY KEY(account_id,group_id));
 INSERT INTO settings VALUES('openai_state_keeper_v1','{"require_valid_state":true,"suspend_old_state_on_reauth":true}');
 UPDATE accounts SET credentials='{"access_token":"old","chatgpt_account_id":"team","chatgpt_user_id":"user"}';
 UPDATE account_quality_states SET payload='{"account_id":1,"scheduling":{"paused":false}}' WHERE account_id=1`)
	require.NoError(t, err)
	return db, repo
}

func assertStateSchedulingAccount(t *testing.T, db *sql.DB, id int64, enabled, owned, pending bool) {
	t.Helper()
	var actualEnabled, actualOwned, actualPending bool
	require.NoError(t, db.QueryRow(`SELECT schedulable,COALESCE(extra->>'quality_schedulable_restore','false')='true',COALESCE(extra->>'state_scheduling_pending','false')='true' FROM accounts WHERE id=$1`, id).Scan(&actualEnabled, &actualOwned, &actualPending))
	require.Equal(t, enabled, actualEnabled)
	require.Equal(t, owned, actualOwned)
	require.Equal(t, pending, actualPending)
}

func TestAccountRepositoryStateSchedulingCreateAndRecovery(t *testing.T) {
	db, repo := stateSchedulingRepository(t)
	ctx := context.Background()
	for _, grouped := range []bool{false, true} {
		for _, manual := range []bool{false, true} {
			a := &service.Account{Name: "new OAuth", Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth, Status: service.StatusActive,
				Schedulable: !manual, Credentials: map[string]any{"access_token": "test-token"}}
			if grouped {
				require.NoError(t, repo.CreateWithAccountGroups(ctx, a, nil))
			} else {
				require.NoError(t, repo.Create(ctx, a))
			}
			assertStateSchedulingAccount(t, db, a.ID, false, !manual, true)
			require.Error(t, repo.SetSchedulable(ctx, a.ID, true))
			require.NoError(t, repo.SyncQualityScheduling(ctx))
			assertStateSchedulingAccount(t, db, a.ID, false, !manual, false)
			var paused bool
			require.NoError(t, db.QueryRow(`SELECT payload->'scheduling'->>'state_required'='true' FROM account_quality_states WHERE account_id=$1`, a.ID).Scan(&paused))
			require.True(t, paused)
			_, err := db.Exec(`UPDATE account_quality_states SET payload=payload||'{"scheduling":{"paused":false}}'::jsonb WHERE account_id=$1`, a.ID)
			require.NoError(t, err)
			require.NoError(t, repo.SyncQualityScheduling(ctx))
			assertStateSchedulingAccount(t, db, a.ID, !manual, false, false)
		}
	}
}

func TestAccountRepositoryStateSchedulingReauthAllWritePaths(t *testing.T) {
	for _, path := range []string{"update", "credentials", "bulk", "bulk-manual", "unchanged", "reuse", "different-user", "manual", "off", "apikey"} {
		t.Run(path, func(t *testing.T) {
			db, repo := stateSchedulingRepository(t)
			ctx := context.Background()
			if path == "manual" {
				require.NoError(t, repo.SetSchedulable(ctx, 1, false))
			}
			if path == "off" || path == "reuse" || path == "different-user" {
				_, err := db.Exec(`UPDATE settings SET value=$1 WHERE key='openai_state_keeper_v1'`, fmt.Sprintf(`{"require_valid_state":%t,"suspend_old_state_on_reauth":false}`, path != "off"))
				require.NoError(t, err)
			}
			if path == "apikey" {
				_, err := db.Exec(`UPDATE accounts SET type='apikey' WHERE id=1`)
				require.NoError(t, err)
			}
			credentials := map[string]any{"access_token": "new", "chatgpt_account_id": "team", "chatgpt_user_id": "user"}
			if path == "unchanged" {
				credentials["access_token"] = "old"
			}
			if path == "different-user" {
				credentials["chatgpt_user_id"] = "other-user"
			}
			switch path {
			case "update":
				a := &service.Account{ID: 1, Name: "updated", Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: true, Credentials: credentials}
				require.NoError(t, repo.Update(ctx, a))
			case "bulk", "bulk-manual":
				updates := service.AccountBulkUpdate{Credentials: credentials}
				if path == "bulk-manual" {
					value := false
					updates.Schedulable = &value
				}
				_, err := repo.BulkUpdate(ctx, []int64{1}, updates)
				require.NoError(t, err)
			default:
				require.NoError(t, repo.UpdateCredentials(ctx, 1, credentials))
			}
			paused := path != "unchanged" && path != "reuse" && path != "off" && path != "apikey"
			owned := paused && path != "manual" && path != "bulk-manual"
			assertStateSchedulingAccount(t, db, 1, !paused, owned, paused)
			require.NoError(t, repo.SyncQualityScheduling(ctx))
			assertStateSchedulingAccount(t, db, 1, !paused, owned, false)
			if paused {
				require.Error(t, repo.SetSchedulable(ctx, 1, true))
				yes := true
				_, err := repo.BulkUpdate(ctx, []int64{1}, service.AccountBulkUpdate{Schedulable: &yes})
				require.Error(t, err)
			}
		})
	}
}

func TestAccountRepositoryStateSchedulingDisableKeepsOtherCauses(t *testing.T) {
	db, repo := stateSchedulingRepository(t)
	ctx := context.Background()
	_, err := db.Exec(`UPDATE accounts SET schedulable=false,extra=extra||'{"quality_schedulable_restore":true}'::jsonb;
 UPDATE account_quality_states SET payload='{"scheduling":{"paused":true,"state_required":true,"quality_paused":false}}';
 UPDATE account_quality_states SET payload='{"scheduling":{"paused":true,"state_required":true,"quality_paused":true}}' WHERE account_id=2;
 UPDATE settings SET value='{"require_valid_state":false}' WHERE key='openai_state_keeper_v1'`)
	require.NoError(t, err)
	require.NoError(t, repo.SyncQualityScheduling(ctx))
	assertStateSchedulingAccount(t, db, 1, true, false, false)
	assertStateSchedulingAccount(t, db, 2, false, true, false)
	assertStateSchedulingAccount(t, db, 3, false, false, false)
	var stillState bool
	require.NoError(t, db.QueryRow(`SELECT COALESCE(payload->'scheduling'->>'state_required','false')='true' FROM account_quality_states WHERE account_id=2`).Scan(&stillState))
	require.False(t, stillState)
}

func TestAccountRepositoryStateSchedulingConcurrentReauthAndSync(t *testing.T) {
	db, repo := stateSchedulingRepository(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := range 10 {
		wg.Add(2)
		go func() { defer wg.Done(); errs <- repo.SyncQualityScheduling(ctx) }()
		go func() {
			defer wg.Done()
			errs <- repo.UpdateCredentials(ctx, 1, map[string]any{"access_token": fmt.Sprint(i)})
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.NoError(t, repo.SyncQualityScheduling(ctx))
	assertStateSchedulingAccount(t, db, 1, false, true, false)
	var raw []byte
	require.NoError(t, db.QueryRow(`SELECT payload FROM account_quality_states WHERE account_id=1`).Scan(&raw))
	var v service.AccountQualityResult
	require.NoError(t, json.Unmarshal(raw, &v))
	require.True(t, v.Scheduling.Paused)
	require.True(t, v.Scheduling.StateRequired)
	require.Zero(t, v.Scheduling.Successes)
	require.NoError(t, repo.SetSchedulable(ctx, 1, false))
	require.NoError(t, repo.UpdateExtra(ctx, 1, map[string]any{"state_scheduling_manual": false, "quality_schedulable_restore": true, "name": "safe"}))
	assertStateSchedulingAccount(t, db, 1, false, false, false)
}

func TestAccountRepositoryStateSchedulingCredentialPolicyMatchesInjection(t *testing.T) {
	db, _ := stateSchedulingRepository(t)
	for _, suspend := range []bool{true, false} {
		_, err := db.Exec(`UPDATE settings SET value=$1 WHERE key='openai_state_keeper_v1'`, fmt.Sprintf(`{"require_valid_state":true,"suspend_old_state_on_reauth":%t}`, suspend))
		require.NoError(t, err)
		for _, mode := range []string{"same", "reauth", "same-token-different-user", "email", "email-case", "email-change", "missing-identity", "empty-token", "added-identity"} {
			old := &service.Account{Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth, Credentials: map[string]any{"access_token": "old", "chatgpt_account_id": "team", "chatgpt_user_id": "user"}}
			current := &service.Account{Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth, Credentials: map[string]any{"access_token": "new", "chatgpt_account_id": "team", "chatgpt_user_id": "user"}}
			switch mode {
			case "same":
				current.Credentials["access_token"] = "old"
			case "same-token-different-user":
				current.Credentials["access_token"], current.Credentials["chatgpt_user_id"] = "old", "other"
			case "email", "email-case", "email-change":
				delete(old.Credentials, "chatgpt_user_id")
				delete(current.Credentials, "chatgpt_user_id")
				old.Credentials["email"], current.Credentials["email"] = "member@example.test", "member@example.test"
				if mode == "email-case" {
					current.Credentials["email"] = " MEMBER@example.test "
				}
				if mode == "email-change" {
					current.Credentials["email"] = "other@example.test"
				}
			case "missing-identity":
				delete(old.Credentials, "chatgpt_user_id")
				delete(current.Credentials, "chatgpt_user_id")
			case "empty-token":
				current.Credentials["access_token"] = ""
			case "added-identity":
				delete(old.Credentials, "chatgpt_user_id")
				current.Credentials["access_token"] = "old"
			}
			previous, err := json.Marshal(old.Credentials)
			require.NoError(t, err)
			incoming, err := json.Marshal(current.Credentials)
			require.NoError(t, err)
			_, err = db.Exec(`UPDATE accounts SET credentials=$1 WHERE id=1`, string(previous))
			require.NoError(t, err)
			var changed bool
			require.NoError(t, db.QueryRow(`SELECT `+stateSchedulingCredentialChangeSQL("$1::jsonb")+` FROM accounts WHERE id=1`, string(incoming)).Scan(&changed))
			want := service.StateSchedulingCredentialsChanged(service.OpenAIStateKeeperSettings{SuspendOldStateOnReauth: suspend}, old, current)
			require.Equal(t, want, changed, "%s suspend=%t", mode, suspend)
		}
	}
}
