package repository

import (
	"context"
	"encoding/json"
	"errors"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
)

const stateSchedulingEnabledSQL = `EXISTS(SELECT 1 FROM settings WHERE key='openai_state_keeper_v1' AND value::jsonb->>'require_valid_state'='true')`

func stateSchedulingIdentitySQL(credentials string) string {
	account := `btrim(COALESCE((` + credentials + `)->>'chatgpt_account_id',''))`
	user := `COALESCE(NULLIF(btrim((` + credentials + `)->>'chatgpt_user_id'),''),'email:'||NULLIF(lower(btrim((` + credentials + `)->>'email')),''))`
	return `(CASE WHEN ` + account + `<>'' AND ` + user + ` IS NOT NULL THEN jsonb_build_array(` + account + `,` + user + `) END)`
}

func stateSchedulingCredentialChangeSQL(incoming string) string {
	oldIdentity, newIdentity := stateSchedulingIdentitySQL("credentials"), stateSchedulingIdentitySQL(incoming)
	return `(platform='openai' AND type='oauth' AND ` + stateSchedulingEnabledSQL + ` AND (
 COALESCE((` + incoming + `)->>'access_token','')='' OR
 (` + oldIdentity + ` IS NOT NULL AND ` + oldIdentity + ` IS DISTINCT FROM ` + newIdentity + `) OR
 COALESCE(credentials->>'access_token','') IS DISTINCT FROM COALESCE((` + incoming + `)->>'access_token','')))`
}

func stateSchedulingPendingExtraSQL(extra string) string {
	return `(` + extra + `) || '{"state_scheduling_pending":true}'::jsonb ||
 CASE WHEN COALESCE(extra->>'state_scheduling_manual','false')<>'true' AND
 (schedulable OR status='error' OR extra->>'quality_schedulable_restore'='true')
 THEN '{"quality_schedulable_restore":true}'::jsonb ELSE '{}'::jsonb END`
}

func stateSchedulingPolicy(ctx context.Context, client *dbent.Client) (service.OpenAIStateKeeperSettings, error) {
	q := service.DefaultOpenAIStateKeeperSettings()
	rows, err := client.QueryContext(ctx, `SELECT value FROM settings WHERE key='openai_state_keeper_v1'`)
	if err != nil {
		return q, err
	}
	defer rows.Close()
	if rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err == nil {
			err = json.Unmarshal(raw, &q)
		}
	}
	if err == nil {
		err = rows.Err()
	}
	return q, err
}

// Account creation and credential writes close the existing switch before the
// account snapshot can be published. Collection and recovery remain asynchronous.
func applyStateSchedulingWrite(ctx context.Context, client *dbent.Client, a *service.Account, existing bool) error {
	if a.Platform != service.PlatformOpenAI || a.Type != service.AccountTypeOAuth {
		return nil
	}
	q, err := stateSchedulingPolicy(ctx, client)
	if err != nil || !q.RequireValidState {
		return err
	}
	if a.Extra == nil {
		a.Extra = map[string]any{}
	}
	changed, restore := !existing, a.Schedulable
	if !existing {
		a.Extra = stripStateSchedulingExtra(a.Extra)
		if !a.Schedulable {
			a.Extra["state_scheduling_manual"] = true
		}
	}
	if existing {
		rows, err := client.QueryContext(ctx, `SELECT credentials,extra,schedulable,status,platform,type FROM accounts WHERE id=$1 AND deleted_at IS NULL FOR UPDATE`, a.ID)
		if err != nil {
			return err
		}
		var credentials, extra []byte
		old := &service.Account{ID: a.ID}
		if !rows.Next() {
			rows.Close()
			return service.ErrAccountNotFound
		}
		err = rows.Scan(&credentials, &extra, &old.Schedulable, &old.Status, &old.Platform, &old.Type)
		rows.Close()
		if err != nil {
			return err
		}
		if err = json.Unmarshal(credentials, &old.Credentials); err != nil {
			return err
		}
		if len(extra) > 0 && string(extra) != "null" {
			if err = json.Unmarshal(extra, &old.Extra); err != nil {
				return err
			}
		}
		for _, key := range []string{"quality_schedulable_restore", "state_scheduling_pending", "state_scheduling_manual"} {
			delete(a.Extra, key)
			if value, ok := old.Extra[key]; ok {
				a.Extra[key] = value
			}
		}
		changed = service.StateSchedulingCredentialsChanged(old, a)
		restore = old.Extra["state_scheduling_manual"] != true && (old.Schedulable || old.Status == service.StatusError || old.Extra["quality_schedulable_restore"] == true)
		if old.Extra["quality_schedulable_restore"] == true || old.Extra["state_scheduling_manual"] == true || old.Extra["state_scheduling_pending"] == true {
			a.Schedulable = false
		}
	}
	if changed {
		a.Schedulable = false
		a.Extra["state_scheduling_pending"] = true
		if restore {
			a.Extra["quality_schedulable_restore"] = true
		}
	}
	return nil
}

const stateSchedulingEnableAllowedSQL = `NOT (platform='openai' AND type='oauth' AND ` + stateSchedulingEnabledSQL + ` AND
 (COALESCE(extra->>'state_scheduling_pending','false')='true' OR NOT EXISTS(SELECT 1 FROM account_quality_states s
 WHERE s.account_id=accounts.id AND COALESCE(s.payload->'scheduling'->>'paused','false')<>'true')))`

var errStateSchedulingEnable = errors.New("账号尚未通过 State 有效期及降智恢复复检，不能开启调度")

func checkStateSchedulingEnable(ctx context.Context, client *dbent.Client, ids []int64) error {
	q, err := stateSchedulingPolicy(ctx, client)
	if err != nil || !q.RequireValidState {
		return err
	}
	rows, err := client.QueryContext(ctx, `SELECT id FROM accounts WHERE id=ANY($1) AND deleted_at IS NULL ORDER BY id FOR UPDATE`, pq.Array(ids))
	if err != nil {
		return err
	}
	for rows.Next() {
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	rows, err = client.QueryContext(ctx, `SELECT id FROM accounts WHERE id=ANY($1) AND deleted_at IS NULL AND NOT (`+stateSchedulingEnableAllowedSQL+`) LIMIT 1`, pq.Array(ids))
	if err != nil {
		return err
	}
	defer rows.Close()
	if rows.Next() {
		return errStateSchedulingEnable
	}
	return rows.Err()
}

func stripStateSchedulingExtra(extra map[string]any) map[string]any {
	out := make(map[string]any, len(extra))
	for key, value := range extra {
		if key != "state_scheduling_pending" && key != "state_scheduling_manual" && key != "quality_schedulable_restore" {
			out[key] = value
		}
	}
	return out
}
