package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"sync"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func qualityGroupsFixture(t *testing.T) (*sql.DB, *accountRepository, service.AccountQualitySettings) {
	t.Helper()
	db, repo := qualityRepositoryDB(t)
	_, err := db.Exec(`CREATE TABLE groups(id BIGINT PRIMARY KEY, platform TEXT NOT NULL DEFAULT 'openai',status TEXT NOT NULL DEFAULT 'active',deleted_at TIMESTAMPTZ);
 CREATE TABLE account_groups(account_id BIGINT REFERENCES accounts(id),group_id BIGINT REFERENCES groups(id),priority INTEGER NOT NULL,created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),PRIMARY KEY(account_id,group_id));
 INSERT INTO groups(id) VALUES(10),(20),(30),(40);
 INSERT INTO account_groups(account_id,group_id,priority) VALUES(1,10,7),(1,20,3),(2,10,1),(3,10,1);
 UPDATE accounts SET extra=COALESCE(extra,'{}'::jsonb)||'{"billing_untouched":0.35}'::jsonb;
 DELETE FROM account_quality_states`)
	require.NoError(t, err)
	q := service.DefaultAccountQualitySettings()
	q.Enabled, q.SwitchGroupOnDegradation, q.Revision, q.DegradationGroupID = true, true, "group-policy", 30
	saveQualityGroupsPolicy(t, db, q)
	return db, repo, q
}

func saveQualityGroupsPolicy(t *testing.T, db *sql.DB, q service.AccountQualitySettings) {
	t.Helper()
	raw, err := json.Marshal(q)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE settings SET value=$1 WHERE key='account_quality_detection_v1'`, string(raw))
	require.NoError(t, err)
}

func saveQualityGroupsResult(t *testing.T, db *sql.DB, q service.AccountQualitySettings, id int64, status string, paused bool) {
	t.Helper()
	v := service.AccountQualityResult{AccountID: id, Revision: q.Revision,
		Overall: service.QualityOverallVerdict{Status: status}, Scheduling: service.QualityScheduling{Paused: paused, QualityPaused: paused}}
	raw, err := json.Marshal(v)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO account_quality_states(account_id,revision,payload) VALUES($1,$2,$3::jsonb)
 ON CONFLICT(account_id) DO UPDATE SET revision=EXCLUDED.revision,payload=EXCLUDED.payload`, id, q.Revision, string(raw))
	require.NoError(t, err)
}

func qualityGroupBindingsForTest(t *testing.T, repo *accountRepository, id int64) []qualityGroupBinding {
	t.Helper()
	groups, err := loadQualityGroupBindings(context.Background(), repo.client, id)
	require.NoError(t, err)
	return groups
}

func TestAccountRepositoryQualityGroupsMoveRestoreAndPauseIndependently(t *testing.T) {
	for _, pause := range []bool{false, true} {
		t.Run(map[bool]string{false: "switch-only", true: "switch-and-pause"}[pause], func(t *testing.T) {
			db, repo, q := qualityGroupsFixture(t)
			q.PauseOnDegradation = pause
			saveQualityGroupsPolicy(t, db, q)
			saveQualityGroupsResult(t, db, q, 1, "degraded", pause)
			ctx := context.Background()
			var wg sync.WaitGroup
			for range 8 {
				wg.Add(1)
				go func() { defer wg.Done(); require.NoError(t, repo.SyncQualityGroupSwitches(ctx)) }()
			}
			wg.Wait()
			require.NoError(t, repo.SyncQualityScheduling(ctx))
			require.Equal(t, []qualityGroupBinding{{ID: 30, Priority: 1}}, qualityGroupBindingsForTest(t, repo, 1))
			var schedulable bool
			var extra string
			require.NoError(t, db.QueryRow(`SELECT schedulable,extra->>'billing_untouched' FROM accounts WHERE id=1`).Scan(&schedulable, &extra))
			require.Equal(t, !pause, schedulable)
			require.Equal(t, "0.35", extra)
			var count int
			require.NoError(t, db.QueryRow(`SELECT count(*) FROM scheduler_outbox WHERE event_type=$1`, service.SchedulerOutboxEventAccountGroupsChanged).Scan(&count))
			require.Equal(t, 1, count, "concurrent or repeated checks only move once")
			// Normal evidence cannot restore while the existing recovery is pending.
			if pause {
				saveQualityGroupsResult(t, db, q, 1, "normal", true)
				require.NoError(t, repo.SyncQualityGroupSwitches(ctx))
				require.Equal(t, []qualityGroupBinding{{ID: 30, Priority: 1}}, qualityGroupBindingsForTest(t, repo, 1))
			}
			saveQualityGroupsResult(t, db, q, 1, "normal", false)
			// A new repository has no in-memory ownership but must restore exactly.
			restarted := newAccountRepositoryWithSQL(repo.client, db, nil)
			require.NoError(t, restarted.SyncQualityGroupSwitches(ctx))
			require.NoError(t, restarted.SyncQualityScheduling(ctx))
			require.Equal(t, []qualityGroupBinding{{ID: 10, Priority: 7}, {ID: 20, Priority: 3}}, qualityGroupBindingsForTest(t, repo, 1))
			require.NoError(t, db.QueryRow(`SELECT schedulable FROM accounts WHERE id=1`).Scan(&schedulable))
			require.True(t, schedulable)
			var owned bool
			require.NoError(t, db.QueryRow(`SELECT extra ? 'quality_group_switch' FROM accounts WHERE id=1`).Scan(&owned))
			require.False(t, owned)
		})
	}
}

func TestAccountRepositoryQualityGroupsDisabledPendingAndStaleResults(t *testing.T) {
	db, repo, q := qualityGroupsFixture(t)
	ctx := context.Background()
	for _, status := range []string{"pending", "normal"} {
		saveQualityGroupsResult(t, db, q, 1, status, false)
		require.NoError(t, repo.SyncQualityGroupSwitches(ctx))
		require.Len(t, qualityGroupBindingsForTest(t, repo, 1), 2)
	}
	old := q
	old.Revision = "old"
	saveQualityGroupsResult(t, db, old, 1, "degraded", false)
	require.NoError(t, repo.SyncQualityGroupSwitches(ctx))
	require.Len(t, qualityGroupBindingsForTest(t, repo, 1), 2)
	saveQualityGroupsResult(t, db, q, 1, "degraded", false)
	q.SwitchGroupOnDegradation = false
	saveQualityGroupsPolicy(t, db, q)
	require.NoError(t, repo.SyncQualityGroupSwitches(ctx))
	require.Len(t, qualityGroupBindingsForTest(t, repo, 1), 2)
	q.SwitchGroupOnDegradation = true
	saveQualityGroupsPolicy(t, db, q)
	require.NoError(t, repo.SyncQualityGroupSwitches(ctx))
	require.Len(t, qualityGroupBindingsForTest(t, repo, 1), 1)
	q.Enabled = false
	saveQualityGroupsPolicy(t, db, q)
	require.NoError(t, repo.SyncQualityGroupSwitches(ctx))
	require.Len(t, qualityGroupBindingsForTest(t, repo, 1), 2)
}

func TestAccountRepositoryQualityGroupsManualOverrideAndDeletedSource(t *testing.T) {
	for _, scenario := range []string{"manual", "manual-empty", "manual-priority", "deleted-source", "ungrouped", "target-change"} {
		t.Run(scenario, func(t *testing.T) {
			db, repo, q := qualityGroupsFixture(t)
			ctx := context.Background()
			if scenario == "ungrouped" {
				_, err := db.Exec(`DELETE FROM account_groups WHERE account_id=1`)
				require.NoError(t, err)
			}
			saveQualityGroupsResult(t, db, q, 1, "degraded", false)
			require.NoError(t, repo.SyncQualityGroupSwitches(ctx))
			switch scenario {
			case "manual-empty":
				require.NoError(t, repo.BindGroups(ctx, 1, nil))
				require.NoError(t, repo.SyncQualityGroupSwitches(ctx))
			case "manual-priority":
				_, err := db.Exec(`UPDATE account_groups SET priority=8 WHERE account_id=1`)
				require.NoError(t, err)
				require.NoError(t, repo.SyncQualityGroupSwitches(ctx))
			case "manual":
				require.NoError(t, repo.BindGroups(ctx, 1, []int64{40}))
				require.NoError(t, repo.SyncQualityGroupSwitches(ctx))
				require.NoError(t, repo.SyncQualityGroupSwitches(ctx))
				require.Equal(t, []qualityGroupBinding{{ID: 40, Priority: 1}}, qualityGroupBindingsForTest(t, repo, 1))
			case "deleted-source":
				_, err := db.Exec(`UPDATE groups SET deleted_at=NOW() WHERE id=10`)
				require.NoError(t, err)
			case "target-change":
				q.DegradationGroupID = 40
				saveQualityGroupsPolicy(t, db, q)
				require.NoError(t, repo.SyncQualityGroupSwitches(ctx))
				require.Equal(t, []qualityGroupBinding{{ID: 40, Priority: 1}}, qualityGroupBindingsForTest(t, repo, 1))
			}
			saveQualityGroupsResult(t, db, q, 1, "normal", false)
			require.NoError(t, repo.SyncQualityGroupSwitches(ctx))
			want := []qualityGroupBinding{{ID: 10, Priority: 7}, {ID: 20, Priority: 3}}
			switch scenario {
			case "manual":
				want = []qualityGroupBinding{{ID: 40, Priority: 1}}
			case "deleted-source":
				want = []qualityGroupBinding{{ID: 20, Priority: 3}}
			case "ungrouped", "manual-empty":
				want = []qualityGroupBinding{}
			case "manual-priority":
				want = []qualityGroupBinding{{ID: 30, Priority: 8}}
			}
			require.Equal(t, want, qualityGroupBindingsForTest(t, repo, 1))
		})
	}
}

func TestAccountRepositoryQualityGroupsConcurrentManualReplacement(t *testing.T) {
	db, repo, q := qualityGroupsFixture(t)
	ctx := context.Background()
	for range 12 {
		require.NoError(t, repo.BindGroups(ctx, 1, []int64{10, 20}))
		saveQualityGroupsResult(t, db, q, 1, "degraded", false)
		require.NoError(t, repo.SyncQualityGroupSwitches(ctx))
		saveQualityGroupsResult(t, db, q, 1, "normal", false)
		start := make(chan struct{})
		results := make(chan error, 2)
		go func() { <-start; results <- repo.SyncQualityGroupSwitches(ctx) }()
		go func() { <-start; results <- repo.BindGroups(ctx, 1, []int64{40}) }()
		close(start)
		require.NoError(t, <-results)
		require.NoError(t, <-results)
		require.NoError(t, repo.SyncQualityGroupSwitches(ctx))
		require.Equal(t, []qualityGroupBinding{{ID: 40, Priority: 1}}, qualityGroupBindingsForTest(t, repo, 1))
	}
}

func TestAccountRepositoryQualityGroupsDeletedTargetCanStillRestore(t *testing.T) {
	for _, disable := range []bool{false, true} {
		t.Run(map[bool]string{false: "recovered", true: "disabled"}[disable], func(t *testing.T) {
			db, repo, q := qualityGroupsFixture(t)
			ctx := context.Background()
			saveQualityGroupsResult(t, db, q, 1, "degraded", false)
			saveQualityGroupsResult(t, db, q, 2, "degraded", false)
			require.NoError(t, repo.SyncQualityGroupSwitches(ctx))
			// Match group deletion: lock group first, then remove its memberships.
			tx, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()
			_, err = tx.Exec(`UPDATE groups SET deleted_at=NOW() WHERE id=30; DELETE FROM account_groups WHERE group_id=30`)
			require.NoError(t, err)
			require.NoError(t, tx.Commit())
			if disable {
				q.SwitchGroupOnDegradation = false
				saveQualityGroupsPolicy(t, db, q)
			} else {
				saveQualityGroupsResult(t, db, q, 1, "normal", false)
			}
			err = repo.SyncQualityGroupSwitches(ctx)
			if disable {
				require.NoError(t, err)
			} else {
				require.Error(t, err, "account 2 is still degraded and its target is gone")
			}
			require.Equal(t, []qualityGroupBinding{{ID: 10, Priority: 7}, {ID: 20, Priority: 3}}, qualityGroupBindingsForTest(t, repo, 1))
		})
	}
}

func TestAccountRepositoryQualityGroupsDoNotClaimManualOrCopiedTarget(t *testing.T) {
	db, repo, q := qualityGroupsFixture(t)
	ctx := context.Background()
	saveQualityGroupsResult(t, db, q, 1, "degraded", false)
	require.NoError(t, repo.SyncQualityGroupSwitches(ctx))
	require.NoError(t, repo.BindGroups(ctx, 2, []int64{30}))
	saveQualityGroupsResult(t, db, q, 2, "degraded", false)
	require.NoError(t, repo.SyncQualityGroupSwitches(ctx))
	var owned bool
	require.NoError(t, db.QueryRow(`SELECT extra ? 'quality_group_switch' FROM accounts WHERE id=2`).Scan(&owned))
	require.False(t, owned, "a manually assigned target must not acquire automatic ownership")
	_, err := db.Exec(`UPDATE accounts SET extra=(SELECT extra FROM accounts WHERE id=1) WHERE id=2`)
	require.NoError(t, err)
	saveQualityGroupsResult(t, db, q, 2, "normal", false)
	require.NoError(t, repo.SyncQualityGroupSwitches(ctx))
	require.Equal(t, []qualityGroupBinding{{ID: 30, Priority: 1}}, qualityGroupBindingsForTest(t, repo, 2))
	require.NoError(t, db.QueryRow(`SELECT extra ? 'quality_group_switch' FROM accounts WHERE id=2`).Scan(&owned))
	require.False(t, owned)
}

func TestAccountRepositoryQualityGroupsOwnershipSurvivesAccountEdits(t *testing.T) {
	db, repo := stateSchedulingRepository(t)
	ctx := context.Background()
	const marker = `{"account_id":1,"active":true,"target":30,"original":[{"id":10,"priority":7}]}`
	_, err := db.Exec(`UPDATE settings SET value='{}' WHERE key='openai_state_keeper_v1'`)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE accounts SET extra=jsonb_build_object('quality_group_switch',$1::jsonb) WHERE id=1`, marker)
	require.NoError(t, err)
	input := func() map[string]any {
		return map[string]any{"quality_group_switch": map[string]any{"target": 999}, "custom": true}
	}
	for _, path := range []string{"update", "extra", "bulk"} {
		switch path {
		case "update":
			require.NoError(t, repo.Update(ctx, &service.Account{ID: 1, Name: "edited", Platform: "openai", Type: "oauth", Status: "active", Schedulable: true, Extra: input()}))
		case "extra":
			require.NoError(t, repo.UpdateExtra(ctx, 1, input()))
		case "bulk":
			_, err := repo.BulkUpdate(ctx, []int64{1}, service.AccountBulkUpdate{Extra: input()})
			require.NoError(t, err)
		}
		var current string
		var custom bool
		require.NoError(t, db.QueryRow(`SELECT extra->>'quality_group_switch',(extra->>'custom')::boolean FROM accounts WHERE id=1`).Scan(&current, &custom))
		require.JSONEq(t, marker, current, path)
		require.True(t, custom)
	}
	_, err = db.Exec(`UPDATE accounts SET extra=extra-'quality_group_switch' WHERE id=1`)
	require.NoError(t, err)
	require.NoError(t, repo.Update(ctx, &service.Account{ID: 1, Name: "stale edit", Platform: "openai", Type: "oauth", Status: "active", Schedulable: true, Extra: input()}))
	var owned bool
	require.NoError(t, db.QueryRow(`SELECT extra ? 'quality_group_switch' FROM accounts WHERE id=1`).Scan(&owned))
	require.False(t, owned, "stale edits cannot resurrect an already restored marker")
	for _, grouped := range []bool{false, true} {
		a := &service.Account{Name: "copy", Platform: "openai", Type: "apikey", Status: "active", Schedulable: true, Extra: input()}
		if grouped {
			require.NoError(t, repo.CreateWithAccountGroups(ctx, a, nil))
		} else {
			require.NoError(t, repo.Create(ctx, a))
		}
		require.NoError(t, db.QueryRow(`SELECT extra ? 'quality_group_switch' FROM accounts WHERE id=$1`, a.ID).Scan(&owned))
		require.False(t, owned, "copies and imports never inherit another account's group restore record")
	}
}

func TestAccountRepositoryQualityGroupsInvalidTargetAndOutboxRollback(t *testing.T) {
	for _, scenario := range []string{"deleted", "disabled", "other-platform", "outbox-failure"} {
		t.Run(scenario, func(t *testing.T) {
			db, repo, q := qualityGroupsFixture(t)
			saveQualityGroupsResult(t, db, q, 1, "degraded", false)
			query := map[string]string{
				"deleted":        "UPDATE groups SET deleted_at=NOW() WHERE id=30",
				"disabled":       "UPDATE groups SET status='inactive' WHERE id=30",
				"other-platform": "UPDATE groups SET platform='claude' WHERE id=30",
				"outbox-failure": "ALTER TABLE scheduler_outbox ADD CONSTRAINT reject_group_event CHECK (account_id<0)",
			}[scenario]
			_, err := db.Exec(query)
			require.NoError(t, err)
			require.Error(t, repo.SyncQualityGroupSwitches(context.Background()))
			require.Len(t, qualityGroupBindingsForTest(t, repo, 1), 2)
			var owned bool
			require.NoError(t, db.QueryRow(`SELECT extra ? 'quality_group_switch' FROM accounts WHERE id=1`).Scan(&owned))
			require.False(t, owned, "group bindings and their restore marker roll back together")
		})
	}
}
