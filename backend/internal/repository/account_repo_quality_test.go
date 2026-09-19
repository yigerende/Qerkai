package repository

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/migrations"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func qualityRepositoryDB(t *testing.T) (*sql.DB, *accountRepository) {
	t.Helper()
	dsn := os.Getenv("QUALITY_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("isolated PostgreSQL URL not provided")
	}
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	require.Contains(t, []string{"localhost", "127.0.0.1"}, u.Hostname())
	root, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	schema := fmt.Sprintf("quality_admission_%d", time.Now().UnixNano())
	_, err = root.Exec("CREATE SCHEMA " + pq.QuoteIdentifier(schema))
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = root.Exec("DROP SCHEMA " + pq.QuoteIdentifier(schema) + " CASCADE"); _ = root.Close() })
	query := u.Query()
	query.Set("search_path", schema)
	u.RawQuery = query.Encode()
	db, err := sql.Open("postgres", u.String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec(`CREATE TABLE accounts(id BIGINT PRIMARY KEY,platform TEXT,type TEXT NOT NULL DEFAULT 'oauth',credentials JSONB NOT NULL DEFAULT '{}',status TEXT,schedulable BOOLEAN,extra JSONB,deleted_at TIMESTAMPTZ,updated_at TIMESTAMPTZ,auto_pause_on_expired BOOLEAN NOT NULL DEFAULT true,expires_at TIMESTAMPTZ);
 CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT);
 CREATE TABLE account_quality_states(account_id BIGINT PRIMARY KEY,revision TEXT NOT NULL DEFAULT '',version TEXT NOT NULL DEFAULT '',payload JSONB);
 CREATE TABLE scheduler_outbox(id BIGSERIAL PRIMARY KEY,event_type TEXT,account_id BIGINT,group_id BIGINT,payload JSONB,dedup_key TEXT);
 CREATE UNIQUE INDEX scheduler_outbox_dedup ON scheduler_outbox(dedup_key) WHERE dedup_key IS NOT NULL;
 INSERT INTO settings VALUES('account_quality_detection_v1','{"enabled":true,"pause_on_degradation":true}');
 INSERT INTO accounts(id,platform,status,schedulable,extra) VALUES(1,'openai','active',true,'{"other":123}'),(2,'openai','active',false,'{}'),(3,'openai','error',true,'{}');
 INSERT INTO account_quality_states(account_id,payload) SELECT id,'{"scheduling":{"paused":true}}'::jsonb FROM accounts`)
	require.NoError(t, err)
	client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
	pauseMigration, err := migrations.FS.ReadFile("240_account_scheduling_paused_at.sql")
	require.NoError(t, err)
	_, err = db.Exec(string(pauseMigration))
	require.NoError(t, err)
	return db, newAccountRepositoryWithSQL(client, db, nil)
}

func TestAccountRepositoryQualityPauseSyncPreservesManualAndErrors(t *testing.T) {
	db, repo := qualityRepositoryDB(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); require.NoError(t, repo.SyncQualityScheduling(ctx)) }()
	}
	wg.Wait()
	var count int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM scheduler_outbox`).Scan(&count))
	require.Equal(t, 2, count, "concurrent syncs emit one event per changed switch")
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM accounts WHERE schedulable=false`).Scan(&count))
	require.Equal(t, 3, count, "quality closes the actual account scheduling switch")
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM accounts WHERE extra->>'quality_schedulable_restore'='true'`).Scan(&count))
	require.Equal(t, 2, count, "an already disabled account must not be owned by quality recovery")
	_, err := db.Exec(`UPDATE account_quality_states SET payload='{"scheduling":{"paused":false}}' WHERE account_id=1`)
	require.NoError(t, err)
	require.NoError(t, repo.SyncQualityScheduling(ctx))
	var owned, manual bool
	var status, other string
	require.NoError(t, db.QueryRow(`SELECT extra ? 'quality_schedulable_restore',schedulable,status,extra->>'other' FROM accounts WHERE id=1`).Scan(&owned, &manual, &status, &other))
	require.False(t, owned)
	require.True(t, manual)
	require.Equal(t, "active", status)
	require.Equal(t, "123", other)
	_, err = db.Exec(`UPDATE settings SET value='{"enabled":true,"pause_on_degradation":false}'`)
	require.NoError(t, err)
	require.NoError(t, repo.SyncQualityScheduling(ctx))
	require.NoError(t, db.QueryRow(`SELECT schedulable FROM accounts WHERE id=2`).Scan(&manual))
	require.False(t, manual)
	require.NoError(t, db.QueryRow(`SELECT status FROM accounts WHERE id=3`).Scan(&status))
	require.Equal(t, "error", status)
	require.NoError(t, db.QueryRow(`SELECT schedulable FROM accounts WHERE id=3`).Scan(&manual))
	require.False(t, manual, "disabling quality must not reopen an invalid account")
	_, err = db.Exec(`UPDATE settings SET value='{"enabled":true,"pause_on_degradation":true}'`)
	require.NoError(t, err)
	require.NoError(t, repo.SyncQualityScheduling(ctx))
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM accounts WHERE extra ? 'quality_schedulable_restore'`).Scan(&count))
	require.Zero(t, count, "reenabling must not revive an old pause")
}

func TestAccountRepositoryQualityPauseSyncRollsBackWithOutboxFailure(t *testing.T) {
	db, repo := qualityRepositoryDB(t)
	_, err := db.Exec(`ALTER TABLE scheduler_outbox ADD CONSTRAINT reject_event CHECK (account_id<0)`)
	require.NoError(t, err)
	require.Error(t, repo.SyncQualityScheduling(context.Background()))
	var count int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM accounts WHERE extra ? 'quality_schedulable_restore'`).Scan(&count))
	require.Zero(t, count)
	var schedulable bool
	require.NoError(t, db.QueryRow(`SELECT schedulable FROM accounts WHERE id=1`).Scan(&schedulable))
	require.True(t, schedulable, "switch changes roll back with the scheduler notification")
	_, err = db.Exec(`ALTER TABLE scheduler_outbox DROP CONSTRAINT reject_event`)
	require.NoError(t, err)
	require.NoError(t, repo.SyncQualityScheduling(context.Background()))
}

func TestAccountRepositoryQualityPauseMigratesLegacyFlagAndRespectsManualChanges(t *testing.T) {
	for _, bulk := range []bool{false, true} {
		t.Run(fmt.Sprint(bulk), func(t *testing.T) {
			db, repo := qualityRepositoryDB(t)
			ctx := context.Background()
			_, err := db.Exec(`UPDATE accounts SET extra=extra||'{"quality_scheduling_paused":true}'::jsonb WHERE id=1`)
			require.NoError(t, err)
			require.NoError(t, repo.SyncQualityScheduling(ctx))
			var schedulable, legacy bool
			require.NoError(t, db.QueryRow(`SELECT schedulable,extra ? 'quality_scheduling_paused' FROM accounts WHERE id=1`).Scan(&schedulable, &legacy))
			require.False(t, schedulable)
			require.False(t, legacy)
			if bulk {
				value := false
				_, err = repo.BulkUpdate(ctx, []int64{1}, service.AccountBulkUpdate{Schedulable: &value})
			} else {
				err = repo.SetSchedulable(ctx, 1, false)
			}
			require.NoError(t, err)
			require.NoError(t, repo.SyncQualityScheduling(ctx))
			_, err = db.Exec(`UPDATE account_quality_states SET payload='{"scheduling":{"paused":false}}' WHERE account_id=1`)
			require.NoError(t, err)
			require.NoError(t, repo.SyncQualityScheduling(ctx))
			require.NoError(t, db.QueryRow(`SELECT schedulable FROM accounts WHERE id=1`).Scan(&schedulable))
			require.False(t, schedulable, "manual disable during recovery revokes automatic reopening")
			require.NoError(t, repo.SetSchedulable(ctx, 1, true))
			require.NoError(t, repo.SyncQualityScheduling(ctx))
			require.NoError(t, db.QueryRow(`SELECT schedulable FROM accounts WHERE id=1`).Scan(&schedulable))
			require.True(t, schedulable)
		})
	}
}

func TestAccountRepositoryQualityPauseDoesNotReopenExpiredAccount(t *testing.T) {
	db, repo := qualityRepositoryDB(t)
	ctx := context.Background()
	require.NoError(t, repo.SyncQualityScheduling(ctx))
	_, err := db.Exec(`UPDATE accounts SET expires_at=NOW()-INTERVAL '1 minute' WHERE id=1;
 UPDATE account_quality_states SET payload='{"scheduling":{"paused":false}}' WHERE account_id=1`)
	require.NoError(t, err)
	require.NoError(t, repo.SyncQualityScheduling(ctx))
	var schedulable bool
	require.NoError(t, db.QueryRow(`SELECT schedulable FROM accounts WHERE id=1`).Scan(&schedulable))
	require.False(t, schedulable)
}
