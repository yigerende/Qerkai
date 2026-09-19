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
	_, err = db.Exec(`CREATE TABLE accounts(id BIGINT PRIMARY KEY,platform TEXT,status TEXT,schedulable BOOLEAN,extra JSONB,deleted_at TIMESTAMPTZ,updated_at TIMESTAMPTZ);
 CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT);
 CREATE TABLE account_quality_states(account_id BIGINT PRIMARY KEY,payload JSONB);
 CREATE TABLE scheduler_outbox(id BIGSERIAL PRIMARY KEY,event_type TEXT,account_id BIGINT,payload JSONB);
 INSERT INTO settings VALUES('account_quality_detection_v1','{"enabled":true,"pause_on_degradation":true}');
 INSERT INTO accounts(id,platform,status,schedulable,extra) VALUES(1,'openai','active',true,'{"other":123}'),(2,'openai','active',false,'{}'),(3,'openai','error',true,'{}');
 INSERT INTO account_quality_states SELECT id,'{"scheduling":{"paused":true}}'::jsonb FROM accounts`)
	require.NoError(t, err)
	client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
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
	require.Equal(t, 3, count, "concurrent syncs emit one event per change")
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM accounts WHERE extra->>'quality_scheduling_paused'='true'`).Scan(&count))
	require.Equal(t, 3, count)
	_, err := db.Exec(`UPDATE account_quality_states SET payload='{"scheduling":{"paused":false}}' WHERE account_id=1`)
	require.NoError(t, err)
	require.NoError(t, repo.SyncQualityScheduling(ctx))
	var paused, manual bool
	var status, other string
	require.NoError(t, db.QueryRow(`SELECT (extra->>'quality_scheduling_paused')::boolean,schedulable,status,extra->>'other' FROM accounts WHERE id=1`).Scan(&paused, &manual, &status, &other))
	require.False(t, paused)
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
	_, err = db.Exec(`UPDATE settings SET value='{"enabled":true,"pause_on_degradation":true}'`)
	require.NoError(t, err)
	require.NoError(t, repo.SyncQualityScheduling(ctx))
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM accounts WHERE extra->>'quality_scheduling_paused'='true'`).Scan(&count))
	require.Zero(t, count, "reenabling must not revive an old pause")
}

func TestAccountRepositoryQualityPauseSyncRollsBackWithOutboxFailure(t *testing.T) {
	db, repo := qualityRepositoryDB(t)
	_, err := db.Exec(`ALTER TABLE scheduler_outbox ADD CONSTRAINT reject_event CHECK (account_id<0)`)
	require.NoError(t, err)
	require.Error(t, repo.SyncQualityScheduling(context.Background()))
	var count int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM accounts WHERE extra->>'quality_scheduling_paused'='true'`).Scan(&count))
	require.Zero(t, count)
	_, err = db.Exec(`ALTER TABLE scheduler_outbox DROP CONSTRAINT reject_event`)
	require.NoError(t, err)
	require.NoError(t, repo.SyncQualityScheduling(context.Background()))
}
