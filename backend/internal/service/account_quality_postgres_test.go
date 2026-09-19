package service

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type qualityPGSettings struct {
	SettingRepository
	db *sql.DB
}

func (r *qualityPGSettings) GetValue(ctx context.Context, key string) (string, error) {
	var v string
	err := r.db.QueryRowContext(ctx, "SELECT value FROM settings WHERE key=$1", key).Scan(&v)
	if err == sql.ErrNoRows {
		return "", ErrSettingNotFound
	}
	return v, err
}
func (r *qualityPGSettings) Set(ctx context.Context, key, v string) error {
	_, err := r.db.ExecContext(ctx, "INSERT INTO settings(key,value) VALUES($1,$2) ON CONFLICT(key) DO UPDATE SET value=excluded.value", key, v)
	return err
}

type qualityPGAudit struct {
	UsageLogRepository
	calls, active, maximum atomic.Int32
}

func (r *qualityPGAudit) LatestModelAudit(_ context.Context, input ModelAuditInput) ([]ModelAuditResult, error) {
	r.calls.Add(1)
	n := r.active.Add(1)
	defer r.active.Add(-1)
	for old := r.maximum.Load(); n > old; old = r.maximum.Load() {
		if r.maximum.CompareAndSwap(old, n) {
			break
		}
	}
	time.Sleep(10 * time.Millisecond)
	now := time.Now().UTC()
	mismatch := true
	id := input.Accounts[0].AccountID
	return []ModelAuditResult{{AccountID: id, Logs: []ModelAuditLog{{ID: id * 10, AccountID: id, CreatedAt: now, SentModel: input.Model, ResponseModel: "other-model", Mismatch: &mismatch}, {ID: id*10 + 1, AccountID: id, CreatedAt: now, SentModel: input.Model, ResponseModel: "other-model", Mismatch: &mismatch}, {ID: id*10 + 2, AccountID: id, CreatedAt: now, SentModel: input.Model, ResponseModel: "other-model", Mismatch: &mismatch}}}}, nil
}
func TestAccountQualityPostgresPersistenceConcurrencyAndMigration(t *testing.T) {
	dsn := os.Getenv("QUALITY_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("isolated PostgreSQL URL not provided")
	}
	parsed, err := url.Parse(dsn)
	require.NoError(t, err)
	require.Contains(t, []string{"127.0.0.1", "localhost"}, parsed.Hostname(), "test must use loopback")
	root, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	defer root.Close()
	schema := fmt.Sprintf("quality_test_%d", time.Now().UnixNano())
	_, err = root.Exec("CREATE SCHEMA " + pq.QuoteIdentifier(schema))
	require.NoError(t, err)
	defer root.Exec("DROP SCHEMA " + pq.QuoteIdentifier(schema) + " CASCADE")
	params := parsed.Query()
	params.Set("search_path", schema)
	parsed.RawQuery = params.Encode()
	db, err := sql.Open("postgres", parsed.String())
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(16)
	_, err = db.Exec(`CREATE TABLE accounts(id BIGINT PRIMARY KEY,name TEXT NOT NULL DEFAULT 'test account', platform TEXT NOT NULL,deleted_at TIMESTAMPTZ);CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT NOT NULL)`)
	require.NoError(t, err)
	migration, err := os.ReadFile("../../migrations/236_account_quality_detection.sql")
	require.NoError(t, err)
	_, err = db.Exec(string(migration))
	require.NoError(t, err)
	_, err = db.Exec(string(migration))
	require.NoError(t, err)
	progressMigration, err := os.ReadFile("../../migrations/237_account_quality_progress.sql")
	require.NoError(t, err)
	_, err = db.Exec(string(progressMigration))
	require.NoError(t, err)
	scopeMigration, err := os.ReadFile("../../migrations/238_account_quality_group_scope.sql")
	require.NoError(t, err)
	for i := 0; i < 2; i++ {
		_, err = db.Exec(string(scopeMigration))
		require.NoError(t, err)
	}
	_, err = db.Exec("INSERT INTO accounts(id,platform) SELECT id,'openai' FROM generate_series(1,32) id")
	require.NoError(t, err)
	settings := &SettingService{settingRepo: &qualityPGSettings{db: db}}
	audit := &qualityPGAudit{}
	svc := &AccountQualityService{db: db, settings: settings, usage: &UsageService{usageRepo: audit}}
	q := DefaultAccountQualitySettings()
	q.Enabled = true
	q.QuestionEnabled = false
	q.ModelAuditEnabled = true
	q.HistoryLimit = 3
	q, err = svc.SaveSettings(context.Background(), q)
	require.NoError(t, err)
	replica := &AccountQualityService{db: db, settings: settings, usage: svc.usage}
	var wg sync.WaitGroup
	for _, instance := range []*AccountQualityService{svc, replica} {
		wg.Add(1)
		go func(s *AccountQualityService) { defer wg.Done(); require.NoError(t, s.runDue(context.Background(), 1)) }(instance)
	}
	wg.Wait()
	require.Equal(t, int32(32), audit.calls.Load())
	require.LessOrEqual(t, audit.maximum.Load(), int32(q.Concurrency))
	ids := []int64{}
	for i := int64(1); i <= 32; i++ {
		ids = append(ids, i)
	}
	out, err := replica.Results(context.Background(), ids)
	require.NoError(t, err)
	require.Len(t, out.Accounts, 32)
	for _, v := range out.Accounts {
		require.True(t, v.Model.Degraded)
		require.Equal(t, 3, v.Model.Failures)
		require.Equal(t, "degraded", v.Overall.Status)
	}
	aggregateSummary, err := svc.Summary(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(32), aggregateSummary["degraded"], "SQL summary and per-account policy must agree")
	require.Equal(t, int32(32), audit.calls.Load(), "result reads must not probe")
	require.NoError(t, svc.Schedule(context.Background(), ids))
	require.NoError(t, svc.runDue(context.Background(), 1))
	out, err = svc.Results(context.Background(), []int64{1})
	require.NoError(t, err)
	require.Equal(t, 3, out.Accounts[0].Model.Failures, "same log IDs must not count twice")
	next := time.Now().Add(time.Minute)
	a := AccountQualityResult{AccountID: 1, Revision: q.Revision, Question: QualityQuestionResult{QuestionID: "kept-question", QualityVerdict: QualityVerdict{Status: "normal", NextAt: &next}}}
	b := AccountQualityResult{AccountID: 1, Revision: q.Revision, Model: QualityModelResult{ResponseModel: "kept-model", QualityVerdict: QualityVerdict{Status: "normal", NextAt: &next}}}
	for kind, v := range []AccountQualityResult{a, b} {
		wg.Add(1)
		go func(kind int, v AccountQualityResult) {
			defer wg.Done()
			require.NoError(t, svc.saveResult(context.Background(), q, v, kind))
		}(kind, v)
	}
	wg.Wait()
	out, err = replica.Results(context.Background(), []int64{1})
	require.NoError(t, err)
	require.Equal(t, "kept-question", out.Accounts[0].Question.QuestionID)
	require.Equal(t, "kept-model", out.Accounts[0].Model.ResponseModel)
	for i := 0; i < 8; i++ {
		require.NoError(t, svc.saveResult(context.Background(), q, a, 0))
	}
	history, err := svc.History(context.Background(), 1, 100)
	require.NoError(t, err)
	require.Len(t, history, 3)
	newer, err := svc.SaveSettings(context.Background(), q)
	require.NoError(t, err)
	require.NotEqual(t, q.Revision, newer.Revision)
	require.NoError(t, svc.saveResult(context.Background(), q, a, 0))
	out, err = svc.Results(context.Background(), []int64{1})
	require.NoError(t, err)
	require.Empty(t, out.Accounts[0].Version, "old revision cannot become current")
	_, err = db.Exec("UPDATE accounts SET deleted_at=NOW() WHERE id=1")
	require.NoError(t, err)
	summary, err := svc.Summary(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(31), summary["total"])
}
