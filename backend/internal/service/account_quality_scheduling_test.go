//go:build unit

package service

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func qualitySchedulingDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("QUALITY_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("isolated PostgreSQL URL not provided")
	}
	parsed, err := url.Parse(dsn)
	require.NoError(t, err)
	require.Contains(t, []string{"localhost", "127.0.0.1"}, parsed.Hostname())
	root, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	schema := fmt.Sprintf("quality_schedule_%d", time.Now().UnixNano())
	_, err = root.Exec("CREATE SCHEMA " + pq.QuoteIdentifier(schema))
	require.NoError(t, err)
	t.Cleanup(func() { root.Exec("DROP SCHEMA " + pq.QuoteIdentifier(schema) + " CASCADE"); root.Close() })
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	db, err := sql.Open("postgres", parsed.String())
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(20)
	_, err = db.Exec(`CREATE TABLE accounts(id BIGINT PRIMARY KEY,name TEXT NOT NULL DEFAULT 'test account',platform TEXT NOT NULL,deleted_at TIMESTAMPTZ);CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);INSERT INTO accounts(id,platform) SELECT id,'openai' FROM generate_series(1,62) id`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE groups(id BIGINT PRIMARY KEY,deleted_at TIMESTAMPTZ); CREATE TABLE account_groups(account_id BIGINT NOT NULL,group_id BIGINT NOT NULL,PRIMARY KEY(account_id,group_id))`)
	require.NoError(t, err)
	for _, name := range []string{"236_account_quality_detection.sql", "237_account_quality_progress.sql", "238_account_quality_group_scope.sql"} {
		raw, err := os.ReadFile("../../migrations/" + name)
		require.NoError(t, err)
		_, err = db.Exec(string(raw))
		require.NoError(t, err)
	}
	return db
}

type qualitySchedulingAccounts struct{ AccountRepository }

func (*qualitySchedulingAccounts) GetByID(_ context.Context, id int64) (*Account, error) {
	return &Account{ID: id, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Concurrency: 1, Credentials: map[string]any{"access_token": "fixture-token"}}, nil
}
func (*qualitySchedulingAccounts) UpdateExtra(context.Context, int64, map[string]any) error {
	return nil
}

type qualitySchedulingUpstream struct {
	HTTPUpstream
	gate            chan struct{}
	started         chan int64
	mu              sync.Mutex
	calls           map[int64]int
	active, maximum atomic.Int32
}

func (u *qualitySchedulingUpstream) DoWithTLS(req *http.Request, _ string, id int64, _ int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	n := u.active.Add(1)
	defer u.active.Add(-1)
	for old := u.maximum.Load(); n > old; old = u.maximum.Load() {
		if u.maximum.CompareAndSwap(old, n) {
			break
		}
	}
	u.mu.Lock()
	u.calls[id]++
	u.mu.Unlock()
	if u.started != nil {
		u.started <- id
	}
	if u.gate != nil {
		select {
		case <-u.gate:
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
	}
	time.Sleep(5 * time.Millisecond)
	if id == 5 {
		return nil, fmt.Errorf("fixture upstream unavailable")
	}
	return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("data: {\"type\":\"response.output_text.delta\",\"delta\":\"21\"}\n\ndata: {\"type\":\"response.completed\"}\n\n"))}, nil
}
func qualitySchedulingService(t *testing.T, db *sql.DB, u *qualitySchedulingUpstream) (*AccountQualityService, AccountQualitySettings) {
	svc := &AccountQualityService{db: db, settings: &SettingService{settingRepo: &qualityPGSettings{db: db}}, usage: &UsageService{usageRepo: &qualityPGAudit{}}, tests: &AccountTestService{accountRepo: &qualitySchedulingAccounts{}, httpUpstream: u}}
	q := DefaultAccountQualitySettings()
	q.Enabled = true
	q.ModelAuditEnabled = true
	q.Mode = "content"
	q.Questions = q.Questions[:1]
	q, err := svc.SaveSettings(context.Background(), q)
	require.NoError(t, err)
	return svc, q
}

func TestAccountQualitySchedulingFairnessProgressAndIndependentClocks(t *testing.T) {
	db := qualitySchedulingDB(t)
	u := &qualitySchedulingUpstream{gate: make(chan struct{}), started: make(chan int64, 128), calls: map[int64]int{}}
	svc, q := qualitySchedulingService(t, db, u)
	ctx := context.Background()
	// Repeated model writes must not postpone an account's first question.
	require.NoError(t, svc.runDue(ctx, 1))
	require.NoError(t, svc.runDue(ctx, 1))
	var before, after time.Time
	require.NoError(t, db.QueryRow(`SELECT question_next_at FROM account_quality_states WHERE account_id=62`).Scan(&before))
	snap, err := svc.Results(ctx, []int64{62})
	require.NoError(t, err)
	require.NoError(t, svc.saveResult(ctx, q, snap.Accounts[0], 1))
	require.NoError(t, db.QueryRow(`SELECT question_next_at FROM account_quality_states WHERE account_id=62`).Scan(&after))
	require.Equal(t, before, after)
	done := make(chan error, 1)
	go func() { done <- svc.runDue(ctx, 0) }()
	for i := 0; i < 4; i++ {
		select {
		case <-u.started:
		case <-time.After(10 * time.Second):
			t.Fatal("workers did not start")
		}
	}
	p, err := svc.Progress(ctx, q)
	require.NoError(t, err)
	require.EqualValues(t, 62, p.Question.Total)
	require.EqualValues(t, 58, p.Question.Pending)
	require.Len(t, p.Question.Batch.Running, 4)
	require.Equal(t, 32, p.Question.Batch.Total)
	snap, err = svc.Results(ctx, []int64{1, 62})
	require.NoError(t, err)
	require.Equal(t, "running", snap.Accounts[0].QuestionExecution)
	require.Equal(t, "queued", snap.Accounts[1].QuestionExecution)
	// A second service instance sees the same progress and cannot launch duplicates.
	replica := &AccountQualityService{db: db, settings: svc.settings, tests: svc.tests, usage: svc.usage}
	require.NoError(t, replica.runDue(ctx, 0))
	require.NoError(t, svc.Schedule(ctx, []int64{62}))
	require.NoError(t, svc.runDue(ctx, 1))
	close(u.gate)
	require.NoError(t, <-done)
	p, err = replica.Progress(ctx, q)
	require.NoError(t, err)
	require.Equal(t, "done", p.Question.Batch.Status)
	require.Equal(t, 32, p.Question.Batch.Done)
	require.Equal(t, 1, p.Question.Batch.Failed)
	require.Contains(t, p.Question.Batch.LastError, "fixture upstream unavailable")
	// Make the already tested accounts due again. Untested 33..62 must go first.
	_, err = db.Exec(`UPDATE account_quality_states SET question_next_at='epoch'::timestamptz WHERE account_id<=32`)
	require.NoError(t, err)
	require.NoError(t, svc.runDue(ctx, 0))
	p, err = svc.Progress(ctx, q)
	require.NoError(t, err)
	require.EqualValues(t, 62, p.Question.Checked)
	require.Zero(t, p.Question.Unchecked)
	require.LessOrEqual(t, u.maximum.Load(), int32(4))
	u.mu.Lock()
	require.Len(t, u.calls, 62)
	for id := int64(33); id <= 62; id++ {
		require.Equal(t, 1, u.calls[id])
	}
	u.mu.Unlock()
	// A forced deadline must survive a simultaneous save by the other detector.
	require.NoError(t, svc.Schedule(ctx, []int64{40}))
	require.NoError(t, db.QueryRow(`SELECT question_next_at FROM account_quality_states WHERE account_id=40`).Scan(&before))
	snap, err = svc.Results(ctx, []int64{40})
	require.NoError(t, err)
	require.NoError(t, svc.saveResult(ctx, q, snap.Accounts[0], 1))
	require.NoError(t, db.QueryRow(`SELECT question_next_at FROM account_quality_states WHERE account_id=40`).Scan(&after))
	require.Equal(t, before, after)
	history, err := svc.History(ctx, 40, 10)
	require.NoError(t, err)
	require.Equal(t, "model", history[0].DetectionKind)
	require.NotNil(t, history[0].RecordedAt)
	require.Equal(t, "question", history[1].DetectionKind)
	// Restart/stale process snapshots never stay "running" indefinitely.
	_, err = db.Exec(`UPDATE account_quality_batches SET payload=jsonb_set(jsonb_set(payload,'{status}','"running"'),'{updated_at}',to_jsonb($1::text)) WHERE kind=0`, time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano))
	require.NoError(t, err)
	batches, err := svc.readQualityBatches(ctx, q)
	require.NoError(t, err)
	require.Equal(t, "interrupted", batches[0].Status)
}

func TestAccountQualitySchedulingCancellationRevisionAndTimeout(t *testing.T) {
	db := qualitySchedulingDB(t)
	u := &qualitySchedulingUpstream{gate: make(chan struct{}), started: make(chan int64, 128), calls: map[int64]int{}}
	svc, q := qualitySchedulingService(t, db, u)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.runDue(ctx, 0) }()
	<-u.started
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	p, err := svc.Progress(context.Background(), q)
	require.NoError(t, err)
	require.Equal(t, "interrupted", p.Question.Batch.Status)
	u.gate = make(chan struct{})
	u.started = make(chan int64, 128)
	go func() { done <- svc.runDue(context.Background(), 0) }()
	<-u.started
	q, err = svc.SaveSettings(context.Background(), q)
	require.NoError(t, err)
	close(u.gate)
	require.NoError(t, <-done)
	p, err = svc.Progress(context.Background(), q)
	require.NoError(t, err)
	require.Nil(t, p.Question.Batch)
	require.Zero(t, p.Question.Checked)
	// Exercise the real context timeout through the existing account test path.
	u.gate = make(chan struct{})
	q.TimeoutSeconds = 5
	_, _, err = svc.testAnswer(context.Background(), 1, q, q.Questions[0])
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
