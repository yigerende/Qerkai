package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

type qualitySettingsRepo struct {
	SettingRepository
	mu  sync.Mutex
	raw string
}

func (r *qualitySettingsRepo) GetValue(context.Context, string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.raw == "" {
		return "", ErrSettingNotFound
	}
	return r.raw, nil
}
func (r *qualitySettingsRepo) Set(_ context.Context, _ string, v string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.raw = v
	return nil
}
func qualityTestSettings(t *testing.T) (*SettingService, AccountQualitySettings) {
	t.Helper()
	q := DefaultAccountQualitySettings()
	q.Enabled = true
	q.Revision = "r1"
	q.UpdatedAt = time.Now().Add(-time.Hour)
	raw, err := json.Marshal(q)
	require.NoError(t, err)
	return &SettingService{settingRepo: &qualitySettingsRepo{raw: string(raw)}}, q
}

func TestAccountQualityDefaultsValidation(t *testing.T) {
	q := DefaultAccountQualitySettings()
	require.NoError(t, q.Validate())
	require.False(t, q.Enabled)
	require.Equal(t, "21", q.Questions[0].Answer)
	for _, mutate := range []func(*AccountQualitySettings){func(q *AccountQualitySettings) { q.Concurrency = 0 }, func(q *AccountQualitySettings) { q.Concurrency = -1 }, func(q *AccountQualitySettings) { q.FailureLimit = 0 }, func(q *AccountQualitySettings) { q.IntervalSeconds = 1 }, func(q *AccountQualitySettings) { q.Questions[0].MatchMode = "bad" }, func(q *AccountQualitySettings) { q.Questions[0].MatchMode = "regex"; q.Questions[0].Answer = "[" }, func(q *AccountQualitySettings) { q.Questions[1].ID = q.Questions[0].ID }, func(q *AccountQualitySettings) { q.Questions = nil }} {
		v := DefaultAccountQualitySettings()
		mutate(&v)
		require.Error(t, v.Validate())
	}
	s := &AccountQualityService{settings: &SettingService{settingRepo: &qualitySettingsRepo{}}}
	for _, concurrency := range []int{9, 40, 1000} {
		q.Concurrency = concurrency
		_, err := s.SaveSettings(context.Background(), q)
		require.NoError(t, err)
		saved, err := s.Settings(context.Background())
		require.NoError(t, err)
		require.Equal(t, concurrency, saved.Concurrency)
	}
}
func TestAccountQualityAnswerModesAndStreaks(t *testing.T) {
	q := DefaultAccountQualitySettings()
	now := time.Now()
	question := q.Questions[0]
	for _, tc := range []struct {
		mode, text string
		ms         int64
		status     string
	}{{"content", "21", 25000, "normal"}, {"content", "FINAL_ANSWER=21", 1, "normal"}, {"content", "answer is 21", 1, "suspect"}, {"content", "FINAL_ANSWER=21\nFINAL_ANSWER=20", 1, "suspect"}, {"time", "wrong", 19999, "normal"}, {"time", "21", 20000, "suspect"}, {"content_time", "21", 20000, "suspect"}} {
		t.Run(fmt.Sprint(tc), func(t *testing.T) {
			q.Mode = tc.mode
			v := QualityQuestionResult{}
			applyQualityAnswer(&v, q, question, tc.text, tc.ms, nil, now, now)
			require.Equal(t, tc.status, v.Status)
			interval := q.IntervalSeconds
			if tc.status == "suspect" {
				interval = q.RetrySeconds
			}
			require.NotNil(t, v.NextAt)
			require.Equal(t, now.Add(time.Duration(interval)*time.Second), *v.NextAt)
		})
	}
	q.Mode = "content"
	v := QualityQuestionResult{}
	applyQualityAnswer(&v, q, question, "bad", 1, nil, now, now)
	require.Equal(t, 1, v.Failures)
	require.Equal(t, "clock", v.NextQuestionID)
	applyQualityAnswer(&v, q, question, "", 1, errors.New("429"), now, now)
	require.Equal(t, 1, v.Failures)
	require.Equal(t, "error", v.Status)
	require.Equal(t, "clock", v.NextQuestionID)
	require.Equal(t, now.Add(time.Duration(q.RetrySeconds)*time.Second), *v.NextAt)
	applyQualityAnswer(&v, q, question, "bad", 1, nil, now, now)
	require.True(t, v.Degraded)
	applyQualityAnswer(&v, q, question, "21", 1, nil, now, now)
	require.Equal(t, 0, v.Failures)
	require.True(t, v.Degraded)
	applyQualityAnswer(&v, q, question, "21", 1, nil, now, now)
	require.False(t, v.Degraded)
	require.Equal(t, 2, v.Successes)
	question.MatchMode = "keyword"
	require.True(t, qualityAnswerMatch(question, "answer 21"))
	question.MatchMode = "regex"
	question.Answer = "^2[01]$"
	require.True(t, qualityAnswerMatch(question, "21"))
	require.False(t, qualityAnswerMatch(question, "121"))
}
func TestAccountQualityModelDedupFreshnessAndRecovery(t *testing.T) {
	q := DefaultAccountQualitySettings()
	q.UpdatedAt = time.Now().Add(-time.Hour)
	now := time.Now()
	yes, no := true, false
	logs := []ModelAuditLog{{ID: 1, CreatedAt: now.Add(-time.Minute), SentModel: q.ModelAuditModel, ResponseModel: "different", Mismatch: &yes}, {ID: 2, CreatedAt: now, SentModel: q.ModelAuditModel, ResponseModel: "different", Mismatch: &yes}}
	v := QualityModelResult{}
	applyQualityModelLogs(&v, q, logs, now)
	require.True(t, v.Degraded)
	require.Equal(t, 2, v.Failures)
	evidence := *v.EvidenceAt
	for i := 0; i < 5; i++ {
		applyQualityModelLogs(&v, q, logs, now.Add(time.Duration(i+1)*time.Minute))
	}
	require.Equal(t, 2, v.Failures)
	require.Equal(t, evidence, *v.EvidenceAt)
	require.True(t, v.NoNewSamples)
	applyQualityModelLogs(&v, q, []ModelAuditLog{{ID: 3, CreatedAt: now.Add(time.Second), SentModel: q.ModelAuditModel, ResponseModel: q.ModelAuditModel + "-2026-09-18", Mismatch: &yes}}, now)
	require.Equal(t, "variant", v.Status)
	require.True(t, v.Degraded)
	applyQualityModelLogs(&v, q, []ModelAuditLog{{ID: 4, CreatedAt: now.Add(2 * time.Second), SentModel: q.ModelAuditModel, ResponseModel: q.ModelAuditModel, Mismatch: &no}}, now)
	require.False(t, v.Degraded)
	require.Equal(t, 2, v.Successes)
	applyQualityModelLogs(&v, q, []ModelAuditLog{{ID: 5, CreatedAt: now.Add(-2 * time.Hour), SentModel: q.ModelAuditModel, ResponseModel: "wrong", Mismatch: &yes}}, now)
	require.False(t, v.Degraded)
	unknown := QualityModelResult{}
	applyQualityModelLogs(&unknown, q, []ModelAuditLog{{ID: 10, CreatedAt: now, SentModel: q.ModelAuditModel}}, now)
	require.Equal(t, "no_samples", unknown.Status)
	require.Nil(t, unknown.EvidenceAt)
}
func TestAccountQualityReadOnlyResultsRevisionAndBounds(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	settings, q := qualityTestSettings(t)
	// A nil test service deliberately proves reads cannot perform upstream probes.
	s := &AccountQualityService{db: db, settings: settings}
	state := AccountQualityResult{AccountID: 7, Revision: q.Revision, Version: "v1"}
	raw, _ := json.Marshal(state)
	mock.ExpectQuery(`SELECT kind,payload FROM account_quality_batches`).WillReturnRows(sqlmock.NewRows([]string{"kind", "payload"}))
	mock.ExpectQuery(`SELECT wanted.id,s.payload`).WithArgs(int64(7), int64(8), q.Revision).WillReturnRows(sqlmock.NewRows([]string{"id", "payload", "question_due", "model_due", "eligible_id", "in_scope", "manual_question", "manual_model"}).AddRow(7, raw, nil, nil, 7, true, false, false).AddRow(8, nil, nil, nil, nil, true, false, false))
	out, err := s.Results(context.Background(), []int64{7, 8})
	require.NoError(t, err)
	require.Len(t, out.Accounts, 2)
	require.Equal(t, "v1", out.Accounts[0].Version)
	require.Empty(t, out.Accounts[1].Version)
	body, _ := json.Marshal(out)
	require.NotContains(t, string(body), "prompt")
	require.NotContains(t, string(body), "questions")
	state.Revision = "obsolete"
	raw, _ = json.Marshal(state)
	mock.ExpectQuery(`SELECT kind,payload FROM account_quality_batches`).WillReturnRows(sqlmock.NewRows([]string{"kind", "payload"}))
	mock.ExpectQuery(`SELECT wanted.id,s.payload`).WithArgs(int64(7), q.Revision).WillReturnRows(sqlmock.NewRows([]string{"id", "payload", "question_due", "model_due", "eligible_id", "in_scope", "manual_question", "manual_model"}).AddRow(7, raw, time.Now().Add(time.Hour), time.Now().Add(time.Hour), 7, true, false, false))
	out, err = s.Results(context.Background(), []int64{7})
	require.NoError(t, err)
	require.Empty(t, out.Accounts[0].Version)
	require.Equal(t, "queued", out.Accounts[0].QuestionExecution)
	for _, ids := range [][]int64{nil, {0}, {1, 1}, make([]int64, 101)} {
		_, err = s.Results(context.Background(), ids)
		require.Error(t, err)
	}
	require.NoError(t, mock.ExpectationsWereMet())
}
func TestAccountQualityReplicaLockAndDisabled(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	settings, _ := qualityTestSettings(t)
	s := &AccountQualityService{db: db, settings: settings}
	mock.ExpectQuery(`SELECT pg_try_advisory_lock`).WithArgs(accountQualityLockID).WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(false))
	require.NoError(t, s.runDue(context.Background(), 0))
	require.NoError(t, mock.ExpectationsWereMet())
	q := DefaultAccountQualitySettings()
	_, err = s.SaveSettings(context.Background(), q)
	require.NoError(t, err)
	require.NoError(t, s.runDue(context.Background(), 0))
	require.Error(t, s.Schedule(context.Background(), nil))
}
func TestAccountQualityCaptureBound(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := &qualityTestWriter{header: make(http.Header), cancel: cancel}
	_, err := w.Write([]byte(strings.Repeat("a", 2<<20)))
	require.NoError(t, err)
	_, err = w.Write([]byte("x"))
	require.Error(t, err)
	require.Error(t, ctx.Err())
	require.Equal(t, 2<<20, w.body.Len())
}
