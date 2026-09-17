package service

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const accountQualitySettingKey = "account_quality_detection_v1"
const accountQualityLockID int64 = 781903245

type AccountQualityService struct {
	db       *sql.DB
	settings *SettingService
	tests    *AccountTestService
	usage    *UsageService
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	runMu    [2]sync.Mutex
}

func ProvideAccountQualityService(db *sql.DB, settings *SettingService, tests *AccountTestService, usage *UsageService) *AccountQualityService {
	ctx, cancel := context.WithCancel(context.Background())
	s := &AccountQualityService{db: db, settings: settings, tests: tests, usage: usage, ctx: ctx, cancel: cancel}
	for kind := 0; kind < 2; kind++ {
		s.wg.Add(1)
		go func(kind int) {
			defer s.wg.Done()
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if err := s.runDue(ctx, kind); err != nil && ctx.Err() == nil {
						slog.Warn("account quality cycle failed", "error", err)
					}
				}
			}
		}(kind)
	}
	return s
}
func (s *AccountQualityService) Stop() { s.cancel(); s.wg.Wait() }
func (s *AccountQualityService) Settings(ctx context.Context) (AccountQualitySettings, error) {
	q := DefaultAccountQualitySettings()
	raw, err := s.settings.settingRepo.GetValue(ctx, accountQualitySettingKey)
	if errors.Is(err, ErrSettingNotFound) {
		return q, nil
	}
	if err != nil {
		return q, err
	}
	err = json.Unmarshal([]byte(raw), &q)
	return q, err
}
func (s *AccountQualityService) SaveSettings(ctx context.Context, q AccountQualitySettings) (AccountQualitySettings, error) {
	if err := q.Validate(); err != nil {
		return q, err
	}
	q.Revision = uuid.NewString()
	q.UpdatedAt = time.Now().UTC()
	raw, err := json.Marshal(q)
	if err != nil {
		return q, err
	}
	err = s.settings.settingRepo.Set(ctx, accountQualitySettingKey, string(raw))
	return q, err
}

type AccountQualitySnapshot struct {
	Version   int                    `json:"version"`
	ServerNow time.Time              `json:"server_now"`
	Settings  AccountQualityPolicy   `json:"settings"`
	Accounts  []AccountQualityResult `json:"accounts"`
}

// Polling clients need policy metadata, not the potentially large prompt bank.
type AccountQualityPolicy struct {
	Enabled           bool   `json:"enabled"`
	Revision          string `json:"revision"`
	QuestionEnabled   bool   `json:"question_enabled"`
	ModelAuditEnabled bool   `json:"model_audit_enabled"`
	FailureLimit      int    `json:"failure_limit"`
	RecoveryLimit     int    `json:"recovery_limit"`
	ModelAuditModel   string `json:"model_audit_model"`
}

func validateQualityIDs(ids []int64) error {
	if len(ids) < 1 || len(ids) > 100 {
		return errors.New("require 1-100 account IDs")
	}
	seen := map[int64]bool{}
	for _, id := range ids {
		if id < 1 || seen[id] {
			return errors.New("account IDs must be positive and unique")
		}
		seen[id] = true
	}
	return nil
}
func qualityIDValues(ids []int64) (string, []any) {
	p := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		p[i] = fmt.Sprintf("($%d::bigint)", i+1)
		args[i] = id
	}
	return strings.Join(p, ","), args
}
func (s *AccountQualityService) Results(ctx context.Context, ids []int64) (AccountQualitySnapshot, error) {
	out := AccountQualitySnapshot{Version: 1, ServerNow: time.Now().UTC(), Accounts: []AccountQualityResult{}}
	if err := validateQualityIDs(ids); err != nil {
		return out, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	q, err := s.Settings(ctx)
	if err != nil {
		return out, err
	}
	out.Settings = AccountQualityPolicy{Enabled: q.Enabled, Revision: q.Revision, QuestionEnabled: q.QuestionEnabled, ModelAuditEnabled: q.ModelAuditEnabled, FailureLimit: q.FailureLimit, RecoveryLimit: q.RecoveryLimit, ModelAuditModel: q.ModelAuditModel}
	values, args := qualityIDValues(ids)
	rows, err := s.db.QueryContext(ctx, `SELECT wanted.id,s.payload FROM (VALUES `+values+`) wanted(id)
 LEFT JOIN accounts a ON a.id=wanted.id AND a.deleted_at IS NULL AND a.platform='openai'
 LEFT JOIN account_quality_states s ON s.account_id=a.id`, args...)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var raw []byte
		if err = rows.Scan(&id, &raw); err != nil {
			return out, err
		}
		v := AccountQualityResult{AccountID: id}
		if len(raw) > 0 {
			if err = json.Unmarshal(raw, &v); err != nil {
				return out, err
			}
		}
		if v.Revision != q.Revision {
			v = AccountQualityResult{AccountID: id, Revision: q.Revision}
		}
		out.Accounts = append(out.Accounts, v)
	}
	return out, rows.Err()
}
func (s *AccountQualityService) History(ctx context.Context, id int64, limit int) ([]AccountQualityResult, error) {
	if id < 1 || limit < 1 || limit > 1000 {
		return nil, errors.New("invalid history request")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `SELECT payload FROM account_quality_history WHERE account_id=$1 ORDER BY id DESC LIMIT $2`, id, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []AccountQualityResult{}
	for rows.Next() {
		var raw []byte
		var v AccountQualityResult
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (s *AccountQualityService) Summary(ctx context.Context) (map[string]int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	q, err := s.Settings(ctx)
	if err != nil {
		return nil, err
	}
	var total, normal, degraded, suspect, failed, mnormal, mfailed, nosamples int64
	err = s.db.QueryRowContext(ctx, `SELECT COUNT(*),
 COUNT(*) FILTER(WHERE s.payload->'question'->>'status'='normal'),
 COUNT(*) FILTER(WHERE s.payload->'question'->>'degraded'='true' OR s.payload->'model'->>'degraded'='true'),
 COUNT(*) FILTER(WHERE s.payload->'question'->>'status'='suspect'),
 COUNT(*) FILTER(WHERE s.payload->'question'->>'status'='error'),
 COUNT(*) FILTER(WHERE s.payload->'model'->>'status' IN ('normal','variant')),
 COUNT(*) FILTER(WHERE s.payload->'model'->>'degraded'='true'),
 COUNT(*) FILTER(WHERE COALESCE(s.payload->'model'->>'status','') IN ('','no_samples'))
 FROM accounts a LEFT JOIN account_quality_states s ON s.account_id=a.id AND s.revision=$1
 WHERE a.deleted_at IS NULL AND a.platform='openai'`, q.Revision).Scan(&total, &normal, &degraded, &suspect, &failed, &mnormal, &mfailed, &nosamples)
	return map[string]int64{"total": total, "normal": normal, "degraded": degraded, "suspect": suspect, "errors": failed, "model_normal": mnormal, "model_degraded": mfailed, "no_samples": nosamples}, err
}

// Scheduling only updates due times. Read endpoints never enqueue paid probes.
func (s *AccountQualityService) Schedule(ctx context.Context, ids []int64) error {
	if len(ids) > 0 {
		if err := validateQualityIDs(ids); err != nil {
			return err
		}
	}
	q, err := s.Settings(ctx)
	if err != nil {
		return err
	}
	if !q.Enabled {
		return errors.New("请先启用并保存降智检测")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	query := `UPDATE account_quality_states SET next_at=NOW(), question_next_at=NOW(), model_next_at=NOW(), payload=payload #- '{question,next_at}' #- '{model,next_at}'`
	args := []any{}
	if len(ids) > 0 {
		values, a := qualityIDValues(ids)
		query += ` WHERE account_id IN (SELECT id FROM (VALUES ` + values + `) wanted(id))`
		args = a
	}
	_, err = s.db.ExecContext(ctx, query, args...)
	return err
}
func (s *AccountQualityService) runDue(ctx context.Context, kind int) error {
	if !s.runMu[kind].TryLock() {
		return nil
	}
	defer s.runMu[kind].Unlock()
	q, err := s.Settings(ctx)
	if err != nil || !q.Enabled {
		return err
	}
	if (kind == 0 && !q.QuestionEnabled) || (kind == 1 && !q.ModelAuditEnabled) {
		return nil
	}
	lockID := accountQualityLockID + int64(kind)
	// Session lock remains held for the whole bounded batch, including requests.
	lockCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	conn, err := s.db.Conn(lockCtx)
	if err != nil {
		cancel()
		return err
	}
	defer conn.Close()
	var acquired bool
	err = conn.QueryRowContext(lockCtx, "SELECT pg_try_advisory_lock($1)", lockID).Scan(&acquired)
	cancel()
	if err != nil || !acquired {
		if err != nil {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
		return err
	}
	defer func() {
		releaseCtx, release := context.WithTimeout(context.Background(), 5*time.Second)
		defer release()
		if _, e := conn.ExecContext(releaseCtx, "SELECT pg_advisory_unlock($1)", lockID); e != nil {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
	}()
	queryCtx, queryCancel := context.WithTimeout(ctx, 5*time.Second)
	dueColumn := "question_next_at"
	if kind == 1 {
		dueColumn = "model_next_at"
	}
	rows, err := s.db.QueryContext(queryCtx, `SELECT a.id,s.payload FROM accounts a LEFT JOIN account_quality_states s ON s.account_id=a.id
 WHERE a.deleted_at IS NULL AND a.platform='openai' AND (s.account_id IS NULL OR s.revision<>$1 OR s.`+dueColumn+`<=NOW())
 ORDER BY COALESCE(s.`+dueColumn+`,'epoch'::timestamptz),a.id LIMIT 32`, q.Revision)
	if err != nil {
		queryCancel()
		return err
	}
	jobs := []AccountQualityResult{}
	for rows.Next() {
		var id int64
		var raw []byte
		if err = rows.Scan(&id, &raw); err != nil {
			break
		}
		v := AccountQualityResult{AccountID: id, Revision: q.Revision}
		if len(raw) > 0 {
			err = json.Unmarshal(raw, &v)
			if err != nil {
				break
			}
		}
		if v.Revision != q.Revision {
			v = AccountQualityResult{AccountID: id, Revision: q.Revision}
		}
		jobs = append(jobs, v)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	queryCancel()
	if err != nil {
		return err
	}
	queue := make(chan AccountQualityResult)
	var wg sync.WaitGroup
	workers := q.Concurrency
	if kind == 1 && workers > 2 {
		workers = 2
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for v := range queue {
				if ctx.Err() != nil {
					return
				}
				if e := s.probe(ctx, q, v, kind); e != nil && ctx.Err() == nil {
					slog.Warn("account quality probe failed", "account_id", v.AccountID, "error", e)
				}
			}
		}()
	}
send:
	for _, v := range jobs {
		select {
		case queue <- v:
		case <-ctx.Done():
			break send
		}
	}
	close(queue)
	wg.Wait()
	return ctx.Err()
}
func (s *AccountQualityService) probe(ctx context.Context, q AccountQualitySettings, v AccountQualityResult, kind int) error {
	current, err := s.Settings(ctx)
	if err != nil {
		return err
	}
	if !current.Enabled || current.Revision != q.Revision {
		return nil
	}
	now := time.Now().UTC()
	changed := false
	if kind == 1 && q.ModelAuditEnabled && (v.Model.NextAt == nil || !now.Before(*v.Model.NextAt)) {
		results, e := s.usage.LatestModelAudit(ctx, ModelAuditInput{Model: q.ModelAuditModel, Accounts: []ModelAuditAccount{{AccountID: v.AccountID, Since: q.UpdatedAt}}})
		if e != nil {
			v.Model.Status = "error"
			v.Model.Error = e.Error()
			v.Model.CheckedAt = &now
			next := now.Add(time.Duration(q.RetrySeconds) * time.Second)
			v.Model.NextAt = &next
		} else {
			logs := []ModelAuditLog{}
			if len(results) > 0 {
				logs = results[0].Logs
			}
			applyQualityModelLogs(&v.Model, q, logs, now)
		}
		changed = true
	}
	if kind == 0 && q.QuestionEnabled && (v.Question.NextAt == nil || !now.Before(*v.Question.NextAt)) {
		active := q.ActiveQuestions()
		if len(active) == 0 {
			return errors.New("no enabled questions")
		}
		question := active[0]
		for _, item := range active {
			if item.ID == v.Question.NextQuestionID {
				question = item
				break
			}
		}
		started := time.Now().UTC()
		answer, duration, e := s.testAnswer(ctx, v.AccountID, q, question)
		applyQualityAnswer(&v.Question, q, question, answer, duration, e, started, time.Now().UTC())
		changed = true
	}
	if !changed {
		return nil
	}
	current, err = s.Settings(ctx)
	if err != nil {
		return err
	}
	if !current.Enabled || current.Revision != q.Revision {
		return nil
	}
	return s.saveResult(ctx, q, v, kind)
}

func (s *AccountQualityService) saveResult(ctx context.Context, q AccountQualitySettings, v AccountQualityResult, kind int) error {
	saveCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(saveCtx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var currentRevision string
	if err = tx.QueryRowContext(saveCtx, `SELECT value::jsonb->>'revision' FROM settings WHERE key=$1 FOR SHARE`, accountQualitySettingKey).Scan(&currentRevision); err != nil {
		return err
	}
	if currentRevision != q.Revision {
		return nil
	}
	_, err = tx.ExecContext(saveCtx, `INSERT INTO account_quality_states(account_id,revision,version,payload) SELECT $1,$2,'','{}'::jsonb FROM accounts WHERE id=$1 AND deleted_at IS NULL ON CONFLICT(account_id) DO NOTHING`, v.AccountID, q.Revision)
	if err != nil {
		return err
	}
	var previous []byte
	if err = tx.QueryRowContext(saveCtx, `SELECT payload FROM account_quality_states WHERE account_id=$1 FOR UPDATE`, v.AccountID).Scan(&previous); err != nil {
		return err
	}
	var saved AccountQualityResult
	if err = json.Unmarshal(previous, &saved); err != nil {
		return err
	}
	if saved.Revision != q.Revision {
		saved = AccountQualityResult{AccountID: v.AccountID, Revision: q.Revision}
	}
	if kind == 0 {
		saved.Question = v.Question
	} else {
		saved.Model = v.Model
	}
	v = saved
	v.Version = uuid.NewString()
	questionNext, modelNext := time.Now().UTC(), time.Now().UTC()
	if v.Question.NextAt != nil {
		questionNext = *v.Question.NextAt
	}
	if v.Model.NextAt != nil {
		modelNext = *v.Model.NextAt
	}
	next := questionNext
	if modelNext.Before(next) {
		next = modelNext
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(saveCtx, `UPDATE account_quality_states SET revision=$2,version=$3,next_at=$4,payload=$5::jsonb,question_next_at=$6,model_next_at=$7,updated_at=NOW() WHERE account_id=$1`, v.AccountID, q.Revision, v.Version, next, string(raw), questionNext, modelNext)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(saveCtx, `INSERT INTO account_quality_history(account_id,payload) SELECT $1,$2::jsonb FROM accounts WHERE id=$1 AND deleted_at IS NULL`, v.AccountID, string(raw))
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(saveCtx, `DELETE FROM account_quality_history WHERE account_id=$1 AND id IN (SELECT id FROM account_quality_history WHERE account_id=$1 ORDER BY id DESC OFFSET $2)`, v.AccountID, q.HistoryLimit)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// Capture the existing test implementation with a strict memory bound.
type qualityTestWriter struct {
	header   http.Header
	body     bytes.Buffer
	cancel   context.CancelFunc
	overflow bool
}

func (w *qualityTestWriter) Header() http.Header { return w.header }
func (w *qualityTestWriter) WriteHeader(int)     {}
func (w *qualityTestWriter) Flush()              {}
func (w *qualityTestWriter) Write(b []byte) (int, error) {
	if w.body.Len()+len(b) > 2<<20 {
		w.overflow = true
		w.cancel()
		return 0, errors.New("quality test output exceeds 2 MiB")
	}
	return w.body.Write(b)
}
func (s *AccountQualityService) testAnswer(ctx context.Context, id int64, q AccountQualitySettings, question QualityQuestion) (string, int64, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(q.TimeoutSeconds)*time.Second)
	defer cancel()
	writer := &qualityTestWriter{header: make(http.Header), cancel: cancel}
	c, _ := gin.CreateTestContext(writer)
	c.Request = (&http.Request{}).WithContext(ctx)
	started := time.Now()
	err := s.tests.TestAccountConnection(c, id, q.Model, question.Prompt, AccountTestModeDefault, AccountTestOptions{ReasoningEffort: q.ReasoningEffort})
	duration := time.Since(started).Milliseconds()
	answer, message := parseTestSSEOutput(writer.body.String())
	if err != nil {
		return "", duration, err
	}
	if ctx.Err() != nil {
		return "", duration, ctx.Err()
	}
	if writer.overflow {
		return "", duration, errors.New("test output too large")
	}
	if message != "" {
		return "", duration, errors.New(message)
	}
	complete := false
	for _, line := range strings.Split(writer.body.String(), "\n") {
		if strings.HasPrefix(line, "data: ") {
			var event TestEvent
			if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event) == nil && event.Type == "test_complete" && event.Success {
				complete = true
			}
		}
	}
	if !complete || strings.TrimSpace(answer) == "" {
		return "", duration, errors.New("测试响应不完整，未计入答题异常")
	}
	if len(answer) > 16384 {
		return "", duration, errors.New("测试答案过长，未计入答题异常")
	}
	return answer, duration, nil
}
