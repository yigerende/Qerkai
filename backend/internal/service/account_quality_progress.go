package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

type QualityRunningAccount struct {
	AccountID int64     `json:"account_id"`
	StartedAt time.Time `json:"started_at"`
}
type QualityBatch struct {
	ID         string                  `json:"id"`
	Revision   string                  `json:"revision"`
	Status     string                  `json:"status"`
	Total      int                     `json:"total"`
	Done       int                     `json:"done"`
	Failed     int                     `json:"failed"`
	Skipped    int                     `json:"skipped"`
	PendingIDs []int64                 `json:"pending_ids"`
	Running    []QualityRunningAccount `json:"running"`
	StartedAt  time.Time               `json:"started_at"`
	UpdatedAt  time.Time               `json:"updated_at"`
	FinishedAt *time.Time              `json:"finished_at,omitempty"`
	LastError  string                  `json:"last_error,omitempty"`
}
type QualityDetectorProgress struct {
	Enabled   bool          `json:"enabled"`
	Total     int64         `json:"total"`
	Checked   int64         `json:"checked"`
	Unchecked int64         `json:"unchecked"`
	Pending   int64         `json:"pending"`
	NextAt    *time.Time    `json:"next_at,omitempty"`
	Batch     *QualityBatch `json:"batch,omitempty"`
}
type AccountQualityProgress struct {
	ServerNow time.Time               `json:"server_now"`
	Question  QualityDetectorProgress `json:"question"`
	Model     QualityDetectorProgress `json:"model"`
}

func (s *AccountQualityService) readQualityBatches(ctx context.Context, q AccountQualitySettings) ([2]*QualityBatch, error) {
	var out [2]*QualityBatch
	rows, err := s.db.QueryContext(ctx, `SELECT kind,payload FROM account_quality_batches`)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var kind int
		var raw []byte
		if err := rows.Scan(&kind, &raw); err != nil {
			return out, err
		}
		var b QualityBatch
		if err := json.Unmarshal(raw, &b); err != nil {
			return out, err
		}
		if kind < 0 || kind > 1 || b.Revision != q.Revision {
			continue
		}
		// A killed process cannot keep the UI stuck at "running" indefinitely.
		if b.Status == "running" && time.Since(b.UpdatedAt) > time.Duration(q.TimeoutSeconds+30)*time.Second {
			b.Status, b.Running = "interrupted", nil
			b.LastError = "检测进程中断，等待后台重新调度"
		}
		out[kind] = &b
	}
	return out, rows.Err()
}

func qualityExecution(id int64, enabled bool, due sql.NullTime, batch *QualityBatch, now time.Time) string {
	if !enabled {
		return "disabled"
	}
	if batch != nil && batch.Status == "running" {
		for _, active := range batch.Running {
			if active.AccountID == id {
				return "running"
			}
		}
	}
	if !due.Valid || !due.Time.After(now) {
		return "queued"
	}
	return "idle"
}

// Only aggregates and two bounded batch snapshots are read; no prompts or logs.
func (s *AccountQualityService) Progress(ctx context.Context, q AccountQualitySettings) (AccountQualityProgress, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	p := AccountQualityProgress{ServerNow: time.Now().UTC()}
	batches, err := s.readQualityBatches(ctx, q)
	if err != nil {
		return p, err
	}
	for kind, target := range []*QualityDetectorProgress{&p.Question, &p.Model} {
		key, column := "question", "question_next_at"
		target.Enabled = q.Enabled && q.QuestionEnabled
		if kind == 1 {
			key, column, target.Enabled = "model", "model_next_at", q.Enabled && q.ModelAuditEnabled
		}
		var next sql.NullTime
		running := []int64{}
		if batches[kind] != nil && batches[kind].Status == "running" {
			for _, item := range batches[kind].Running {
				running = append(running, item.AccountID)
			}
		}
		args := []any{q.Revision, pq.Array(running)}
		scope := qualityGroupScope(q, &args)
		err := s.db.QueryRowContext(ctx, `SELECT COUNT(*),
   COUNT(*) FILTER (WHERE s.revision=$1 AND s.payload->'`+key+`'->>'checked_at' IS NOT NULL),
   COUNT(*) FILTER (WHERE (s.account_id IS NULL OR s.`+column+`<=NOW()) AND a.id<>ALL($2::bigint[])),
   MIN(s.`+column+`) FILTER (WHERE s.`+column+`>NOW())
   FROM accounts a LEFT JOIN account_quality_states s ON s.account_id=a.id
   WHERE a.deleted_at IS NULL AND a.platform='openai' AND `+scope, args...).Scan(&target.Total, &target.Checked, &target.Pending, &next)
		if err != nil {
			return p, err
		}
		target.Unchecked = target.Total - target.Checked
		target.Batch = batches[kind]
		if next.Valid {
			target.NextAt = &next.Time
		}
		if !target.Enabled {
			target.Pending, target.NextAt = 0, nil
		}
	}
	return p, nil
}

type qualityBatchTracker struct {
	mu      sync.Mutex
	service *AccountQualityService
	kind    int
	value   QualityBatch
}

func (s *AccountQualityService) newQualityBatch(ctx context.Context, q AccountQualitySettings, kind int, jobs []AccountQualityResult) *qualityBatchTracker {
	b := &qualityBatchTracker{service: s, kind: kind, value: QualityBatch{ID: uuid.NewString(), Revision: q.Revision, Status: "running", Total: len(jobs), StartedAt: time.Now().UTC(), PendingIDs: []int64{}, Running: []QualityRunningAccount{}}}
	for _, v := range jobs {
		b.value.PendingIDs = append(b.value.PendingIDs, v.AccountID)
	}
	b.publish(ctx)
	return b
}

// Caller holds mu, so a slow write cannot overwrite a later worker update.
func (b *qualityBatchTracker) publish(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	b.value.UpdatedAt = time.Now().UTC()
	raw, err := json.Marshal(b.value)
	if err == nil {
		_, err = b.service.db.ExecContext(ctx, `INSERT INTO account_quality_batches(kind,payload) VALUES($1,$2::jsonb)
   ON CONFLICT(kind) DO UPDATE SET payload=excluded.payload,updated_at=NOW()`, b.kind, string(raw))
	}
	if err != nil {
		slog.Warn("account quality progress save failed", "kind", b.kind, "error", err)
	}
}
func (b *qualityBatchTracker) start(ctx context.Context, id int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.value.PendingIDs = slices.DeleteFunc(b.value.PendingIDs, func(v int64) bool { return v == id })
	b.value.Running = append(b.value.Running, QualityRunningAccount{AccountID: id, StartedAt: time.Now().UTC()})
	b.publish(ctx)
}
func (b *qualityBatchTracker) complete(ctx context.Context, id int64, probeErr error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.value.Running = slices.DeleteFunc(b.value.Running, func(v QualityRunningAccount) bool { return v.AccountID == id })
	b.value.Done++
	var raw []byte
	if probeErr == nil {
		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		probeErr = b.service.db.QueryRowContext(readCtx, `SELECT payload FROM account_quality_states WHERE account_id=$1`, id).Scan(&raw)
		cancel()
	}
	var result AccountQualityResult
	if probeErr == nil {
		probeErr = json.Unmarshal(raw, &result)
	}
	verdict := result.Question.QualityVerdict
	if b.kind == 1 {
		verdict = result.Model.QualityVerdict
	}
	if probeErr != nil {
		b.value.Failed++
		b.value.LastError = probeErr.Error()
	} else if result.Revision != b.value.Revision || verdict.CheckedAt == nil || verdict.CheckedAt.Before(b.value.StartedAt) {
		b.value.Skipped++
	} else if verdict.Status == "error" {
		b.value.Failed++
		b.value.LastError = verdict.Error
	}
	b.publish(ctx)
}
func (b *qualityBatchTracker) finish(ctx context.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now().UTC()
	b.value.FinishedAt = &now
	b.value.Status = "done"
	if ctx.Err() != nil || b.value.Done < b.value.Total {
		b.value.Status = "interrupted"
	}
	b.value.Running = []QualityRunningAccount{}
	// Persist shutdown status even after the service context has been cancelled.
	b.publish(context.Background())
}
