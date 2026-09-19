package repository

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

func modelAuditQuery(input service.ModelAuditInput) (string, []any) {
	args := []any{strings.TrimSpace(input.Model)}
	values := make([]string, 0, len(input.Accounts))
	for _, a := range input.Accounts {
		values = append(values, fmt.Sprintf("($%d::bigint,$%d::timestamptz)", len(args)+1, len(args)+2))
		args = append(args, a.AccountID, a.Since)
	}
	// Each lateral scan uses the existing account_id/created_at index. No COUNT,
	// OFFSET, association hydration, or changes to the ordinary usage query.
	query := `SELECT recent.id,recent.account_id,recent.created_at,recent.requested_model,recent.sent_model,recent.response_model,recent.upstream_model_mismatch,recent.request_started_at
 FROM (VALUES ` + strings.Join(values, ",") + `) AS wanted(account_id,since)
 CROSS JOIN LATERAL (
 SELECT id,account_id,created_at,COALESCE(NULLIF(TRIM(requested_model),''),model) AS requested_model,
 COALESCE(NULLIF(TRIM(upstream_model),''),model) AS sent_model,
 COALESCE(upstream_response_model,'') AS response_model,upstream_model_mismatch,
 CASE WHEN duration_ms IS NOT NULL AND duration_ms>=0 THEN created_at-duration_ms*INTERVAL '1 millisecond' END AS request_started_at
 FROM usage_logs WHERE account_id=wanted.account_id AND created_at>=wanted.since
 AND LOWER(COALESCE(NULLIF(TRIM(upstream_model),''),TRIM(model)))=LOWER($1)
 ORDER BY created_at DESC,id DESC LIMIT 3
 ) recent ORDER BY recent.account_id,recent.created_at,recent.id`
	return query, args
}
func (r *usageLogRepository) LatestModelAudit(ctx context.Context, input service.ModelAuditInput) ([]service.ModelAuditResult, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}
	query, args := modelAuditQuery(input)
	rows, err := r.sql.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]service.ModelAuditResult, len(input.Accounts))
	indices := map[int64]int{}
	for i, a := range input.Accounts {
		indices[a.AccountID] = i
		out[i] = service.ModelAuditResult{AccountID: a.AccountID, Logs: []service.ModelAuditLog{}}
	}
	for rows.Next() {
		var log service.ModelAuditLog
		var mismatch sql.NullBool
		var started sql.NullTime
		if err = rows.Scan(&log.ID, &log.AccountID, &log.CreatedAt, &log.RequestedModel, &log.SentModel, &log.ResponseModel, &mismatch, &started); err != nil {
			return nil, err
		}
		if mismatch.Valid {
			v := mismatch.Bool
			log.Mismatch = &v
		}
		if started.Valid {
			log.RequestStartedAt = &started.Time
		}
		if i, ok := indices[log.AccountID]; ok {
			out[i].Logs = append(out[i].Logs, log)
		}
	}
	return out, rows.Err()
}
