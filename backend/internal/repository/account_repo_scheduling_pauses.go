package repository

import (
	"context"
	"errors"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
)

func (r *accountRepository) ListAccountSchedulingPauses(ctx context.Context, input service.AccountSchedulingPausesInput) (*service.AccountSchedulingPauses, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}
	result := &service.AccountSchedulingPauses{Version: 1, Accounts: []service.AccountSchedulingPause{}}
	// The database supplies both timestamps so caller clock skew cannot shorten a pause.
	clockRows, err := r.sql.QueryContext(ctx, `SELECT clock_timestamp()`)
	if err != nil {
		return nil, err
	}
	if !clockRows.Next() {
		clockRows.Close()
		return nil, errors.New("database clock unavailable")
	}
	err = clockRows.Scan(&result.ServerNow)
	clockRows.Close()
	if err != nil {
		return nil, err
	}
	query := `SELECT id,name,platform,type,COALESCE(credentials->>'email',''),scheduling_paused_at
 FROM accounts WHERE deleted_at IS NULL AND schedulable IS FALSE AND scheduling_paused_at IS NOT NULL AND id>$1`
	args := []any{input.AfterID, input.Limit + 1}
	if len(input.AccountIDs) > 0 {
		query += ` AND id=ANY($3::bigint[])`
		args = append(args, pq.Array(input.AccountIDs))
	}
	rows, err := r.sql.QueryContext(ctx, query+` ORDER BY id LIMIT $2`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var v service.AccountSchedulingPause
		if err = rows.Scan(&v.AccountID, &v.Name, &v.Platform, &v.Type, &v.Email, &v.SchedulingPausedAt); err != nil {
			return nil, err
		}
		v.PausedSeconds = max(0, int64(result.ServerNow.Sub(v.SchedulingPausedAt)/time.Second))
		result.Accounts = append(result.Accounts, v)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if len(result.Accounts) > input.Limit {
		result.HasMore = true
		result.Accounts = result.Accounts[:input.Limit]
		result.NextAfterID = result.Accounts[len(result.Accounts)-1].AccountID
	}
	return result, nil
}
