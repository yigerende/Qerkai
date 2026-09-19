package repository

import (
	"context"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

// Never change schedulable, status or authentication cooldowns. The separate
// flag survives restarts and is removed when the administrator disables it.
func (r *accountRepository) SyncQualityScheduling(ctx context.Context) error {
	tx, err := r.client.Tx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	client := tx.Client()
	if _, err = client.ExecContext(ctx, "SELECT pg_advisory_xact_lock(781903249)"); err != nil {
		return err
	}
	const enabled = `EXISTS(SELECT 1 FROM settings WHERE key='account_quality_detection_v1' AND value::jsonb->>'enabled'='true' AND value::jsonb->>'pause_on_degradation'='true')`
	if _, err = client.ExecContext(ctx, `UPDATE account_quality_states SET payload=payload-'scheduling' WHERE payload->'scheduling'->>'paused'='true' AND NOT (`+enabled+`)`); err != nil {
		return err
	}
	rows, err := client.QueryContext(ctx, `WITH desired AS (
 SELECT a.id, (`+enabled+` AND COALESCE(s.payload->'scheduling'->>'paused','false')='true') AS paused
 FROM accounts a LEFT JOIN account_quality_states s ON s.account_id=a.id
 WHERE a.deleted_at IS NULL AND a.platform='openai'
), changed AS (
 UPDATE accounts a SET extra=COALESCE(a.extra,'{}'::jsonb)||jsonb_build_object('quality_scheduling_paused',d.paused),updated_at=NOW()
 FROM desired d WHERE a.id=d.id AND COALESCE(a.extra->>'quality_scheduling_paused','false')<>d.paused::text RETURNING a.id
), notified AS (
 INSERT INTO scheduler_outbox(event_type,account_id,payload) SELECT $1,id,'{}'::jsonb FROM changed RETURNING account_id
) SELECT account_id FROM notified`, service.SchedulerOutboxEventAccountChanged)
	if err != nil {
		return err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			break
		}
		ids = append(ids, id)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	r.syncSchedulerAccountSnapshots(ctx, ids)
	return nil
}
