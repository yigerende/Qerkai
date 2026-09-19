package repository

import (
	"context"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

// Quality uses the same schedulable field as the existing account switch.
// The restore marker is provenance only; no scheduler reads it.
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
	const qualityPaused = `(` + enabled + ` AND (COALESCE(s.payload->'scheduling'->>'state_required','false')<>'true' OR s.payload->'scheduling'->>'quality_paused'='true'))`
	const stateRequired = `(` + stateSchedulingEnabledSQL + ` AND a.type='oauth' AND s.payload->'scheduling'->>'state_required'='true')`
	// New accounts and reauthorizations seed the existing recovery queue. The
	// pending marker is consumed in this same transaction, once per account write.
	if _, err = client.ExecContext(ctx, `WITH pending AS MATERIALIZED (
 SELECT a.id FROM accounts a WHERE a.deleted_at IS NULL AND a.platform='openai' AND a.type='oauth'
 AND a.extra->>'state_scheduling_pending'='true' AND `+stateSchedulingEnabledSQL+` ORDER BY a.id FOR UPDATE
 ), seeded AS (INSERT INTO account_quality_states(account_id,revision,version,payload)
 SELECT a.id,COALESCE(q.value::jsonb->>'revision',''),'state-required',
 jsonb_build_object('account_id',a.id,'revision',COALESCE(q.value::jsonb->>'revision',''),'scheduling',
 jsonb_build_object('paused',true,'state_required',true,'successes',0,'since',NOW(),'next_at',NOW()))
 FROM pending a JOIN settings q ON q.key='account_quality_detection_v1'
 ON CONFLICT(account_id) DO UPDATE SET payload=jsonb_set(account_quality_states.payload,'{scheduling}',
 COALESCE(account_quality_states.payload->'scheduling','{}'::jsonb) ||
 jsonb_build_object('quality_paused',COALESCE(account_quality_states.payload->'scheduling'->>'quality_paused','false')='true' OR
 (account_quality_states.payload->'scheduling'->>'paused'='true' AND COALESCE(account_quality_states.payload->'scheduling'->>'state_required','false')<>'true'),
 'paused',true,'state_required',true,'successes',0,'next_at',NOW())) RETURNING account_id)
 UPDATE accounts SET extra=extra-'state_scheduling_pending' WHERE id IN (SELECT account_id FROM seeded)`); err != nil {
		return err
	}
	if _, err = client.ExecContext(ctx, `UPDATE accounts SET extra=extra-'state_scheduling_pending'
 WHERE extra ? 'state_scheduling_pending' AND NOT (`+stateSchedulingEnabledSQL+`)`); err != nil {
		return err
	}
	if _, err = client.ExecContext(ctx, `UPDATE account_quality_states s SET payload=payload-'scheduling' FROM accounts a
 WHERE a.id=s.account_id AND s.payload->'scheduling'->>'paused'='true' AND NOT COALESCE(`+qualityPaused+` OR `+stateRequired+`,FALSE)`); err != nil {
		return err
	}
	if _, err = client.ExecContext(ctx, `UPDATE account_quality_states s SET payload=jsonb_set(payload,'{scheduling}',
 ((payload->'scheduling')-'state_required'-'quality_paused') || jsonb_build_object('state_required',COALESCE(`+stateRequired+`,false),'quality_paused',COALESCE(`+qualityPaused+`,false))) FROM accounts a
 WHERE a.id=s.account_id AND s.payload->'scheduling'->>'paused'='true' AND
 ((s.payload->'scheduling'->>'state_required'='true' AND NOT (`+stateSchedulingEnabledSQL+`)) OR
 (s.payload->'scheduling'->>'quality_paused'='true' AND NOT (`+enabled+`)))`); err != nil {
		return err
	}
	rows, err := client.QueryContext(ctx, `WITH desired AS MATERIALIZED (
 SELECT a.id, (COALESCE(`+qualityPaused+` OR `+stateRequired+`,FALSE) AND COALESCE(s.payload->'scheduling'->>'paused','false')='true') AS paused
 FROM accounts a LEFT JOIN account_quality_states s ON s.account_id=a.id
 WHERE a.deleted_at IS NULL AND a.platform='openai'
 AND (a.extra ? 'quality_schedulable_restore' OR a.extra ? 'quality_scheduling_paused'
   OR (a.schedulable AND COALESCE(`+qualityPaused+` OR `+stateRequired+`,FALSE) AND s.payload->'scheduling'->>'paused'='true'))
 FOR UPDATE OF a
), changed AS (
 UPDATE accounts a SET
 schedulable=CASE WHEN d.paused THEN FALSE
   WHEN a.extra->>'quality_schedulable_restore'='true' AND a.status='active'
     AND (NOT a.auto_pause_on_expired OR a.expires_at IS NULL OR a.expires_at>NOW()) THEN TRUE
   ELSE a.schedulable END,
 extra=(COALESCE(a.extra,'{}'::jsonb)-'quality_scheduling_paused'-'quality_schedulable_restore') ||
   CASE WHEN d.paused AND (a.schedulable OR a.extra->>'quality_schedulable_restore'='true')
     THEN '{"quality_schedulable_restore":true}'::jsonb ELSE '{}'::jsonb END,
 updated_at=NOW()
 FROM desired d WHERE a.id=d.id AND (
   (d.paused AND a.schedulable) OR
   (NOT d.paused AND a.extra ? 'quality_schedulable_restore') OR
   a.extra ? 'quality_scheduling_paused'
 ) RETURNING a.id
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
