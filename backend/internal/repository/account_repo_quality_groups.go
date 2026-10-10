package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
)

type qualityGroupBinding struct {
	ID       int64 `json:"id"`
	Priority int   `json:"priority"`
}

type qualityGroupSwitch struct {
	AccountID int64                 `json:"account_id"`
	Active    bool                  `json:"active"`
	Original  []qualityGroupBinding `json:"original"`
	Target    int64                 `json:"target"`
	Since     time.Time             `json:"since"`
}

// Only the quality transaction owns this metadata. Generic account edits must
// not overwrite it, and copied/imported accounts must not inherit it.
func stripQualityGroupSwitchExtra(extra map[string]any) map[string]any {
	if _, exists := extra["quality_group_switch"]; !exists {
		return extra
	}
	clean := make(map[string]any, len(extra)-1)
	for key, value := range extra {
		if key != "quality_group_switch" {
			clean[key] = value
		}
	}
	return clean
}

// Persist memberships, restoration ownership and scheduler notifications in
// one transaction. Only background quality processing calls this method.
func (r *accountRepository) SyncQualityGroupSwitches(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := r.client.Tx(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	client := tx.Client()
	rows, err := client.QueryContext(ctx, `SELECT pg_try_advisory_xact_lock(781903250)`)
	if err != nil {
		return err
	}
	var locked bool
	if rows.Next() {
		err = rows.Scan(&locked)
	}
	if err == nil {
		err = rows.Err()
	}
	_ = rows.Close()
	if err != nil || !locked {
		return err
	}
	rows, err = client.QueryContext(ctx, `SELECT value FROM settings WHERE key='account_quality_detection_v1' FOR SHARE`)
	if err != nil {
		return err
	}
	var raw []byte
	if rows.Next() {
		err = rows.Scan(&raw)
	}
	if err == nil {
		err = rows.Err()
	}
	_ = rows.Close()
	if err != nil || len(raw) == 0 {
		return err
	}
	q := service.DefaultAccountQualitySettings()
	if err = json.Unmarshal(raw, &q); err != nil {
		return err
	}
	enabled := q.Enabled && q.SwitchGroupOnDegradation && q.DegradationGroupID > 0
	rows, err = client.QueryContext(ctx, `SELECT a.id, a.extra->'quality_group_switch', COALESCE(s.revision,''),
 COALESCE(s.payload->'overall'->>'status',''), COALESCE(s.payload->'scheduling'->>'paused','false')='true'
 FROM accounts a LEFT JOIN account_quality_states s ON s.account_id=a.id
 WHERE a.deleted_at IS NULL AND a.platform='openai' AND (
 a.extra ? 'quality_group_switch' OR ($1 AND s.revision=$2 AND s.payload->'overall'->>'status'='degraded'))
 ORDER BY a.id FOR UPDATE OF a`, enabled, q.Revision)
	if err != nil {
		return err
	}
	type candidate struct {
		id       int64
		marker   *qualityGroupSwitch
		revision string
		status   string
		paused   bool
	}
	var candidates []candidate
	for rows.Next() {
		var item candidate
		var marker []byte
		if err = rows.Scan(&item.id, &marker, &item.revision, &item.status, &item.paused); err != nil {
			break
		}
		if len(marker) > 0 {
			if err = json.Unmarshal(marker, &item.marker); err != nil {
				break
			}
		}
		candidates = append(candidates, item)
	}
	if err == nil {
		err = rows.Err()
	}
	_ = rows.Close()
	if err != nil {
		return err
	}
	if len(candidates) == 0 {
		return tx.Commit()
	}
	accountIDs := make([]int64, 0, len(candidates))
	groupIDs := []int64{q.DegradationGroupID}
	for _, item := range candidates {
		accountIDs = append(accountIDs, item.id)
		if item.marker != nil && item.marker.AccountID == item.id {
			groupIDs = append(groupIDs, item.marker.Target)
			for _, original := range item.marker.Original {
				groupIDs = append(groupIDs, original.ID)
			}
		}
	}
	// Group deletion locks the group before its bindings. Use that same order,
	// and read live groups once for the whole batch rather than per account.
	groups, err := lockQualitySwitchGroups(ctx, client, accountIDs, groupIDs)
	if err != nil {
		return err
	}
	var changed []int64
	var targetErr error
	for _, item := range candidates {
		current, e := loadQualityGroupBindings(ctx, client, item.id)
		if e != nil {
			return e
		}
		marker := item.marker
		if marker != nil && marker.AccountID != item.id {
			// Imported or copied metadata cannot claim another account's groups.
			marker = nil
		}
		if marker != nil && marker.Active && (len(current) != 1 || current[0].ID != marker.Target || current[0].Priority != 1) {
			// An administrator changed memberships after our move. Keep their
			// choice throughout this episode. Deleting the target group itself
			// must still allow restoration once normal or disabled.
			if len(current) != 0 || groups[marker.Target].live {
				marker.Active = false
			}
		}
		normal := item.revision == q.Revision && item.status == "normal" && !item.paused
		degraded := item.revision == q.Revision && item.status == "degraded"
		desired := current
		if marker != nil && (!enabled || normal) {
			if marker.Active {
				// Deleted source groups must never be recreated by recovery.
				desired = []qualityGroupBinding{}
				for _, original := range marker.Original {
					if groups[original.ID].live {
						desired = append(desired, original)
					}
				}
			}
			marker = nil
		} else if enabled && degraded && (marker == nil || marker.Active) &&
			(marker != nil || len(current) != 1 || current[0].ID != q.DegradationGroupID) {
			// Accounts already assigned to the target manually need no move.
			if !groups[q.DegradationGroupID].targetAllowed {
				// A missing target may block new moves, but must not block other
				// accounts from restoring their groups after recovery.
				targetErr = fmt.Errorf("降智切换目标分组 #%d 已删除、禁用或不支持 OpenAI", q.DegradationGroupID)
				continue
			}
			if marker == nil {
				marker = &qualityGroupSwitch{AccountID: item.id, Active: true, Original: current, Since: time.Now().UTC()}
			}
			marker.Target = q.DegradationGroupID
			desired = []qualityGroupBinding{{ID: marker.Target, Priority: 1}}
		}
		if !reflect.DeepEqual(current, desired) {
			if _, err = client.ExecContext(ctx, `DELETE FROM account_groups WHERE account_id=$1`, item.id); err != nil {
				return err
			}
			for _, binding := range desired {
				if _, err = client.ExecContext(ctx, `INSERT INTO account_groups(account_id,group_id,priority) VALUES($1,$2,$3)`, item.id, binding.ID, binding.Priority); err != nil {
					return err
				}
			}
			ids := make([]int64, 0, len(current)+len(desired))
			for _, binding := range append(append([]qualityGroupBinding{}, current...), desired...) {
				ids = append(ids, binding.ID)
			}
			if err = enqueueSchedulerOutbox(ctx, client, service.SchedulerOutboxEventAccountGroupsChanged, &item.id, nil, buildSchedulerGroupPayload(ids)); err != nil {
				return err
			}
			changed = append(changed, item.id)
		}
		// JSON comparison makes repeated background synchronizations read-only.
		if marker == nil {
			_, err = client.ExecContext(ctx, `UPDATE accounts SET extra=extra-'quality_group_switch',updated_at=NOW() WHERE id=$1 AND extra ? 'quality_group_switch'`, item.id)
		} else {
			encoded, e := json.Marshal(marker)
			if e != nil {
				return e
			}
			_, err = client.ExecContext(ctx, `UPDATE accounts SET extra=jsonb_set(COALESCE(extra,'{}'::jsonb),'{quality_group_switch}',$2::jsonb),updated_at=NOW()
 WHERE id=$1 AND (extra->'quality_group_switch') IS DISTINCT FROM $2::jsonb`, item.id, string(encoded))
		}
		if err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	r.syncSchedulerAccountSnapshots(ctx, changed)
	return targetErr
}

func loadQualityGroupBindings(ctx context.Context, client sqlExecutor, id int64) ([]qualityGroupBinding, error) {
	rows, err := client.QueryContext(ctx, `SELECT group_id,priority FROM account_groups WHERE account_id=$1 ORDER BY group_id FOR UPDATE`, id)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []qualityGroupBinding{}
	for rows.Next() {
		var v qualityGroupBinding
		if err := rows.Scan(&v.ID, &v.Priority); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

type qualitySwitchGroup struct {
	live, targetAllowed bool
}

func lockQualitySwitchGroups(ctx context.Context, client sqlExecutor, accountIDs, groupIDs []int64) (map[int64]qualitySwitchGroup, error) {
	rows, err := client.QueryContext(ctx, `SELECT id,deleted_at IS NULL,
 deleted_at IS NULL AND platform='openai' AND status='active' FROM groups
 WHERE id=ANY($1::bigint[]) OR id IN (SELECT group_id FROM account_groups WHERE account_id=ANY($2::bigint[]))
 ORDER BY id FOR SHARE`, pq.Array(groupIDs), pq.Array(accountIDs))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	groups := map[int64]qualitySwitchGroup{}
	for rows.Next() {
		var id int64
		var group qualitySwitchGroup
		if err := rows.Scan(&id, &group.live, &group.targetAllowed); err != nil {
			return nil, err
		}
		groups[id] = group
	}
	return groups, rows.Err()
}
