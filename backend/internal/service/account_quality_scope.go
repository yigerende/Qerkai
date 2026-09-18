package service

import (
	"context"
	"fmt"
	"time"

	"github.com/lib/pq"
)

// EXISTS keeps multi-group accounts unique without loading account IDs into memory.
// The caller's account table alias is a; deleted groups never admit new probes.
func qualityGroupScope(q AccountQualitySettings, args *[]any) string {
	if q.AllGroups {
		return "TRUE"
	}
	if len(q.GroupIDs) == 0 {
		return "FALSE"
	}
	*args = append(*args, pq.Array(q.GroupIDs))
	return fmt.Sprintf(`EXISTS (SELECT 1 FROM account_groups ag JOIN groups g ON g.id=ag.group_id AND g.deleted_at IS NULL WHERE ag.account_id=a.id AND ag.group_id=ANY($%d::bigint[]))`, len(*args))
}

func qualityManualScope(kind int, revisionParam string) string {
	column := "question_requested_at"
	if kind == 1 {
		column = "model_requested_at"
	}
	return `(s.manual_revision=` + revisionParam + ` AND s.` + column + ` IS NOT NULL)`
}

func (s *AccountQualityService) qualityAccountInScope(ctx context.Context, q AccountQualitySettings, id int64, kind int) (bool, error) {
	if q.AllGroups {
		return true, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	args := []any{q.Revision, id}
	scope := qualityGroupScope(q, &args)
	var eligible bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM accounts a LEFT JOIN account_quality_states s ON s.account_id=a.id WHERE a.id=$2 AND a.deleted_at IS NULL AND a.platform='openai' AND (`+scope+` OR `+qualityManualScope(kind, "$1")+`))`, args...).Scan(&eligible)
	return eligible, err
}
