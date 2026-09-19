package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"time"
)

type QualitySelectedAccount struct {
	AccountID       int64      `json:"account_id"`
	Name            string     `json:"name"`
	SkipReason      string     `json:"skip_reason,omitempty"`
	QuestionChecked *time.Time `json:"question_checked_at,omitempty"`
	ModelChecked    *time.Time `json:"model_checked_at,omitempty"`
}

type QualitySelectedRun struct {
	Revision        string                   `json:"revision"`
	QuestionEnabled bool                     `json:"question_enabled"`
	ModelEnabled    bool                     `json:"model_audit_enabled"`
	Accounts        []QualitySelectedAccount `json:"accounts"`
}

// Capture result baselines and enqueue only the selected accounts atomically.
// An already running check may satisfy this request, without duplicate workers.
func (s *AccountQualityService) ScheduleSelected(ctx context.Context, ids []int64, revision string) (QualitySelectedRun, error) {
	out := QualitySelectedRun{Accounts: []QualitySelectedAccount{}}
	if err := validateQualityIDs(ids); err != nil {
		return out, err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	var raw []byte
	err = tx.QueryRowContext(ctx, `SELECT value FROM settings WHERE key=$1 FOR SHARE`, accountQualitySettingKey).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return out, errors.New("请先启用并保存降智检测")
	}
	if err != nil {
		return out, err
	}
	q := DefaultAccountQualitySettings()
	if err = json.Unmarshal(raw, &q); err != nil {
		return out, err
	}
	if !q.Enabled || (!q.QuestionEnabled && !q.ModelAuditEnabled) {
		return out, errors.New("请先启用并保存降智检测")
	}
	if revision != "" && revision != q.Revision {
		return out, errors.New("检测配置已变更，请重新提交")
	}
	out.Revision, out.QuestionEnabled, out.ModelEnabled = q.Revision, q.QuestionEnabled, q.ModelAuditEnabled
	ordered := append([]int64(nil), ids...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	for _, id := range ordered {
		item := QualitySelectedAccount{AccountID: id}
		var platform string
		err = tx.QueryRowContext(ctx, `SELECT name,platform FROM accounts WHERE id=$1 AND deleted_at IS NULL`, id).Scan(&item.Name, &platform)
		if errors.Is(err, sql.ErrNoRows) {
			item.SkipReason = "账号不存在或已删除"
		} else if err != nil {
			return out, err
		} else if platform != PlatformOpenAI {
			item.SkipReason = "仅支持 OpenAI 账号"
		}
		if item.SkipReason != "" {
			out.Accounts = append(out.Accounts, item)
			continue
		}
		initial, _ := json.Marshal(AccountQualityResult{AccountID: id, Revision: q.Revision})
		_, err = tx.ExecContext(ctx, `INSERT INTO account_quality_states(account_id,revision,version,payload,question_next_at,model_next_at) VALUES($1,$2,'',$3::jsonb,'epoch','epoch') ON CONFLICT(account_id) DO NOTHING`, id, q.Revision, string(initial))
		if err != nil {
			return out, err
		}
		if err = tx.QueryRowContext(ctx, `SELECT payload FROM account_quality_states WHERE account_id=$1 FOR UPDATE`, id).Scan(&raw); err != nil {
			return out, err
		}
		var previous AccountQualityResult
		if err = json.Unmarshal(raw, &previous); err != nil {
			return out, err
		}
		if previous.Revision == q.Revision {
			item.QuestionChecked, item.ModelChecked = previous.Question.CheckedAt, previous.Model.CheckedAt
		}
		_, err = tx.ExecContext(ctx, `UPDATE account_quality_states SET
 question_next_at=CASE WHEN $2 THEN LEAST(question_next_at,NOW()) ELSE question_next_at END,
 model_next_at=CASE WHEN $3 THEN LEAST(model_next_at,NOW()) ELSE model_next_at END,
 manual_revision=$4,
 question_requested_at=CASE WHEN $2 THEN NOW() ELSE NULL END,
 model_requested_at=CASE WHEN $3 THEN NOW() ELSE NULL END,
 next_at=LEAST(next_at,NOW()) WHERE account_id=$1`, id, q.QuestionEnabled, q.ModelAuditEnabled, q.Revision)
		if err != nil {
			return out, err
		}
		out.Accounts = append(out.Accounts, item)
	}
	if err = tx.Commit(); err != nil {
		return out, err
	}
	if s.schedulingEnabled(q) {
		for _, item := range out.Accounts {
			if item.SkipReason == "" {
				s.notifyQualityCollection(item.AccountID)
			}
		}
	}
	return out, nil
}
