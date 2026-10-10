package service

import (
	"context"
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
)

func (s *AccountQualityService) validateQualityGroupSwitch(ctx context.Context, q AccountQualitySettings) error {
	if !q.Enabled || !q.SwitchGroupOnDegradation {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var valid bool
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM groups WHERE id=$1 AND deleted_at IS NULL AND status='active' AND platform='openai')`, q.DegradationGroupID).Scan(&valid); err != nil {
		return err
	}
	if !valid {
		return infraerrors.BadRequest("QUALITY_GROUP_INVALID", "降智切换目标必须是可用的 OpenAI 分组")
	}
	return nil
}
