package service

import (
	"context"
	"errors"
	"time"
)

type AccountSchedulingPausesInput struct {
	AccountIDs []int64
	AfterID    int64
	Limit      int
}

func (v AccountSchedulingPausesInput) Validate() error {
	if v.AfterID < 0 || v.Limit < 1 || v.Limit > 100 {
		return errors.New("after_id must be nonnegative and limit must be 1-100")
	}
	if len(v.AccountIDs) > 0 {
		return (AccountRecentRequestsInput{AccountIDs: v.AccountIDs}).Validate()
	}
	return nil
}

type AccountSchedulingPause struct {
	AccountID          int64     `json:"account_id"`
	Name               string    `json:"name"`
	Platform           string    `json:"platform"`
	Type               string    `json:"type"`
	Email              string    `json:"email"`
	SchedulingPausedAt time.Time `json:"scheduling_paused_at"`
	PausedSeconds      int64     `json:"paused_seconds"`
}

type AccountSchedulingPauses struct {
	Version     int                      `json:"version"`
	ServerNow   time.Time                `json:"server_now"`
	Accounts    []AccountSchedulingPause `json:"accounts"`
	HasMore     bool                     `json:"has_more"`
	NextAfterID int64                    `json:"next_after_id"`
}

type AccountSchedulingPausesReader interface {
	ListAccountSchedulingPauses(context.Context, AccountSchedulingPausesInput) (*AccountSchedulingPauses, error)
}

func (s *adminServiceImpl) ListAccountSchedulingPauses(ctx context.Context, input AccountSchedulingPausesInput) (*AccountSchedulingPauses, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}
	repo, ok := s.accountRepo.(AccountSchedulingPausesReader)
	if !ok {
		return nil, errors.New("account scheduling pauses unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return repo.ListAccountSchedulingPauses(ctx, input)
}
