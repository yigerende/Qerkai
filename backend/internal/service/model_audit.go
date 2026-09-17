package service

import (
	"context"
	"errors"
	"strings"
	"time"
)

type ModelAuditAccount struct {
	AccountID int64     `json:"account_id"`
	Since     time.Time `json:"since"`
}
type ModelAuditInput struct {
	Accounts []ModelAuditAccount `json:"accounts"`
	Model    string              `json:"model"`
}
type ModelAuditLog struct {
	ID             int64     `json:"id"`
	AccountID      int64     `json:"account_id"`
	CreatedAt      time.Time `json:"created_at"`
	RequestedModel string    `json:"requested_model"`
	SentModel      string    `json:"sent_model"`
	ResponseModel  string    `json:"response_model"`
	Mismatch       *bool     `json:"mismatch"`
}
type ModelAuditResult struct {
	AccountID int64           `json:"account_id"`
	Logs      []ModelAuditLog `json:"logs"`
}

func (v ModelAuditInput) Validate() error {
	if len(v.Accounts) < 1 || len(v.Accounts) > 10 || strings.TrimSpace(v.Model) == "" || len(v.Model) > 200 {
		return errors.New("require 1-10 accounts and a model of at most 200 bytes")
	}
	seen := map[int64]bool{}
	for _, a := range v.Accounts {
		if a.AccountID < 1 || a.Since.IsZero() || seen[a.AccountID] {
			return errors.New("each account requires a unique positive ID and a nonzero since timestamp")
		}
		seen[a.AccountID] = true
	}
	return nil
}

// Optional repository capability keeps the existing usage interfaces unchanged.
type ModelAuditRepository interface {
	LatestModelAudit(context.Context, ModelAuditInput) ([]ModelAuditResult, error)
}

func (s *UsageService) LatestModelAudit(ctx context.Context, input ModelAuditInput) ([]ModelAuditResult, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}
	repo, ok := s.usageRepo.(ModelAuditRepository)
	if !ok {
		return nil, errors.New("model audit repository unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return repo.LatestModelAudit(ctx, input)
}
