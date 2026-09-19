package service

import (
	"context"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/pagination"
)

// AccountQualityListRepository keeps quality filtering confined to admin lists.
type AccountQualityListRepository interface {
	ListWithQualityFilter(context.Context, pagination.PaginationParams, string, string, string, string, int64, string, string, AccountQualitySettings) ([]Account, *pagination.PaginationResult, error)
}

func validateAccountQualityFilter(status string) error {
	switch status {
	case "", "degraded", "normal", "pending":
		return nil
	default:
		return infraerrors.BadRequest("INVALID_QUALITY_FILTER", "invalid account quality filter")
	}
}
