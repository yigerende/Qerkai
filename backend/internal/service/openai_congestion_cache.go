package service

import (
	"context"
	"time"
)

// Optional extension keeps other providers and existing cache implementations unchanged.
type OpenAICongestionCache interface {
	ReadCongestionSession(context.Context, int64, string) (pending string, protected bool, err error)
	ReserveCongestionSession(context.Context, int64, string, int64, string, time.Duration) (bool, error)
	CommitCongestionSession(context.Context, int64, string, string, int64, int64, time.Duration, time.Duration, string, time.Duration) (bool, error)
	ReleaseCongestionSession(context.Context, int64, string, string) error
	SetSessionAccountIDIfUnchanged(context.Context, int64, string, int64, int64, time.Duration, string, time.Duration) (bool, error)
	DeleteSessionAccountIDIfUnchanged(context.Context, int64, string, int64, string) (bool, error)
}
