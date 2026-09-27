package service

import (
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/basispoints"
)

// The core reads upstream events on its own goroutine. Store the measured
// instant atomically; reconstructed delivery events must not manufacture TTFT.
type bpsFirstToken struct {
	start time.Time
	mode  string
	first atomic.Int64 // elapsed nanoseconds + 1; zero means no matching event
}

func (t *bpsFirstToken) options() basispoints.StreamOptions {
	return basispoints.StreamOptions{
		ForwardNotifications: t.mode == OpenAITTFTModeNetwork,
		ObserveEvent: func(event, data string) {
			if t.first.Load() == 0 && openAIStreamDataStartsTTFT(data, event, false, t.mode) {
				t.first.CompareAndSwap(0, time.Since(t.start).Nanoseconds()+1)
			}
		},
		ResetAttempt: func() { t.first.Store(0) },
	}
}

func (t *bpsFirstToken) milliseconds() *int {
	if elapsed := t.first.Load(); elapsed > 0 {
		ms := int((elapsed - 1) / int64(time.Millisecond))
		return &ms
	}
	return nil
}
