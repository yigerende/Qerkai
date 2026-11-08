//go:build unit

package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func congestionFrames(s *OpenAIGatewayService, id int64, model string, n int, d time.Duration) {
	s.congestionMonitor.mu.Lock()
	defer s.congestionMonitor.mu.Unlock()
	if s.congestionMonitor.accounts == nil {
		s.congestionMonitor.accounts = map[int64]*congestionAccountHealth{}
	}
	h := s.congestionMonitor.accounts[id]
	if h == nil {
		h = &congestionAccountHealth{active: map[*openAICongestionAttempt]struct{}{}}
		s.congestionMonitor.accounts[id] = h
	}
	h.last = time.Now()
	for i := 0; i < n; i++ {
		h.frames = append(h.frames, congestionFrame{time.Now(), d, model})
	}
}

func TestOpenAICongestionFortyOfThousandAndMasterOff(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		s, cache, accounts := congestionFixture(enabled, 4, 1)
		accounts[0].Concurrency = 1000
		accounts[1].Concurrency = 1000
		s.concurrencyService = NewConcurrencyService(schedulerTestConcurrencyCache{loadMap: map[int64]*AccountLoadInfo{1: {AccountID: 1, CurrentConcurrency: 40, LoadRate: 4}, 2: {AccountID: 2, CurrentConcurrency: 10, LoadRate: 1}}, acquireResults: map[int64]bool{1: true, 2: true}})
		require.NoError(t, cache.SetSessionAccountID(context.Background(), 7, "openai:chat", 1, time.Hour))
		congestionFrames(s, 1, "gpt-6-astra", 5, 20*time.Second)
		congestionFrames(s, 2, "gpt-6-astra", 5, 3*time.Second)
		ctx := congestionContext(t, s)
		sel, err := congestionSelect(s, ctx, "chat")
		require.NoError(t, err)
		if enabled {
			require.EqualValues(t, 2, sel.Account.ID)
		} else {
			require.EqualValues(t, 1, sel.Account.ID)
			require.Nil(t, congestionRequest(ctx))
			require.Zero(t, cache.calls.Load())
		}
		releaseLegacySchedulerDecisionSelection(sel)
	}
}

func TestOpenAICongestionErrorCategoriesAndStalledRatio(t *testing.T) {
	s, _, _ := congestionFixture(true, 4, 0)
	c := DefaultOpenAICongestionSettings()
	now := time.Now()
	s.congestionMonitor.accounts = map[int64]*congestionAccountHealth{1: {active: map[*openAICongestionAttempt]struct{}{}}}
	h := s.congestionMonitor.accounts[1]
	for i := 0; i < 5; i++ {
		h.samples = append(h.samples, congestionSample{at: now, model: "m"})
		if i < 3 {
			h.samples[i].status = 429
		}
	}
	require.Equal(t, "repeated_upstream_errors", s.congestionMonitor.snapshot(1, "m", 4, now, c).reason)
	c.Error429 = false
	require.Empty(t, s.congestionMonitor.snapshot(1, "m", 4, now, c).reason)
	for i := 0; i < 3; i++ {
		h.samples[i].network = true
		h.samples[i].status = 0
	}
	require.Equal(t, "repeated_upstream_errors", s.congestionMonitor.snapshot(1, "m", 4, now, c).reason)
	c.ErrorNetwork = false
	require.Empty(t, s.congestionMonitor.snapshot(1, "m", 4, now, c).reason)
	c.ErrorNetwork = true
	c.ErrorPercent = 61
	require.Empty(t, s.congestionMonitor.snapshot(1, "m", 4, now, c).reason)
	c.ErrorsEnabled = false
	for i := 0; i < 10; i++ {
		a := &openAICongestionAttempt{model: "m", started: now.Add(-21 * time.Second)}
		if i >= 5 {
			a.frame.Store(true)
		}
		h.active[a] = struct{}{}
	}
	c.StalledPercent = 50
	require.Equal(t, "stalled_upstream_requests", s.congestionMonitor.snapshot(1, "m", 4, now, c).reason)
	c.StalledPercent = 51
	require.Empty(t, s.congestionMonitor.snapshot(1, "m", 4, now, c).reason)
}

func TestOpenAICongestionSlowAtLowOccupancyAndFasterTarget(t *testing.T) {
	for _, tc := range []struct {
		name          string
		change        func(*OpenAICongestionSettings)
		targetModel   string
		targetCount   int
		targetLatency time.Duration
		want          int64
	}{
		{"40 percent still migrates", nil, "gpt-6-astra", 5, 3 * time.Second, 2},
		{"slow trigger off", func(c *OpenAICongestionSettings) { c.SlowEnabled = false }, "gpt-6-astra", 5, 3 * time.Second, 1},
		{"optional load floor", func(c *OpenAICongestionSettings) { c.SlowMinLoadPercent = 80 }, "gpt-6-astra", 5, 3 * time.Second, 1},
		{"higher latency threshold", func(c *OpenAICongestionSettings) { c.SlowSeconds = 30 }, "gpt-6-astra", 5, 3 * time.Second, 1},
		{"more source samples", func(c *OpenAICongestionSettings) { c.SlowMinSamples = 6 }, "gpt-6-astra", 5, 3 * time.Second, 1},
		{"target not sufficiently faster", func(c *OpenAICongestionSettings) { c.TargetFasterPercent = 90 }, "gpt-6-astra", 5, 3 * time.Second, 1},
		{"other model cannot prove faster", nil, "gpt-5.5", 5, 3 * time.Second, 1},
		{"unknown target waits", nil, "gpt-6-astra", 0, 0, 1},
		{"unknown target explicitly allowed", func(c *OpenAICongestionSettings) { c.TargetAllowUnknown = true }, "gpt-6-astra", 0, 0, 2},
		{"comparison explicitly off", func(c *OpenAICongestionSettings) { c.TargetRequireFaster = false }, "gpt-6-astra", 0, 0, 2},
		{"target load ceiling", func(c *OpenAICongestionSettings) { c.TargetMaxLoadPercent = 5 }, "gpt-6-astra", 5, 3 * time.Second, 1},
		{"target free slots", func(c *OpenAICongestionSettings) { c.TargetMinFreeSlots = 101 }, "gpt-6-astra", 5, 3 * time.Second, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, cache, _ := congestionFixture(true, 40, 10)
			c := DefaultOpenAICongestionSettings()
			c.Enabled = true
			if tc.change != nil {
				tc.change(&c)
			}
			s.settingService.cacheOpenAICongestionSettings(c)
			require.NoError(t, cache.SetSessionAccountID(context.Background(), 7, "openai:chat", 1, time.Hour))
			congestionFrames(s, 1, "gpt-6-astra", 5, 20*time.Second)
			congestionFrames(s, 2, tc.targetModel, tc.targetCount, tc.targetLatency)
			selected, err := congestionSelect(s, congestionContext(t, s), "chat")
			require.NoError(t, err)
			defer releaseLegacySchedulerDecisionSelection(selected)
			require.Equal(t, tc.want, selected.Account.ID)
		})
	}
}

func TestOpenAICongestionFirstFrameSamplesBeforeCompletion(t *testing.T) {
	s, _, accounts := congestionFixture(true, 4, 0)
	ctx := congestionContext(t, s)
	congestionRequest(ctx).model = "gpt-6-astra"
	finishes := make([]func(*OpenAIForwardResult, error) error, 0, 5)
	for i := 0; i < 5; i++ {
		forward, finish := s.beginCongestionForward(ctx, &accounts[0], nil)
		a := forward.Value(congestionAttemptKey{}).(*openAICongestionAttempt)
		a.started = time.Now().Add(-20 * time.Second)
		observeCongestionEvent(forward, nil, "")
		observeCongestionEvent(forward, nil, "")
		finishes = append(finishes, finish)
	}
	health := s.congestionMonitor.snapshot(1, "gpt-6-astra", 4, time.Now(), DefaultOpenAICongestionSettings())
	require.Equal(t, "slow_first_frame", health.reason)
	require.Equal(t, 5, health.frameCount)
	require.Len(t, s.congestionMonitor.accounts[1].active, 5)
	require.Empty(t, s.congestionMonitor.accounts[1].samples)
	for _, finish := range finishes {
		require.NoError(t, finish(&OpenAIForwardResult{}, nil))
	}
	require.Len(t, s.congestionMonitor.accounts[1].frames, 5, "completion must not duplicate first-response samples")
	require.Empty(t, s.congestionMonitor.snapshot(1, "gpt-5.5", 4, time.Now(), DefaultOpenAICongestionSettings()).reason)
}

func TestOpenAICongestionWindowRatiosAndRuleSwitches(t *testing.T) {
	s, _, _ := congestionFixture(true, 4, 0)
	c := DefaultOpenAICongestionSettings()
	now := time.Now()
	congestionFrames(s, 1, "m", 2, 20*time.Second)
	congestionFrames(s, 1, "m", 3, time.Second)
	require.Empty(t, s.congestionMonitor.snapshot(1, "m", 4, now, c).reason)
	c.SlowPercent = 40
	require.Equal(t, "slow_first_frame", s.congestionMonitor.snapshot(1, "m", 4, now, c).reason)
	c.WindowSeconds = 10
	require.Empty(t, s.congestionMonitor.snapshot(1, "m", 4, now.Add(11*time.Second), c).reason)
	c.WindowSeconds = 30
	require.Equal(t, "slow_first_frame", s.congestionMonitor.snapshot(1, "m", 4, now.Add(11*time.Second), c).reason)
	congestionBad(s, 2)
	c = DefaultOpenAICongestionSettings()
	require.Equal(t, "repeated_upstream_errors", s.congestionMonitor.snapshot(2, "gpt-6-astra", 4, now, c).reason)
	c.Error5xx = false
	require.Empty(t, s.congestionMonitor.snapshot(2, "gpt-6-astra", 4, now, c).reason)
	c.Error5xx = true
	c.ErrorMinCount = 7
	require.Empty(t, s.congestionMonitor.snapshot(2, "gpt-6-astra", 4, now, c).reason)
	c.ErrorMinCount = 3
	c.ErrorsEnabled = false
	require.Empty(t, s.congestionMonitor.snapshot(2, "gpt-6-astra", 4, now, c).reason)
}

func TestOpenAICongestionStalledSettingsAndFullTrigger(t *testing.T) {
	s, _, accounts := congestionFixture(true, 40, 10)
	ctx := congestionContext(t, s)
	congestionRequest(ctx).model = "m"
	c := DefaultOpenAICongestionSettings()
	var finishes []func(*OpenAIForwardResult, error) error
	for i := 0; i < 5; i++ {
		forward, finish := s.beginCongestionForward(ctx, &accounts[0], nil)
		forward.Value(congestionAttemptKey{}).(*openAICongestionAttempt).started = time.Now().Add(-21 * time.Second)
		finishes = append(finishes, finish)
	}
	require.Equal(t, "stalled_upstream_requests", s.congestionMonitor.snapshot(1, "m", 40, time.Now(), c).reason)
	c.StalledSeconds = 30
	require.Empty(t, s.congestionMonitor.snapshot(1, "m", 40, time.Now(), c).reason)
	c.StalledSeconds = 20
	c.StalledMinRequests = 6
	require.Empty(t, s.congestionMonitor.snapshot(1, "m", 40, time.Now(), c).reason)
	c.StalledMinRequests = 5
	c.StalledEnabled = false
	require.Empty(t, s.congestionMonitor.snapshot(1, "m", 40, time.Now(), c).reason)
	for _, finish := range finishes {
		finish(nil, context.Canceled)
	}
	for _, enabled := range []bool{true, false} {
		s, cache, _ := congestionFixture(true, 40, 10)
		c = DefaultOpenAICongestionSettings()
		c.Enabled = true
		c.FullEnabled = enabled
		c.FullLoadPercent = 40
		c.FullWaitSeconds = 0
		s.settingService.cacheOpenAICongestionSettings(c)
		require.NoError(t, cache.SetSessionAccountID(context.Background(), 7, "openai:chat", 1, time.Hour))
		sel, err := congestionSelect(s, congestionContext(t, s), "chat")
		require.NoError(t, err)
		if enabled {
			require.EqualValues(t, 2, sel.Account.ID)
		} else {
			require.EqualValues(t, 1, sel.Account.ID)
		}
		releaseLegacySchedulerDecisionSelection(sel)
	}
}

func TestOpenAICongestionSampleLimitAndNewSessionSwitch(t *testing.T) {
	s, _, accounts := congestionFixture(true, 40, 10)
	c := DefaultOpenAICongestionSettings()
	c.Enabled = true
	c.SampleLimit = 5
	s.settingService.cacheOpenAICongestionSettings(c)
	ctx := congestionContext(t, s)
	for i := 0; i < 20; i++ {
		forward, finish := s.beginCongestionForward(ctx, &accounts[0], nil)
		observeCongestionEvent(forward, nil, "")
		require.NoError(t, finish(&OpenAIForwardResult{}, nil))
	}
	require.Len(t, s.congestionMonitor.accounts[1].frames, 5)
	require.Len(t, s.congestionMonitor.accounts[1].samples, 5)
	s, _, _ = congestionFixture(true, 40, 10)
	c = DefaultOpenAICongestionSettings()
	c.Enabled = true
	c.AvoidUnhealthyNewSessions = false
	s.settingService.cacheOpenAICongestionSettings(c)
	congestionBad(s, 1)
	sel, err := congestionSelect(s, congestionContext(t, s), "new")
	require.NoError(t, err)
	defer releaseLegacySchedulerDecisionSelection(sel)
	require.EqualValues(t, 1, sel.Account.ID)
}

func TestOpenAICongestionStallNotHiddenByOlderFastSamples(t *testing.T) {
	s, cache, accounts := congestionFixture(true, 4, 1)
	require.NoError(t, cache.SetSessionAccountID(context.Background(), 7, "openai:chat", 1, time.Hour))
	congestionFrames(s, 1, "gpt-6-astra", 5, time.Second)
	congestionFrames(s, 2, "gpt-6-astra", 5, 3*time.Second)
	ctx := congestionContext(t, s)
	congestionRequest(ctx).model = "gpt-6-astra"
	for i := 0; i < 5; i++ {
		forward, finish := s.beginCongestionForward(ctx, &accounts[0], nil)
		forward.Value(congestionAttemptKey{}).(*openAICongestionAttempt).started = time.Now().Add(-21 * time.Second)
		defer finish(nil, context.Canceled)
	}
	sel, err := congestionSelect(s, ctx, "chat")
	require.NoError(t, err)
	defer releaseLegacySchedulerDecisionSelection(sel)
	require.EqualValues(t, 2, sel.Account.ID)
}
