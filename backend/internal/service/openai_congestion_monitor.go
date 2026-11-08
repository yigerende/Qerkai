package service

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"
)

type congestionSample struct {
	at      time.Time
	bad     bool
	status  int
	network bool
	model   string
}
type congestionFrame struct {
	at    time.Time
	first time.Duration
	model string
}
type congestionAccountHealth struct {
	samples []congestionSample
	frames  []congestionFrame
	active  map[*openAICongestionAttempt]struct{}
	last    time.Time
}
type openAICongestionMonitor struct {
	mu        sync.Mutex
	accounts  map[int64]*congestionAccountHealth
	lastSweep time.Time
}
type congestionHealthSnapshot struct {
	reason     string
	frameCount int
	average    time.Duration
}

func (m *openAICongestionMonitor) start(a *openAICongestionAttempt) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.accounts == nil {
		m.accounts = make(map[int64]*congestionAccountHealth)
	}
	now := time.Now()
	if now.Sub(m.lastSweep) > time.Minute {
		for id, h := range m.accounts {
			if len(h.active) == 0 && now.Sub(h.last) > time.Hour {
				delete(m.accounts, id)
			}
		}
		m.lastSweep = now
	}
	h := m.accounts[a.account]
	if h == nil {
		h = &congestionAccountHealth{active: make(map[*openAICongestionAttempt]struct{})}
		m.accounts[a.account] = h
	}
	h.active[a] = struct{}{}
	h.last = now
}

// Called exactly once when the first upstream bytes arrive, without waiting for
// the answer to complete. No stream parsing or background queue on this path.
func (m *openAICongestionMonitor) recordFrame(a *openAICongestionAttempt) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h := m.accounts[a.account]
	if h == nil {
		return
	}
	h.last = time.Now()
	h.frames = append(h.frames, congestionFrame{h.last, time.Duration(a.firstFrameNS.Load() - 1), a.model})
	if len(h.frames) > a.config.SampleLimit {
		copy(h.frames, h.frames[len(h.frames)-a.config.SampleLimit:])
		h.frames = h.frames[:a.config.SampleLimit]
	}
}
func (m *openAICongestionMonitor) finish(a *openAICongestionAttempt, result *OpenAIForwardResult, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h := m.accounts[a.account]
	if h == nil {
		return
	}
	delete(h.active, a)
	h.last = time.Now()
	if (err != nil && (errors.Is(err, context.Canceled) || (a.client != nil && a.client.Err() != nil))) || (err == nil && result == nil) {
		return
	}
	sample := congestionSample{at: h.last, model: a.model}
	var failover *UpstreamFailoverError
	var network net.Error
	if errors.As(err, &failover) {
		sample.status = failover.StatusCode
		sample.bad = sample.status == 429 || sample.status >= 500
	}
	if errors.As(err, &network) {
		sample.network = true
		sample.bad = true
	}
	if err != nil && !sample.bad {
		return
	}
	h.samples = append(h.samples, sample)
	if len(h.samples) > a.config.SampleLimit {
		copy(h.samples, h.samples[len(h.samples)-a.config.SampleLimit:])
		h.samples = h.samples[:a.config.SampleLimit]
	}
	// Buffered paths without a byte observer may still provide a measured TTFT.
	if a.firstFrameNS.Load() == 0 && result != nil && result.FirstTokenMs != nil {
		h.frames = append(h.frames, congestionFrame{h.last, time.Duration(*result.FirstTokenMs) * time.Millisecond, a.model})
		if len(h.frames) > a.config.SampleLimit {
			copy(h.frames, h.frames[len(h.frames)-a.config.SampleLimit:])
			h.frames = h.frames[:a.config.SampleLimit]
		}
	}
}
func (m *openAICongestionMonitor) snapshot(id int64, model string, load int, now time.Time, c OpenAICongestionSettings) congestionHealthSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	h := m.accounts[id]
	out := congestionHealthSnapshot{}
	if h == nil {
		return out
	}
	window := time.Duration(c.WindowSeconds) * time.Second
	n, bad, slow := 0, 0, 0
	for _, v := range h.samples {
		if (model == "" || v.model == model) && now.Sub(v.at) <= window {
			n++
			if (v.status == 429 && c.Error429) || (v.status >= 500 && v.status <= 599 && c.Error5xx) || (v.network && c.ErrorNetwork) {
				bad++
			}
		}
	}
	for _, v := range h.frames {
		if (model == "" || v.model == model) && now.Sub(v.at) <= window {
			out.frameCount++
			out.average += v.first
			if v.first >= time.Duration(c.SlowSeconds)*time.Second {
				slow++
			}
		}
	}
	if out.frameCount > 0 {
		out.average /= time.Duration(out.frameCount)
	}
	if c.ErrorsEnabled && n >= c.ErrorMinSamples && bad >= c.ErrorMinCount && bad*100 >= n*c.ErrorPercent {
		out.reason = "repeated_upstream_errors"
		return out
	}
	active, stalled := 0, 0
	for a := range h.active {
		if model != "" && a.model != model {
			continue
		}
		if a.client != nil && a.client.Err() != nil {
			continue
		}
		active++
		if !a.frame.Load() && now.Sub(a.started) >= time.Duration(c.StalledSeconds)*time.Second {
			stalled++
		}
	}
	if c.StalledEnabled && stalled >= c.StalledMinRequests && stalled*100 >= active*c.StalledPercent {
		out.reason = "stalled_upstream_requests"
		return out
	}
	if c.SlowEnabled && load >= c.SlowMinLoadPercent && out.frameCount >= c.SlowMinSamples && slow*100 >= out.frameCount*c.SlowPercent {
		out.reason = "slow_first_frame"
	}
	return out
}
