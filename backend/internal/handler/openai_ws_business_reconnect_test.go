//go:build unit

package handler

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestOpenAIWSBusinessRetryReconnect(t *testing.T) {
	for _, mode := range []string{"handshake502", "handshake503", "reconnectbefore", "reconnectafter", "reconnectlate", "exhausted"} {
		t.Run(mode, func(t *testing.T) {
			service.ClearOpenAIUpstream5xxRetryLog()
			t.Cleanup(service.ClearOpenAIUpstream5xxRetryLog)
			f := newWSBusinessFixture(t, service.OpenAIUpstream5xxRetryConfig{Enabled: true, SameAccount: 3, Total: 5, Delay: time.Millisecond}, 3, 0)
			key, extra := "one503/"+mode, 0
			switch mode {
			case "handshake502", "handshake503":
				f.handshakeStatus = func(n int64) int {
					if n == 2 {
						if mode == "handshake503" {
							return 503
						}
						return 502
					}
					return 0
				}
			case "exhausted":
				f.handshakeStatus = func(n int64) int {
					if n > 1 {
						return 502
					}
					return 0
				}
			case "reconnectbefore", "reconnectafter":
				key, extra = mode+"/test", 1
			case "reconnectlate":
				key = mode + "/test"
			}
			resp, err := f.request(context.Background(), key)
			require.NoError(t, err)
			events, err := readWSBusinessSSE(resp)
			require.NoError(t, err)
			page := service.GetOpenAIUpstream5xxRetryLog(service.OpenAIUpstream5xxRetryLogFilter{})
			require.NotEmpty(t, page.Entries)
			entry := page.Entries[0]
			require.Equal(t, 1, entry.RetryCount)
			require.NotEmpty(t, entry.RuleID)
			if mode == "exhausted" {
				require.Equal(t, "ws_retry_count_exhausted", entry.StopReason)
				require.Equal(t, 5, entry.WSRetryCount)
				require.Equal(t, "failed", entry.WSRetryStatus)
				require.Contains(t, entry.FinalMessage, "502")
				require.EqualValues(t, 7, f.handshakes.Load())
				require.Equal(t, "response.failed", events[len(events)-1].Get("type").String())
			} else if mode == "reconnectlate" {
				f.verify(t, key, events, 1, false)
				require.Zero(t, entry.WSRetryCount)
				require.Contains(t, entry.StopReason, "output_started")
			} else {
				f.verify(t, key, events, 1, true, extra)
				require.Equal(t, 1, entry.WSRetryCount)
				require.Equal(t, "recovered", entry.WSRetryStatus)
				require.Empty(t, entry.StopReason)
			}
			t.Logf("mode=%s business=%d ws=%d ws_status=%s stop=%s", mode, entry.RetryCount, entry.WSRetryCount, entry.WSRetryStatus, entry.StopReason)
		})
	}
}

func TestOpenAIWSBusinessRetryCustomRulesHTTP(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			service.ClearOpenAIUpstream5xxRetryLog()
			t.Cleanup(service.ClearOpenAIUpstream5xxRetryLog)
			rules := []service.OpenAIUpstream5xxRetryRule{{ID: "custom", Name: "Custom failure", Enabled: enabled, StatusCode: 502, MatchMode: "all", Keywords: []string{"temporary custom", "failure"}}}
			f := newWSBusinessFixture(t, service.OpenAIUpstream5xxRetryConfig{Enabled: true, SameAccount: 3, Total: 5, Delay: time.Millisecond, Rules: rules}, 3, 0)
			resp, err := f.request(context.Background(), "custom/1")
			require.NoError(t, err)
			events, err := readWSBusinessSSE(resp)
			require.NoError(t, err)
			page := service.GetOpenAIUpstream5xxRetryLog(service.OpenAIUpstream5xxRetryLogFilter{})
			if enabled {
				f.verify(t, "custom/1", events, 1, true)
				require.Equal(t, "custom", page.Entries[0].RuleID)
			} else {
				require.Zero(t, page.Total)
				require.NotEqual(t, "response.completed", events[len(events)-1].Get("type").String())
			}
		})
	}
}

func TestOpenAIWSBusinessRetryRuleSelectionsControlForwarding(t *testing.T) {
	for _, selection := range []string{"only502", "only503", "none", "empty", "switchOff"} {
		t.Run(selection, func(t *testing.T) {
			service.ClearOpenAIUpstream5xxRetryLog()
			t.Cleanup(service.ClearOpenAIUpstream5xxRetryLog)
			rules := service.DefaultOpenAIUpstream5xxRetryRules()
			rules[0].Enabled = selection == "only502" || selection == "switchOff"
			rules[1].Enabled = selection == "only503" || selection == "switchOff"
			if selection == "empty" {
				rules = []service.OpenAIUpstream5xxRetryRule{}
			}
			f := newWSBusinessFixture(t, service.OpenAIUpstream5xxRetryConfig{Enabled: selection != "switchOff", SameAccount: 3, Total: 5, Delay: time.Millisecond, Rules: rules}, 3, 0)
			for _, mode := range []string{"one", "one503"} {
				key := mode + "/" + selection
				resp, err := f.request(context.Background(), key)
				require.NoError(t, err)
				events, err := readWSBusinessSSE(resp)
				require.NoError(t, err)
				matched := (selection == "only502" && mode == "one") || (selection == "only503" && mode == "one503")
				if matched {
					f.verify(t, key, events, 1, true)
				} else {
					// The legacy error writer may generate its own terminal response ID.
					require.NotEmpty(t, events)
					require.NotEqual(t, "response.completed", events[len(events)-1].Get("type").String())
					f.mu.Lock()
					attempts, observation := len(f.attempts[key]), f.observations[key]
					f.mu.Unlock()
					require.Equal(t, 1, attempts)
					require.Zero(t, observation.count)
					require.True(t, observation.requestError)
				}
			}
			page := service.GetOpenAIUpstream5xxRetryLog(service.OpenAIUpstream5xxRetryLogFilter{})
			if selection == "only502" || selection == "only503" {
				require.Equal(t, 1, page.Stats.Intercepted)
				require.Equal(t, 1, page.Stats.Succeeded)
			} else {
				require.Zero(t, page.Total)
			}
		})
	}
}

func TestOpenAIWSBusinessRetryReconnectConcurrentSimulation(t *testing.T) {
	if testing.Short() {
		t.Skip("real WS connection recovery simulation")
	}
	for _, concurrency := range []int{50, 200} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
			service.ClearOpenAIUpstream5xxRetryLog()
			t.Cleanup(service.ClearOpenAIUpstream5xxRetryLog)
			f := newWSBusinessFixture(t, service.OpenAIUpstream5xxRetryConfig{Enabled: true, SameAccount: 3, Total: 5, Delay: time.Millisecond}, 5, 0)
			const total = 300
			jobs := make(chan int)
			var wg sync.WaitGroup
			start := time.Now()
			for worker := 0; worker < concurrency; worker++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for n := range jobs {
						mode := []string{"one503", "reconnectbefore", "reconnectafter"}[n%3]
						key := fmt.Sprintf("%s/mixed-%d", mode, n)
						ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
						resp, err := f.request(ctx, key)
						if err != nil {
							t.Errorf("%s: %v", key, err)
							cancel()
							continue
						}
						events, err := readWSBusinessSSE(resp)
						cancel()
						if err != nil {
							t.Errorf("%s: %v", key, err)
							continue
						}
						extra := 0
						if n%3 != 0 {
							extra = 1
						}
						f.verify(t, key, events, 1, true, extra)
					}
				}()
			}
			for n := 0; n < total; n++ {
				jobs <- n
			}
			close(jobs)
			wg.Wait()
			page := service.GetOpenAIUpstream5xxRetryLog(service.OpenAIUpstream5xxRetryLogFilter{Limit: total * 2})
			require.Equal(t, total, page.Stats.Intercepted)
			require.Equal(t, total, page.Stats.Succeeded)
			require.Zero(t, page.Stats.Exhausted)
			wsTotal := 0
			for _, entry := range page.Entries {
				if entry.Event == "succeeded" {
					wsTotal += entry.WSRetryCount
				}
			}
			require.Equal(t, 200, wsTotal)
			f.usage.mu.Lock()
			defer f.usage.mu.Unlock()
			require.Len(t, f.usage.logs, total)
			for _, entry := range f.usage.logs {
				require.Equal(t, 1, *entry.OpenAIUpstream5xxRetryCount)
			}
			t.Logf("requests=%d concurrency=%d peak=%d business_retries=%d ws_retries=%d elapsed=%s", total, concurrency, f.peak.Load(), total, wsTotal, time.Since(start))
		})
	}
}
