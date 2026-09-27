//go:build unit

package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestBPSGatewayConcurrentStreams(t *testing.T) {
	oldMode := gin.Mode()
	gin.SetMode(gin.TestMode)
	defer gin.SetMode(oldMode)
	for _, count := range []int{100, 500} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			gateway, first := bpsTestGateway(t)
			second := *first
			second.ID = first.ID + 1
			accounts := []*Account{first, &second}
			var calls atomic.Int32
			gateway.httpUpstream = &keeperHTTPStub{do: func(req *http.Request, _ string, id int64, _ int) (*http.Response, error) {
				calls.Add(1)
				select {
				case <-req.Context().Done():
					return nil, req.Context().Err()
				case <-time.After(5 * time.Millisecond):
				}
				body := strings.ReplaceAll(bpsTextFixture, "hello", fmt.Sprintf("account-%d", id))
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
			}}
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			var peak atomic.Uint64
			peak.Store(before.HeapAlloc)
			done := make(chan struct{})
			samplerDone := make(chan struct{})
			go func() {
				defer close(samplerDone)
				ticker := time.NewTicker(time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-done:
						return
					case <-ticker.C:
						var now runtime.MemStats
						runtime.ReadMemStats(&now)
						if now.HeapAlloc > peak.Load() {
							peak.Store(now.HeapAlloc)
						}
					}
				}
			}()
			start := time.Now()
			ready := make(chan struct{})
			errorsC := make(chan error, count)
			var wg sync.WaitGroup
			for i := 0; i < count; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-ready
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					c, rec := downstreamTestContext(11)
					account := accounts[i%len(accounts)]
					result, err := gateway.Forward(ctx, c, account, []byte(`{"model":"gpt-6-astra","stream":true,"input":"hi"}`))
					if err != nil {
						errorsC <- err
						return
					}
					if result.Usage.CacheReadInputTokens != 8 || !strings.Contains(rec.Body.String(), fmt.Sprintf("account-%d", account.ID)) || strings.Count(rec.Body.String(), `"type":"response.completed"`) != 1 {
						errorsC <- fmt.Errorf("incorrect stream or account isolation: %d", i)
					}
				}(i)
			}
			close(ready)
			wg.Wait()
			close(errorsC)
			close(done)
			<-samplerDone
			elapsed := time.Since(start)
			runtime.ReadMemStats(&after)
			for err := range errorsC {
				t.Error(err)
			}
			require.EqualValues(t, count, calls.Load())
			t.Logf("concurrency=%d calls=%d elapsed=%s allocated=%.2fMiB peak_heap=%.2fMiB", count, calls.Load(), elapsed, float64(after.TotalAlloc-before.TotalAlloc)/(1<<20), float64(peak.Load())/(1<<20))
		})
	}
}

func TestBPSDisabledSnapshotNeverWaitsForExpiredSQL(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	repo := &forceWSRepoStub{getValueFn: func(ctx context.Context, key string) (string, error) {
		close(started)
		select {
		case <-release:
			return "", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}}
	s := NewSettingService(repo, &config.Config{})
	s.bpsSettingsCache.Store(newCachedOpenAIBPS(DefaultOpenAIBPSSettings(), -time.Second))
	defer func() {
		close(release)
		require.Eventually(t, func() bool { return !s.bpsSettingsRefreshing.Load() }, time.Second, time.Millisecond)
	}()
	start := time.Now()
	require.False(t, s.bpsSettings(context.Background()).settings.Enabled)
	require.Less(t, time.Since(start), 100*time.Millisecond)
	<-started
	for i := 0; i < 1000; i++ {
		require.False(t, s.bpsSettings(context.Background()).settings.Enabled)
	}
	require.Equal(t, 1, repo.callCount())
}

func BenchmarkBPSDisabledRoute(b *testing.B) {
	s := NewSettingService(&settingRepoStub{values: map[string]string{}}, &config.Config{})
	s.bpsSettingsCache.Store(newCachedOpenAIBPS(DefaultOpenAIBPSSettings(), time.Hour))
	gateway := &OpenAIGatewayService{settingService: s}
	account := &Account{Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	body := []byte(`{"model":"gpt-6-astra","input":"hi"}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if gateway.bpsRoute(context.Background(), nil, account, body, "") != nil {
			b.Fatal("disabled route matched")
		}
	}
}
