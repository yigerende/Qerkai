//go:build unit

package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

// 强制上游 WS 开关的取值性能测试。
//
// 目的：用实测（而非「代码看起来一样」）证明该设置的读取路径与上游
// IsBackendModeEnabled 具有相同的性能特征 —— 热路径不查库、并发不击穿、
// 写入后即时生效。
//
// 本文件带 //go:build unit 标签，与上游 setting_service_backend_mode_test.go 一致，
// 需用 `go test -tags unit` 运行。

type forceWSRepoStub struct {
	mu         sync.Mutex
	calls      int
	getValueFn func(ctx context.Context, key string) (string, error)
}

func (s *forceWSRepoStub) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *forceWSRepoStub) Get(ctx context.Context, key string) (*Setting, error) {
	panic("unexpected Get call")
}

func (s *forceWSRepoStub) GetValue(ctx context.Context, key string) (string, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	if s.getValueFn == nil {
		panic("unexpected GetValue call")
	}
	return s.getValueFn(ctx, key)
}

func (s *forceWSRepoStub) Set(ctx context.Context, key, value string) error {
	panic("unexpected Set call")
}

func (s *forceWSRepoStub) GetMultiple(ctx context.Context, keys []string) (map[string]string, error) {
	panic("unexpected GetMultiple call")
}

func (s *forceWSRepoStub) SetMultiple(ctx context.Context, settings map[string]string) error {
	panic("unexpected SetMultiple call")
}

func (s *forceWSRepoStub) GetAll(ctx context.Context) (map[string]string, error) {
	panic("unexpected GetAll call")
}

func (s *forceWSRepoStub) Delete(ctx context.Context, key string) error {
	panic("unexpected Delete call")
}

// resetForceUpstreamWSTestState 清空缓存与注册的 SettingService，
// 并在测试结束后还原，避免污染同包其他测试。
func resetForceUpstreamWSTestState(t *testing.T) {
	t.Helper()

	prevSvc := forceUpstreamWSSettings.Load()
	forceUpstreamWSCache.Store((*cachedForceUpstreamWS)(nil))
	forceUpstreamWSSF.Forget(SettingKeyForceOpenAIUpstreamWS)
	t.Cleanup(func() {
		forceUpstreamWSCache.Store((*cachedForceUpstreamWS)(nil))
		forceUpstreamWSSF.Forget(SettingKeyForceOpenAIUpstreamWS)
		forceUpstreamWSSettings.Store(prevSvc)
	})
}

// TestForceUpstreamWSEnabled_CachesAcrossRequests 是回答
// 「每次请求进来是否都要查表」的实测：1000 次调用只允许查库 1 次。
func TestForceUpstreamWSEnabled_CachesAcrossRequests(t *testing.T) {
	resetForceUpstreamWSTestState(t)

	repo := &forceWSRepoStub{
		getValueFn: func(ctx context.Context, key string) (string, error) {
			require.Equal(t, SettingKeyForceOpenAIUpstreamWS, key)
			return "true", nil
		},
	}
	NewSettingService(repo, &config.Config{})

	for i := 0; i < 1000; i++ {
		require.True(t, ForceUpstreamWSEnabled(), "call %d", i)
	}
	require.Equal(t, 1, repo.callCount(), "缓存未生效：1000 次调用查库次数应为 1")
}

// TestForceUpstreamWSEnabled_ConcurrentReadsHitDBOnce 验证 singleflight
// 在缓存冷启动时防止并发击穿（thundering herd）。
func TestForceUpstreamWSEnabled_ConcurrentReadsHitDBOnce(t *testing.T) {
	resetForceUpstreamWSTestState(t)

	repo := &forceWSRepoStub{
		getValueFn: func(ctx context.Context, key string) (string, error) {
			// 放慢查询，放大并发窗口，确保多个 goroutine 同时进入。
			time.Sleep(20 * time.Millisecond)
			return "true", nil
		},
	}
	NewSettingService(repo, &config.Config{})

	const goroutines = 64
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			require.True(t, ForceUpstreamWSEnabled())
		}()
	}
	wg.Wait()

	require.Equal(t, 1, repo.callCount(), "singleflight 未生效：并发冷启动应只查库 1 次")
}

// TestForceUpstreamWSEnabled_NotFoundCachesFullTTL 验证设置尚未创建
// （全新安装或从未保存过）时按默认关闭缓存完整 TTL，不会每次请求都打库。
func TestForceUpstreamWSEnabled_NotFoundCachesFullTTL(t *testing.T) {
	resetForceUpstreamWSTestState(t)

	repo := &forceWSRepoStub{
		getValueFn: func(ctx context.Context, key string) (string, error) {
			return "", ErrSettingNotFound
		},
	}
	NewSettingService(repo, &config.Config{})

	for i := 0; i < 100; i++ {
		require.False(t, ForceUpstreamWSEnabled())
	}
	require.Equal(t, 1, repo.callCount(), "ErrSettingNotFound 应按默认值缓存完整 TTL")

	cached, ok := forceUpstreamWSCache.Load().(*cachedForceUpstreamWS)
	require.True(t, ok)
	require.NotNil(t, cached)
	remaining := time.Duration(cached.expiresAt - time.Now().UnixNano())
	require.Greater(t, remaining, forceUpstreamWSErrorTTL,
		"未创建的设置应使用完整 TTL(60s) 而非错误 TTL(5s)")
}

// TestForceUpstreamWSEnabled_DBErrorUsesShortTTL 验证读库失败时
// 回落为关闭（保持上游默认行为）且使用短 TTL 以便快速恢复。
func TestForceUpstreamWSEnabled_DBErrorUsesShortTTL(t *testing.T) {
	resetForceUpstreamWSTestState(t)

	repo := &forceWSRepoStub{
		getValueFn: func(ctx context.Context, key string) (string, error) {
			return "", errors.New("db unavailable")
		},
	}
	NewSettingService(repo, &config.Config{})

	require.False(t, ForceUpstreamWSEnabled(), "读库失败应回落为关闭")
	require.Equal(t, 1, repo.callCount())

	cached, ok := forceUpstreamWSCache.Load().(*cachedForceUpstreamWS)
	require.True(t, ok)
	require.NotNil(t, cached)
	remaining := time.Duration(cached.expiresAt - time.Now().UnixNano())
	require.LessOrEqual(t, remaining, forceUpstreamWSErrorTTL,
		"读库失败应使用短 TTL(5s) 以便快速恢复")
}

// TestForceUpstreamWSEnabled_ExpiredCacheRefetches 验证 TTL 过期后重新查库。
func TestForceUpstreamWSEnabled_ExpiredCacheRefetches(t *testing.T) {
	resetForceUpstreamWSTestState(t)

	value := "false"
	repo := &forceWSRepoStub{
		getValueFn: func(ctx context.Context, key string) (string, error) {
			return value, nil
		},
	}
	NewSettingService(repo, &config.Config{})

	require.False(t, ForceUpstreamWSEnabled())
	require.Equal(t, 1, repo.callCount())

	// 手动让缓存过期，模拟 TTL 到期。
	forceUpstreamWSCache.Store(&cachedForceUpstreamWS{
		value:     false,
		expiresAt: time.Now().Add(-time.Second).UnixNano(),
	})
	value = "true"

	require.True(t, ForceUpstreamWSEnabled(), "TTL 过期后应重新查库并取到新值")
	require.Equal(t, 2, repo.callCount())
}

// TestForceUpstreamWSEnabled_RefreshTakesEffectWithoutDB 验证保存设置后
// 调用 refreshForceUpstreamWSCache 即时生效，且不产生新的查库。
// 这是「保存立即生效」的实测依据。
func TestForceUpstreamWSEnabled_RefreshTakesEffectWithoutDB(t *testing.T) {
	resetForceUpstreamWSTestState(t)

	repo := &forceWSRepoStub{
		getValueFn: func(ctx context.Context, key string) (string, error) {
			return "false", nil
		},
	}
	NewSettingService(repo, &config.Config{})

	require.False(t, ForceUpstreamWSEnabled())
	require.Equal(t, 1, repo.callCount())

	// 模拟管理后台保存 force_openai_upstream_ws=true 后的缓存刷新。
	refreshForceUpstreamWSCache(true)

	require.True(t, ForceUpstreamWSEnabled(), "保存后应即时生效，无需等待 TTL")
	require.Equal(t, 1, repo.callCount(), "刷新缓存不应触发额外查库")
}

// BenchmarkForceUpstreamWSEnabled 测量热路径开销。
// 命中缓存时只做一次 atomic.Load + 一次时间比较，与上游同类设置一致。
func BenchmarkForceUpstreamWSEnabled(b *testing.B) {
	prevSvc := forceUpstreamWSSettings.Load()
	prevCache := forceUpstreamWSCache.Load()
	defer func() {
		forceUpstreamWSSettings.Store(prevSvc)
		if cached, ok := prevCache.(*cachedForceUpstreamWS); ok {
			forceUpstreamWSCache.Store(cached)
		} else {
			forceUpstreamWSCache.Store((*cachedForceUpstreamWS)(nil))
		}
	}()

	repo := &forceWSRepoStub{
		getValueFn: func(ctx context.Context, key string) (string, error) {
			return "true", nil
		},
	}
	NewSettingService(repo, &config.Config{})
	ForceUpstreamWSEnabled() // 预热缓存

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ForceUpstreamWSEnabled()
	}
	b.StopTimer()

	if repo.callCount() != 1 {
		b.Fatalf("benchmark 期间发生了额外查库：%d 次", repo.callCount())
	}
}
