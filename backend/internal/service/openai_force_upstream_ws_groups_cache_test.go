//go:build unit

package service

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func resetForceWSGroupsCacheForTest(t *testing.T, repo *forceWSRepoStub) {
	t.Helper()
	previousSettings := forceUpstreamWSSettings.Load()
	previousCache := forceUpstreamWSGroupsCache.Load()
	forceUpstreamWSSettings.Store(&SettingService{settingRepo: repo})
	forceUpstreamWSGroupsCache.Store(nil)
	t.Cleanup(func() {
		forceUpstreamWSSettings.Store(previousSettings)
		forceUpstreamWSGroupsCache.Store(previousCache)
	})
	t.Cleanup(setForceUpstreamWSForTest(true))
}

func TestForceWSGroupScopeConcurrentCache(t *testing.T) {
	repo := &forceWSRepoStub{getValueFn: func(context.Context, string) (string, error) {
		time.Sleep(10 * time.Millisecond)
		return "[11]", nil
	}}
	resetForceWSGroupsCacheForTest(t, repo)
	var wg sync.WaitGroup
	var mismatches atomic.Int64
	start := make(chan struct{})
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 10; j++ {
				if !ForceUpstreamWSEnabledForGroup(11) || ForceUpstreamWSEnabledForGroup(22) {
					mismatches.Add(1)
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	require.Zero(t, mismatches.Load())
	require.Equal(t, 1, repo.callCount())
}

func TestForceWSGroupScopeReadFailureAndDisabled(t *testing.T) {
	repo := &forceWSRepoStub{getValueFn: func(context.Context, string) (string, error) {
		return "", errors.New("database unavailable")
	}}
	resetForceWSGroupsCacheForTest(t, repo)
	for i := 0; i < 100; i++ {
		require.False(t, ForceUpstreamWSEnabledForGroup(11))
	}
	require.Equal(t, 1, repo.callCount())
	forceUpstreamWSGroupsCache.Store(nil)
	off := false
	forceUpstreamWSOverride.Store(&off)
	require.False(t, ForceUpstreamWSEnabledForGroup(11))
	require.Equal(t, 1, repo.callCount(), "disabled master must skip the group lookup")
}

func TestForceWSGroupScopeSaveWinsOverStaleRead(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	repo := &forceWSRepoStub{getValueFn: func(context.Context, string) (string, error) {
		close(started)
		<-release
		return "null", nil
	}}
	resetForceWSGroupsCacheForTest(t, repo)
	result := make(chan bool, 1)
	go func() { result <- ForceUpstreamWSEnabledForGroup(22) }()
	<-started
	refreshForceUpstreamWSGroupsCache([]int64{11})
	close(release)
	require.False(t, <-result)
	require.True(t, ForceUpstreamWSEnabledForGroup(11))
	require.False(t, ForceUpstreamWSEnabledForGroup(22))
}
