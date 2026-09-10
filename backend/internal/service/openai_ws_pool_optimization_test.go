package service

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestOpenAIWSOptimizedPoolClaimsWarmConnectionAndProtectsOwnership(t *testing.T) {
	cfg := &config.Config{}
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 8
	cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 8
	cfg.Gateway.OpenAIWS.DialTimeoutSeconds = 1
	pool := newOpenAIWSConnPool(cfg)
	defer pool.Close()

	account := &Account{ID: 4101, Name: "optimized", Platform: PlatformOpenAI, Type: AccountTypeOAuth, Concurrency: 8}
	ap := pool.getOrCreateAccountPool(account.ID)
	prewarm := newOpenAIWSConn("prewarm", account.ID, &openAIWSFakeConn{}, nil)
	prewarm.poolRole = openAIWSConnRolePrewarm
	standby := newOpenAIWSConn("standby", account.ID, &openAIWSFakeConn{}, nil)
	standby.poolRole = openAIWSConnRoleStandby
	ap.conns[prewarm.id] = prewarm
	ap.conns[standby.id] = standby

	leaseA, err := pool.Acquire(context.Background(), openAIWSAcquireRequest{Account: account, WSURL: "wss://example.test", PoolOptimized: true, SessionHash: "session-a"})
	require.NoError(t, err)
	require.Equal(t, "prewarm", leaseA.ConnID())
	require.Equal(t, openAIWSConnRoleSessionPrimary, prewarm.poolRole)
	require.Equal(t, "session-a", prewarm.ownerSession)
	leaseA.Release()

	leaseB, err := pool.Acquire(context.Background(), openAIWSAcquireRequest{Account: account, WSURL: "wss://example.test", PoolOptimized: true, SessionHash: "session-b"})
	require.NoError(t, err)
	require.Equal(t, "standby", leaseB.ConnID(), "new sessions must not borrow another session's idle connection")
	leaseB.Release()

	leaseA2, err := pool.Acquire(context.Background(), openAIWSAcquireRequest{Account: account, WSURL: "wss://example.test", PoolOptimized: true, SessionHash: "session-a"})
	require.NoError(t, err)
	require.Equal(t, "prewarm", leaseA2.ConnID(), "existing sessions should return to their owned connection")
	leaseA2.Release()
}

func TestOpenAIWSPoolOptimizationDefaultsAndDialFloor(t *testing.T) {
	settings := parseOpenAIWSPoolOptimizationValues(nil)
	require.Equal(t, 3, settings.PrewarmIdle)
	require.Equal(t, 3, settings.StandbyIdle)
	require.Equal(t, 24, settings.MaxConns)
	require.Equal(t, 400, settings.DialIntervalMS)

	settings = parseOpenAIWSPoolOptimizationValues(map[string]string{SettingKeyOpenAIWSOptimizedDialIntervalMS: "100"})
	require.Equal(t, 400, settings.DialIntervalMS)
}

func TestOpenAIWSOptimizedPoolPrefersSessionPrimaryThenStandby(t *testing.T) {
	refreshForceUpstreamWSCache(true)
	refreshOpenAIWSPoolOptimizationSettings(&SystemSettings{OpenAIWSPoolOptimizationEnabled: true})
	t.Cleanup(func() { refreshForceUpstreamWSCache(false); refreshOpenAIWSPoolOptimizationSettings(&SystemSettings{}) })
	cfg := &config.Config{}
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 8
	cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 8
	pool := newOpenAIWSConnPool(cfg)
	defer pool.Close()

	account := &Account{ID: 4102, Name: "affinity", Platform: PlatformOpenAI, Type: AccountTypeOAuth, Concurrency: 8}
	ap := pool.getOrCreateAccountPool(account.ID)
	primary := newOpenAIWSConn("primary", account.ID, &openAIWSFakeConn{}, nil)
	primary.poolRole, primary.ownerSession = openAIWSConnRoleSessionPrimary, "session-a"
	standby := newOpenAIWSConn("session-standby", account.ID, &openAIWSFakeConn{}, nil)
	standby.poolRole, standby.ownerSession = openAIWSConnRoleSessionStandby, "session-a"
	ap.conns[standby.id] = standby
	ap.conns[primary.id] = primary

	lease, err := pool.Acquire(context.Background(), openAIWSAcquireRequest{Account: account, WSURL: "wss://example.test", PoolOptimized: true, SessionHash: "session-a"})
	require.NoError(t, err)
	require.Equal(t, primary.id, lease.ConnID())

	lease2, err := pool.Acquire(context.Background(), openAIWSAcquireRequest{Account: account, WSURL: "wss://example.test", PoolOptimized: true, SessionHash: "session-a"})
	require.NoError(t, err)
	require.Equal(t, standby.id, lease2.ConnID())
	lease2.Release()
	lease.Release()
}

func TestOpenAIWSOptimizedPoolPromotesReplacementPrimary(t *testing.T) {
	refreshForceUpstreamWSCache(true)
	refreshOpenAIWSPoolOptimizationSettings(&SystemSettings{OpenAIWSPoolOptimizationEnabled: true})
	t.Cleanup(func() { refreshForceUpstreamWSCache(false); refreshOpenAIWSPoolOptimizationSettings(&SystemSettings{}) })
	cfg := &config.Config{}
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 8
	pool := newOpenAIWSConnPool(cfg)
	defer pool.Close()

	account := &Account{ID: 4103, Name: "replacement", Platform: PlatformOpenAI, Type: AccountTypeOAuth, Concurrency: 8}
	ap := pool.getOrCreateAccountPool(account.ID)
	existingStandby := newOpenAIWSConn("old-standby", account.ID, &openAIWSFakeConn{}, nil)
	existingStandby.poolRole, existingStandby.ownerSession = openAIWSConnRoleSessionStandby, "session-a"
	publicStandby := newOpenAIWSConn("public-standby", account.ID, &openAIWSFakeConn{}, nil)
	publicStandby.poolRole = openAIWSConnRoleStandby
	ap.conns[existingStandby.id] = existingStandby
	ap.conns[publicStandby.id] = publicStandby

	require.False(t, pool.hasSessionPrimaryLocked(ap, "session-a"))
	pool.claimOptimizedConnLocked(ap, publicStandby, "session-a", false, time.Now())
	require.Equal(t, openAIWSConnRoleSessionPrimary, publicStandby.poolRole)
}

func TestOpenAIWSPoolOptimizationDisabledIgnoresSessionOwnership(t *testing.T) {
	refreshForceUpstreamWSCache(true)
	refreshOpenAIWSPoolOptimizationSettings(&SystemSettings{OpenAIWSPoolOptimizationEnabled: true})
	t.Cleanup(func() {
		refreshForceUpstreamWSCache(false)
		refreshOpenAIWSPoolOptimizationSettings(&SystemSettings{})
	})

	pool := newOpenAIWSConnPool(&config.Config{})
	defer pool.Close()
	conn := newOpenAIWSConn("owned", 4104, &openAIWSFakeConn{}, nil)
	conn.ownerSession = "session-a"
	conn.ownerUntil = time.Now().Add(time.Hour)
	require.True(t, pool.connHasActiveOwner(conn, time.Now()))

	refreshOpenAIWSPoolOptimizationSettings(&SystemSettings{OpenAIWSPoolOptimizationEnabled: false})
	require.False(t, pool.connHasActiveOwner(conn, time.Now()), "disabled optimization must restore legacy cleanup behavior")
}

func TestOpenAIWSOptimizedPoolReplacesOldestIdleOwnedConnAtCapacity(t *testing.T) {
	refreshForceUpstreamWSCache(true)
	refreshOpenAIWSPoolOptimizationSettings(&SystemSettings{
		OpenAIWSPoolOptimizationEnabled: true, OpenAIWSOptimizedMaxConnsPerAccount: 2,
		OpenAIWSOptimizedQueuePerConn: 1, OpenAIWSOptimizedTargetUtilization: 0.8,
		OpenAIWSOptimizedIdleRecycleSeconds: 300, OpenAIWSOptimizedMaxAgeSeconds: 3600,
		OpenAIWSOptimizedHealthIntervalSeconds: 30, OpenAIWSOptimizedSessionTTLSeconds: 3600,
		OpenAIWSOptimizedDialIntervalMS: 400,
	})
	t.Cleanup(func() { refreshForceUpstreamWSCache(false); refreshOpenAIWSPoolOptimizationSettings(&SystemSettings{}) })

	pool := newOpenAIWSConnPool(&config.Config{})
	defer pool.Close()
	pool.setClientDialerForTest(&openAIWSFakeDialer{})
	account := &Account{ID: 4105, Name: "full", Platform: PlatformOpenAI, Type: AccountTypeOAuth, Concurrency: 2}
	ap := pool.getOrCreateAccountPool(account.ID)
	oldBackup := newOpenAIWSConn("old-backup", account.ID, &openAIWSFakeConn{}, nil)
	oldBackup.poolRole, oldBackup.ownerSession = openAIWSConnRoleSessionStandby, "session-a"
	oldBackup.ownerUntil = time.Now().Add(time.Hour)
	oldBackup.lastUsedNano.Store(time.Now().Add(-time.Minute).UnixNano())
	newPrimary := newOpenAIWSConn("new-primary", account.ID, &openAIWSFakeConn{}, nil)
	newPrimary.poolRole, newPrimary.ownerSession = openAIWSConnRoleSessionPrimary, "session-b"
	newPrimary.ownerUntil = time.Now().Add(time.Hour)
	ap.conns[oldBackup.id] = oldBackup
	ap.conns[newPrimary.id] = newPrimary
	ap.lastCleanupAt = time.Now()

	lease, err := pool.Acquire(context.Background(), openAIWSAcquireRequest{
		Account: account, WSURL: "wss://example.test", PoolOptimized: true, SessionHash: "session-c",
	})
	require.NoError(t, err)
	require.NotEqual(t, oldBackup.id, lease.ConnID())
	require.Equal(t, openAIWSConnRoleSessionPrimary, lease.conn.poolRole)
	require.Equal(t, "session-c", lease.conn.ownerSession)
	require.NotContains(t, ap.conns, oldBackup.id)
	require.Contains(t, ap.conns, newPrimary.id, "an idle backup should be retired before a session primary")
	lease.Release()
}
