package repository

import (
	"context"
	"fmt"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestOpenAICongestionConcurrentMigrationKeepsOneBinding(t *testing.T) {
	m := miniredis.RunT(t)
	r := redis.NewClient(&redis.Options{Addr: m.Addr()})
	t.Cleanup(func() { _ = r.Close() })
	c := &gatewayCache{rdb: r}
	ctx := context.Background()
	require.NoError(t, c.SetSessionAccountID(ctx, 4, "session", 1, time.Hour))
	var winners atomic.Int32
	var selected atomic.Int64
	var owner string
	var mu sync.Mutex
	var wg sync.WaitGroup
	errors := make(chan error, 200)
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			token := fmt.Sprint(i)
			ok, err := c.ReserveCongestionSession(ctx, 4, "session", 1, token, time.Minute)
			if err != nil {
				errors <- err
			}
			if ok {
				winners.Add(1)
				selected.Store(int64(i + 2))
				mu.Lock()
				owner = token
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	require.EqualValues(t, 1, winners.Load())
	bound, err := c.GetSessionAccountID(ctx, 4, "session")
	require.NoError(t, err)
	require.EqualValues(t, 1, bound, "reservation must not publish an unproven binding")
	ok, err := c.CommitCongestionSession(ctx, 4, "session", owner, 1, selected.Load(), time.Hour, 2*time.Minute, "", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	for i := 0; i < 100; i++ {
		ok, err = c.SetSessionAccountIDIfUnchanged(ctx, 4, "session", 1, 1, time.Hour, "", time.Minute)
		require.NoError(t, err)
		require.False(t, ok, "old requests must not undo the migration")
	}
	bound, err = c.GetSessionAccountID(ctx, 4, "session")
	require.NoError(t, err)
	require.Equal(t, selected.Load(), bound)
	ok, err = c.ReserveCongestionSession(ctx, 4, "session", bound, "another", time.Minute)
	require.NoError(t, err)
	require.False(t, ok, "protect new binding from immediate oscillation")
	m.FastForward(121 * time.Second)
	bound, err = c.GetSessionAccountID(ctx, 4, "session")
	require.NoError(t, err)
	require.Equal(t, selected.Load(), bound, "protection expiry does not switch back")
	ok, err = c.ReserveCongestionSession(ctx, 4, "session", bound, "another", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestOpenAICongestionFailureIsolationAndExpiredOwner(t *testing.T) {
	m := miniredis.RunT(t)
	r := redis.NewClient(&redis.Options{Addr: m.Addr()})
	defer r.Close()
	c := &gatewayCache{rdb: r}
	ctx := context.Background()
	for _, group := range []int64{1, 2} {
		require.NoError(t, c.SetSessionAccountID(ctx, group, "same", 10, time.Hour))
	}
	ok, err := c.ReserveCongestionSession(ctx, 1, "same", 10, "first", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	pending, protected, err := c.ReadCongestionSession(ctx, 2, "same")
	require.NoError(t, err)
	require.Empty(t, pending)
	require.False(t, protected)
	require.NoError(t, c.ReleaseCongestionSession(ctx, 1, "same", "wrong-owner"))
	pending, _, err = c.ReadCongestionSession(ctx, 1, "same")
	require.NoError(t, err)
	require.Equal(t, "first", pending)
	require.NoError(t, c.ReleaseCongestionSession(ctx, 1, "same", "first"))
	bound, err := c.GetSessionAccountID(ctx, 1, "same")
	require.NoError(t, err)
	require.EqualValues(t, 10, bound)
	ok, err = c.ReserveCongestionSession(ctx, 1, "same", 10, "second", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = c.CommitCongestionSession(ctx, 1, "same", "first", 10, 20, time.Hour, time.Minute, "", time.Minute)
	require.NoError(t, err)
	require.False(t, ok)
	m.FastForward(61 * time.Second)
	ok, err = c.CommitCongestionSession(ctx, 1, "same", "second", 10, 20, time.Hour, time.Minute, "", time.Minute)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestOpenAICongestionLegacyBindingAndStaleDelete(t *testing.T) {
	m := miniredis.RunT(t)
	r := redis.NewClient(&redis.Options{Addr: m.Addr()})
	defer r.Close()
	c := &gatewayCache{rdb: r}
	ctx := context.Background()
	require.NoError(t, c.SetSessionAccountID(ctx, 7, "old", 1, time.Minute))
	// Upgrade a legacy-only sticky binding without losing compatibility.
	ok, err := c.SetSessionAccountIDIfUnchanged(ctx, 7, "primary", 1, 1, time.Hour, "old", 10*time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = c.ReserveCongestionSession(ctx, 7, "primary", 1, "winner", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = c.DeleteSessionAccountIDIfUnchanged(ctx, 7, "primary", 1, "old")
	require.NoError(t, err)
	require.False(t, ok, "an in-flight migration protects its source binding")
	ok, err = c.CommitCongestionSession(ctx, 7, "primary", "winner", 1, 2, time.Hour, 2*time.Minute, "old", 10*time.Minute)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = c.DeleteSessionAccountIDIfUnchanged(ctx, 7, "primary", 1, "old")
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = c.SetSessionAccountIDIfUnchanged(ctx, 7, "primary", 1, 1, time.Hour, "old", 10*time.Minute)
	require.NoError(t, err)
	require.False(t, ok)
	for _, key := range []string{"primary", "old"} {
		id, err := c.GetSessionAccountID(ctx, 7, key)
		require.NoError(t, err)
		require.EqualValues(t, 2, id)
	}
}
