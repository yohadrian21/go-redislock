package redislock_test

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yohadrian21/go-redislock"
)

// The integration tests run against a real Redis server, where keys expire
// in real time. They are skipped unless REDIS_ADDR is set:
//
//	REDIS_ADDR=localhost:6379 go test -race ./...

func newIntegration(t *testing.T) (locker *redislock.Client, rdb *redis.Client, key string) {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set; skipping integration test")
	}

	rdb = redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Ping(t.Context()).Err(); err != nil {
		t.Fatalf("connect to Redis at %s: %v", addr, err)
	}

	key = "redislock-test:" + t.Name() + ":" + rand.Text()
	t.Cleanup(func() { rdb.Del(context.Background(), key, key+":fence") })
	return redislock.New(rdb), rdb, key
}

func TestIntegrationMutualExclusion(t *testing.T) {
	locker, _, key := newIntegration(t)
	assertMutualExclusion(t, locker, key)
}

func TestIntegrationExpiry(t *testing.T) {
	locker, _, key := newIntegration(t)
	const ttl = 100 * time.Millisecond

	stale := mustTryLock(t, locker, key, ttl)
	if _, err := locker.TryLock(t.Context(), key, ttl); !errors.Is(err, redislock.ErrNotObtained) {
		t.Fatalf("TryLock on held key: err = %v, want ErrNotObtained", err)
	}

	time.Sleep(ttl + 50*time.Millisecond)
	if _, err := stale.TTL(t.Context()); !errors.Is(err, redislock.ErrNotHeld) {
		t.Errorf("TTL after expiry: err = %v, want ErrNotHeld", err)
	}

	current := mustTryLock(t, locker, key, 10*time.Second)
	if err := stale.Unlock(t.Context()); !errors.Is(err, redislock.ErrNotHeld) {
		t.Errorf("stale Unlock: err = %v, want ErrNotHeld", err)
	}
	if err := stale.Refresh(t.Context(), time.Minute); !errors.Is(err, redislock.ErrNotHeld) {
		t.Errorf("stale Refresh: err = %v, want ErrNotHeld", err)
	}
	if ttl, err := current.TTL(t.Context()); err != nil || ttl < 9*time.Second {
		t.Errorf("new holder TTL() = %v, %v; want about 10s", ttl, err)
	}
}

func TestIntegrationLockWaitsForCrashedHolder(t *testing.T) {
	locker, _, key := newIntegration(t)
	const ttl = 200 * time.Millisecond

	start := time.Now()
	mustTryLock(t, locker, key, ttl) // never released, as if its holder crashed

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	lock, err := locker.Lock(ctx, key, time.Second)
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if elapsed := time.Since(start); elapsed < ttl/2 {
		t.Errorf("Lock obtained after %v, before the crashed holder's %v TTL ran out", elapsed, ttl)
	}
	if err := lock.Unlock(t.Context()); err != nil {
		t.Errorf("Unlock: %v", err)
	}
}

func TestIntegrationAutoRefreshKeepsLockAlive(t *testing.T) {
	locker, _, key := newIntegration(t)
	const ttl = 200 * time.Millisecond
	lock := mustTryLock(t, locker, key, ttl)

	ctx, stop := lock.AutoRefresh(t.Context())
	time.Sleep(5 * ttl)

	if _, err := locker.TryLock(t.Context(), key, ttl); !errors.Is(err, redislock.ErrNotObtained) {
		t.Errorf("TryLock while the watchdog runs: err = %v, want ErrNotObtained", err)
	}
	if err := context.Cause(ctx); err != nil {
		t.Errorf("watchdog context canceled while the lock was held: %v", err)
	}

	stop()
	if err := lock.Unlock(t.Context()); err != nil {
		t.Errorf("Unlock: %v", err)
	}
}

func TestIntegrationAutoRefreshDetectsLostLock(t *testing.T) {
	locker, rdb, key := newIntegration(t)
	lock := mustTryLock(t, locker, key, 300*time.Millisecond)

	ctx, stop := lock.AutoRefresh(t.Context())
	defer stop()

	// Simulate another client taking over, e.g. after a failover lost the key.
	if err := rdb.Set(t.Context(), key, "another-client", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}

	waitDone(t, ctx, 2*time.Second)
	if err := context.Cause(ctx); !errors.Is(err, redislock.ErrNotHeld) {
		t.Errorf("cause = %v, want ErrNotHeld", err)
	}
	if got := rdb.Get(t.Context(), key).Val(); got != "another-client" {
		t.Errorf("watchdog changed another client's lock to %q", got)
	}
}

func TestIntegrationScriptCacheFlush(t *testing.T) {
	locker, rdb, key := newIntegration(t)
	lock := mustTryLock(t, locker, key, time.Second)

	// After a restart or SCRIPT FLUSH, EVALSHA fails with NOSCRIPT and the
	// scripts must be sent again.
	if err := rdb.ScriptFlush(t.Context()).Err(); err != nil {
		t.Fatal(err)
	}

	if err := lock.Refresh(t.Context(), time.Second); err != nil {
		t.Errorf("Refresh after SCRIPT FLUSH: %v", err)
	}
	if err := lock.Unlock(t.Context()); err != nil {
		t.Errorf("Unlock after SCRIPT FLUSH: %v", err)
	}
}
