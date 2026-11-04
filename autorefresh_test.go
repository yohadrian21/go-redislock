package redislock_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yohadrian21/go-redislock"
)

func TestAutoRefreshKeepsLockAlive(t *testing.T) {
	locker, mr := newLocker(t)
	const ttl = 300 * time.Millisecond
	lock := mustTryLock(t, locker, testKey, ttl)

	ctx, stop := lock.AutoRefresh(t.Context())
	defer stop()

	// miniredis only advances its clock on FastForward, so age the key by
	// hand and wait for the watchdog to restore the full TTL each time.
	for range 3 {
		mr.FastForward(ttl - 50*time.Millisecond)
		waitFor(t, 2*time.Second, func() bool { return mr.TTL(testKey) == ttl })
	}

	if err := context.Cause(ctx); err != nil {
		t.Errorf("watchdog context canceled while the lock was held: %v", err)
	}
}

func TestAutoRefreshDetectsLostLock(t *testing.T) {
	locker, mr := newLocker(t)
	const ttl = time.Second
	lock := mustTryLock(t, locker, testKey, ttl)

	ctx, stop := lock.AutoRefresh(t.Context())
	defer stop()
	time.Sleep(50 * time.Millisecond) // let the initial refresh run

	start := time.Now()
	if err := mr.Set(testKey, "another-client"); err != nil {
		t.Fatal(err)
	}

	waitDone(t, ctx.Done(), 2*time.Second)
	if err := context.Cause(ctx); !errors.Is(err, redislock.ErrNotHeld) {
		t.Errorf("cause = %v, want ErrNotHeld", err)
	}
	if elapsed := time.Since(start); elapsed > ttl*9/10 {
		t.Errorf("took %v to notice the lost lock, want about a third of the %v TTL", elapsed, ttl)
	}
	if got, _ := mr.Get(testKey); got != "another-client" {
		t.Errorf("watchdog changed another client's lock to %q", got)
	}
}

func TestAutoRefreshGivesUpWhenRedisIsUnreachable(t *testing.T) {
	locker, mr := newLocker(t)
	const ttl = 300 * time.Millisecond
	start := time.Now()
	lock := mustTryLock(t, locker, testKey, ttl)

	ctx, stop := lock.AutoRefresh(t.Context())
	defer stop()
	mr.SetError("ERR connection lost")

	waitDone(t, ctx.Done(), 2*time.Second)
	if err := context.Cause(ctx); !errors.Is(err, redislock.ErrNotHeld) {
		t.Errorf("cause = %v, want ErrNotHeld", err)
	}
	if elapsed := time.Since(start); elapsed < ttl {
		t.Errorf("canceled after %v, before the %v TTL ran out", elapsed, ttl)
	}
}

func TestAutoRefreshStop(t *testing.T) {
	locker, mr := newLocker(t)
	const ttl = 300 * time.Millisecond
	lock := mustTryLock(t, locker, testKey, ttl)

	ctx, stop := lock.AutoRefresh(t.Context())
	stop()

	waitDone(t, ctx.Done(), time.Second)
	if err := context.Cause(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("cause = %v, want context.Canceled", err)
	}

	time.Sleep(50 * time.Millisecond) // let an in-flight refresh land
	mr.FastForward(ttl - 50*time.Millisecond)
	time.Sleep(ttl)
	if got := mr.TTL(testKey); got != 50*time.Millisecond {
		t.Errorf("TTL = %v after stop, want 50ms: the watchdog kept refreshing", got)
	}
	if err := lock.Unlock(t.Context()); err != nil {
		t.Errorf("Unlock after stop: %v", err)
	}
}

func TestAutoRefreshStopsWithParentContext(t *testing.T) {
	locker, _ := newLocker(t)
	lock := mustTryLock(t, locker, testKey, time.Second)

	parent, cancel := context.WithCancel(t.Context())
	ctx, stop := lock.AutoRefresh(parent)
	defer stop()
	cancel()

	waitDone(t, ctx.Done(), time.Second)
	if err := context.Cause(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("cause = %v, want context.Canceled", err)
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %v", timeout)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func waitDone(t *testing.T, done <-chan struct{}, timeout time.Duration) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatalf("context not done within %v", timeout)
	}
}
