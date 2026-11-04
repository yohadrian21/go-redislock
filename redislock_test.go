package redislock_test

import (
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/yohadrian21/go-redislock"
)

const testKey = "test:lock"

// newLocker returns a Client backed by a fresh in-memory miniredis server.
// miniredis only expires keys when the test calls FastForward, which makes
// expiry tests deterministic.
func newLocker(t *testing.T) (*redislock.Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return redislock.New(rdb), mr
}

func mustTryLock(t *testing.T, locker *redislock.Client, key string, ttl time.Duration, opts ...redislock.Option) *redislock.Lock {
	t.Helper()
	lock, err := locker.TryLock(t.Context(), key, ttl, opts...)
	if err != nil {
		t.Fatalf("TryLock(%q): %v", key, err)
	}
	return lock
}

func TestTryLock(t *testing.T) {
	locker, mr := newLocker(t)

	lock := mustTryLock(t, locker, testKey, time.Second)

	if got := lock.Key(); got != testKey {
		t.Errorf("Key() = %q, want %q", got, testKey)
	}
	if got := mr.TTL(testKey); got != time.Second {
		t.Errorf("key TTL = %v, want %v", got, time.Second)
	}
	if _, err := locker.TryLock(t.Context(), testKey, time.Second); !errors.Is(err, redislock.ErrNotObtained) {
		t.Errorf("TryLock on held key: err = %v, want ErrNotObtained", err)
	}
}

func TestTryLockUsesRandomTokens(t *testing.T) {
	locker, mr := newLocker(t)

	mustTryLock(t, locker, "a", time.Second)
	mustTryLock(t, locker, "b", time.Second)

	a, _ := mr.Get("a")
	b, _ := mr.Get("b")
	if len(a) < 16 || a == b {
		t.Errorf("tokens %q and %q are not unique random values", a, b)
	}
}

func TestTryLockRejectsInvalidTTL(t *testing.T) {
	locker, mr := newLocker(t)

	for _, ttl := range []time.Duration{0, -time.Second, 500 * time.Microsecond} {
		_, err := locker.TryLock(t.Context(), testKey, ttl)
		if err == nil || errors.Is(err, redislock.ErrNotObtained) {
			t.Errorf("TryLock with ttl %v: err = %v, want a validation error", ttl, err)
		}
	}
	if mr.Exists(testKey) {
		t.Error("an invalid TTL created the lock key")
	}
}

func TestUnlock(t *testing.T) {
	locker, mr := newLocker(t)
	lock := mustTryLock(t, locker, testKey, time.Second)

	if err := lock.Unlock(t.Context()); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	if mr.Exists(testKey) {
		t.Error("lock key still exists after Unlock")
	}
	if err := lock.Unlock(t.Context()); !errors.Is(err, redislock.ErrNotHeld) {
		t.Errorf("second Unlock: err = %v, want ErrNotHeld", err)
	}

	mustTryLock(t, locker, testKey, time.Second)
}

func TestRefresh(t *testing.T) {
	locker, _ := newLocker(t)
	lock := mustTryLock(t, locker, testKey, time.Second)

	if err := lock.Refresh(t.Context(), 3*time.Second); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if ttl, err := lock.TTL(t.Context()); err != nil || ttl != 3*time.Second {
		t.Errorf("TTL() = %v, %v; want %v, nil", ttl, err, 3*time.Second)
	}
	if err := lock.Refresh(t.Context(), 0); err == nil || errors.Is(err, redislock.ErrNotHeld) {
		t.Errorf("Refresh with zero ttl: err = %v, want a validation error", err)
	}
}

func TestRefreshKeepsLockPastOriginalTTL(t *testing.T) {
	locker, mr := newLocker(t)
	lock := mustTryLock(t, locker, testKey, time.Second)

	mr.FastForward(800 * time.Millisecond)
	if err := lock.Refresh(t.Context(), time.Second); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	mr.FastForward(800 * time.Millisecond)

	if ttl, err := lock.TTL(t.Context()); err != nil || ttl != 200*time.Millisecond {
		t.Errorf("TTL() = %v, %v; want %v, nil", ttl, err, 200*time.Millisecond)
	}
}

func TestTTL(t *testing.T) {
	locker, mr := newLocker(t)
	lock := mustTryLock(t, locker, testKey, time.Second)

	mr.FastForward(300 * time.Millisecond)
	if ttl, err := lock.TTL(t.Context()); err != nil || ttl != 700*time.Millisecond {
		t.Errorf("TTL() = %v, %v; want %v, nil", ttl, err, 700*time.Millisecond)
	}

	mr.FastForward(700 * time.Millisecond)
	if _, err := lock.TTL(t.Context()); !errors.Is(err, redislock.ErrNotHeld) {
		t.Errorf("TTL() after expiry: err = %v, want ErrNotHeld", err)
	}
}

func TestExpiredLockCannotTouchNewHolder(t *testing.T) {
	locker, mr := newLocker(t)
	stale := mustTryLock(t, locker, testKey, time.Second)

	mr.FastForward(time.Second)
	current := mustTryLock(t, locker, testKey, 5*time.Second)

	if err := stale.Unlock(t.Context()); !errors.Is(err, redislock.ErrNotHeld) {
		t.Errorf("stale Unlock: err = %v, want ErrNotHeld", err)
	}
	if err := stale.Refresh(t.Context(), time.Minute); !errors.Is(err, redislock.ErrNotHeld) {
		t.Errorf("stale Refresh: err = %v, want ErrNotHeld", err)
	}
	if _, err := stale.TTL(t.Context()); !errors.Is(err, redislock.ErrNotHeld) {
		t.Errorf("stale TTL: err = %v, want ErrNotHeld", err)
	}

	if ttl, err := current.TTL(t.Context()); err != nil || ttl != 5*time.Second {
		t.Errorf("new holder TTL() = %v, %v; want %v, nil", ttl, err, 5*time.Second)
	}
}

func TestFencingTokens(t *testing.T) {
	locker, _ := newLocker(t)

	for want := int64(1); want <= 3; want++ {
		lock := mustTryLock(t, locker, testKey, time.Second, redislock.WithFencing())
		if got := lock.FencingToken(); got != want {
			t.Errorf("FencingToken() = %d, want %d", got, want)
		}

		// A failed attempt must not consume a token.
		if _, err := locker.TryLock(t.Context(), testKey, time.Second, redislock.WithFencing()); !errors.Is(err, redislock.ErrNotObtained) {
			t.Fatalf("TryLock on held key: err = %v, want ErrNotObtained", err)
		}

		if err := lock.Unlock(t.Context()); err != nil {
			t.Fatalf("Unlock: %v", err)
		}
	}

	if got := mustTryLock(t, locker, testKey, time.Second).FencingToken(); got != 0 {
		t.Errorf("FencingToken() without WithFencing = %d, want 0", got)
	}
}

func TestFencingTokenSurvivesExpiry(t *testing.T) {
	locker, mr := newLocker(t)

	first := mustTryLock(t, locker, testKey, time.Second, redislock.WithFencing())
	mr.FastForward(time.Second)
	second := mustTryLock(t, locker, testKey, time.Second, redislock.WithFencing())

	if second.FencingToken() <= first.FencingToken() {
		t.Errorf("token after expiry = %d, want more than %d", second.FencingToken(), first.FencingToken())
	}
}

func TestRedisErrorsArePassedThrough(t *testing.T) {
	locker, mr := newLocker(t)
	lock := mustTryLock(t, locker, testKey, time.Second)

	mr.SetError("ERR server unavailable")

	if _, err := locker.TryLock(t.Context(), "other", time.Second); err == nil || errors.Is(err, redislock.ErrNotObtained) {
		t.Errorf("TryLock: err = %v, want the server error", err)
	}
	if err := lock.Unlock(t.Context()); err == nil || errors.Is(err, redislock.ErrNotHeld) {
		t.Errorf("Unlock: err = %v, want the server error", err)
	}
}
