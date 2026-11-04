// Package redislock implements distributed locks on Redis.
//
// A lock is a Redis key that holds a random token and expires after a TTL.
// It is acquired with SET NX PX, so only one client can create the key, and
// it is released or extended by Lua scripts that only touch the key while it
// still holds the caller's token. A client whose lock has expired therefore
// can never delete or extend a lock that someone else acquired since.
//
// Basic usage:
//
//	locker := redislock.New(rdb)
//
//	lock, err := locker.Lock(ctx, "orders:42", 10*time.Second)
//	if err != nil {
//		return err
//	}
//	defer lock.Unlock(context.WithoutCancel(ctx))
//
// Because locks expire, a holder can lose its lock while it is still working,
// for example during a long GC pause or a network partition. Use
// [Lock.AutoRefresh] to keep a lock alive and learn when it is lost, and
// [WithFencing] to get fencing tokens that let the protected resource reject
// writes from stale holders.
//
// Locks live on a single Redis primary. This is not the Redlock algorithm:
// if the primary fails over before a lock is replicated, two clients can hold
// the same lock. See the README for details.
package redislock

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	// ErrNotObtained is returned when a lock cannot be acquired because
	// another client holds it.
	ErrNotObtained = errors.New("redislock: lock not obtained")

	// ErrNotHeld is returned when an operation requires the lock but the
	// caller no longer holds it, because it expired, was released, or was
	// acquired by another client.
	ErrNotHeld = errors.New("redislock: lock not held")
)

// fenceSuffix is appended to a lock key to name its fencing token counter.
const fenceSuffix = ":fence"

// Client acquires locks. It is safe for concurrent use.
type Client struct {
	rdb redis.Scripter
}

// New returns a Client that stores locks in rdb, which can be any go-redis
// client, such as *redis.Client, *redis.ClusterClient or
// redis.UniversalClient.
func New(rdb redis.Scripter) *Client {
	return &Client{rdb: rdb}
}

// TryLock makes a single attempt to acquire the lock named key for ttl. It
// returns [ErrNotObtained] if another client holds the lock.
//
// ttl must be at least one millisecond.
func (c *Client) TryLock(ctx context.Context, key string, ttl time.Duration, opts ...Option) (*Lock, error) {
	return c.obtain(ctx, key, ttl, newOptions(opts))
}

// Lock acquires the lock named key for ttl, waiting while another client
// holds it. Between attempts it sleeps according to [WithBackoff] and
// [WithJitter].
//
// By default Lock keeps retrying until ctx is done, so pass a context with a
// deadline or use [WithMaxRetries] to bound the wait. If ctx ends first, the
// returned error wraps both [ErrNotObtained] and the context's error. If the
// retries run out, Lock returns ErrNotObtained. Other errors, such as a
// failed connection to Redis, are returned immediately.
//
// ttl must be at least one millisecond.
func (c *Client) Lock(ctx context.Context, key string, ttl time.Duration, opts ...Option) (*Lock, error) {
	o := newOptions(opts)
	for attempt := 1; ; attempt++ {
		lock, err := c.obtain(ctx, key, ttl, o)
		switch {
		case err == nil:
			return lock, nil
		case ctx.Err() != nil:
			return nil, fmt.Errorf("%w: %w", ErrNotObtained, context.Cause(ctx))
		case !errors.Is(err, ErrNotObtained):
			return nil, err
		case o.maxRetries >= 0 && attempt > o.maxRetries:
			return nil, ErrNotObtained
		}

		timer := time.NewTimer(o.delay(attempt))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("%w: %w", ErrNotObtained, context.Cause(ctx))
		case <-timer.C:
		}
	}
}

func (c *Client) obtain(ctx context.Context, key string, ttl time.Duration, o *options) (*Lock, error) {
	if err := checkTTL(ttl); err != nil {
		return nil, err
	}

	keys := []string{key}
	if o.fencing {
		keys = append(keys, key+fenceSuffix)
	}
	token := rand.Text()
	start := time.Now()

	fence, err := obtainScript.Run(ctx, c.rdb, keys, token, ttl.Milliseconds()).Int64()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNotObtained
	}
	if err != nil {
		return nil, err
	}

	return &Lock{
		rdb:        c.rdb,
		key:        key,
		token:      token,
		fence:      fence,
		ttl:        ttl,
		obtainedAt: start,
	}, nil
}

func checkTTL(ttl time.Duration) error {
	if ttl < time.Millisecond {
		return fmt.Errorf("redislock: ttl must be at least 1ms, got %v", ttl)
	}
	return nil
}
