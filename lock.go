package redislock

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// Lock is a lock acquired by [Client.TryLock] or [Client.Lock]. Its methods
// are safe for concurrent use.
//
// A Lock does not guarantee that the lock is still held: it expires when its
// TTL runs out unless it is refreshed. Methods that need the lock return
// [ErrNotHeld] once it is gone.
type Lock struct {
	rdb   redis.Scripter
	key   string
	token string
	fence int64
	ttl   time.Duration

	// obtainedAt is read from the local clock before the lock was requested,
	// so the lock is known to be held until at least obtainedAt+ttl.
	obtainedAt time.Time
}

// Key returns the Redis key of the lock.
func (l *Lock) Key() string {
	return l.key
}

// FencingToken returns the fencing token issued when the lock was acquired
// with [WithFencing], or 0 if fencing was not requested.
//
// Tokens for a key strictly increase with every acquisition. Send the token
// with each write the lock protects, and have the storage reject any write
// whose token is lower than the highest one it has accepted. That stops a
// client whose lock expired mid-operation from overwriting newer data.
func (l *Lock) FencingToken() int64 {
	return l.fence
}

// Refresh resets the lock's TTL to ttl, counted from now. It returns
// [ErrNotHeld] if the lock has expired or is held by another client.
//
// ttl must be at least one millisecond.
func (l *Lock) Refresh(ctx context.Context, ttl time.Duration) error {
	if err := checkTTL(ttl); err != nil {
		return err
	}
	err := refreshScript.Run(ctx, l.rdb, []string{l.key}, l.token, ttl.Milliseconds()).Err()
	if errors.Is(err, redis.Nil) {
		return ErrNotHeld
	}
	return err
}

// Unlock releases the lock. It returns [ErrNotHeld] if the lock has already
// expired or is held by another client; another client's lock is never
// deleted.
func (l *Lock) Unlock(ctx context.Context) error {
	err := releaseScript.Run(ctx, l.rdb, []string{l.key}, l.token).Err()
	if errors.Is(err, redis.Nil) {
		return ErrNotHeld
	}
	return err
}

// TTL returns how long the lock has left before it expires. It returns
// [ErrNotHeld] if the lock has expired or is held by another client.
func (l *Lock) TTL(ctx context.Context) (time.Duration, error) {
	ms, err := ttlScript.Run(ctx, l.rdb, []string{l.key}, l.token).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, ErrNotHeld
	}
	if err != nil {
		return 0, err
	}
	return time.Duration(ms) * time.Millisecond, nil
}
