package redislock

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// AutoRefresh starts a watchdog goroutine that keeps the lock alive. It
// refreshes the lock to the TTL it was acquired with right away and then
// every third of that TTL, so a single failed refresh does not lose the lock.
//
// The returned context is derived from ctx. It is canceled with cause
// [ErrNotHeld] as soon as the lock is lost: when a refresh finds that the lock
// expired or was taken over, or when refreshes keep failing, for example
// because Redis is unreachable, until the TTL runs out. Do the work the lock
// protects under this context, and use [context.Cause] to tell a lost lock
// from an ordinary cancellation.
//
// Call the returned stop function to stop refreshing, typically right before
// [Lock.Unlock]. Canceling ctx also stops the watchdog. Neither releases the
// lock.
func (l *Lock) AutoRefresh(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancelCause(ctx)

	// expire cancels ctx once the lock may have expired, even while a refresh
	// is still waiting for Redis to answer.
	expired := fmt.Errorf("%w: no successful refresh before the TTL ran out", ErrNotHeld)
	expire := time.AfterFunc(time.Until(l.obtainedAt.Add(l.ttl)), func() { cancel(expired) })

	go func() {
		defer expire.Stop()

		ticker := time.NewTicker(l.ttl / 3)
		defer ticker.Stop()

		for {
			start := time.Now()
			// Errors other than ErrNotHeld are retried on the next tick; if
			// they persist, expire fires once the lock may have run out.
			switch err := l.Refresh(ctx, l.ttl); {
			case err == nil:
				expire.Reset(time.Until(start.Add(l.ttl)))
			case errors.Is(err, ErrNotHeld):
				cancel(ErrNotHeld)
				return
			}

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	return ctx, func() { cancel(nil) }
}
