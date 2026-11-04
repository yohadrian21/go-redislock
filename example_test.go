package redislock_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yohadrian21/go-redislock"
)

func Example() {
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr}) // e.g. "localhost:6379"
	defer rdb.Close()

	locker := redislock.New(rdb)

	// Wait up to 5 seconds for the lock, then hold it for at most 30 seconds.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	lock, err := locker.Lock(ctx, "example:invoice:42", 30*time.Second)
	if err != nil {
		panic(err)
	}
	fmt.Println("lock acquired")

	// ... work that must not run concurrently ...

	if err := lock.Unlock(context.Background()); err != nil {
		panic(err)
	}
	fmt.Println("lock released")

	// Output:
	// lock acquired
	// lock released
}

func ExampleClient_TryLock() {
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	locker := redislock.New(rdb)
	ctx := context.Background()

	lock, err := locker.TryLock(ctx, "example:nightly-report", time.Minute)
	if err != nil {
		panic(err)
	}

	// A second worker gives up right away instead of waiting.
	_, err = locker.TryLock(ctx, "example:nightly-report", time.Minute)
	if errors.Is(err, redislock.ErrNotObtained) {
		fmt.Println("the report is already being generated")
	}

	if err := lock.Unlock(ctx); err != nil {
		panic(err)
	}

	// Output: the report is already being generated
}

func ExampleClient_Lock() {
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	locker := redislock.New(rdb)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	lock, err := locker.Lock(ctx, "example:inventory:sku-123", 10*time.Second,
		redislock.WithBackoff(redislock.ExponentialBackoff(20*time.Millisecond, 500*time.Millisecond)),
		redislock.WithJitter(1),
		redislock.WithMaxRetries(20),
	)
	if errors.Is(err, redislock.ErrNotObtained) {
		fmt.Println("inventory is busy, try again later")
		return
	}
	if err != nil {
		panic(err)
	}

	fmt.Println("updating inventory")

	if err := lock.Unlock(context.Background()); err != nil {
		panic(err)
	}

	// Output: updating inventory
}

func ExampleLock_AutoRefresh() {
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	locker := redislock.New(rdb)

	lock, err := locker.TryLock(context.Background(), "example:migration", 5*time.Second)
	if err != nil {
		panic(err)
	}

	// The watchdog keeps the lock alive for as long as the job runs. If the
	// lock is lost anyway, it cancels ctx so the job stops instead of racing
	// the next holder.
	ctx, stop := lock.AutoRefresh(context.Background())
	err = runJob(ctx)
	stop()

	switch {
	case errors.Is(context.Cause(ctx), redislock.ErrNotHeld):
		fmt.Println("lost the lock, job aborted")
	case err != nil:
		panic(err)
	default:
		fmt.Println("job finished")
	}

	if err := lock.Unlock(context.Background()); err != nil {
		panic(err)
	}

	// Output: job finished
}

// runJob stands in for long-running work that stops when ctx is canceled.
func runJob(ctx context.Context) error {
	select {
	case <-time.After(100 * time.Millisecond):
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func ExampleWithFencing() {
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	locker := redislock.New(rdb)
	ctx := context.Background()

	first, err := locker.TryLock(ctx, "example:ledger", time.Second, redislock.WithFencing())
	if err != nil {
		panic(err)
	}
	if err := first.Unlock(ctx); err != nil {
		panic(err)
	}

	second, err := locker.TryLock(ctx, "example:ledger", time.Second, redislock.WithFencing())
	if err != nil {
		panic(err)
	}

	// Storage that remembers the highest token it has accepted can now
	// reject writes that still carry the first holder's token.
	fmt.Println(second.FencingToken() > first.FencingToken())

	if err := second.Unlock(ctx); err != nil {
		panic(err)
	}

	// Output: true
}
