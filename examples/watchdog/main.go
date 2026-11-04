// Command watchdog runs a job that outlasts its lock's TTL, using
// AutoRefresh to keep the lock alive until the job is done.
//
// Pass -lose-lock to delete the lock key halfway through, as a failover or
// an operator might, and watch the job abort instead of carrying on unsafely.
//
// It needs a Redis server, at localhost:6379 unless REDIS_ADDR says otherwise:
//
//	go run ./examples/watchdog [-lose-lock]
package main

import (
	"cmp"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yohadrian21/go-redislock"
)

func main() {
	loseLock := flag.Bool("lose-lock", false, "delete the lock key halfway through the job")
	flag.Parse()

	if err := run(*loseLock); err != nil {
		log.Fatal(err)
	}
}

func run(loseLock bool) error {
	rdb := redis.NewClient(&redis.Options{Addr: cmp.Or(os.Getenv("REDIS_ADDR"), "localhost:6379")})
	defer rdb.Close()

	locker := redislock.New(rdb)
	ctx := context.Background()
	const (
		key   = "examples:watchdog:job"
		ttl   = time.Second
		steps = 6
	)

	lock, err := locker.TryLock(ctx, key, ttl)
	if err != nil {
		return fmt.Errorf("acquire lock: %w", err)
	}
	fmt.Printf("job started: %d steps of 500ms under a lock with a %v TTL\n", steps, ttl)

	jobCtx, stop := lock.AutoRefresh(ctx)
	defer stop()

	for step := 1; step <= steps; step++ {
		select {
		case <-jobCtx.Done():
			return fmt.Errorf("job aborted at step %d: %w", step, context.Cause(jobCtx))
		case <-time.After(500 * time.Millisecond):
		}

		left, err := lock.TTL(ctx)
		if err != nil {
			return fmt.Errorf("read TTL: %w", err)
		}
		fmt.Printf("step %d/%d done, lock TTL %v\n", step, steps, left.Round(10*time.Millisecond))

		if loseLock && step == steps/2 {
			fmt.Println("simulating a lost lock: deleting", key)
			if err := rdb.Del(ctx, key).Err(); err != nil {
				return err
			}
		}
	}

	stop()
	if err := lock.Unlock(ctx); err != nil {
		return fmt.Errorf("release lock: %w", err)
	}
	fmt.Println("job finished, lock released")
	return nil
}
