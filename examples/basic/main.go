// Command basic shows the core API: Lock with a timeout, TryLock, TTL and
// Unlock.
//
// It needs a Redis server, at localhost:6379 unless REDIS_ADDR says otherwise:
//
//	go run ./examples/basic
package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yohadrian21/go-redislock"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	rdb := redis.NewClient(&redis.Options{Addr: cmp.Or(os.Getenv("REDIS_ADDR"), "localhost:6379")})
	defer rdb.Close()

	locker := redislock.New(rdb)
	ctx := context.Background()
	const key = "examples:basic:report"

	// Wait at most 5 seconds for the lock, then hold it for at most 30 seconds.
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	lock, err := locker.Lock(waitCtx, key, 30*time.Second)
	if err != nil {
		return fmt.Errorf("acquire lock: %w", err)
	}
	fmt.Println("worker 1: acquired", lock.Key())

	// A worker that would rather skip the job than wait uses TryLock.
	if _, err := locker.TryLock(ctx, key, 30*time.Second); errors.Is(err, redislock.ErrNotObtained) {
		fmt.Println("worker 2: lock is busy, skipping")
	} else if err != nil {
		return fmt.Errorf("try lock: %w", err)
	}

	ttl, err := lock.TTL(ctx)
	if err != nil {
		return fmt.Errorf("read TTL: %w", err)
	}
	fmt.Printf("worker 1: lock expires in %v unless released\n", ttl.Round(time.Second))

	if err := lock.Unlock(ctx); err != nil {
		return fmt.Errorf("release lock: %w", err)
	}
	fmt.Println("worker 1: released")

	// Releasing twice is harmless: Unlock never deletes a lock it does not hold.
	if err := lock.Unlock(ctx); errors.Is(err, redislock.ErrNotHeld) {
		fmt.Println("worker 1: second Unlock reports:", err)
	}
	return nil
}
