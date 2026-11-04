// Command fencing shows why fencing tokens matter.
//
// Client A takes the lock and then stalls, as it might during a long GC pause,
// until its lock expires. Client B takes the lock and writes. When A resumes
// and writes too, unaware that its lock is gone, the storage rejects the write
// because A's fencing token is older than B's.
//
// It needs a Redis server, at localhost:6379 unless REDIS_ADDR says otherwise:
//
//	go run ./examples/fencing
package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yohadrian21/go-redislock"
)

// storage stands in for a database or file store that remembers the highest
// fencing token it has accepted and refuses writes carrying an older one.
type storage struct {
	mu      sync.Mutex
	highest int64
	value   string
}

func (s *storage) Write(token int64, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if token < s.highest {
		return fmt.Errorf("write %q rejected: token %d is older than %d", value, token, s.highest)
	}
	s.highest, s.value = token, value
	return nil
}

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
	store := &storage{}
	const (
		key = "examples:fencing:ledger"
		ttl = 200 * time.Millisecond
	)

	a, err := locker.TryLock(ctx, key, ttl, redislock.WithFencing())
	if err != nil {
		return fmt.Errorf("client A: %w", err)
	}
	fmt.Printf("client A: acquired the lock with token %d, then stalls\n", a.FencingToken())
	time.Sleep(ttl + 100*time.Millisecond)

	b, err := locker.TryLock(ctx, key, 10*time.Second, redislock.WithFencing())
	if err != nil {
		return fmt.Errorf("client B: %w", err)
	}
	fmt.Printf("client B: acquired the expired lock with token %d\n", b.FencingToken())
	if err := store.Write(b.FencingToken(), "written by B"); err != nil {
		return fmt.Errorf("client B: %w", err)
	}
	fmt.Println("client B: write accepted")

	err = store.Write(a.FencingToken(), "written by A")
	if err == nil {
		return errors.New("client A: stale write was accepted")
	}
	fmt.Println("client A:", err)
	fmt.Printf("stored value: %q\n", store.value)

	if err := b.Unlock(ctx); err != nil {
		return fmt.Errorf("client B: %w", err)
	}
	return nil
}
