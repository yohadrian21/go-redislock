package redislock_test

import (
	"log"
	"os"
	"testing"

	"github.com/alicebob/miniredis/v2"
)

// redisAddr is the Redis server the examples use: $REDIS_ADDR when set, as in
// CI, or else an in-memory miniredis, so that plain `go test` needs no setup.
var redisAddr = os.Getenv("REDIS_ADDR")

func TestMain(m *testing.M) {
	if redisAddr == "" {
		mr, err := miniredis.Run()
		if err != nil {
			log.Fatal(err)
		}
		defer mr.Close()
		redisAddr = mr.Addr()
	}
	m.Run()
}
