package redislock

import "github.com/redis/go-redis/v9"

// Each script returns false (redis.Nil in Go) when the lock is not available
// to the caller, so callers can map that single case to a sentinel error.
var (
	// obtainScript takes the lock if it is free. When a second key is given,
	// it also increments that fencing counter in the same atomic step, so a
	// newer holder always receives a larger token.
	//
	// KEYS[1]: lock key, KEYS[2]: optional fencing counter key.
	// ARGV[1]: token, ARGV[2]: TTL in milliseconds.
	obtainScript = redis.NewScript(`
if not redis.call("SET", KEYS[1], ARGV[1], "NX", "PX", ARGV[2]) then
	return false
end
if KEYS[2] then
	return redis.call("INCR", KEYS[2])
end
return 0
`)

	// refreshScript resets the TTL if the caller still holds the lock.
	//
	// KEYS[1]: lock key. ARGV[1]: token, ARGV[2]: TTL in milliseconds.
	refreshScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return false
`)

	// releaseScript deletes the lock if the caller still holds it.
	//
	// KEYS[1]: lock key. ARGV[1]: token.
	releaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return false
`)

	// ttlScript returns the remaining TTL in milliseconds if the caller still
	// holds the lock.
	//
	// KEYS[1]: lock key. ARGV[1]: token.
	ttlScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("PTTL", KEYS[1])
end
return false
`)
)
