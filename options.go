package redislock

import (
	"math/rand/v2"
	"time"
)

// Option configures how [Client.TryLock] and [Client.Lock] acquire a lock.
// Options that control retries only affect Lock.
type Option func(*options)

type options struct {
	fencing    bool
	backoff    Backoff
	jitter     float64
	maxRetries int
}

func newOptions(opts []Option) *options {
	o := &options{
		backoff:    ExponentialBackoff(10*time.Millisecond, 500*time.Millisecond),
		jitter:     0.5,
		maxRetries: -1,
	}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// delay returns how long Lock waits before the given retry, with jitter.
func (o *options) delay(retry int) time.Duration {
	d := o.backoff(retry)
	if spread := time.Duration(float64(d) * o.jitter); spread > 0 {
		d -= rand.N(spread)
	}
	return d
}

// WithFencing makes the lock carry a fencing token, available from
// [Lock.FencingToken].
//
// Tokens come from a counter stored at the lock key plus ":fence". It is
// incremented with INCR in the same atomic script that takes the lock and
// never expires, so tokens keep increasing across holders. In Redis Cluster
// both keys must hash to the same slot: wrap the lock name in a hash tag,
// such as "{orders:42}".
func WithFencing() Option {
	return func(o *options) {
		o.fencing = true
	}
}

// WithBackoff sets how long [Client.Lock] waits before each retry. The
// default is ExponentialBackoff(10*time.Millisecond, 500*time.Millisecond).
// A nil b keeps the default.
func WithBackoff(b Backoff) Option {
	return func(o *options) {
		if b != nil {
			o.backoff = b
		}
	}
}

// WithJitter sets the fraction of each backoff delay that is randomized, so
// clients competing for a lock spread out their retries instead of retrying
// in lockstep. A delay d becomes a random duration in (d - fraction*d, d].
//
// 0 disables jitter and 1 picks uniformly from (0, d], known as full jitter.
// The default is 0.5. Values outside [0, 1] are clamped.
func WithJitter(fraction float64) Option {
	return func(o *options) {
		switch {
		case fraction > 1:
			o.jitter = 1
		case fraction > 0:
			o.jitter = fraction
		default: // also catches NaN
			o.jitter = 0
		}
	}
}

// WithMaxRetries limits [Client.Lock] to n retries after its first attempt.
// With n == 0, Lock makes a single attempt like [Client.TryLock]. A negative
// n, the default, retries until the context is done.
func WithMaxRetries(n int) Option {
	return func(o *options) {
		o.maxRetries = n
	}
}

// Backoff returns how long to wait before the given retry. Retries are
// numbered from 1.
type Backoff func(retry int) time.Duration

// ConstantBackoff waits d before every retry.
func ConstantBackoff(d time.Duration) Backoff {
	return func(int) time.Duration {
		return d
	}
}

// ExponentialBackoff waits base before the first retry and doubles the wait
// for each following retry, up to limit.
func ExponentialBackoff(base, limit time.Duration) Backoff {
	return func(retry int) time.Duration {
		d := base
		for range retry - 1 {
			if d >= limit/2 {
				return limit
			}
			d *= 2
		}
		return min(d, limit)
	}
}
