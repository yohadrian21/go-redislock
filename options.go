package redislock

// Option configures how [Client.TryLock] and [Client.Lock] acquire a lock.
type Option func(*options)

type options struct {
	fencing bool
}

func newOptions(opts []Option) *options {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}
	return o
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
