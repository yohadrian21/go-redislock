package redislock

import (
	"math"
	"testing"
	"time"
)

func TestExponentialBackoff(t *testing.T) {
	backoff := ExponentialBackoff(10*time.Millisecond, 100*time.Millisecond)

	tests := []struct {
		retry int
		want  time.Duration
	}{
		{1, 10 * time.Millisecond},
		{2, 20 * time.Millisecond},
		{3, 40 * time.Millisecond},
		{4, 80 * time.Millisecond},
		{5, 100 * time.Millisecond},
		{6, 100 * time.Millisecond},
		{math.MaxInt, 100 * time.Millisecond},
	}
	for _, tt := range tests {
		if got := backoff(tt.retry); got != tt.want {
			t.Errorf("backoff(%d) = %v, want %v", tt.retry, got, tt.want)
		}
	}
}

func TestExponentialBackoffDoesNotOverflow(t *testing.T) {
	backoff := ExponentialBackoff(time.Nanosecond, math.MaxInt64)

	if got := backoff(200); got != math.MaxInt64 {
		t.Errorf("backoff(200) = %v, want %v", got, time.Duration(math.MaxInt64))
	}
}

func TestConstantBackoff(t *testing.T) {
	backoff := ConstantBackoff(42 * time.Millisecond)

	for _, retry := range []int{1, 2, 1000} {
		if got := backoff(retry); got != 42*time.Millisecond {
			t.Errorf("backoff(%d) = %v, want 42ms", retry, got)
		}
	}
}

func TestDelayAppliesJitter(t *testing.T) {
	const d = 100 * time.Millisecond

	tests := []struct {
		jitter float64
		lowest time.Duration // delays must be in (lowest, d]
	}{
		{0.5, d / 2},
		{1, 0},
	}
	for _, tt := range tests {
		o := newOptions([]Option{WithBackoff(ConstantBackoff(d)), WithJitter(tt.jitter)})
		seen := make(map[time.Duration]bool)
		for range 1000 {
			got := o.delay(1)
			if got <= tt.lowest || got > d {
				t.Fatalf("jitter %v: delay %v outside (%v, %v]", tt.jitter, got, tt.lowest, d)
			}
			seen[got] = true
		}
		if len(seen) < 100 {
			t.Errorf("jitter %v: only %d distinct delays in 1000 samples", tt.jitter, len(seen))
		}
	}
}

func TestDelayWithoutJitter(t *testing.T) {
	o := newOptions([]Option{WithBackoff(ConstantBackoff(time.Second)), WithJitter(0)})

	for range 100 {
		if got := o.delay(1); got != time.Second {
			t.Fatalf("delay = %v, want exactly 1s", got)
		}
	}
}

func TestWithJitterClamps(t *testing.T) {
	tests := []struct {
		in, want float64
	}{
		{-1, 0},
		{0, 0},
		{0.3, 0.3},
		{2, 1},
		{math.NaN(), 0},
	}
	for _, tt := range tests {
		if got := newOptions([]Option{WithJitter(tt.in)}).jitter; got != tt.want {
			t.Errorf("WithJitter(%v) set %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestWithBackoffIgnoresNil(t *testing.T) {
	if newOptions([]Option{WithBackoff(nil)}).backoff == nil {
		t.Error("WithBackoff(nil) removed the default backoff")
	}
}
