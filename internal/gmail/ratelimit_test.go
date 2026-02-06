package gmail

import (
	"context"
	"sync"
	"testing"
	"time"
)

// mockClock provides deterministic time control for tests.
type mockClock struct {
	mu          sync.Mutex
	current     time.Time
	timers      []mockTimer
	timerNotify chan struct{}
	notifyOnce  sync.Once
}

type mockTimer struct {
	deadline time.Time
	ch       chan time.Time
}

func newMockClock() *mockClock {
	return &mockClock{
		current:     time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		timerNotify: make(chan struct{}, 1),
	}
}

// ensureNotifyChannel lazily initializes timerNotify to prevent blocking on a
// nil channel if mockClock{} is instantiated directly without newMockClock().
func (c *mockClock) ensureNotifyChannel() {
	c.notifyOnce.Do(func() {
		if c.timerNotify == nil {
			c.timerNotify = make(chan struct{}, 1)
		}
	})
}

func (c *mockClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current
}

func (c *mockClock) After(d time.Duration) <-chan time.Time {
	c.ensureNotifyChannel()
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	deadline := c.current.Add(d)
	if !c.current.Before(deadline) {
		ch <- c.current
		return ch
	}
	c.timers = append(c.timers, mockTimer{deadline: deadline, ch: ch})
	// Notify waiters that a new timer was registered.
	select {
	case c.timerNotify <- struct{}{}:
	default:
	}
	return ch
}

// TimerCount returns the number of pending timers.
func (c *mockClock) TimerCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers)
}

// waitForTimers blocks until the mock clock has at least n pending timers.
func waitForTimers(t *testing.T, clk *mockClock, n int) {
	t.Helper()
	clk.ensureNotifyChannel()
	timeout := time.After(2 * time.Second)
	for clk.TimerCount() < n {
		select {
		case <-clk.timerNotify:
		case <-timeout:
			t.Fatalf("timed out waiting for %d timer(s); have %d", n, clk.TimerCount())
		}
	}
}

// Advance moves the clock forward and fires any pending timers.
func (c *mockClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.current = c.current.Add(d)
	now := c.current
	var remaining []mockTimer
	for _, t := range c.timers {
		if !now.Before(t.deadline) {
			t.ch <- now
		} else {
			remaining = append(remaining, t)
		}
	}
	c.timers = remaining
	c.mu.Unlock()
}

// newTestLimiterWithClock creates a rate limiter using the given mock clock.
func newTestLimiterWithClock(clk *mockClock) *RateLimiter {
	return newRateLimiter(clk, defaultQPS)
}

// rlFixture encapsulates the mock clock and rate limiter for test setup.
type rlFixture struct {
	clk *mockClock
	rl  *RateLimiter
}

func newRLFixture() *rlFixture {
	clk := newMockClock()
	return &rlFixture{
		clk: clk,
		rl:  newTestLimiterWithClock(clk),
	}
}

// drain sets tokens to zero.
func (f *rlFixture) drain() {
	f.rl.mu.Lock()
	defer f.rl.mu.Unlock()
	f.rl.tokens = 0
}

// state returns a snapshot of the limiter's internal fields under the mutex.
func (f *rlFixture) state() (tokens, refillRate float64, throttledUntil time.Time) {
	f.rl.mu.Lock()
	defer f.rl.mu.Unlock()
	return f.rl.tokens, f.rl.refillRate, f.rl.throttledUntil
}

// assertAvailable checks the current available tokens.
func (f *rlFixture) assertAvailable(t *testing.T, expected float64) {
	t.Helper()
	if got := f.rl.Available(); got != expected {
		t.Errorf("Available() = %v, want %v", got, expected)
	}
}

// acquireAsync runs Acquire in a background goroutine and returns a channel
// that receives the result. It waits for the goroutine to either register a
// timer on the mock clock or complete immediately.
func (f *rlFixture) acquireAsync(t *testing.T, ctx context.Context, op Operation) <-chan error {
	t.Helper()
	f.clk.ensureNotifyChannel()
	timersBefore := f.clk.TimerCount()
	ch := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		ch <- f.rl.Acquire(ctx, op)
		close(done)
	}()
	// Wait until either a new timer appears or the goroutine completes.
	timeout := time.After(2 * time.Second)
	for {
		select {
		case <-f.clk.timerNotify:
			if f.clk.TimerCount() > timersBefore {
				return ch
			}
		case <-done:
			return ch
		case <-timeout:
			t.Fatal("acquireAsync: timed out waiting for timer or completion")
			return ch
		}
	}
}

func TestOperationCost(t *testing.T) {
	tests := []struct {
		op   Operation
		cost int
	}{
		{OpMessagesGet, 5},
		{OpMessagesGetRaw, 5},
		{OpMessagesList, 5},
		{OpLabelsList, 1},
		{OpHistoryList, 2},
		{OpMessagesTrash, 5},
		{OpMessagesDelete, 10},
		{OpMessagesBatchDelete, 50},
		{OpProfile, 1},
		{Operation(999), 1}, // Unknown operation defaults to 1
	}

	for _, tc := range tests {
		got := tc.op.Cost()
		if got != tc.cost {
			t.Errorf("Operation(%d).Cost() = %d, want %d", tc.op, got, tc.cost)
		}
	}
}

func TestNewRateLimiter(t *testing.T) {
	rl := NewRateLimiter(5.0)

	if rl.capacity != DefaultCapacity {
		t.Errorf("capacity = %v, want %v", rl.capacity, DefaultCapacity)
	}

	if rl.tokens != DefaultCapacity {
		t.Errorf("initial tokens = %v, want %v", rl.tokens, DefaultCapacity)
	}

	if rl.refillRate != DefaultRefillRate {
		t.Errorf("refillRate = %v, want %v", rl.refillRate, DefaultRefillRate)
	}
}

func TestNewRateLimiter_ScaledQPS(t *testing.T) {
	rl := NewRateLimiter(2.5)
	expectedRate := DefaultRefillRate * 0.5
	if rl.refillRate != expectedRate {
		t.Errorf("refillRate at 2.5 QPS = %v, want %v", rl.refillRate, expectedRate)
	}

	rl = NewRateLimiter(10.0)
	if rl.refillRate != DefaultRefillRate {
		t.Errorf("refillRate at 10 QPS = %v, want %v (capped)", rl.refillRate, DefaultRefillRate)
	}
}

func TestNewRateLimiter_NilClockPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("newRateLimiter(nil, ...) should panic")
		}
	}()
	newRateLimiter(nil, 5.0)
}

func TestRateLimiter_TryAcquire(t *testing.T) {
	f := newRLFixture()

	if !f.rl.TryAcquire(OpProfile) {
		t.Error("TryAcquire(OpProfile) should succeed when bucket is full")
	}

	f.drain()

	if f.rl.TryAcquire(OpMessagesBatchDelete) {
		t.Error("TryAcquire(OpMessagesBatchDelete) should fail when bucket is empty")
	}
}

func TestRateLimiter_Acquire_Success(t *testing.T) {
	f := newRLFixture()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := f.rl.Acquire(ctx, OpProfile)
	if err != nil {
		t.Errorf("Acquire() error = %v", err)
	}
}

func TestRateLimiter_Acquire_ContextCancelled(t *testing.T) {
	f := newRLFixture()
	f.drain()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := f.rl.Acquire(ctx, OpMessagesGet)
	if err != context.Canceled {
		t.Errorf("Acquire() with cancelled context = %v, want context.Canceled", err)
	}
}

func TestRateLimiter_Acquire_ContextTimeout(t *testing.T) {
	f := newRLFixture()
	f.drain()
	// Set a very slow refill so tokens won't accumulate
	f.rl.mu.Lock()
	f.rl.refillRate = 0.001
	f.rl.mu.Unlock()

	// Use context.WithCancel and cancel via the mock clock to avoid
	// mixing real-time deadlines with mock clock advancement.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Schedule cancellation when the mock clock advances past 50ms
	go func() {
		<-f.clk.After(50 * time.Millisecond)
		cancel()
	}()
	waitForTimers(t, f.clk, 1)

	done := f.acquireAsync(t, ctx, OpMessagesBatchDelete)

	// Advance mock clock past the cancel point
	f.clk.Advance(100 * time.Millisecond)

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Errorf("Acquire() = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Acquire() did not return after context cancelled")
	}
}

func TestRateLimiter_Refill(t *testing.T) {
	f := newRLFixture()
	f.drain()

	f.assertAvailable(t, 0)

	// Advance clock by 1 second: should refill 250 tokens
	f.clk.Advance(1 * time.Second)

	f.assertAvailable(t, DefaultCapacity)
}

func TestRateLimiter_Available(t *testing.T) {
	f := newRLFixture()

	f.assertAvailable(t, DefaultCapacity)

	f.rl.TryAcquire(OpMessagesGet) // cost 5

	f.assertAvailable(t, float64(DefaultCapacity-5))
}

func TestRateLimiter_Concurrent(t *testing.T) {
	// Use real clock for concurrency test since goroutine scheduling is inherent
	rl := NewRateLimiter(5.0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	errors := make(chan error, 100)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := rl.Acquire(ctx, OpProfile); err != nil {
				errors <- err
			}
		}()
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("concurrent Acquire() error = %v", err)
	}
}

func TestRateLimiter_CapacityLimit(t *testing.T) {
	f := newRLFixture()

	// Advance time significantly — tokens should still be capped
	f.clk.Advance(10 * time.Second)

	avail := f.rl.Available()
	if avail > float64(DefaultCapacity) {
		t.Errorf("Available() = %v, should not exceed capacity %v", avail, DefaultCapacity)
	}
}

func TestRateLimiter_Throttle(t *testing.T) {
	t.Run("DrainsTokensAndBlocksRefill", func(t *testing.T) {
		f := newRLFixture()

		f.rl.Throttle(100 * time.Millisecond)

		f.assertAvailable(t, 0)

		// Advance 50ms (still within throttle) — tokens should remain 0
		f.clk.Advance(50 * time.Millisecond)
		f.assertAvailable(t, 0)

		// Advance past throttle expiry
		f.clk.Advance(60 * time.Millisecond)
		if got := f.rl.Available(); got <= 0 {
			t.Errorf("Available() after throttle expiry = %v, expected > 0", got)
		}
	})

	t.Run("RecoverRate", func(t *testing.T) {
		f := newRLFixture()

		f.rl.Throttle(10 * time.Millisecond)

		_, rate, _ := f.state()
		if rate != DefaultRefillRate*0.5 {
			t.Errorf("refillRate after Throttle = %v, want %v", rate, DefaultRefillRate*0.5)
		}

		f.rl.RecoverRate()

		_, rate, _ = f.state()
		if rate != DefaultRefillRate {
			t.Errorf("refillRate after RecoverRate = %v, want %v", rate, DefaultRefillRate)
		}
	})

	t.Run("DoesNotShortenBackoff", func(t *testing.T) {
		f := newRLFixture()

		f.rl.Throttle(200 * time.Millisecond)
		_, _, first := f.state()

		f.rl.Throttle(50 * time.Millisecond)
		_, _, second := f.state()

		if second.Before(first) {
			t.Errorf("Throttle shortened existing backoff: first=%v, second=%v", first, second)
		}
	})

	t.Run("ExtendsBackoff", func(t *testing.T) {
		f := newRLFixture()

		f.rl.Throttle(50 * time.Millisecond)
		_, _, first := f.state()

		f.clk.Advance(30 * time.Millisecond)
		f.rl.Throttle(50 * time.Millisecond)
		_, _, second := f.state()

		if !second.After(first) {
			t.Errorf("Throttle did not extend backoff: first=%v, second=%v", first, second)
		}
	})

	t.Run("AutoRecoverRate", func(t *testing.T) {
		f := newRLFixture()

		f.rl.Throttle(50 * time.Millisecond)

		_, rate, _ := f.state()
		if rate != DefaultRefillRate*0.5 {
			t.Errorf("refillRate after Throttle = %v, want %v", rate, DefaultRefillRate*0.5)
		}

		f.clk.Advance(100 * time.Millisecond)
		f.rl.Available() // triggers refill and auto-recovery

		_, rate, _ = f.state()
		if rate != DefaultRefillRate {
			t.Errorf("refillRate after throttle expiry = %v, want %v", rate, DefaultRefillRate)
		}
	})
}

func TestRateLimiter_Acquire_WaitsForThrottle(t *testing.T) {
	f := newRLFixture()

	f.rl.Throttle(100 * time.Millisecond)

	done := f.acquireAsync(t, context.Background(), OpProfile)

	// Advance past throttle — Acquire should complete
	f.clk.Advance(150 * time.Millisecond)

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Acquire() error = %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Acquire() did not complete after advancing clock past throttle")
	}
}

func TestMockClock_ZeroValueSafe(t *testing.T) {
	// Verify that a zero-value mockClock{} won't block forever due to nil channel.
	clk := &mockClock{}

	// After should work without hanging
	ch := clk.After(10 * time.Millisecond)
	if ch == nil {
		t.Fatal("After() returned nil channel")
	}

	// timerNotify should be lazily initialized
	if clk.timerNotify == nil {
		t.Fatal("timerNotify should be initialized after After() call")
	}
}
