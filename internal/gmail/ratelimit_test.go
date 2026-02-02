package gmail

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"
)

// mockClock provides deterministic time control for tests.
type mockClock struct {
	mu      sync.Mutex
	current time.Time
	timers  []mockTimer
}

type mockTimer struct {
	deadline time.Time
	ch       chan time.Time
}

func newMockClock() *mockClock {
	return &mockClock{current: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *mockClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current
}

func (c *mockClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	deadline := c.current.Add(d)
	if !c.current.Before(deadline) {
		ch <- c.current
		return ch
	}
	c.timers = append(c.timers, mockTimer{deadline: deadline, ch: ch})
	return ch
}

// TimerCount returns the number of pending timers.
func (c *mockClock) TimerCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers)
}

// waitForTimers spins until the mock clock has at least n pending timers,
// avoiding wall-clock sleeps in mock-clock-based tests.
func waitForTimers(t *testing.T, clk *mockClock, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for clk.TimerCount() < n {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d timer(s); have %d", n, clk.TimerCount())
		}
		runtime.Gosched()
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
	return &RateLimiter{
		clock:          clk,
		tokens:         DefaultCapacity,
		capacity:       DefaultCapacity,
		refillRate:     DefaultRefillRate,
		baseRefillRate: DefaultRefillRate,
		lastRefill:     clk.Now(),
	}
}

// setTokens directly sets the token count for deterministic testing.
func setTokens(rl *RateLimiter, count float64) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.tokens = count
}

// getRefillRate safely reads the refill rate under the mutex.
func getRefillRate(rl *RateLimiter) float64 {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return rl.refillRate
}

// getThrottledUntil safely reads the throttledUntil field under the mutex.
func getThrottledUntil(rl *RateLimiter) time.Time {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return rl.throttledUntil
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

func TestRateLimiter_TryAcquire(t *testing.T) {
	clk := newMockClock()
	rl := newTestLimiterWithClock(clk)

	if !rl.TryAcquire(OpProfile) {
		t.Error("TryAcquire(OpProfile) should succeed when bucket is full")
	}

	// Drain via direct state manipulation
	setTokens(rl, 0)

	if rl.TryAcquire(OpMessagesBatchDelete) {
		t.Error("TryAcquire(OpMessagesBatchDelete) should fail when bucket is empty")
	}
}

func TestRateLimiter_Acquire_Success(t *testing.T) {
	clk := newMockClock()
	rl := newTestLimiterWithClock(clk)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := rl.Acquire(ctx, OpProfile)
	if err != nil {
		t.Errorf("Acquire() error = %v", err)
	}
}

func TestRateLimiter_Acquire_ContextCancelled(t *testing.T) {
	clk := newMockClock()
	rl := newTestLimiterWithClock(clk)
	setTokens(rl, 0)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := rl.Acquire(ctx, OpMessagesGet)
	if err != context.Canceled {
		t.Errorf("Acquire() with cancelled context = %v, want context.Canceled", err)
	}
}

func TestRateLimiter_Acquire_ContextTimeout(t *testing.T) {
	clk := newMockClock()
	rl := newTestLimiterWithClock(clk)
	setTokens(rl, 0)
	// Set a very slow refill so tokens won't accumulate
	rl.mu.Lock()
	rl.refillRate = 0.001
	rl.mu.Unlock()

	// Use context.WithCancel and cancel via the mock clock to avoid
	// mixing real-time deadlines with mock clock advancement.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Schedule cancellation when the mock clock advances past 50ms
	go func() {
		<-clk.After(50 * time.Millisecond)
		cancel()
	}()

	done := make(chan error, 1)
	go func() {
		done <- rl.Acquire(ctx, OpMessagesBatchDelete)
	}()

	// Wait for both goroutines to register their mock clock timers
	// (cancel goroutine's clk.After + Acquire's internal clk.After)
	waitForTimers(t, clk, 2)

	// Advance mock clock past the cancel point
	clk.Advance(100 * time.Millisecond)

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
	clk := newMockClock()
	rl := newTestLimiterWithClock(clk)
	setTokens(rl, 0)

	initial := rl.Available()
	if initial != 0 {
		t.Fatalf("expected 0 tokens, got %v", initial)
	}

	// Advance clock by 1 second: should refill 250 tokens
	clk.Advance(1 * time.Second)

	after := rl.Available()
	if after != DefaultCapacity {
		t.Errorf("Available() after 1s = %v, want %v", after, float64(DefaultCapacity))
	}
}

func TestRateLimiter_Available(t *testing.T) {
	clk := newMockClock()
	rl := newTestLimiterWithClock(clk)

	initial := rl.Available()
	if initial != DefaultCapacity {
		t.Errorf("Available() = %v, want %v", initial, float64(DefaultCapacity))
	}

	rl.TryAcquire(OpMessagesGet) // cost 5

	after := rl.Available()
	expected := float64(DefaultCapacity - 5)
	if after != expected {
		t.Errorf("Available() after acquire = %v, want %v", after, expected)
	}
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
	clk := newMockClock()
	rl := newTestLimiterWithClock(clk)

	// Advance time significantly — tokens should still be capped
	clk.Advance(10 * time.Second)

	avail := rl.Available()
	if avail > float64(DefaultCapacity) {
		t.Errorf("Available() = %v, should not exceed capacity %v", avail, DefaultCapacity)
	}
}

func TestRateLimiter_Throttle(t *testing.T) {
	t.Run("DrainsTokensAndBlocksRefill", func(t *testing.T) {
		clk := newMockClock()
		rl := newTestLimiterWithClock(clk)

		rl.Throttle(100 * time.Millisecond)

		if got := rl.Available(); got != 0 {
			t.Errorf("Available() after Throttle = %v, want 0", got)
		}

		// Advance 50ms (still within throttle) — tokens should remain 0
		clk.Advance(50 * time.Millisecond)
		if got := rl.Available(); got != 0 {
			t.Errorf("Available() during throttle = %v, want 0", got)
		}

		// Advance past throttle expiry
		clk.Advance(60 * time.Millisecond)
		if got := rl.Available(); got <= 0 {
			t.Errorf("Available() after throttle expiry = %v, expected > 0", got)
		}
	})

	t.Run("RecoverRate", func(t *testing.T) {
		clk := newMockClock()
		rl := newTestLimiterWithClock(clk)

		rl.Throttle(10 * time.Millisecond)

		if got := getRefillRate(rl); got != DefaultRefillRate*0.5 {
			t.Errorf("refillRate after Throttle = %v, want %v", got, DefaultRefillRate*0.5)
		}

		rl.RecoverRate()

		if got := getRefillRate(rl); got != DefaultRefillRate {
			t.Errorf("refillRate after RecoverRate = %v, want %v", got, DefaultRefillRate)
		}
	})

	t.Run("DoesNotShortenBackoff", func(t *testing.T) {
		clk := newMockClock()
		rl := newTestLimiterWithClock(clk)

		rl.Throttle(200 * time.Millisecond)
		first := getThrottledUntil(rl)

		rl.Throttle(50 * time.Millisecond)
		second := getThrottledUntil(rl)

		if second.Before(first) {
			t.Errorf("Throttle shortened existing backoff: first=%v, second=%v", first, second)
		}
	})

	t.Run("ExtendsBackoff", func(t *testing.T) {
		clk := newMockClock()
		rl := newTestLimiterWithClock(clk)

		rl.Throttle(50 * time.Millisecond)
		first := getThrottledUntil(rl)

		clk.Advance(30 * time.Millisecond)
		rl.Throttle(50 * time.Millisecond)
		second := getThrottledUntil(rl)

		if !second.After(first) {
			t.Errorf("Throttle did not extend backoff: first=%v, second=%v", first, second)
		}
	})

	t.Run("AutoRecoverRate", func(t *testing.T) {
		clk := newMockClock()
		rl := newTestLimiterWithClock(clk)

		rl.Throttle(50 * time.Millisecond)

		if got := getRefillRate(rl); got != DefaultRefillRate*0.5 {
			t.Errorf("refillRate after Throttle = %v, want %v", got, DefaultRefillRate*0.5)
		}

		clk.Advance(100 * time.Millisecond)
		rl.Available() // triggers refill and auto-recovery

		if got := getRefillRate(rl); got != DefaultRefillRate {
			t.Errorf("refillRate after throttle expiry = %v, want %v", got, DefaultRefillRate)
		}
	})
}

func TestRateLimiter_Acquire_WaitsForThrottle(t *testing.T) {
	clk := newMockClock()
	rl := newTestLimiterWithClock(clk)

	rl.Throttle(100 * time.Millisecond)

	ctx := context.Background()
	done := make(chan error, 1)
	go func() {
		done <- rl.Acquire(ctx, OpProfile)
	}()

	// Advance past throttle — Acquire should complete
	clk.Advance(150 * time.Millisecond)

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Acquire() error = %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Acquire() did not complete after advancing clock past throttle")
	}
}

func TestRateLimiter_NilClock(t *testing.T) {
	// A zero-value RateLimiter (nil clock) should not panic on any public method.
	rl := &RateLimiter{
		tokens:         DefaultCapacity,
		capacity:       DefaultCapacity,
		refillRate:     DefaultRefillRate,
		baseRefillRate: DefaultRefillRate,
	}

	// Available should work and return a sane value.
	if avail := rl.Available(); avail <= 0 {
		t.Errorf("Available() with nil clock = %v, want > 0", avail)
	}

	// TryAcquire should succeed when tokens are available.
	if !rl.TryAcquire(OpProfile) {
		t.Error("TryAcquire(OpProfile) with nil clock should succeed")
	}

	// Acquire should succeed immediately when tokens are available.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rl.Acquire(ctx, OpProfile); err != nil {
		t.Errorf("Acquire(OpProfile) with nil clock error = %v", err)
	}

	// Throttle should not panic.
	rl.Throttle(10 * time.Millisecond)
}
