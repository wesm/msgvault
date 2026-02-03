package gmail

import (
	"context"
	"sync"
	"time"
)

// Clock abstracts time operations for testability.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

// Operation represents a Gmail API operation with its quota cost.
type Operation int

const (
	OpMessagesGet         Operation = iota // 5 units
	OpMessagesGetRaw                       // 5 units
	OpMessagesList                         // 5 units
	OpLabelsList                           // 1 unit
	OpHistoryList                          // 2 units
	OpMessagesTrash                        // 5 units
	OpMessagesDelete                       // 10 units
	OpMessagesBatchDelete                  // 50 units
	OpProfile                              // 1 unit
)

// operationCosts maps operations to their quota costs.
var operationCosts = map[Operation]int{
	OpMessagesGet:         5,
	OpMessagesGetRaw:      5,
	OpMessagesList:        5,
	OpLabelsList:          1,
	OpHistoryList:         2,
	OpMessagesTrash:       5,
	OpMessagesDelete:      10,
	OpMessagesBatchDelete: 50,
	OpProfile:             1,
}

// Cost returns the quota cost for an operation.
func (o Operation) Cost() int {
	if cost, ok := operationCosts[o]; ok {
		return cost
	}
	return 1
}

// DefaultCapacity is the default token bucket capacity (Gmail's per-user quota).
const DefaultCapacity = 250

// DefaultRefillRate is tokens per second at the default rate.
const DefaultRefillRate = 250.0

// realClock implements Clock using the standard time package.
type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// RateLimiter implements a token bucket rate limiter for Gmail API calls.
// It is safe for concurrent use and supports adaptive throttling.
type RateLimiter struct {
	mu             sync.Mutex
	clock          Clock
	tokens         float64
	capacity       float64
	refillRate     float64 // tokens per second
	baseRefillRate float64 // original refill rate for recovery
	lastRefill     time.Time
	throttledUntil time.Time // when throttled, don't refill until this time
}

// MinQPS is the minimum allowed QPS to prevent division by zero.
const MinQPS = 0.1

// NewRateLimiter creates a rate limiter with the specified QPS.
// A qps of 5 is the default safe rate for Gmail API.
// QPS is clamped to a minimum of MinQPS (0.1) to prevent division by zero.
func NewRateLimiter(qps float64) *RateLimiter {
	// Clamp QPS to valid range to prevent division by zero
	if qps < MinQPS {
		qps = MinQPS
	}

	// Scale refill rate based on QPS setting
	// Default is 5 QPS which maps to 250 tokens/sec
	scaleFactor := qps / 5.0
	if scaleFactor > 1.0 {
		scaleFactor = 1.0 // Don't exceed default rate
	}

	refillRate := DefaultRefillRate * scaleFactor
	return &RateLimiter{
		clock:          realClock{},
		tokens:         DefaultCapacity,
		capacity:       DefaultCapacity,
		refillRate:     refillRate,
		baseRefillRate: refillRate,
		lastRefill:     time.Now(),
	}
}

// ensureClock defaults clock to realClock{} if nil. Must be called with lock held.
func (r *RateLimiter) ensureClock() {
	if r.clock == nil {
		r.clock = realClock{}
	}
}

// Acquire blocks until the required tokens are available.
// Returns an error if the context is cancelled.
func (r *RateLimiter) Acquire(ctx context.Context, op Operation) error {
	cost := float64(op.Cost())

	for {
		r.mu.Lock()
		r.ensureClock()
		now := r.clock.Now()

		// If we're in a throttle period, wait until it expires
		if now.Before(r.throttledUntil) {
			waitTime := r.throttledUntil.Sub(now)
			r.mu.Unlock()

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-r.clock.After(waitTime):
				continue // Throttle expired, retry
			}
		}

		r.refill()

		if r.tokens >= cost {
			r.tokens -= cost
			r.mu.Unlock()
			return nil
		}

		// Calculate wait time based on token deficit
		deficit := cost - r.tokens
		waitTime := time.Duration(deficit/r.refillRate*1000) * time.Millisecond
		if waitTime < 10*time.Millisecond {
			waitTime = 10 * time.Millisecond
		}
		r.mu.Unlock()

		// Wait with context cancellation support
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.clock.After(waitTime):
			// Continue to retry
		}
	}
}

// TryAcquire attempts to acquire tokens without blocking.
// Returns true if successful, false if insufficient tokens.
func (r *RateLimiter) TryAcquire(op Operation) bool {
	cost := float64(op.Cost())

	r.mu.Lock()
	defer r.mu.Unlock()

	r.ensureClock()
	r.refill()

	if r.tokens >= cost {
		r.tokens -= cost
		return true
	}
	return false
}

// refill adds tokens based on elapsed time. Must be called with lock held.
func (r *RateLimiter) refill() {
	now := r.clock.Now()

	// If we're in a throttle period, don't refill yet
	if now.Before(r.throttledUntil) {
		r.lastRefill = now
		return
	}

	// If throttle just expired, restore the base refill rate
	if r.refillRate < r.baseRefillRate && !r.throttledUntil.IsZero() {
		r.refillRate = r.baseRefillRate
	}

	elapsed := now.Sub(r.lastRefill).Seconds()
	r.lastRefill = now

	r.tokens += elapsed * r.refillRate
	if r.tokens > r.capacity {
		r.tokens = r.capacity
	}
}

// Available returns the current number of available tokens.
func (r *RateLimiter) Available() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureClock()
	r.refill()
	return r.tokens
}

// Throttle temporarily reduces the rate when we hit API rate limits.
// This provides adaptive back-pressure when Gmail returns 429/403 quota errors.
func (r *RateLimiter) Throttle(duration time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.ensureClock()
	now := r.clock.Now()
	newThrottleEnd := now.Add(duration)

	// Don't shorten an existing throttle window (e.g., 429 shouldn't shorten a 403 backoff)
	if newThrottleEnd.After(r.throttledUntil) {
		r.throttledUntil = newThrottleEnd
	}

	// Reset lastRefill to throttle end to prevent crediting elapsed time after throttle expires
	r.lastRefill = r.throttledUntil

	// Drain existing tokens to force waiting
	r.tokens = 0
	// Reduce refill rate to 50% for gradual recovery
	r.refillRate = r.baseRefillRate * 0.5
}

// RecoverRate restores the original refill rate after throttling.
func (r *RateLimiter) RecoverRate() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.refillRate = r.baseRefillRate
}
