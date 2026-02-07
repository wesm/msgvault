package api

import (
	"net/http"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/time/rate"
)

// CORSConfig holds CORS configuration.
type CORSConfig struct {
	AllowedOrigins   []string
	AllowedMethods   []string
	AllowedHeaders   []string
	AllowCredentials bool
	MaxAge           int // Preflight cache duration in seconds
}

// DefaultCORSConfig returns a permissive CORS configuration for local development.
func DefaultCORSConfig() CORSConfig {
	return CORSConfig{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-API-Key"},
		AllowCredentials: false,
		MaxAge:           86400, // 24 hours
	}
}

// CORSMiddleware returns a middleware that handles CORS headers.
func CORSMiddleware(cfg CORSConfig) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")

			// Check if origin is allowed
			allowed := false
			for _, o := range cfg.AllowedOrigins {
				if o == "*" || o == origin {
					allowed = true
					break
				}
			}

			if allowed && origin != "" {
				w.Header().Set("Access-Control-Allow-Origin", origin)

				if cfg.AllowCredentials {
					w.Header().Set("Access-Control-Allow-Credentials", "true")
				}

				// Handle preflight
				if r.Method == "OPTIONS" {
					w.Header().Set("Access-Control-Allow-Methods", strings.Join(cfg.AllowedMethods, ", "))
					w.Header().Set("Access-Control-Allow-Headers", strings.Join(cfg.AllowedHeaders, ", "))
					if cfg.MaxAge > 0 {
						w.Header().Set("Access-Control-Max-Age", strconv.Itoa(cfg.MaxAge))
					}
					w.WriteHeader(http.StatusNoContent)
					return
				}
			}

			next.ServeHTTP(w, r)
		})
	}
}

// RateLimiter provides per-IP rate limiting.
type RateLimiter struct {
	mu       sync.Mutex
	limiters map[string]*rate.Limiter
	rate     rate.Limit
	burst    int
}

// NewRateLimiter creates a new rate limiter.
// rate is requests per second, burst is the maximum burst size.
func NewRateLimiter(rps float64, burst int) *RateLimiter {
	return &RateLimiter{
		limiters: make(map[string]*rate.Limiter),
		rate:     rate.Limit(rps),
		burst:    burst,
	}
}

// getLimiter returns the rate limiter for the given key (usually IP address).
func (rl *RateLimiter) getLimiter(key string) *rate.Limiter {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	limiter, exists := rl.limiters[key]
	if !exists {
		limiter = rate.NewLimiter(rl.rate, rl.burst)
		rl.limiters[key] = limiter
	}
	return limiter
}

// Allow checks if a request from the given key should be allowed.
func (rl *RateLimiter) Allow(key string) bool {
	return rl.getLimiter(key).Allow()
}

// RateLimitMiddleware returns a middleware that rate limits requests by IP.
func RateLimitMiddleware(limiter *RateLimiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Use X-Real-IP if set (from RealIP middleware), otherwise RemoteAddr
			ip := r.Header.Get("X-Real-IP")
			if ip == "" {
				ip = r.RemoteAddr
			}

			if !limiter.Allow(ip) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`{"error":"rate_limit_exceeded","message":"Too many requests. Please slow down."}`))
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
