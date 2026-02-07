// Package api provides the HTTP API server for msgvault.
package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/wesm/msgvault/internal/config"
	"github.com/wesm/msgvault/internal/scheduler"
	"github.com/wesm/msgvault/internal/store"
)

// Server represents the HTTP API server.
type Server struct {
	cfg       *config.Config
	store     *store.Store
	scheduler *scheduler.Scheduler
	logger    *slog.Logger
	router    chi.Router
	server    *http.Server
}

// NewServer creates a new API server.
func NewServer(cfg *config.Config, store *store.Store, sched *scheduler.Scheduler, logger *slog.Logger) *Server {
	s := &Server{
		cfg:       cfg,
		store:     store,
		scheduler: sched,
		logger:    logger,
	}
	s.router = s.setupRouter()
	return s
}

// setupRouter configures the chi router with all routes and middleware.
func (s *Server) setupRouter() chi.Router {
	r := chi.NewRouter()

	// Standard middleware
	r.Use(chimw.RequestID)
	r.Use(s.loggerMiddleware)
	r.Use(chimw.Recoverer)
	r.Use(chimw.Timeout(60 * time.Second))

	// CORS middleware (disabled by default — no origins allowed)
	r.Use(CORSMiddleware(CORSConfig{}))

	// Rate limiting (10 req/sec with burst of 20)
	rateLimiter := NewRateLimiter(10, 20)
	r.Use(RateLimitMiddleware(rateLimiter))

	// Health check (no auth required)
	r.Get("/health", s.handleHealth)

	// API routes (auth required)
	r.Route("/api/v1", func(r chi.Router) {
		// Apply API key authentication
		r.Use(s.authMiddleware)

		// Stats
		r.Get("/stats", s.handleStats)

		// Messages
		r.Get("/messages", s.handleListMessages)
		r.Get("/messages/{id}", s.handleGetMessage)

		// Search
		r.Get("/search", s.handleSearch)

		// Accounts and sync
		r.Get("/accounts", s.handleListAccounts)
		r.Post("/sync/{account}", s.handleTriggerSync)

		// Scheduler status
		r.Get("/scheduler/status", s.handleSchedulerStatus)
	})

	return r
}

// Start begins listening for HTTP requests.
func (s *Server) Start() error {
	bindAddr := s.cfg.Server.BindAddr
	if bindAddr == "" {
		bindAddr = "127.0.0.1"
	}
	addr := fmt.Sprintf("%s:%d", bindAddr, s.cfg.Server.APIPort)

	if s.cfg.Server.APIKey == "" {
		s.logger.Warn("API server running without authentication — set [server] api_key in config.toml")
	}

	s.server = &http.Server{
		Addr:         addr,
		Handler:      s.router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	s.logger.Info("starting API server", "addr", addr)
	return s.server.ListenAndServe()
}

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("shutting down API server")
	return s.server.Shutdown(ctx)
}

// Router returns the chi router for testing.
func (s *Server) Router() chi.Router {
	return s.router
}

// loggerMiddleware logs HTTP requests.
func (s *Server) loggerMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := chimw.NewWrapResponseWriter(w, r.ProtoMajor)

		defer func() {
			s.logger.Info("http request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", ww.Status(),
				"bytes", ww.BytesWritten(),
				"duration", time.Since(start),
				"request_id", chimw.GetReqID(r.Context()),
			)
		}()

		next.ServeHTTP(ww, r)
	})
}

// authMiddleware validates the API key.
func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip auth if no API key configured
		if s.cfg.Server.APIKey == "" {
			next.ServeHTTP(w, r)
			return
		}

		// Check Authorization header
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			// Also check X-API-Key header
			authHeader = r.Header.Get("X-API-Key")
		}

		// Strip "Bearer " prefix if present
		if len(authHeader) > 7 && authHeader[:7] == "Bearer " {
			authHeader = authHeader[7:]
		}

		if authHeader != s.cfg.Server.APIKey {
			s.logger.Warn("unauthorized API request",
				"path", r.URL.Path,
				"remote_addr", r.RemoteAddr,
			)
			writeError(w, http.StatusUnauthorized, "unauthorized", "Invalid or missing API key")
			return
		}

		next.ServeHTTP(w, r)
	})
}

// handleHealth returns a simple health check response.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}
