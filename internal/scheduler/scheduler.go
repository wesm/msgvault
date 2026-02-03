// Package scheduler provides cron-based scheduling for automated email sync.
package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/wesm/msgvault/internal/config"
)

// SyncFunc is the callback invoked when a scheduled sync should run.
// It receives the account email and should perform incremental sync + cache build.
type SyncFunc func(ctx context.Context, email string) error

// Scheduler manages cron-based email sync scheduling.
type Scheduler struct {
	cron     *cron.Cron
	syncFunc SyncFunc
	logger   *slog.Logger

	mu        sync.RWMutex
	jobs      map[string]cron.EntryID // email -> cron entry ID
	schedules map[string]string       // email -> cron expression
	running   map[string]bool         // email -> currently syncing
	lastRun   map[string]time.Time    // email -> last successful run
	lastErr   map[string]error        // email -> last error
}

// New creates a new Scheduler with the given sync callback.
func New(syncFunc SyncFunc) *Scheduler {
	return &Scheduler{
		cron: cron.New(cron.WithParser(cron.NewParser(
			cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow,
		))),
		syncFunc:  syncFunc,
		logger:    slog.Default(),
		jobs:      make(map[string]cron.EntryID),
		schedules: make(map[string]string),
		running:   make(map[string]bool),
		lastRun:   make(map[string]time.Time),
		lastErr:   make(map[string]error),
	}
}

// WithLogger sets the logger for the scheduler.
func (s *Scheduler) WithLogger(logger *slog.Logger) *Scheduler {
	s.logger = logger
	return s
}

// AddAccount schedules sync for an account using the given cron expression.
// Returns an error if the cron expression is invalid.
func (s *Scheduler) AddAccount(email, cronExpr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Remove existing schedule if present
	if entryID, exists := s.jobs[email]; exists {
		s.cron.Remove(entryID)
		delete(s.jobs, email)
		delete(s.schedules, email)
	}

	// Validate and add the cron job
	entryID, err := s.cron.AddFunc(cronExpr, func() {
		s.runSync(email)
	})
	if err != nil {
		return fmt.Errorf("invalid cron expression %q: %w", cronExpr, err)
	}

	s.jobs[email] = entryID
	s.schedules[email] = cronExpr
	s.logger.Info("scheduled sync",
		"email", email,
		"schedule", cronExpr,
		"next_run", s.cron.Entry(entryID).Next)

	return nil
}

// AddAccountsFromConfig adds all enabled accounts from the config.
// Returns the number of accounts scheduled and any errors encountered.
func (s *Scheduler) AddAccountsFromConfig(cfg *config.Config) (int, []error) {
	var errors []error
	scheduled := 0

	for _, acc := range cfg.ScheduledAccounts() {
		if err := s.AddAccount(acc.Email, acc.Schedule); err != nil {
			errors = append(errors, fmt.Errorf("%s: %w", acc.Email, err))
		} else {
			scheduled++
		}
	}

	return scheduled, errors
}

// RemoveAccount removes the schedule for an account.
func (s *Scheduler) RemoveAccount(email string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if entryID, exists := s.jobs[email]; exists {
		s.cron.Remove(entryID)
		delete(s.jobs, email)
		delete(s.schedules, email)
		s.logger.Info("removed schedule", "email", email)
	}
}

// Start begins executing scheduled jobs.
func (s *Scheduler) Start() {
	s.cron.Start()
	s.logger.Info("scheduler started", "jobs", len(s.jobs))
}

// Stop gracefully stops the scheduler and waits for running jobs to complete.
func (s *Scheduler) Stop() context.Context {
	s.logger.Info("scheduler stopping")
	return s.cron.Stop()
}

// runSync executes sync for an account (called by cron).
func (s *Scheduler) runSync(email string) {
	// Prevent concurrent syncs for the same account
	s.mu.Lock()
	if s.running[email] {
		s.mu.Unlock()
		s.logger.Warn("skipping sync, already running", "email", email)
		return
	}
	s.running[email] = true
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.running[email] = false
		s.mu.Unlock()
	}()

	s.logger.Info("starting scheduled sync", "email", email)
	start := time.Now()

	ctx := context.Background()
	err := s.syncFunc(ctx, email)

	s.mu.Lock()
	if err != nil {
		s.lastErr[email] = err
		s.logger.Error("scheduled sync failed",
			"email", email,
			"duration", time.Since(start),
			"error", err)
	} else {
		s.lastRun[email] = time.Now()
		s.lastErr[email] = nil
		s.logger.Info("scheduled sync completed",
			"email", email,
			"duration", time.Since(start))
	}
	s.mu.Unlock()
}

// TriggerSync manually triggers a sync for an account (outside of schedule).
// Returns an error if a sync is already running for this account.
func (s *Scheduler) TriggerSync(email string) error {
	s.mu.RLock()
	if s.running[email] {
		s.mu.RUnlock()
		return fmt.Errorf("sync already running for %s", email)
	}
	s.mu.RUnlock()

	go s.runSync(email)
	return nil
}

// Status returns the current status of all scheduled accounts.
func (s *Scheduler) Status() []AccountStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var statuses []AccountStatus
	for email, entryID := range s.jobs {
		entry := s.cron.Entry(entryID)
		status := AccountStatus{
			Email:    email,
			Running:  s.running[email],
			LastRun:  s.lastRun[email],
			NextRun:  entry.Next,
			Schedule: s.schedules[email],
		}
		if err := s.lastErr[email]; err != nil {
			status.LastError = err.Error()
		}
		statuses = append(statuses, status)
	}
	return statuses
}

// AccountStatus represents the sync status of a scheduled account.
type AccountStatus struct {
	Email     string    `json:"email"`
	Running   bool      `json:"running"`
	LastRun   time.Time `json:"last_run,omitempty"`
	NextRun   time.Time `json:"next_run"`
	Schedule  string    `json:"schedule"`
	LastError string    `json:"last_error,omitempty"`
}

// ValidateCronExpr validates a cron expression without scheduling anything.
func ValidateCronExpr(expr string) error {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	_, err := parser.Parse(expr)
	if err != nil {
		return fmt.Errorf("invalid cron expression: %w", err)
	}
	return nil
}
