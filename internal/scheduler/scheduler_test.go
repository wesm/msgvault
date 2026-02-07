package scheduler

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wesm/msgvault/internal/config"
)

func TestNew(t *testing.T) {
	s := New(func(ctx context.Context, email string) error {
		return nil
	})

	if s == nil {
		t.Fatal("New() returned nil")
	}
	if s.cron == nil {
		t.Error("cron is nil")
	}
	if s.jobs == nil {
		t.Error("jobs map is nil")
	}
}

func TestAddAccount(t *testing.T) {
	s := New(func(ctx context.Context, email string) error {
		return nil
	})

	// Valid cron expression
	if err := s.AddAccount("test@gmail.com", "0 2 * * *"); err != nil {
		t.Errorf("AddAccount() with valid cron = %v, want nil", err)
	}

	// Check job was added
	s.mu.RLock()
	_, exists := s.jobs["test@gmail.com"]
	s.mu.RUnlock()

	if !exists {
		t.Error("job was not added to jobs map")
	}
}

func TestAddAccountInvalidCron(t *testing.T) {
	s := New(func(ctx context.Context, email string) error {
		return nil
	})

	err := s.AddAccount("test@gmail.com", "invalid cron")
	if err == nil {
		t.Error("AddAccount() with invalid cron = nil, want error")
	}
}

func TestAddAccountReplacesExisting(t *testing.T) {
	s := New(func(ctx context.Context, email string) error {
		return nil
	})

	// Add initial schedule
	if err := s.AddAccount("test@gmail.com", "0 2 * * *"); err != nil {
		t.Fatalf("AddAccount() = %v", err)
	}

	s.mu.RLock()
	firstID := s.jobs["test@gmail.com"]
	s.mu.RUnlock()

	// Replace with new schedule
	if err := s.AddAccount("test@gmail.com", "0 3 * * *"); err != nil {
		t.Fatalf("AddAccount() replacement = %v", err)
	}

	s.mu.RLock()
	secondID := s.jobs["test@gmail.com"]
	s.mu.RUnlock()

	if firstID == secondID {
		t.Error("job ID was not updated after replacement")
	}
}

func TestRemoveAccount(t *testing.T) {
	s := New(func(ctx context.Context, email string) error {
		return nil
	})

	if err := s.AddAccount("test@gmail.com", "0 2 * * *"); err != nil {
		t.Fatalf("AddAccount: %v", err)
	}
	s.RemoveAccount("test@gmail.com")

	s.mu.RLock()
	_, exists := s.jobs["test@gmail.com"]
	s.mu.RUnlock()

	if exists {
		t.Error("job still exists after RemoveAccount()")
	}
}

func TestRemoveAccountNonExistent(t *testing.T) {
	s := New(func(ctx context.Context, email string) error {
		return nil
	})

	// Should not panic
	s.RemoveAccount("nonexistent@gmail.com")
}

func TestAddAccountsFromConfig(t *testing.T) {
	s := New(func(ctx context.Context, email string) error {
		return nil
	})

	cfg := &config.Config{
		Accounts: []config.AccountSchedule{
			{Email: "user1@gmail.com", Schedule: "0 1 * * *", Enabled: true},
			{Email: "user2@gmail.com", Schedule: "0 2 * * *", Enabled: true},
			{Email: "disabled@gmail.com", Schedule: "0 3 * * *", Enabled: false},
			{Email: "noschedule@gmail.com", Schedule: "", Enabled: true},
		},
	}

	scheduled, errs := s.AddAccountsFromConfig(cfg)

	if len(errs) != 0 {
		t.Errorf("AddAccountsFromConfig() errors = %v", errs)
	}
	if scheduled != 2 {
		t.Errorf("AddAccountsFromConfig() scheduled = %d, want 2", scheduled)
	}

	// Check only enabled accounts with schedules were added
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, ok := s.jobs["user1@gmail.com"]; !ok {
		t.Error("user1@gmail.com should be scheduled")
	}
	if _, ok := s.jobs["user2@gmail.com"]; !ok {
		t.Error("user2@gmail.com should be scheduled")
	}
	if _, ok := s.jobs["disabled@gmail.com"]; ok {
		t.Error("disabled@gmail.com should not be scheduled")
	}
	if _, ok := s.jobs["noschedule@gmail.com"]; ok {
		t.Error("noschedule@gmail.com should not be scheduled")
	}
}

func TestAddAccountsFromConfigWithErrors(t *testing.T) {
	s := New(func(ctx context.Context, email string) error {
		return nil
	})

	cfg := &config.Config{
		Accounts: []config.AccountSchedule{
			{Email: "valid@gmail.com", Schedule: "0 1 * * *", Enabled: true},
			{Email: "invalid@gmail.com", Schedule: "not a cron", Enabled: true},
		},
	}

	scheduled, errs := s.AddAccountsFromConfig(cfg)

	if scheduled != 1 {
		t.Errorf("scheduled = %d, want 1", scheduled)
	}
	if len(errs) != 1 {
		t.Errorf("len(errs) = %d, want 1", len(errs))
	}
}

func TestStartStop(t *testing.T) {
	s := New(func(ctx context.Context, email string) error {
		return nil
	})

	s.Start()
	ctx := s.Stop()

	// Wait for stop
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Error("Stop() did not complete in time")
	}
}

func TestIsRunning(t *testing.T) {
	s := New(func(ctx context.Context, email string) error {
		return nil
	})

	// Not running before Start
	if s.IsRunning() {
		t.Error("IsRunning() = true before Start()")
	}

	s.Start()

	// Running after Start
	if !s.IsRunning() {
		t.Error("IsRunning() = false after Start()")
	}

	ctx := s.Stop()

	// Not running after Stop
	if s.IsRunning() {
		t.Error("IsRunning() = true after Stop()")
	}

	// Wait for stop
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Error("Stop() did not complete in time")
	}
}

func TestStopCancelsRunningSync(t *testing.T) {
	syncStarted := make(chan struct{})
	s := New(func(ctx context.Context, email string) error {
		close(syncStarted)
		<-ctx.Done()
		return ctx.Err()
	})

	if err := s.AddAccount("test@gmail.com", "0 0 1 1 *"); err != nil {
		t.Fatalf("AddAccount: %v", err)
	}

	// Trigger sync
	if err := s.TriggerSync("test@gmail.com"); err != nil {
		t.Fatalf("TriggerSync: %v", err)
	}

	// Wait for sync to start
	select {
	case <-syncStarted:
	case <-time.After(time.Second):
		t.Fatal("sync did not start")
	}

	// Stop should cancel the running sync
	ctx := s.Stop()

	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Error("Stop() did not complete after cancelling sync")
	}

	// Verify the error was recorded
	statuses := s.Status()
	for _, status := range statuses {
		if status.Email == "test@gmail.com" {
			if status.LastError == "" {
				t.Error("expected error after cancelled sync")
			}
			return
		}
	}
}

func TestTriggerSync(t *testing.T) {
	var called atomic.Int32
	s := New(func(ctx context.Context, email string) error {
		called.Add(1)
		time.Sleep(50 * time.Millisecond)
		return nil
	})

	if err := s.AddAccount("test@gmail.com", "0 0 1 1 *"); err != nil {
		t.Fatalf("AddAccount: %v", err)
	}

	// Trigger manually
	err := s.TriggerSync("test@gmail.com")
	if err != nil {
		t.Errorf("TriggerSync() = %v", err)
	}

	// Wait for sync to start
	time.Sleep(10 * time.Millisecond)

	// Second trigger should fail (already running)
	err = s.TriggerSync("test@gmail.com")
	if err == nil {
		t.Error("TriggerSync() while running = nil, want error")
	}

	// Wait for completion
	time.Sleep(100 * time.Millisecond)

	if called.Load() != 1 {
		t.Errorf("syncFunc called %d times, want 1", called.Load())
	}
}

func TestSyncPreventsDoubleRun(t *testing.T) {
	var concurrent atomic.Int32
	var maxConcurrent atomic.Int32

	s := New(func(ctx context.Context, email string) error {
		c := concurrent.Add(1)
		if c > maxConcurrent.Load() {
			maxConcurrent.Store(c)
		}
		time.Sleep(50 * time.Millisecond)
		concurrent.Add(-1)
		return nil
	})

	if err := s.AddAccount("test@gmail.com", "0 0 1 1 *"); err != nil {
		t.Fatalf("AddAccount: %v", err)
	}

	// Try to trigger multiple times concurrently
	for i := 0; i < 5; i++ {
		_ = s.TriggerSync("test@gmail.com")
	}

	time.Sleep(200 * time.Millisecond)

	if maxConcurrent.Load() > 1 {
		t.Errorf("max concurrent = %d, want 1", maxConcurrent.Load())
	}
}

func TestStatus(t *testing.T) {
	s := New(func(ctx context.Context, email string) error {
		return nil
	})

	if err := s.AddAccount("test@gmail.com", "0 2 * * *"); err != nil {
		t.Fatalf("AddAccount: %v", err)
	}
	if err := s.AddAccount("other@gmail.com", "0 3 * * *"); err != nil {
		t.Fatalf("AddAccount: %v", err)
	}
	s.Start()
	defer s.Stop()

	statuses := s.Status()

	if len(statuses) != 2 {
		t.Errorf("len(Status()) = %d, want 2", len(statuses))
	}

	// Find test@gmail.com status
	var found bool
	for _, status := range statuses {
		if status.Email == "test@gmail.com" {
			found = true
			if status.Running {
				t.Error("status.Running = true, want false")
			}
			if status.NextRun.IsZero() {
				t.Error("status.NextRun is zero")
			}
			break
		}
	}
	if !found {
		t.Error("test@gmail.com not found in status")
	}
}

func TestStatusAfterSyncSuccess(t *testing.T) {
	s := New(func(ctx context.Context, email string) error {
		return nil
	})

	if err := s.AddAccount("test@gmail.com", "0 0 1 1 *"); err != nil {
		t.Fatalf("AddAccount: %v", err)
	}
	if err := s.TriggerSync("test@gmail.com"); err != nil {
		t.Fatalf("TriggerSync: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	statuses := s.Status()
	for _, status := range statuses {
		if status.Email == "test@gmail.com" {
			if status.LastRun.IsZero() {
				t.Error("LastRun should be set after successful sync")
			}
			if status.LastError != "" {
				t.Errorf("LastError = %q, want empty", status.LastError)
			}
			return
		}
	}
	t.Error("test@gmail.com not found in status")
}

func TestStatusAfterSyncError(t *testing.T) {
	s := New(func(ctx context.Context, email string) error {
		return errors.New("sync failed")
	})

	if err := s.AddAccount("test@gmail.com", "0 0 1 1 *"); err != nil {
		t.Fatalf("AddAccount: %v", err)
	}
	if err := s.TriggerSync("test@gmail.com"); err != nil {
		t.Fatalf("TriggerSync: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	statuses := s.Status()
	for _, status := range statuses {
		if status.Email == "test@gmail.com" {
			if status.LastError == "" {
				t.Error("LastError should be set after failed sync")
			}
			return
		}
	}
	t.Error("test@gmail.com not found in status")
}

func TestTriggerSyncAfterStop(t *testing.T) {
	s := New(func(ctx context.Context, email string) error {
		return nil
	})

	if err := s.AddAccount("test@gmail.com", "0 0 1 1 *"); err != nil {
		t.Fatalf("AddAccount: %v", err)
	}

	ctx := s.Stop()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("Stop() did not complete in time")
	}

	err := s.TriggerSync("test@gmail.com")
	if err == nil {
		t.Error("TriggerSync() after Stop() = nil, want error")
	}
}

func TestValidateCronExpr(t *testing.T) {
	tests := []struct {
		expr    string
		wantErr bool
	}{
		{"0 2 * * *", false},    // 2am daily
		{"*/15 * * * *", false}, // Every 15 minutes
		{"0 0 1 * *", false},    // Monthly on 1st
		{"0 0 * * 0", false},    // Weekly on Sunday
		{"invalid", true},
		{"* * * * * *", true}, // Too many fields
		{"", true},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			err := ValidateCronExpr(tt.expr)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateCronExpr(%q) error = %v, wantErr = %v", tt.expr, err, tt.wantErr)
			}
		})
	}
}
