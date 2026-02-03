package cmd

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/cobra"
)

// TestExecuteContext_CancellationPropagates verifies that context cancellation
// from ExecuteContext propagates to command handlers.
func TestExecuteContext_CancellationPropagates(t *testing.T) {
	// Track whether context was cancelled
	var contextWasCancelled atomic.Bool

	// Create a test command that waits for context cancellation
	testCmd := &cobra.Command{
		Use:   "test-cancel",
		Short: "Test command for context cancellation",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			select {
			case <-ctx.Done():
				contextWasCancelled.Store(true)
				return ctx.Err()
			case <-time.After(5 * time.Second):
				return nil
			}
		},
	}

	// Add the test command to root
	rootCmd.AddCommand(testCmd)
	defer func() {
		// Clean up: remove test command
		rootCmd.RemoveCommand(testCmd)
	}()

	// Create a cancellable context
	ctx, cancel := context.WithCancel(context.Background())

	// Start ExecuteContext in a goroutine
	done := make(chan error, 1)
	go func() {
		// Set args to run our test command
		rootCmd.SetArgs([]string{"test-cancel"})
		done <- ExecuteContext(ctx)
	}()

	// Give the command time to start
	time.Sleep(50 * time.Millisecond)

	// Cancel the context (simulates SIGINT/SIGTERM)
	cancel()

	// Wait for execution to complete
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Errorf("expected context.Canceled error, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ExecuteContext did not return after context cancellation")
	}

	// Verify the command observed the cancellation
	if !contextWasCancelled.Load() {
		t.Error("command did not observe context cancellation")
	}
}

// TestExecute_UsesBackgroundContext verifies Execute() works with background context.
func TestExecute_UsesBackgroundContext(t *testing.T) {
	// Create a simple command that completes immediately
	completed := make(chan struct{})
	testCmd := &cobra.Command{
		Use:   "test-execute",
		Short: "Test command for Execute",
		RunE: func(cmd *cobra.Command, args []string) error {
			close(completed)
			return nil
		},
	}

	rootCmd.AddCommand(testCmd)
	defer rootCmd.RemoveCommand(testCmd)

	rootCmd.SetArgs([]string{"test-execute"})
	err := Execute()
	if err != nil {
		t.Fatalf("Execute() returned error: %v", err)
	}

	select {
	case <-completed:
		// Success
	case <-time.After(time.Second):
		t.Fatal("command did not complete")
	}
}
