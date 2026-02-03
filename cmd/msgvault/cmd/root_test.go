package cmd

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/cobra"
)

// newTestRootCmd creates a fresh root command for testing, avoiding mutation
// of the global rootCmd which could cause race conditions in parallel tests.
func newTestRootCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "msgvault",
		Short: "Offline email archive tool",
	}
}

// TestExecuteContext_CancellationPropagates verifies that context cancellation
// from ExecuteContext propagates to command handlers.
func TestExecuteContext_CancellationPropagates(t *testing.T) {
	// Track whether context was cancelled
	var contextWasCancelled atomic.Bool

	// Signal when the command handler has started waiting on ctx.Done()
	handlerStarted := make(chan struct{})

	// Create a fresh root command for this test
	testRoot := newTestRootCmd()

	// Create a test command that waits for context cancellation
	testCmd := &cobra.Command{
		Use:   "test-cancel",
		Short: "Test command for context cancellation",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			// Signal that we're now waiting for cancellation
			close(handlerStarted)
			select {
			case <-ctx.Done():
				contextWasCancelled.Store(true)
				return ctx.Err()
			case <-time.After(5 * time.Second):
				return nil
			}
		},
	}

	testRoot.AddCommand(testCmd)

	// Create a cancellable context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure cleanup even if test fails early

	// Start ExecuteContext in a goroutine
	done := make(chan error, 1)
	go func() {
		testRoot.SetArgs([]string{"test-cancel"})
		done <- testRoot.ExecuteContext(ctx)
	}()

	// Wait for handler to start (synchronization instead of sleep)
	select {
	case <-handlerStarted:
		// Handler is now waiting on ctx.Done()
	case <-time.After(2 * time.Second):
		t.Fatal("command handler did not start in time")
	}

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
	// Create a fresh root command for this test
	testRoot := newTestRootCmd()

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

	testRoot.AddCommand(testCmd)

	testRoot.SetArgs([]string{"test-execute"})
	err := testRoot.Execute()
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

// TestExecuteContext_DelegatesToRootCmd verifies ExecuteContext passes context to rootCmd.
func TestExecuteContext_DelegatesToRootCmd(t *testing.T) {
	// This test verifies the actual Execute/ExecuteContext functions work,
	// but uses a minimal approach to avoid side effects from the real rootCmd's
	// PersistentPreRunE (which loads config, etc.)

	// We test that ExecuteContext returns an error for unknown commands,
	// which proves it's actually executing through the command tree.
	ctx := context.Background()
	oldArgs := rootCmd.Args
	defer func() { rootCmd.SetArgs(nil) }()

	rootCmd.SetArgs([]string{"__nonexistent_command_for_test__"})
	err := ExecuteContext(ctx)
	if err == nil {
		t.Error("expected error for unknown command, got nil")
	}

	rootCmd.Args = oldArgs
}

// TestExecute_DelegatesToExecuteContext verifies Execute calls ExecuteContext.
func TestExecute_DelegatesToExecuteContext(t *testing.T) {
	defer func() { rootCmd.SetArgs(nil) }()

	rootCmd.SetArgs([]string{"__nonexistent_command_for_test__"})
	err := Execute()
	if err == nil {
		t.Error("expected error for unknown command, got nil")
	}
}
