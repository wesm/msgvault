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

// TestExecuteContext_PropagatesContext verifies ExecuteContext passes context to command handlers.
//
// NOTE: This test modifies the package-level rootCmd variable and must NOT use t.Parallel().
// Running this test in parallel with other tests that access rootCmd would cause data races.
func TestExecuteContext_PropagatesContext(t *testing.T) {
	// Save and restore global rootCmd to avoid state leakage between tests.
	// This pattern requires sequential test execution - do not add t.Parallel().
	savedRootCmd := rootCmd
	defer func() { rootCmd = savedRootCmd }()

	// Create a test root command
	testRoot := newTestRootCmd()

	// Track the context received by the command
	type ctxKey string
	var receivedCtx context.Context
	testCmd := &cobra.Command{
		Use:   "test-ctx",
		Short: "Test command for context verification",
		RunE: func(cmd *cobra.Command, args []string) error {
			receivedCtx = cmd.Context()
			return nil
		},
	}
	testRoot.AddCommand(testCmd)

	// Replace global rootCmd for this test
	rootCmd = testRoot

	// Create a context with a custom value
	testKey := ctxKey("test-key")
	testValue := "test-value"
	ctx := context.WithValue(context.Background(), testKey, testValue)

	testRoot.SetArgs([]string{"test-ctx"})
	err := ExecuteContext(ctx)
	if err != nil {
		t.Fatalf("ExecuteContext returned unexpected error: %v", err)
	}

	// Verify the context was propagated
	if receivedCtx == nil {
		t.Fatal("command did not receive context")
	}
	if got := receivedCtx.Value(testKey); got != testValue {
		t.Errorf("context value mismatch: got %v, want %v", got, testValue)
	}
}

// TestExecute_UsesBackgroundContextInHandler verifies Execute provides background context to handlers.
//
// NOTE: This test modifies the package-level rootCmd variable and must NOT use t.Parallel().
// Running this test in parallel with other tests that access rootCmd would cause data races.
func TestExecute_UsesBackgroundContextInHandler(t *testing.T) {
	// Save and restore global rootCmd to avoid state leakage between tests.
	// This pattern requires sequential test execution - do not add t.Parallel().
	savedRootCmd := rootCmd
	defer func() { rootCmd = savedRootCmd }()

	// Create a test root command
	testRoot := newTestRootCmd()

	// Track the context received by the command
	var receivedCtx context.Context
	testCmd := &cobra.Command{
		Use:   "test-bg-ctx",
		Short: "Test command for background context",
		RunE: func(cmd *cobra.Command, args []string) error {
			receivedCtx = cmd.Context()
			return nil
		},
	}
	testRoot.AddCommand(testCmd)

	// Replace global rootCmd for this test
	rootCmd = testRoot

	testRoot.SetArgs([]string{"test-bg-ctx"})
	err := Execute()
	if err != nil {
		t.Fatalf("Execute returned unexpected error: %v", err)
	}

	// Verify the command received a non-nil context (should be background context)
	if receivedCtx == nil {
		t.Fatal("command did not receive context")
	}

	// Background context should not have any deadline
	if deadline, ok := receivedCtx.Deadline(); ok {
		t.Errorf("expected no deadline from background context, got %v", deadline)
	}

	// Background context should not be cancelled
	select {
	case <-receivedCtx.Done():
		t.Error("background context should not be done")
	default:
		// Expected: context is not done
	}
}
