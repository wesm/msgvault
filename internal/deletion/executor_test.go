package deletion

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wesm/msgvault/internal/gmail"
	"github.com/wesm/msgvault/internal/testutil"
)

// trackingProgress records progress events for testing
type trackingProgress struct {
	mu          sync.Mutex
	startTotal  int
	progressLog []struct{ processed, succeeded, failed int }
	completed   bool
	finalSucc   int
	finalFail   int
}

func (p *trackingProgress) OnStart(total int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.startTotal = total
}

func (p *trackingProgress) OnProgress(processed, succeeded, failed int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.progressLog = append(p.progressLog, struct{ processed, succeeded, failed int }{processed, succeeded, failed})
}

func (p *trackingProgress) OnComplete(succeeded, failed int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.completed = true
	p.finalSucc = succeeded
	p.finalFail = failed
}

// TestContext encapsulates common test dependencies for executor tests.
type TestContext struct {
	Mgr      *Manager
	MockAPI  *gmail.DeletionMockAPI
	Exec     *Executor
	Progress *trackingProgress
	Dir      string
	t        *testing.T
}

// NewTestContext creates a new test context with all dependencies initialized.
func NewTestContext(t *testing.T) *TestContext {
	t.Helper()
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	store := testutil.NewTestStore(t)
	mockAPI := gmail.NewDeletionMockAPI()
	progress := &trackingProgress{}

	exec := NewExecutor(mgr, store, mockAPI).WithProgress(progress)

	return &TestContext{
		Mgr:      mgr,
		MockAPI:  mockAPI,
		Exec:     exec,
		Progress: progress,
		Dir:      tmpDir,
		t:        t,
	}
}

// CreateManifest creates a manifest with the given name and Gmail IDs.
func (c *TestContext) CreateManifest(name string, ids []string) *Manifest {
	c.t.Helper()
	manifest, err := c.Mgr.CreateManifest(name, ids, Filters{})
	if err != nil {
		c.t.Fatalf("CreateManifest(%q) error = %v", name, err)
	}
	return manifest
}

// Execute runs the executor with default options.
func (c *TestContext) Execute(manifestID string) error {
	return c.Exec.Execute(context.Background(), manifestID, nil)
}

// ExecuteWithOpts runs the executor with custom options.
func (c *TestContext) ExecuteWithOpts(manifestID string, opts *ExecuteOptions) error {
	return c.Exec.Execute(context.Background(), manifestID, opts)
}

// ExecuteBatch runs the batch executor.
func (c *TestContext) ExecuteBatch(manifestID string) error {
	return c.Exec.ExecuteBatch(context.Background(), manifestID)
}

// AssertResult verifies the final success and failure counts.
func (c *TestContext) AssertResult(wantSucc, wantFail int) {
	c.t.Helper()
	if c.Progress.finalSucc != wantSucc {
		c.t.Errorf("finalSucc = %d, want %d", c.Progress.finalSucc, wantSucc)
	}
	if c.Progress.finalFail != wantFail {
		c.t.Errorf("finalFail = %d, want %d", c.Progress.finalFail, wantFail)
	}
}

// AssertCompleted verifies that OnComplete was called.
func (c *TestContext) AssertCompleted() {
	c.t.Helper()
	if !c.Progress.completed {
		c.t.Error("OnComplete was not called")
	}
}

// AssertNotCompleted verifies that OnComplete was not called.
func (c *TestContext) AssertNotCompleted() {
	c.t.Helper()
	if c.Progress.completed {
		c.t.Error("OnComplete was called unexpectedly")
	}
}

// AssertTrashCalls verifies the number of TrashMessage calls.
func (c *TestContext) AssertTrashCalls(want int) {
	c.t.Helper()
	if len(c.MockAPI.TrashCalls) != want {
		c.t.Errorf("TrashCalls = %d, want %d", len(c.MockAPI.TrashCalls), want)
	}
}

// AssertDeleteCalls verifies the number of DeleteMessage calls.
func (c *TestContext) AssertDeleteCalls(want int) {
	c.t.Helper()
	if len(c.MockAPI.DeleteCalls) != want {
		c.t.Errorf("DeleteCalls = %d, want %d", len(c.MockAPI.DeleteCalls), want)
	}
}

// AssertCompletedCount verifies the number of completed manifests.
func (c *TestContext) AssertCompletedCount(want int) {
	c.t.Helper()
	completed, err := c.Mgr.ListCompleted()
	if err != nil {
		c.t.Fatalf("ListCompleted() error = %v", err)
	}
	if len(completed) != want {
		c.t.Errorf("ListCompleted() = %d, want %d", len(completed), want)
	}
}

// AssertFailedCount verifies the number of failed manifests.
func (c *TestContext) AssertFailedCount(want int) {
	c.t.Helper()
	failed, err := c.Mgr.ListFailed()
	if err != nil {
		c.t.Fatalf("ListFailed() error = %v", err)
	}
	if len(failed) != want {
		c.t.Errorf("ListFailed() = %d, want %d", len(failed), want)
	}
}

// AssertManifestExecution verifies the persisted execution state of a manifest.
func (c *TestContext) AssertManifestExecution(id string, wantSucc, wantFail int, wantFailedIDs ...string) {
	c.t.Helper()
	m, _, err := c.Mgr.GetManifest(id)
	if err != nil {
		c.t.Fatalf("GetManifest(%q) failed: %v", id, err)
	}
	if m.Execution.Succeeded != wantSucc {
		c.t.Errorf("Persisted Succeeded = %d, want %d", m.Execution.Succeeded, wantSucc)
	}
	if m.Execution.Failed != wantFail {
		c.t.Errorf("Persisted Failed = %d, want %d", m.Execution.Failed, wantFail)
	}
	if len(m.Execution.FailedIDs) != len(wantFailedIDs) {
		c.t.Errorf("FailedIDs count = %d, want %d", len(m.Execution.FailedIDs), len(wantFailedIDs))
	} else {
		for i, id := range wantFailedIDs {
			if m.Execution.FailedIDs[i] != id {
				c.t.Errorf("FailedIDs[%d] = %q, want %q", i, m.Execution.FailedIDs[i], id)
			}
		}
	}
}

// SimulateTrashError injects a trash error for a specific message ID.
func (c *TestContext) SimulateTrashError(msgID string) {
	c.MockAPI.TrashErrors[msgID] = errors.New("simulated trash error")
}

// SimulateDeleteError injects a delete error for a specific message ID.
func (c *TestContext) SimulateDeleteError(msgID string) {
	c.MockAPI.DeleteErrors[msgID] = errors.New("simulated delete error")
}

// SimulateNotFound injects a 404 not-found error for a specific message ID.
func (c *TestContext) SimulateNotFound(msgID string) {
	c.MockAPI.SetNotFoundError(msgID)
}

// SimulateBatchDeleteError sets the batch delete operation to fail.
func (c *TestContext) SimulateBatchDeleteError() {
	c.MockAPI.BatchDeleteError = errors.New("simulated batch error")
}

// AssertBatchDeleteCalls verifies the number of BatchDeleteMessages calls.
func (c *TestContext) AssertBatchDeleteCalls(want int) {
	c.t.Helper()
	if len(c.MockAPI.BatchDeleteCalls) != want {
		c.t.Errorf("BatchDeleteCalls = %d, want %d", len(c.MockAPI.BatchDeleteCalls), want)
	}
}

// msgIDs generates sequential message IDs like "msg0", "msg1", ..., "msg(n-1)".
func msgIDs(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("msg%d", i)
	}
	return ids
}

// deleteOpts returns ExecuteOptions configured for permanent delete.
func deleteOpts(batchSize int) *ExecuteOptions {
	return &ExecuteOptions{
		Method:    MethodDelete,
		BatchSize: batchSize,
		Resume:    true,
	}
}

// trashOpts returns ExecuteOptions configured for trash with a custom batch size.
func trashOpts(batchSize int) *ExecuteOptions {
	return &ExecuteOptions{
		Method:    MethodTrash,
		BatchSize: batchSize,
		Resume:    true,
	}
}

// SimulateScopeError injects an insufficient scope error for a specific message ID.
func (c *TestContext) SimulateScopeError(msgID string) {
	scopeErr := fmt.Errorf("googleapi: Error 403: Insufficient Permission: ACCESS_TOKEN_SCOPE_INSUFFICIENT")
	c.MockAPI.TrashErrors[msgID] = scopeErr
	c.MockAPI.DeleteErrors[msgID] = scopeErr
}

// SimulateBatchScopeError sets the batch delete operation to fail with a scope error.
func (c *TestContext) SimulateBatchScopeError() {
	c.MockAPI.BatchDeleteError = fmt.Errorf("googleapi: Error 403: Insufficient Permission: ACCESS_TOKEN_SCOPE_INSUFFICIENT")
}

// AssertInProgressCount verifies the number of in-progress manifests.
func (c *TestContext) AssertInProgressCount(want int) {
	c.t.Helper()
	inProgress, err := c.Mgr.ListInProgress()
	if err != nil {
		c.t.Fatalf("ListInProgress() error = %v", err)
	}
	if len(inProgress) != want {
		c.t.Errorf("ListInProgress() = %d, want %d", len(inProgress), want)
	}
}

// AssertManifestLastProcessedIndex verifies the persisted LastProcessedIndex.
func (c *TestContext) AssertManifestLastProcessedIndex(id string, want int) {
	c.t.Helper()
	m, _, err := c.Mgr.GetManifest(id)
	if err != nil {
		c.t.Fatalf("GetManifest(%q) failed: %v", id, err)
	}
	if m.Execution == nil {
		c.t.Fatalf("manifest %q has nil Execution", id)
	}
	if m.Execution.LastProcessedIndex != want {
		c.t.Errorf("LastProcessedIndex = %d, want %d", m.Execution.LastProcessedIndex, want)
	}
}

func TestNullProgress(t *testing.T) {
	// NullProgress should not panic
	p := NullProgress{}
	p.OnStart(10)
	p.OnProgress(5, 4, 1)
	p.OnComplete(9, 1)
}

func TestDefaultExecuteOptions(t *testing.T) {
	opts := DefaultExecuteOptions()

	if opts.Method != MethodTrash {
		t.Errorf("Method = %q, want %q", opts.Method, MethodTrash)
	}
	if opts.BatchSize != 100 {
		t.Errorf("BatchSize = %d, want 100", opts.BatchSize)
	}
	if !opts.Resume {
		t.Error("Resume = false, want true")
	}
}

func TestNewExecutor(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	store := testutil.NewTestStore(t)
	mockAPI := gmail.NewDeletionMockAPI()

	exec := NewExecutor(mgr, store, mockAPI)
	if exec == nil {
		t.Fatal("NewExecutor returned nil")
	}
}

func TestExecutor_WithLogger(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	store := testutil.NewTestStore(t)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	exec := NewExecutor(mgr, store, gmail.NewDeletionMockAPI()).WithLogger(logger)

	if exec.logger != logger {
		t.Error("WithLogger did not set logger")
	}
}

func TestExecutor_WithProgress(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	store := testutil.NewTestStore(t)

	progress := &trackingProgress{}
	exec := NewExecutor(mgr, store, gmail.NewDeletionMockAPI()).WithProgress(progress)

	if exec.progress != progress {
		t.Error("WithProgress did not set progress")
	}
}

func TestExecutor_Execute_Success(t *testing.T) {
	ctx := NewTestContext(t)
	manifest := ctx.CreateManifest("test deletion", []string{"msg1", "msg2", "msg3"})

	if err := ctx.Execute(manifest.ID); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	ctx.AssertTrashCalls(3)
	ctx.AssertCompleted()
	ctx.AssertResult(3, 0)
	ctx.AssertCompletedCount(1)
}

func TestExecutor_Execute_WithDeleteMethod(t *testing.T) {
	ctx := NewTestContext(t)
	manifest := ctx.CreateManifest("permanent delete", []string{"msg1", "msg2"})

	if err := ctx.ExecuteWithOpts(manifest.ID, deleteOpts(100)); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	ctx.AssertDeleteCalls(2)
	ctx.AssertTrashCalls(0)
}

func TestExecutor_Execute_WithFailures(t *testing.T) {
	ctx := NewTestContext(t)
	ctx.SimulateTrashError("msg2")

	manifest := ctx.CreateManifest("partial failure", []string{"msg1", "msg2", "msg3"})

	if err := ctx.Execute(manifest.ID); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	ctx.AssertResult(2, 1)
	ctx.AssertCompletedCount(1)
	ctx.AssertManifestExecution(manifest.ID, 2, 1, "msg2")
}

func TestExecutor_Execute_AllFail(t *testing.T) {
	ctx := NewTestContext(t)
	ctx.SimulateTrashError("msg1")
	ctx.SimulateTrashError("msg2")

	manifest := ctx.CreateManifest("total failure", []string{"msg1", "msg2"})

	if err := ctx.Execute(manifest.ID); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	ctx.AssertFailedCount(1)
}

func TestExecutor_Execute_ContextCancelled(t *testing.T) {
	ctx := NewTestContext(t)

	manifest := ctx.CreateManifest("interrupt test", msgIDs(100))

	// Cancel context immediately
	execCtx, cancel := context.WithCancel(context.Background())
	cancel()

	err := ctx.Exec.Execute(execCtx, manifest.ID, nil)
	if err != context.Canceled {
		t.Errorf("Execute() error = %v, want context.Canceled", err)
	}

	ctx.AssertNotCompleted()

	// Manifest should remain in in_progress (for resume)
	inProgress, err := ctx.Mgr.ListInProgress()
	if err != nil {
		t.Fatalf("ListInProgress() error = %v", err)
	}
	if len(inProgress) != 1 {
		t.Errorf("ListInProgress() = %d, want 1", len(inProgress))
	}
}

func TestExecutor_Execute_SmallBatchSize(t *testing.T) {
	ctx := NewTestContext(t)
	manifest := ctx.CreateManifest("small batch test", []string{"msg1", "msg2", "msg3", "msg4", "msg5"})

	if err := ctx.ExecuteWithOpts(manifest.ID, trashOpts(2)); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	ctx.AssertTrashCalls(5)
}

func TestExecutor_Execute_ManifestNotFound(t *testing.T) {
	ctx := NewTestContext(t)

	err := ctx.Execute("nonexistent-id")
	if err == nil {
		t.Error("Execute() should error for nonexistent manifest")
	}
}

func TestExecutor_Execute_InvalidStatus(t *testing.T) {
	ctx := NewTestContext(t)
	manifest := ctx.CreateManifest("completed test", []string{"msg1"})

	// Execute to completion
	if err := ctx.Execute(manifest.ID); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	// Try to execute again
	err := ctx.Execute(manifest.ID)
	if err == nil {
		t.Error("Execute() should error for completed manifest")
	}
}

func TestExecutor_Execute_ResumeFromInProgress(t *testing.T) {
	tc := NewTestContext(t)

	// Create a manifest that's already in_progress with some progress
	gmailIDs := msgIDs(5)
	manifest := NewManifest("in-progress resume", gmailIDs)
	manifest.Status = StatusInProgress
	manifest.Execution = &Execution{
		StartedAt:          time.Now().Add(-time.Hour),
		Method:             MethodTrash,
		Succeeded:          2,
		Failed:             0,
		LastProcessedIndex: 2, // Already processed msg1 and msg2
	}
	if err := tc.Mgr.SaveManifest(manifest); err != nil {
		t.Fatalf("SaveManifest() error = %v", err)
	}

	if err := tc.ExecuteWithOpts(manifest.ID, trashOpts(100)); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	// Should only process msg3, msg4, msg5 (skipping msg1, msg2)
	tc.AssertTrashCalls(3)

	// Verify final counts include all 5
	tc.AssertManifestExecution(manifest.ID, 5, 0)
}

func TestExecutor_ExecuteBatch_Success(t *testing.T) {
	ctx := NewTestContext(t)
	manifest := ctx.CreateManifest("batch delete", []string{"msg1", "msg2", "msg3"})

	if err := ctx.ExecuteBatch(manifest.ID); err != nil {
		t.Fatalf("ExecuteBatch() error = %v", err)
	}

	ctx.AssertBatchDeleteCalls(1)
	if len(ctx.MockAPI.BatchDeleteCalls[0]) != 3 {
		t.Errorf("BatchDeleteCalls[0] length = %d, want 3", len(ctx.MockAPI.BatchDeleteCalls[0]))
	}
	ctx.AssertCompleted()
	ctx.AssertResult(3, 0)
	ctx.AssertCompletedCount(1)
}

func TestExecutor_ExecuteBatch_LargeBatch(t *testing.T) {
	ctx := NewTestContext(t)

	// Create manifest with >1000 messages (Gmail batch limit)
	manifest := ctx.CreateManifest("large batch", msgIDs(1500))

	if err := ctx.ExecuteBatch(manifest.ID); err != nil {
		t.Fatalf("ExecuteBatch() error = %v", err)
	}

	// Should be split into 2 batches (1000 + 500)
	ctx.AssertBatchDeleteCalls(2)
	if len(ctx.MockAPI.BatchDeleteCalls[0]) != 1000 {
		t.Errorf("BatchDeleteCalls[0] length = %d, want 1000", len(ctx.MockAPI.BatchDeleteCalls[0]))
	}
	if len(ctx.MockAPI.BatchDeleteCalls[1]) != 500 {
		t.Errorf("BatchDeleteCalls[1] length = %d, want 500", len(ctx.MockAPI.BatchDeleteCalls[1]))
	}
}

func TestExecutor_ExecuteBatch_WithBatchError(t *testing.T) {
	ctx := NewTestContext(t)
	ctx.SimulateBatchDeleteError()

	manifest := ctx.CreateManifest("batch fallback", []string{"msg1", "msg2", "msg3"})

	if err := ctx.ExecuteBatch(manifest.ID); err != nil {
		t.Fatalf("ExecuteBatch() error = %v", err)
	}

	// Should have attempted batch, then fallen back to individual
	ctx.AssertBatchDeleteCalls(1)
	ctx.AssertDeleteCalls(3)
}

func TestExecutor_ExecuteBatch_InvalidStatus(t *testing.T) {
	ctx := NewTestContext(t)
	manifest := ctx.CreateManifest("wrong status", []string{"msg1"})

	// Move to in_progress
	if err := ctx.Mgr.MoveManifest(manifest.ID, StatusPending, StatusInProgress); err != nil {
		t.Fatalf("MoveManifest() error = %v", err)
	}

	err := ctx.ExecuteBatch(manifest.ID)
	if err == nil {
		t.Error("ExecuteBatch() should error for non-pending manifest")
	}
}

func TestExecutor_ExecuteBatch_ContextCancelled(t *testing.T) {
	ctx := NewTestContext(t)

	manifest := ctx.CreateManifest("cancel batch", msgIDs(2500))

	// Cancel context immediately
	execCtx, cancel := context.WithCancel(context.Background())
	cancel()

	err := ctx.Exec.ExecuteBatch(execCtx, manifest.ID)
	if err != context.Canceled {
		t.Errorf("ExecuteBatch() error = %v, want context.Canceled", err)
	}

	ctx.AssertNotCompleted()
}

func TestExecutor_ExecuteBatch_ManifestNotFound(t *testing.T) {
	ctx := NewTestContext(t)

	err := ctx.ExecuteBatch("nonexistent-id")
	if err == nil {
		t.Error("ExecuteBatch() should error for nonexistent manifest")
	}
}

// TestExecutor_Execute_NotFoundTreatedAsSuccess verifies that 404 (already deleted)
// is treated as success, making deletion idempotent.
func TestExecutor_Execute_NotFoundTreatedAsSuccess(t *testing.T) {
	ctx := NewTestContext(t)
	ctx.SimulateNotFound("msg2")

	manifest := ctx.CreateManifest("idempotent test", []string{"msg1", "msg2", "msg3"})

	if err := ctx.Execute(manifest.ID); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	ctx.AssertResult(3, 0)
	ctx.AssertCompletedCount(1)
	ctx.AssertManifestExecution(manifest.ID, 3, 0)
}

// TestExecutor_ExecuteBatch_FallbackNotFoundTreatedAsSuccess verifies that
// when batch delete fails and falls back to individual deletes,
// 404 errors are still treated as success.
func TestExecutor_ExecuteBatch_FallbackNotFoundTreatedAsSuccess(t *testing.T) {
	ctx := NewTestContext(t)
	ctx.SimulateBatchDeleteError()
	ctx.SimulateNotFound("msg2")

	manifest := ctx.CreateManifest("batch fallback 404", []string{"msg1", "msg2", "msg3"})

	if err := ctx.ExecuteBatch(manifest.ID); err != nil {
		t.Fatalf("ExecuteBatch() error = %v", err)
	}

	ctx.AssertResult(3, 0)
}

// TestExecutor_ExecuteBatch_FallbackWithNon404Failures verifies that
// non-404 failures during fallback are properly counted as failures.
func TestExecutor_ExecuteBatch_FallbackWithNon404Failures(t *testing.T) {
	ctx := NewTestContext(t)
	ctx.SimulateBatchDeleteError()
	ctx.SimulateDeleteError("msg2")

	manifest := ctx.CreateManifest("batch fallback failures", []string{"msg1", "msg2", "msg3"})

	if err := ctx.ExecuteBatch(manifest.ID); err != nil {
		t.Fatalf("ExecuteBatch() error = %v", err)
	}

	ctx.AssertResult(2, 1)
}

// TestExecutor_Execute_WithDeleteMethod_404 tests 404 handling with permanent delete method.
func TestExecutor_Execute_WithDeleteMethod_404(t *testing.T) {
	ctx := NewTestContext(t)
	ctx.SimulateNotFound("msg2")

	manifest := ctx.CreateManifest("delete method 404", []string{"msg1", "msg2", "msg3"})

	if err := ctx.ExecuteWithOpts(manifest.ID, deleteOpts(100)); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	ctx.AssertResult(3, 0)
}

// TestExecutor_Execute_MixedErrors tests mixed success/404/error.
func TestExecutor_Execute_MixedErrors(t *testing.T) {
	ctx := NewTestContext(t)
	ctx.SimulateNotFound("msg2")
	ctx.SimulateTrashError("msg4")

	manifest := ctx.CreateManifest("mixed errors test", msgIDs(5))

	if err := ctx.Execute(manifest.ID); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	ctx.AssertResult(4, 1)
}

// TestExecutor_ExecuteBatch_FallbackMixed tests batch fallback with mixed results.
func TestExecutor_ExecuteBatch_FallbackMixed(t *testing.T) {
	ctx := NewTestContext(t)
	ctx.SimulateBatchDeleteError()
	ctx.SimulateNotFound("msg2")
	ctx.SimulateDeleteError("msg3")

	manifest := ctx.CreateManifest("batch fallback mixed", msgIDs(4))

	if err := ctx.ExecuteBatch(manifest.ID); err != nil {
		t.Fatalf("ExecuteBatch() error = %v", err)
	}

	ctx.AssertResult(3, 1)
	if len(ctx.MockAPI.BatchDeleteCalls) != 1 {
		t.Errorf("BatchDeleteCalls = %d, want 1", len(ctx.MockAPI.BatchDeleteCalls))
	}
	if len(ctx.MockAPI.DeleteCalls) != 4 {
		t.Errorf("DeleteCalls = %d, want 4 (fallback)", len(ctx.MockAPI.DeleteCalls))
	}
}

// TestExecutor_ExecuteBatch_RetriesFailedIDs verifies that resuming a batch
// execution retries previously failed message IDs.
func TestExecutor_ExecuteBatch_RetriesFailedIDs(t *testing.T) {
	tc := NewTestContext(t)

	// Create a manifest that's already in_progress with failed IDs
	gmailIDs := msgIDs(5)
	manifest := NewManifest("retry test", gmailIDs)
	manifest.Status = StatusInProgress
	manifest.Execution = &Execution{
		StartedAt:          time.Now().Add(-time.Hour),
		Method:             MethodDelete,
		Succeeded:          2,
		Failed:             3,
		FailedIDs:          []string{"msg2", "msg3", "msg4"},
		LastProcessedIndex: 5, // All processed, but 3 failed
	}
	if err := tc.Mgr.SaveManifest(manifest); err != nil {
		t.Fatalf("SaveManifest() error = %v", err)
	}

	if err := tc.ExecuteBatch(manifest.ID); err != nil {
		t.Fatalf("ExecuteBatch() error = %v", err)
	}

	// The 3 previously failed IDs should be retried via individual delete
	tc.AssertDeleteCalls(3)
	// All should succeed now (no errors injected)
	tc.AssertResult(5, 0)
	tc.AssertCompletedCount(1)
}

// TestExecutor_ExecuteBatch_RetryPartialSuccess verifies that retried IDs that
// still fail are tracked correctly.
func TestExecutor_ExecuteBatch_RetryPartialSuccess(t *testing.T) {
	tc := NewTestContext(t)
	tc.SimulateDeleteError("msg3") // msg3 still fails on retry

	gmailIDs := msgIDs(5)
	manifest := NewManifest("retry partial", gmailIDs)
	manifest.Status = StatusInProgress
	manifest.Execution = &Execution{
		StartedAt:          time.Now().Add(-time.Hour),
		Method:             MethodDelete,
		Succeeded:          2,
		Failed:             3,
		FailedIDs:          []string{"msg2", "msg3", "msg4"},
		LastProcessedIndex: 5,
	}
	if err := tc.Mgr.SaveManifest(manifest); err != nil {
		t.Fatalf("SaveManifest() error = %v", err)
	}

	if err := tc.ExecuteBatch(manifest.ID); err != nil {
		t.Fatalf("ExecuteBatch() error = %v", err)
	}

	// msg2, msg4 succeed on retry; msg3 still fails
	tc.AssertResult(4, 1)
	tc.AssertCompletedCount(1)
}

// TestNullProgress_AllMethods exercises all NullProgress methods for coverage.
func TestNullProgress_AllMethods(t *testing.T) {
	p := NullProgress{}
	// These are no-ops but we need to call them for coverage
	p.OnStart(100)
	p.OnProgress(50, 40, 10)
	p.OnComplete(90, 10)
	// If we get here without panic, the test passes
}

// TestExecutor_Execute_ScopeError verifies that scope errors propagate immediately
// and checkpoint state is saved.
func TestExecutor_Execute_ScopeError(t *testing.T) {
	ctx := NewTestContext(t)
	ctx.SimulateScopeError("msg1")

	manifest := ctx.CreateManifest("scope error test", []string{"msg0", "msg1", "msg2"})

	err := ctx.Execute(manifest.ID)
	if err == nil {
		t.Fatal("Execute() should return error for scope error")
	}
	if !strings.Contains(err.Error(), "ACCESS_TOKEN_SCOPE_INSUFFICIENT") {
		t.Errorf("error should contain scope message, got: %v", err)
	}

	ctx.AssertNotCompleted()
	ctx.AssertInProgressCount(1)
	// msg0 succeeded, msg1 hit scope error — checkpoint should be at index 1
	ctx.AssertManifestLastProcessedIndex(manifest.ID, 1)
	ctx.AssertManifestExecution(manifest.ID, 1, 0)
}

// TestExecutor_ExecuteBatch_ScopeError verifies that batch scope errors propagate
// immediately and checkpoint state is saved.
func TestExecutor_ExecuteBatch_ScopeError(t *testing.T) {
	ctx := NewTestContext(t)
	ctx.SimulateBatchScopeError()

	manifest := ctx.CreateManifest("batch scope error", []string{"msg0", "msg1", "msg2"})

	err := ctx.ExecuteBatch(manifest.ID)
	if err == nil {
		t.Fatal("ExecuteBatch() should return error for scope error")
	}
	if !strings.Contains(err.Error(), "ACCESS_TOKEN_SCOPE_INSUFFICIENT") {
		t.Errorf("error should contain scope message, got: %v", err)
	}

	ctx.AssertNotCompleted()
	ctx.AssertInProgressCount(1)
	ctx.AssertManifestLastProcessedIndex(manifest.ID, 0)
}

// TestExecutor_ExecuteBatch_FallbackScopeError verifies that scope errors during
// individual delete fallback propagate with correct per-item checkpoint.
func TestExecutor_ExecuteBatch_FallbackScopeError(t *testing.T) {
	ctx := NewTestContext(t)
	ctx.SimulateBatchDeleteError() // Force fallback to individual deletes
	ctx.SimulateScopeError("msg2") // Third message hits scope error

	manifest := ctx.CreateManifest("fallback scope error", []string{"msg0", "msg1", "msg2", "msg3"})

	err := ctx.ExecuteBatch(manifest.ID)
	if err == nil {
		t.Fatal("ExecuteBatch() should return error for scope error in fallback")
	}
	if !strings.Contains(err.Error(), "ACCESS_TOKEN_SCOPE_INSUFFICIENT") {
		t.Errorf("error should contain scope message, got: %v", err)
	}

	ctx.AssertNotCompleted()
	ctx.AssertInProgressCount(1)
	// msg0 and msg1 succeeded, msg2 hit scope error at index 2
	ctx.AssertManifestLastProcessedIndex(manifest.ID, 2)
	ctx.AssertManifestExecution(manifest.ID, 2, 0)
}
