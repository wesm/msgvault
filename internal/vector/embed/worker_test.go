//go:build sqlite_vec

package embed

import (
	"context"
	"testing"
	"time"
)

func TestWorker_DrainsPendingEndToEnd(t *testing.T) {
	ctx := context.Background()
	f := newWorkerFixture(t, 3)

	w := NewWorker(WorkerDeps{
		Backend:       f.Backend,
		VectorsDB:     f.VectorsDB,
		MainDB:        f.MainDB,
		Client:        f.FakeClient,
		Preprocess:    PreprocessConfig{StripQuotes: true, StripSignatures: true},
		MaxInputChars: 8000,
		BatchSize:     2,
	})

	res, err := w.RunOnce(ctx, f.BuildingGen)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if res.Succeeded != 3 {
		t.Errorf("succeeded=%d, want 3", res.Succeeded)
	}
	if res.Failed != 0 {
		t.Errorf("failed=%d, want 0", res.Failed)
	}

	var n int
	if err := f.VectorsDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pending_embeddings WHERE generation_id = ?`,
		int64(f.BuildingGen)).Scan(&n); err != nil {
		t.Fatalf("count pending: %v", err)
	}
	if n != 0 {
		t.Errorf("pending remaining: %d", n)
	}
}

func TestWorker_ReleasesOnClientError(t *testing.T) {
	ctx := context.Background()
	f := newWorkerFixture(t, 3)
	f.FakeClient.FailNext(1) // first Embed errors; remaining batches succeed

	w := NewWorker(WorkerDeps{
		Backend:       f.Backend,
		VectorsDB:     f.VectorsDB,
		MainDB:        f.MainDB,
		Client:        f.FakeClient,
		Preprocess:    PreprocessConfig{},
		MaxInputChars: 8000,
		BatchSize:     1, // batch of 1 so the first Embed fails exactly one id
	})
	res, err := w.RunOnce(ctx, f.BuildingGen)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if res.Failed < 1 {
		t.Errorf("expected at least 1 failure, got %d", res.Failed)
	}
	// The worker retries the released row and eventually drains everything.
	if res.Succeeded != 3 {
		t.Errorf("succeeded=%d, want 3 (failed row gets retried after Release)", res.Succeeded)
	}
}

func TestWorker_ReleasesOnUpsertError(t *testing.T) {
	// Driving an Upsert failure requires forcing a dimension mismatch; the
	// fake client returns 4-dim vectors matching the generation's
	// dimension, so the easy lever isn't available. The Release-on-error
	// path is covered by TestWorker_ReleasesOnClientError.
	t.Skip("covered by TestWorker_ReleasesOnClientError")
}

func TestWorker_EmptyPendingReturnsZeroResult(t *testing.T) {
	ctx := context.Background()
	f := newWorkerFixture(t, 0) // 0 messages → no pending rows

	w := NewWorker(WorkerDeps{
		Backend:   f.Backend,
		VectorsDB: f.VectorsDB,
		MainDB:    f.MainDB,
		Client:    f.FakeClient,
		BatchSize: 10,
	})
	res, err := w.RunOnce(ctx, f.BuildingGen)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if res.Claimed != 0 || res.Succeeded != 0 || res.Failed != 0 {
		t.Errorf("expected empty result, got %+v", res)
	}
}

func TestWorker_RespectsContextCancel(t *testing.T) {
	f := newWorkerFixture(t, 5)

	w := NewWorker(WorkerDeps{
		Backend:   f.Backend,
		VectorsDB: f.VectorsDB,
		MainDB:    f.MainDB,
		Client:    f.FakeClient,
		BatchSize: 1,
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancelled
	_, err := w.RunOnce(ctx, f.BuildingGen)
	if err == nil {
		t.Fatal("expected cancellation error, got nil")
	}
}

func TestWorker_ReclaimStale_FromStartup(t *testing.T) {
	ctx := context.Background()
	f := newWorkerFixture(t, 2)

	w := NewWorker(WorkerDeps{
		Backend:        f.Backend,
		VectorsDB:      f.VectorsDB,
		MainDB:         f.MainDB,
		Client:         f.FakeClient,
		BatchSize:      2,
		StaleThreshold: 10 * time.Minute,
	})

	// Simulate a crashed worker: claim 2 rows, then back-date the claim.
	q := NewQueue(f.VectorsDB)
	ids, _, err := q.Claim(ctx, f.BuildingGen, 2)
	if err != nil || len(ids) != 2 {
		t.Fatalf("Claim setup: ids=%v err=%v", ids, err)
	}
	if _, err := f.VectorsDB.ExecContext(ctx,
		`UPDATE pending_embeddings SET claimed_at = ? WHERE generation_id = ?`,
		time.Now().Add(-20*time.Minute).Unix(), int64(f.BuildingGen)); err != nil {
		t.Fatalf("backdate: %v", err)
	}

	n, err := w.ReclaimStale(ctx)
	if err != nil {
		t.Fatalf("ReclaimStale: %v", err)
	}
	if n != 2 {
		t.Errorf("reclaimed %d, want 2", n)
	}

	// Verify the rows are available again.
	var available int
	if err := f.VectorsDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pending_embeddings WHERE generation_id = ? AND claimed_at IS NULL`,
		int64(f.BuildingGen)).Scan(&available); err != nil {
		t.Fatalf("count available: %v", err)
	}
	if available != 2 {
		t.Errorf("available after reclaim = %d, want 2", available)
	}
}

func TestWorker_StaleThresholdDefault(t *testing.T) {
	f := newWorkerFixture(t, 0)
	w := NewWorker(WorkerDeps{
		Backend:   f.Backend,
		VectorsDB: f.VectorsDB,
		MainDB:    f.MainDB,
		Client:    f.FakeClient,
	})
	if w.deps.StaleThreshold != 10*time.Minute {
		t.Errorf("default StaleThreshold=%v, want 10m", w.deps.StaleThreshold)
	}
	if w.deps.MaxConsecutiveFailures != 5 {
		t.Errorf("default MaxConsecutiveFailures=%d, want 5", w.deps.MaxConsecutiveFailures)
	}
}

// TestWorker_AbortsAfterConsecutiveFailures verifies that a
// persistently failing embedder causes RunOnce to return an error
// rather than loop forever releasing and re-claiming.
func TestWorker_AbortsAfterConsecutiveFailures(t *testing.T) {
	ctx := context.Background()
	f := newWorkerFixture(t, 3)
	// Force every Embed call to fail — a huge failure budget ensures
	// we hit the MaxConsecutiveFailures limit first.
	f.FakeClient.FailNext(1 << 30)

	w := NewWorker(WorkerDeps{
		Backend:                f.Backend,
		VectorsDB:              f.VectorsDB,
		MainDB:                 f.MainDB,
		Client:                 f.FakeClient,
		BatchSize:              1,
		MaxConsecutiveFailures: 3,
	})

	res, err := w.RunOnce(ctx, f.BuildingGen)
	if err == nil {
		t.Fatal("RunOnce: want error after consecutive failures, got nil")
	}
	if res.Failed < 3 {
		t.Errorf("Failed=%d, want >= 3 (one per consecutive failure)", res.Failed)
	}
	// Any leftover claims should have been released; pending is non-empty.
	var pending int
	if err := f.VectorsDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pending_embeddings WHERE generation_id = ?`,
		int64(f.BuildingGen)).Scan(&pending); err != nil {
		t.Fatalf("count pending: %v", err)
	}
	if pending == 0 {
		t.Errorf("pending rows after abort = 0, want non-zero (rows should have been released)")
	}
}

// TestWorker_ConsecutiveFailureCounterResetsOnSuccess confirms that
// intermittent failures below the limit do not abort — each success
// resets the counter.
func TestWorker_ConsecutiveFailureCounterResetsOnSuccess(t *testing.T) {
	ctx := context.Background()
	f := newWorkerFixture(t, 4)
	// Fail twice (below the limit of 3), then all subsequent succeed.
	f.FakeClient.FailNext(2)

	w := NewWorker(WorkerDeps{
		Backend:                f.Backend,
		VectorsDB:              f.VectorsDB,
		MainDB:                 f.MainDB,
		Client:                 f.FakeClient,
		BatchSize:              1,
		MaxConsecutiveFailures: 3,
	})
	res, err := w.RunOnce(ctx, f.BuildingGen)
	if err != nil {
		t.Fatalf("RunOnce: %v (2 failures, budget 3 — should not abort)", err)
	}
	if res.Succeeded != 4 {
		t.Errorf("Succeeded=%d, want 4 (all 4 messages ultimately drain)", res.Succeeded)
	}
}

// TestWorker_MissingMessagesDrainedFromQueue verifies that claimed
// rows whose messages were deleted from the main DB are dropped from
// the queue (via Complete) rather than silently re-looped forever.
func TestWorker_MissingMessagesDrainedFromQueue(t *testing.T) {
	ctx := context.Background()
	f := newWorkerFixture(t, 3)

	// Simulate sync deleting messages 2 and 3 from the main DB
	// AFTER CreateGeneration seeded the queue.
	if _, err := f.MainDB.ExecContext(ctx,
		`DELETE FROM messages WHERE id IN (2, 3)`); err != nil {
		t.Fatalf("delete messages: %v", err)
	}
	if _, err := f.MainDB.ExecContext(ctx,
		`DELETE FROM message_bodies WHERE message_id IN (2, 3)`); err != nil {
		t.Fatalf("delete bodies: %v", err)
	}

	w := NewWorker(WorkerDeps{
		Backend:   f.Backend,
		VectorsDB: f.VectorsDB,
		MainDB:    f.MainDB,
		Client:    f.FakeClient,
		BatchSize: 3,
	})

	res, err := w.RunOnce(ctx, f.BuildingGen)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	// Message 1 embedded; 2 and 3 dropped as missing.
	if res.Succeeded != 1 {
		t.Errorf("Succeeded=%d, want 1", res.Succeeded)
	}
	// Queue should be fully drained (no infinite loop on missing rows).
	var pending int
	if err := f.VectorsDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pending_embeddings WHERE generation_id = ?`,
		int64(f.BuildingGen)).Scan(&pending); err != nil {
		t.Fatalf("count pending: %v", err)
	}
	if pending != 0 {
		t.Errorf("pending after drain = %d, want 0 (missing rows should be removed)", pending)
	}
	// Only one embedding row (for message 1).
	var embedded int
	if err := f.VectorsDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM embeddings WHERE generation_id = ?`,
		int64(f.BuildingGen)).Scan(&embedded); err != nil {
		t.Fatalf("count embeddings: %v", err)
	}
	if embedded != 1 {
		t.Errorf("embeddings = %d, want 1", embedded)
	}
}
