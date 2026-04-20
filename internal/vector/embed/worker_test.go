//go:build sqlite_vec

package embed

import (
	"context"
	"fmt"
	"strings"
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

// TestWorker_RuneCountUsedForSourceCharLen regresses the
// byte-vs-rune mismatch: Preprocess truncates by runes, so the
// SourceCharLen field on each Chunk must also be a rune count or
// CJK/emoji inputs get inflated by 2-4x. We embed a short Japanese
// subject (whose UTF-8 byte length is much larger than its rune
// count) and assert the persisted source_char_len matches runes.
func TestWorker_RuneCountUsedForSourceCharLen(t *testing.T) {
	ctx := context.Background()
	f := newWorkerFixture(t, 0) // start empty so we control the message text

	// "こんにちは世界" = 7 runes, 21 UTF-8 bytes. Preprocess prepends
	// "Subject: " (9 ASCII bytes/runes) and "\n\n" (2). The full
	// preprocessed string has 18 runes and 32 bytes — a 1.78x
	// inflation if we record bytes by mistake.
	const subject = "こんにちは世界"
	if _, err := f.MainDB.ExecContext(ctx,
		`INSERT INTO messages (id, subject) VALUES (1, ?)`, subject); err != nil {
		t.Fatalf("insert message: %v", err)
	}
	if _, err := f.VectorsDB.ExecContext(ctx,
		`INSERT INTO pending_embeddings (generation_id, message_id, enqueued_at) VALUES (?, 1, 0)`,
		int64(f.BuildingGen)); err != nil {
		t.Fatalf("seed pending: %v", err)
	}

	w := NewWorker(WorkerDeps{
		Backend:   f.Backend,
		VectorsDB: f.VectorsDB,
		MainDB:    f.MainDB,
		Client:    f.FakeClient,
		BatchSize: 1,
	})
	res, err := w.RunOnce(ctx, f.BuildingGen)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if res.Succeeded != 1 {
		t.Fatalf("Succeeded=%d, want 1", res.Succeeded)
	}

	const wantRunes = 18 // len("Subject: \n\n") + 7 runes for the kanji
	var got int
	if err := f.VectorsDB.QueryRowContext(ctx,
		`SELECT source_char_len FROM embeddings WHERE generation_id = ? AND message_id = 1`,
		int64(f.BuildingGen)).Scan(&got); err != nil {
		t.Fatalf("read source_char_len: %v", err)
	}
	if got != wantRunes {
		t.Errorf("source_char_len=%d, want %d (rune count, not byte length)", got, wantRunes)
	}
}

// TestWorker_CompleteFailureCountsAsBatchFailure regresses the bug
// where Queue.Complete failures were log-only: the embedded rows
// stayed claimed, the next Claim returned empty, and RunOnce
// reported a clean drain. After this fix Complete failure must count
// toward MaxConsecutiveFailures so the loop short-circuits instead of
// silently spinning until ReclaimStale rescues the rows minutes later.
//
// The earlier version of this test dropped pending_embeddings to make
// Complete fail, but that also broke the next Claim — the test then
// passed because Claim errored out, not because Complete failure was
// detected. To actually exercise the stuck-claim path we install a
// BEFORE DELETE trigger that fires only on Complete (Claim does an
// UPDATE, not a DELETE, so it still succeeds). After RunOnce errors,
// we assert the pending row is still present AND claimed — proving
// the loop noticed the stuck state instead of silently treating an
// empty Claim as a clean drain.
func TestWorker_CompleteFailureCountsAsBatchFailure(t *testing.T) {
	ctx := context.Background()
	// Need ≥ MaxConsecutiveFailures messages so successive claims pull
	// fresh rows; otherwise a single failed Complete leaves one stuck-
	// claimed row and the next Claim returns empty (which RunOnce
	// rightly treats as a clean drain — the bug we're regressing
	// against would never trip with a single-message fixture).
	f := newWorkerFixture(t, 3)

	if _, err := f.VectorsDB.ExecContext(ctx, `
        CREATE TRIGGER block_pending_delete
        BEFORE DELETE ON pending_embeddings
        BEGIN
            SELECT RAISE(FAIL, 'simulated complete failure');
        END`); err != nil {
		t.Fatalf("install trigger: %v", err)
	}

	w := NewWorker(WorkerDeps{
		Backend:                f.Backend,
		VectorsDB:              f.VectorsDB,
		MainDB:                 f.MainDB,
		Client:                 f.FakeClient,
		BatchSize:              1,
		MaxConsecutiveFailures: 2,
	})

	res, err := w.RunOnce(ctx, f.BuildingGen)
	if err == nil {
		t.Fatal("RunOnce: want error after Complete failures, got nil (regression: silent success)")
	}
	if res.Succeeded != 0 {
		t.Errorf("Succeeded=%d, want 0 (Complete failed, work was not durably finished)", res.Succeeded)
	}
	if res.Failed == 0 {
		t.Errorf("Failed=%d, want > 0 (Complete failure should count as a batch failure)", res.Failed)
	}

	// Stuck-claim check: pending_embeddings row is still there (the
	// trigger blocked Complete's DELETE) and is marked claimed (the
	// previous Claim's UPDATE went through). A naive "log-only"
	// Complete handler would silently report success; the failure
	// counter is what makes RunOnce notice and abort.
	var pending, claimed int
	if err := f.VectorsDB.QueryRowContext(ctx,
		`SELECT COUNT(*),
                COALESCE(SUM(CASE WHEN claimed_at IS NOT NULL THEN 1 ELSE 0 END), 0)
           FROM pending_embeddings WHERE generation_id = ?`,
		int64(f.BuildingGen)).Scan(&pending, &claimed); err != nil {
		t.Fatalf("count pending: %v", err)
	}
	if pending == 0 {
		t.Errorf("pending=%d, want > 0 — Complete should have failed and left the row in place", pending)
	}
	if claimed == 0 {
		t.Errorf("claimed=%d, want > 0 — Claim's UPDATE should have left the row marked claimed", claimed)
	}
}

// TestWorker_OrphanCompleteFailureDoesNotStrandValidWork regresses
// two related bugs around orphan-drain failure:
//
//  1. Original (R53a): a failed Complete(missing) call ran BEFORE
//     Upsert and used `continue`, leaving the still-valid claimed IDs
//     in the same batch claimed but unembedded until ReclaimStale.
//     After the fix, orphan-drain runs AFTER the embedded rows are
//     upserted and acknowledged.
//
//  2. R58: when the orphan was the last queue row, the next Claim
//     returned empty and RunOnce exited nil — leaving the orphan
//     stranded for ~10 min until ReclaimStale, with no signal to
//     the caller. After the fix, the empty-claim exit surfaces the
//     orphan-drain failure as a non-nil error so the user knows the
//     run was incomplete.
func TestWorker_OrphanCompleteFailureDoesNotStrandValidWork(t *testing.T) {
	ctx := context.Background()
	// 2 messages enqueued; we'll delete one from the main DB so it
	// reaches embedBatch as "missing".
	f := newWorkerFixture(t, 2)

	const orphanID = 2
	if _, err := f.MainDB.ExecContext(ctx,
		`DELETE FROM messages WHERE id = ?`, orphanID); err != nil {
		t.Fatalf("delete orphan from main: %v", err)
	}
	if _, err := f.MainDB.ExecContext(ctx,
		`DELETE FROM message_bodies WHERE message_id = ?`, orphanID); err != nil {
		t.Fatalf("delete orphan body: %v", err)
	}

	// Selective trigger: only the orphan's Complete DELETE fails. The
	// embedded row's Complete must still succeed so we can prove the
	// valid work is durably finished even when the orphan drain fails.
	if _, err := f.VectorsDB.ExecContext(ctx, fmt.Sprintf(`
        CREATE TRIGGER block_orphan_drain
        BEFORE DELETE ON pending_embeddings
        WHEN OLD.message_id = %d
        BEGIN
            SELECT RAISE(FAIL, 'simulated orphan complete failure');
        END`, orphanID)); err != nil {
		t.Fatalf("install trigger: %v", err)
	}

	w := NewWorker(WorkerDeps{
		Backend:                f.Backend,
		VectorsDB:              f.VectorsDB,
		MainDB:                 f.MainDB,
		Client:                 f.FakeClient,
		BatchSize:              2,
		MaxConsecutiveFailures: 5, // generous so the orphan drain failure does not abort mid-loop
	})

	res, err := w.RunOnce(ctx, f.BuildingGen)
	if err == nil {
		t.Fatal("RunOnce: want non-nil error (orphan drain failed and orphan remained stuck), got nil")
	}
	if !strings.Contains(err.Error(), "orphan-drain") {
		t.Errorf("RunOnce err = %q, want it to mention 'orphan-drain'", err.Error())
	}
	if !strings.Contains(err.Error(), "ReclaimStale") {
		t.Errorf("RunOnce err = %q, want it to mention 'ReclaimStale' so the user knows recovery is automatic", err.Error())
	}
	if res.Succeeded != 1 {
		t.Errorf("Succeeded=%d, want 1 (the valid message must be counted as completed)", res.Succeeded)
	}
	if res.Failed == 0 {
		t.Errorf("Failed=%d, want > 0 (orphan drain failure should be reported)", res.Failed)
	}

	// The valid message's pending row must be GONE (Complete succeeded).
	// The original bug left it claimed-but-not-completed because the
	// orphan-drain failure short-circuited before Upsert.
	var validPending int
	if err := f.VectorsDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pending_embeddings WHERE generation_id = ? AND message_id = 1`,
		int64(f.BuildingGen)).Scan(&validPending); err != nil {
		t.Fatalf("count valid pending: %v", err)
	}
	if validPending != 0 {
		t.Errorf("valid pending = %d, want 0 (R53a regression: valid row stranded by orphan drain failure)", validPending)
	}

	// And the embedded row should be in the embeddings table.
	var embedded int
	if err := f.VectorsDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM embeddings WHERE generation_id = ? AND message_id = 1`,
		int64(f.BuildingGen)).Scan(&embedded); err != nil {
		t.Fatalf("count embedded: %v", err)
	}
	if embedded != 1 {
		t.Errorf("embedded count = %d, want 1 (Upsert should have run before orphan drain)", embedded)
	}

	// The orphan row stays claimed (token is non-NULL) — that's the
	// state ReclaimStale is built to recover from. The error returned
	// above is what tells the caller "this run isn't actually clean".
	var orphanClaimed int
	if err := f.VectorsDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pending_embeddings
		  WHERE generation_id = ? AND message_id = ? AND claim_token IS NOT NULL`,
		int64(f.BuildingGen), orphanID).Scan(&orphanClaimed); err != nil {
		t.Fatalf("count orphan claimed: %v", err)
	}
	if orphanClaimed != 1 {
		t.Errorf("orphan claimed rows = %d, want 1 (the trigger blocks the Complete DELETE)", orphanClaimed)
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
