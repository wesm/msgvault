//go:build sqlite_vec

package cmd

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/wesm/msgvault/internal/vector"
	"github.com/wesm/msgvault/internal/vector/sqlitevec"
)

// openTestBackend opens a fresh in-memory-ish sqlitevec backend with a
// single pre-seeded message so CreateGeneration has something to enqueue.
func openTestBackend(t *testing.T) *sqlitevec.Backend {
	t.Helper()
	ctx := context.Background()
	if err := sqlitevec.RegisterExtension(); err != nil {
		t.Fatalf("RegisterExtension: %v", err)
	}

	dir := t.TempDir()
	mainPath := filepath.Join(dir, "main.db")
	main, err := sql.Open("sqlite3", mainPath)
	if err != nil {
		t.Fatalf("open main: %v", err)
	}
	t.Cleanup(func() { _ = main.Close() })
	schema := `
CREATE TABLE messages (
    id INTEGER PRIMARY KEY,
    deleted_from_source_at DATETIME
);`
	if _, err := main.Exec(schema); err != nil {
		t.Fatalf("schema: %v", err)
	}
	if _, err := main.Exec(`INSERT INTO messages (id) VALUES (1)`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	b, err := sqlitevec.Open(ctx, sqlitevec.Options{
		Path:      filepath.Join(dir, "vectors.db"),
		MainPath:  mainPath,
		Dimension: 4,
		MainDB:    main,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

// openStderrSink returns a *os.File pointing at /dev/null so
// pickEmbedGeneration's status prints do not clutter test output.
func openStderrSink(t *testing.T) *os.File {
	t.Helper()
	f, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open /dev/null: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

// TestPickEmbedGeneration_ResumesBuildingGeneration covers the main
// recovery path: after a partial full-rebuild, running `msgvault
// embed` (without --full-rebuild) must return the existing building
// generation and report rebuildInProgress=true, so activation logic
// still runs when pending drains to zero. Previously this path
// errored out with ErrIndexBuilding.
func TestPickEmbedGeneration_ResumesBuildingGeneration(t *testing.T) {
	ctx := context.Background()
	b := openTestBackend(t)

	// Simulate an interrupted full rebuild: a building generation
	// exists but no active generation.
	gen, err := b.CreateGeneration(ctx, "fake", 4)
	if err != nil {
		t.Fatalf("CreateGeneration: %v", err)
	}

	gotGen, rebuildInProgress, err := pickEmbedGeneration(ctx, b, embedGenerationOpts{
		FullRebuild: false,
		Model:       "fake",
		Dimension:   4,
		Fingerprint: "fake:4",
		Stderr:      openStderrSink(t),
	})
	if err != nil {
		t.Fatalf("pickEmbedGeneration: %v (should resume, not error)", err)
	}
	if gotGen != gen {
		t.Errorf("gotGen=%d, want %d", gotGen, gen)
	}
	if !rebuildInProgress {
		t.Errorf("rebuildInProgress=false, want true (building generation)")
	}
}

// TestPickEmbedGeneration_NoGenerations_HintsFullRebuild covers the
// "fresh install" path: default-mode embed with no generations must
// surface a clear hint rather than silently doing nothing.
func TestPickEmbedGeneration_NoGenerations_HintsFullRebuild(t *testing.T) {
	ctx := context.Background()
	b := openTestBackend(t)

	_, _, err := pickEmbedGeneration(ctx, b, embedGenerationOpts{
		FullRebuild: false,
		Model:       "fake",
		Dimension:   4,
		Fingerprint: "fake:4",
		Stderr:      openStderrSink(t),
	})
	if err == nil {
		t.Fatal("expected error when no generations exist")
	}
	if !errors.Is(err, vector.ErrNotEnabled) {
		// Intentional: we wrap the underlying error with a hint, but the
		// underlying sentinel should still be errors.Is-reachable so
		// upstream callers can branch on it.
		t.Errorf("err = %v, want wrapping ErrNotEnabled", err)
	}
}

// TestPickEmbedGeneration_ResumeFingerprintMismatch rejects a resume
// when the in-progress rebuild was started with a different model or
// dimension than the current config — continuing would silently
// embed against the wrong model.
func TestPickEmbedGeneration_ResumeFingerprintMismatch(t *testing.T) {
	ctx := context.Background()
	b := openTestBackend(t)
	if _, err := b.CreateGeneration(ctx, "old-model", 4); err != nil {
		t.Fatalf("CreateGeneration: %v", err)
	}

	_, _, err := pickEmbedGeneration(ctx, b, embedGenerationOpts{
		FullRebuild: false,
		Model:       "new-model",
		Dimension:   4,
		Fingerprint: "new-model:4",
		Stderr:      openStderrSink(t),
	})
	if err == nil {
		t.Fatal("expected fingerprint mismatch error, got nil")
	}
	var buf bytes.Buffer
	_, _ = buf.WriteString(err.Error())
	if !bytes.Contains(buf.Bytes(), []byte("fingerprint")) {
		t.Errorf("error should mention fingerprint, got %q", err.Error())
	}
}

// TestPickEmbedGeneration_PrefersBuildingOverActive_MatchingFingerprint
// regression-guards the precedence bug where pickEmbedGeneration
// targeted an existing active generation even when a building
// generation for the configured model was in flight. The user
// expectation is that `msgvault embed` drains the in-progress build
// (so it can be activated) rather than continuing to top up the old
// active generation.
func TestPickEmbedGeneration_PrefersBuildingOverActive_MatchingFingerprint(t *testing.T) {
	ctx := context.Background()
	b := openTestBackend(t)

	// Build state: an active generation exists, and a second building
	// generation has been created for the SAME model+dim (the typical
	// "I want to refresh my index" pattern).
	activeGen, err := b.CreateGeneration(ctx, "fake", 4)
	if err != nil {
		t.Fatalf("CreateGeneration (active): %v", err)
	}
	if err := b.ActivateGeneration(ctx, activeGen); err != nil {
		t.Fatalf("ActivateGeneration: %v", err)
	}
	buildingGen, err := b.CreateGeneration(ctx, "fake", 4)
	if err != nil {
		t.Fatalf("CreateGeneration (building): %v", err)
	}

	gotGen, rebuildInProgress, err := pickEmbedGeneration(ctx, b, embedGenerationOpts{
		FullRebuild: false,
		Model:       "fake",
		Dimension:   4,
		Fingerprint: "fake:4",
		Stderr:      openStderrSink(t),
	})
	if err != nil {
		t.Fatalf("pickEmbedGeneration: %v", err)
	}
	if gotGen != buildingGen {
		t.Errorf("gotGen=%d, want building=%d (preferring active=%d would leave the build stranded)",
			gotGen, buildingGen, activeGen)
	}
	if !rebuildInProgress {
		t.Errorf("rebuildInProgress=false, want true (we picked the building generation)")
	}
}

// TestPickEmbedGeneration_RejectsBuildingWithMismatchedFingerprint
// regression-guards the case where an active generation matches the
// config but a building generation exists for a DIFFERENT model. The
// previous code called ResolveActive first, found the matching active,
// and silently topped it up — leaving the mismatched build stranded
// without any warning. The new precedence-then-mismatch flow should
// either resume a matching build or refuse with a clear error.
func TestPickEmbedGeneration_RejectsBuildingWithMismatchedFingerprint(t *testing.T) {
	ctx := context.Background()
	b := openTestBackend(t)

	// State: building generation exists for an old model. No active
	// generation, and config now points at a different model.
	if _, err := b.CreateGeneration(ctx, "old-model", 4); err != nil {
		t.Fatalf("CreateGeneration (building): %v", err)
	}

	_, _, err := pickEmbedGeneration(ctx, b, embedGenerationOpts{
		FullRebuild: false,
		Model:       "new-model",
		Dimension:   4,
		Fingerprint: "new-model:4",
		Stderr:      openStderrSink(t),
	})
	if err == nil {
		t.Fatal("expected error for mismatched-fingerprint building generation, got nil")
	}
	var buf bytes.Buffer
	_, _ = buf.WriteString(err.Error())
	if !bytes.Contains(buf.Bytes(), []byte("fingerprint")) {
		t.Errorf("error should mention fingerprint, got %q", err.Error())
	}
}

// TestPickEmbedGeneration_StaleActivePlusMatchingBuilding covers the
// "stale active + matching building" combination R51a calls out: an
// older active generation exists with a fingerprint that no longer
// matches the configured model, and a newer building generation
// matches. The configured-model build must be drained instead of the
// stale active one being topped up — otherwise the new build stays
// stuck in `building` indefinitely.
func TestPickEmbedGeneration_StaleActivePlusMatchingBuilding(t *testing.T) {
	ctx := context.Background()
	b := openTestBackend(t)

	staleActive, err := b.CreateGeneration(ctx, "old-model", 4)
	if err != nil {
		t.Fatalf("CreateGeneration (stale active): %v", err)
	}
	if err := b.ActivateGeneration(ctx, staleActive); err != nil {
		t.Fatalf("ActivateGeneration: %v", err)
	}
	matchingBuilding, err := b.CreateGeneration(ctx, "new-model", 4)
	if err != nil {
		t.Fatalf("CreateGeneration (matching building): %v", err)
	}

	gotGen, rebuildInProgress, err := pickEmbedGeneration(ctx, b, embedGenerationOpts{
		FullRebuild: false,
		Model:       "new-model",
		Dimension:   4,
		Fingerprint: "new-model:4",
		Stderr:      openStderrSink(t),
	})
	if err != nil {
		t.Fatalf("pickEmbedGeneration: %v (should resume matching build)", err)
	}
	if gotGen != matchingBuilding {
		t.Errorf("gotGen=%d, want building=%d (stale active=%d must not steal precedence)",
			gotGen, matchingBuilding, staleActive)
	}
	if !rebuildInProgress {
		t.Errorf("rebuildInProgress=false, want true")
	}
}

// TestPickEmbedGeneration_ActivePlusMismatchedBuildingRejected covers
// the case where the active generation matches the configured
// fingerprint AND a building generation exists for a different model.
// Silently topping up the active would leave the wrong-model build
// stranded forever; the user has to explicitly retire or activate it
// before embedding can proceed. Regression for the bug where the code
// only rejected mismatched builds via the ErrIndexBuilding branch and
// missed this active-also-matches case.
func TestPickEmbedGeneration_ActivePlusMismatchedBuildingRejected(t *testing.T) {
	ctx := context.Background()
	b := openTestBackend(t)

	matchingActive, err := b.CreateGeneration(ctx, "fake", 4)
	if err != nil {
		t.Fatalf("CreateGeneration (active): %v", err)
	}
	if err := b.ActivateGeneration(ctx, matchingActive); err != nil {
		t.Fatalf("ActivateGeneration: %v", err)
	}
	if _, err := b.CreateGeneration(ctx, "old-model", 4); err != nil {
		t.Fatalf("CreateGeneration (stale building): %v", err)
	}

	_, _, err = pickEmbedGeneration(ctx, b, embedGenerationOpts{
		FullRebuild: false,
		Model:       "fake",
		Dimension:   4,
		Fingerprint: "fake:4",
		Stderr:      openStderrSink(t),
	})
	if err == nil {
		t.Fatal("expected error when a mismatched building exists alongside matching active, got nil")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("fingerprint")) {
		t.Errorf("error should mention fingerprint, got %q", err.Error())
	}
}

// TestPickEmbedGeneration_FullRebuildAbortsWhenDeclined verifies the
// Confirm hook short-circuits when the user declines a rebuild.
func TestPickEmbedGeneration_FullRebuildAbortsWhenDeclined(t *testing.T) {
	ctx := context.Background()
	b := openTestBackend(t)

	_, _, err := pickEmbedGeneration(ctx, b, embedGenerationOpts{
		FullRebuild: true,
		Model:       "fake",
		Dimension:   4,
		Fingerprint: "fake:4",
		Confirm:     func() bool { return false },
		Stderr:      openStderrSink(t),
	})
	if err == nil {
		t.Fatal("expected abort error, got nil")
	}
}

// TestPickEmbedGeneration_ResumeReseedsUnseededBuilding regression-
// guards the crash-window bug where a process that died between
// inserting the building row and committing the initial seed would
// leave the queue empty; a later `msgvault embed` would then "drain"
// zero rows and silently activate an unseeded generation. The resume
// path must call EnsureSeeded on the matched build before returning,
// reseeding pending_embeddings so the activation gate sees real work
// (or the absence of any) instead of a vacuous empty queue.
func TestPickEmbedGeneration_ResumeReseedsUnseededBuilding(t *testing.T) {
	ctx := context.Background()
	b := openTestBackend(t)

	// Step 1: create a building gen the normal way (which seeds + marks
	// seeded_at).
	gen, err := b.CreateGeneration(ctx, "fake", 4)
	if err != nil {
		t.Fatalf("CreateGeneration: %v", err)
	}

	// Step 2: simulate the crash window — clear pending_embeddings and
	// blank seeded_at so the next resume must reseed. This mirrors the
	// state after a process dies between the building-row insert and
	// the seedPending commit.
	if _, err := b.DB().ExecContext(ctx,
		`DELETE FROM pending_embeddings WHERE generation_id = ?`, int64(gen)); err != nil {
		t.Fatalf("clear pending: %v", err)
	}
	if _, err := b.DB().ExecContext(ctx,
		`UPDATE index_generations SET seeded_at = NULL WHERE id = ?`, int64(gen)); err != nil {
		t.Fatalf("clear seeded_at: %v", err)
	}

	// Sanity: pending really is empty before the resume.
	var pendingBefore int
	if err := b.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pending_embeddings WHERE generation_id = ?`, int64(gen)).Scan(&pendingBefore); err != nil {
		t.Fatalf("count pending before: %v", err)
	}
	if pendingBefore != 0 {
		t.Fatalf("pending count before resume = %d, want 0 (test setup wrong)", pendingBefore)
	}

	// Step 3: run pickEmbedGeneration on the resume path.
	gotGen, rebuildInProgress, err := pickEmbedGeneration(ctx, b, embedGenerationOpts{
		FullRebuild: false,
		Model:       "fake",
		Dimension:   4,
		Fingerprint: "fake:4",
		Stderr:      openStderrSink(t),
	})
	if err != nil {
		t.Fatalf("pickEmbedGeneration: %v", err)
	}
	if gotGen != gen {
		t.Errorf("gotGen=%d, want %d", gotGen, gen)
	}
	if !rebuildInProgress {
		t.Error("rebuildInProgress=false, want true")
	}

	// Step 4: pending_embeddings should now contain the message we
	// seeded in openTestBackend (id=1). Without EnsureSeeded on the
	// resume path, this would still be 0.
	var pendingAfter int
	if err := b.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pending_embeddings WHERE generation_id = ?`, int64(gen)).Scan(&pendingAfter); err != nil {
		t.Fatalf("count pending after: %v", err)
	}
	if pendingAfter != 1 {
		t.Errorf("pending count after resume = %d, want 1 (EnsureSeeded should have reseeded)", pendingAfter)
	}
	// And seeded_at should be set so a subsequent resume skips the work.
	var seededAt sql.NullInt64
	if err := b.DB().QueryRowContext(ctx,
		`SELECT seeded_at FROM index_generations WHERE id = ?`, int64(gen)).Scan(&seededAt); err != nil {
		t.Fatalf("read seeded_at: %v", err)
	}
	if !seededAt.Valid {
		t.Error("seeded_at still NULL after resume, want set")
	}
}
