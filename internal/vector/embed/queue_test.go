//go:build sqlite_vec

package embed

import (
	"context"
	"sort"
	"testing"
	"time"
)

func TestQueue_ClaimReleaseComplete(t *testing.T) {
	ctx := context.Background()
	db := openVectorsDBWithPending(t, 5)
	q := NewQueue(db)

	ids, token, err := q.Claim(ctx, 1, 3)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(ids) != 3 || token == "" {
		t.Fatalf("claimed ids=%v token=%q, want 3 ids and non-empty token", ids, token)
	}

	// Second claim sees only 2 available.
	more, token2, err := q.Claim(ctx, 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(more) != 2 || token2 == token {
		t.Errorf("second claim got %d ids (want 2) / token collision=%v", len(more), token2 == token)
	}

	if err := q.Release(ctx, 1, token, ids); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if got := countAvailable(t, db, 1); got != 3 {
		t.Errorf("available after release = %d, want 3", got)
	}

	// Now complete the second batch; pending count should drop by 2.
	if err := q.Complete(ctx, 1, token2, more); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	var total int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pending_embeddings`).Scan(&total); err != nil {
		t.Fatalf("total: %v", err)
	}
	if total != 3 {
		t.Errorf("pending total after complete = %d, want 3 (5 - 2)", total)
	}
}

func TestQueue_Claim_EmptyBatchIsNoop(t *testing.T) {
	ctx := context.Background()
	db := openVectorsDBWithPending(t, 1)
	q := NewQueue(db)
	ids, token, err := q.Claim(ctx, 1, 0)
	if err != nil {
		t.Fatalf("Claim(0): %v", err)
	}
	if len(ids) != 0 || token != "" {
		t.Errorf("expected empty ids and token, got ids=%v token=%q", ids, token)
	}
}

func TestQueue_Claim_NoAvailableReturnsEmpty(t *testing.T) {
	ctx := context.Background()
	db := openVectorsDBWithPending(t, 0)
	q := NewQueue(db)
	ids, token, err := q.Claim(ctx, 1, 10)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(ids) != 0 || token != "" {
		t.Errorf("expected empty ids and token with no available, got %v %q", ids, token)
	}
}

func TestQueue_Complete_WrongTokenNoop(t *testing.T) {
	ctx := context.Background()
	db := openVectorsDBWithPending(t, 2)
	q := NewQueue(db)
	ids, _, err := q.Claim(ctx, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	// Wrong token — rows should remain.
	if err := q.Complete(ctx, 1, "deadbeef", ids); err != nil {
		t.Fatalf("Complete with wrong token: %v", err)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pending_embeddings`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("rows remaining = %d, want 2 (Complete should not delete on token mismatch)", n)
	}
}

func TestQueue_Release_WrongTokenNoop(t *testing.T) {
	ctx := context.Background()
	db := openVectorsDBWithPending(t, 2)
	q := NewQueue(db)
	ids, _, err := q.Claim(ctx, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Release(ctx, 1, "deadbeef", ids); err != nil {
		t.Fatalf("Release with wrong token: %v", err)
	}
	if got := countAvailable(t, db, 1); got != 0 {
		t.Errorf("available after wrong-token release = %d, want 0 (still claimed)", got)
	}
}

func TestQueue_ReclaimStale(t *testing.T) {
	ctx := context.Background()
	db := openVectorsDBWithPending(t, 2)
	q := NewQueue(db)
	_, _, err := q.Claim(ctx, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	// Back-date the claim past the threshold.
	if _, err := db.ExecContext(ctx,
		`UPDATE pending_embeddings SET claimed_at = ? WHERE generation_id = 1`,
		time.Now().Add(-20*time.Minute).Unix()); err != nil {
		t.Fatal(err)
	}
	n, err := q.ReclaimStale(ctx, 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("reclaimed %d, want 2", n)
	}
	if got := countAvailable(t, db, 1); got != 2 {
		t.Errorf("available after reclaim = %d, want 2", got)
	}
}

func TestQueue_Complete_EmptyIDsIsNoop(t *testing.T) {
	ctx := context.Background()
	db := openVectorsDBWithPending(t, 1)
	q := NewQueue(db)
	if err := q.Complete(ctx, 1, "token", nil); err != nil {
		t.Errorf("Complete(nil): %v", err)
	}
}

// TestQueue_Claim_ReturnsIDsAscending verifies that Claim's returned
// slice is sorted ascending regardless of the order SQLite's
// UPDATE...RETURNING clause produces rows. Callers (the Worker) pair
// ids with fetched message rows by position, so a non-deterministic
// order would cause silent vector↔message mixups.
func TestQueue_Claim_ReturnsIDsAscending(t *testing.T) {
	ctx := context.Background()
	db := openVectorsDBWithPending(t, 10)
	q := NewQueue(db)

	ids, _, err := q.Claim(ctx, 1, 10)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(ids) != 10 {
		t.Fatalf("len(ids) = %d, want 10", len(ids))
	}
	if !sort.SliceIsSorted(ids, func(i, j int) bool { return ids[i] < ids[j] }) {
		t.Errorf("ids not ascending: %v", ids)
	}
}

// TestQueue_Complete_AfterReclaim_PreservesNewClaim simulates the
// stale-worker-completing-late race: worker A claims rows, stalls
// long enough for ReclaimStale to clear the claim, worker B
// re-claims the same rows, then worker A finally finishes and calls
// Complete with its old token. The token check must prevent A from
// deleting B's row.
func TestQueue_Complete_AfterReclaim_PreservesNewClaim(t *testing.T) {
	ctx := context.Background()
	db := openVectorsDBWithPending(t, 2)
	q := NewQueue(db)

	idsA, tokenA, err := q.Claim(ctx, 1, 2)
	if err != nil {
		t.Fatalf("Claim A: %v", err)
	}
	if len(idsA) != 2 {
		t.Fatalf("Claim A ids=%v, want 2", idsA)
	}

	// Back-date A's claim past the threshold, then reclaim.
	if _, err := db.ExecContext(ctx,
		`UPDATE pending_embeddings SET claimed_at = ? WHERE generation_id = 1`,
		time.Now().Add(-20*time.Minute).Unix()); err != nil {
		t.Fatal(err)
	}
	if n, err := q.ReclaimStale(ctx, 10*time.Minute); err != nil || n != 2 {
		t.Fatalf("ReclaimStale: n=%d err=%v, want n=2 err=nil", n, err)
	}

	idsB, tokenB, err := q.Claim(ctx, 1, 2)
	if err != nil {
		t.Fatalf("Claim B: %v", err)
	}
	if len(idsB) != 2 || tokenB == tokenA {
		t.Fatalf("Claim B ids=%v token=%q (A=%q)", idsB, tokenB, tokenA)
	}

	// Stale worker A finishes and calls Complete with its dead token.
	// The token check must keep B's rows intact.
	if err := q.Complete(ctx, 1, tokenA, idsA); err != nil {
		t.Fatalf("Complete(stale tokenA): %v", err)
	}
	var remaining int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pending_embeddings`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 2 {
		t.Fatalf("pending rows after stale Complete = %d, want 2 (stale token must not delete)", remaining)
	}

	// B's claim should still be intact (claim_token matches tokenB).
	var claimed int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM pending_embeddings WHERE claim_token = ?`, tokenB).Scan(&claimed); err != nil {
		t.Fatal(err)
	}
	if claimed != 2 {
		t.Errorf("rows still holding B's token = %d, want 2", claimed)
	}

	// B can now legitimately Complete.
	if err := q.Complete(ctx, 1, tokenB, idsB); err != nil {
		t.Fatalf("Complete(tokenB): %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM pending_embeddings`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Errorf("pending rows after B's Complete = %d, want 0", remaining)
	}
}
