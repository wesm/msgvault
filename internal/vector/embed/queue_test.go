//go:build sqlite_vec

package embed

import (
	"context"
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
