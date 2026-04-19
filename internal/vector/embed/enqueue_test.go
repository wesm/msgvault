//go:build sqlite_vec

package embed

import (
	"context"
	"testing"
)

func TestEnqueuer_NoGenerations_Noop(t *testing.T) {
	ctx := context.Background()
	db := openVectorsDBForEnqueue(t)
	e := NewEnqueuer(db)
	if err := e.EnqueueMessages(ctx, []int64{1, 2, 3}); err != nil {
		t.Fatalf("EnqueueMessages with no generations: %v", err)
	}
	// Should be no pending rows.
	var n int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pending_embeddings`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("pending count = %d, want 0", n)
	}
}

func TestEnqueuer_ActiveGenerationOnly(t *testing.T) {
	ctx := context.Background()
	db := openVectorsDBForEnqueue(t)
	insertGenerationStatic(t, db, 1, "active")
	e := NewEnqueuer(db)
	if err := e.EnqueueMessages(ctx, []int64{10, 11}); err != nil {
		t.Fatal(err)
	}
	assertPending(t, db, 1, 2)
}

func TestEnqueuer_ActiveAndBuilding_DualEnqueue(t *testing.T) {
	ctx := context.Background()
	db := openVectorsDBForEnqueue(t)
	insertGenerationStatic(t, db, 1, "active")
	insertGenerationStatic(t, db, 2, "building")
	insertGenerationStatic(t, db, 3, "retired") // should NOT receive.
	e := NewEnqueuer(db)
	if err := e.EnqueueMessages(ctx, []int64{100}); err != nil {
		t.Fatal(err)
	}
	assertPending(t, db, 1, 1)
	assertPending(t, db, 2, 1)
	assertPending(t, db, 3, 0)
}

func TestEnqueuer_DuplicateIDs_Ignored(t *testing.T) {
	ctx := context.Background()
	db := openVectorsDBForEnqueue(t)
	insertGenerationStatic(t, db, 1, "active")
	e := NewEnqueuer(db)
	if err := e.EnqueueMessages(ctx, []int64{42}); err != nil {
		t.Fatal(err)
	}
	// Second call with same ID should not error; count still 1.
	if err := e.EnqueueMessages(ctx, []int64{42, 42}); err != nil {
		t.Fatal(err)
	}
	assertPending(t, db, 1, 1)
}

func TestEnqueuer_EmptyIDs_Noop(t *testing.T) {
	ctx := context.Background()
	db := openVectorsDBForEnqueue(t)
	insertGenerationStatic(t, db, 1, "active")
	e := NewEnqueuer(db)
	if err := e.EnqueueMessages(ctx, nil); err != nil {
		t.Errorf("EnqueueMessages(nil): %v", err)
	}
	if err := e.EnqueueMessages(ctx, []int64{}); err != nil {
		t.Errorf("EnqueueMessages([]): %v", err)
	}
	assertPending(t, db, 1, 0)
}
