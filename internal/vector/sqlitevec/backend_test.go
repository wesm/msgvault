//go:build sqlite_vec

package sqlitevec

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/wesm/msgvault/internal/vector"
)

func newBackendForTest(t *testing.T) (*Backend, context.Context) {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "vectors.db")
	main := openMainDBWithOneMessage(t)
	b, err := Open(ctx, Options{
		Path:      path,
		Dimension: 768,
		MainDB:    main,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b, ctx
}

func TestBackend_CreateActivateRetire(t *testing.T) {
	b, ctx := newBackendForTest(t)

	gid, err := b.CreateGeneration(ctx, "nomic-embed-text-v1.5", 768)
	if err != nil {
		t.Fatalf("CreateGeneration: %v", err)
	}

	bg, err := b.BuildingGeneration(ctx)
	if err != nil || bg == nil || bg.ID != gid {
		t.Fatalf("BuildingGeneration got (%v, %v), want id=%d", bg, err, gid)
	}
	if _, err := b.ActiveGeneration(ctx); err == nil {
		t.Fatal("ActiveGeneration should error before activation")
	}

	if err := b.ActivateGeneration(ctx, gid); err != nil {
		t.Fatalf("ActivateGeneration: %v", err)
	}
	g, err := b.ActiveGeneration(ctx)
	if err != nil {
		t.Fatalf("ActiveGeneration after activate: %v", err)
	}
	if g.State != vector.GenerationActive {
		t.Errorf("State=%q want active", g.State)
	}
	if g.Fingerprint != "nomic-embed-text-v1.5:768" {
		t.Errorf("Fingerprint=%q", g.Fingerprint)
	}

	if err := b.RetireGeneration(ctx, gid); err != nil {
		t.Fatalf("RetireGeneration: %v", err)
	}
	if _, err := b.ActiveGeneration(ctx); err == nil {
		t.Fatal("ActiveGeneration should error after retire")
	}
}

func TestBackend_CreateGeneration_SeedsPending(t *testing.T) {
	b, ctx := newBackendForTest(t)
	gid, err := b.CreateGeneration(ctx, "m", 768)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	var n int
	if err := b.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pending_embeddings WHERE generation_id = ?`, gid,
	).Scan(&n); err != nil {
		t.Fatalf("count pending: %v", err)
	}
	if n != 1 {
		t.Errorf("pending count = %d, want 1", n)
	}
}

func TestBackend_CreateGeneration_SkipsDeletedMessages(t *testing.T) {
	b := openBackendWithOneDeletedMessage(t)
	t.Cleanup(func() { _ = b.Close() })
	ctx := context.Background()
	gid, err := b.CreateGeneration(ctx, "m", 768)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	var n int
	_ = b.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pending_embeddings WHERE generation_id = ?`, gid).Scan(&n)
	if n != 0 {
		t.Errorf("pending count for deleted message = %d, want 0", n)
	}
}

func TestBackend_Upsert_InsertsAndClearsPending(t *testing.T) {
	b, ctx := newBackendForTest(t)
	gid, _ := b.CreateGeneration(ctx, "m", 768)

	vec := make([]float32, 768)
	for i := range vec {
		vec[i] = 0.1
	}
	chunks := []vector.Chunk{{MessageID: 1, Vector: vec, SourceCharLen: 42}}
	if err := b.Upsert(ctx, gid, chunks); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	// embeddings row exists
	var n int
	_ = b.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM embeddings WHERE generation_id = ? AND message_id = 1`, gid).Scan(&n)
	if n != 1 {
		t.Errorf("embeddings count = %d, want 1", n)
	}

	// vectors_vec_d768 row exists
	_ = b.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM vectors_vec_d768 WHERE generation_id = ? AND message_id = 1`, gid).Scan(&n)
	if n != 1 {
		t.Errorf("vectors_vec_d768 count = %d, want 1", n)
	}

	// pending row gone
	_ = b.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pending_embeddings WHERE generation_id = ? AND message_id = 1`, gid).Scan(&n)
	if n != 0 {
		t.Errorf("pending count = %d, want 0 after upsert", n)
	}
}

func TestBackend_Upsert_DimensionMismatch(t *testing.T) {
	b, ctx := newBackendForTest(t)
	gid, _ := b.CreateGeneration(ctx, "m", 768)

	short := make([]float32, 64) // wrong dim
	err := b.Upsert(ctx, gid, []vector.Chunk{{MessageID: 1, Vector: short}})
	if !errors.Is(err, vector.ErrDimensionMismatch) {
		t.Errorf("err = %v, want ErrDimensionMismatch", err)
	}
}
