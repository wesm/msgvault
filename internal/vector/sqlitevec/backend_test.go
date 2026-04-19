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
	gid, err := b.CreateGeneration(ctx, "m", 768)
	if err != nil {
		t.Fatalf("CreateGeneration: %v", err)
	}

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
	if err := b.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM embeddings WHERE generation_id = ? AND message_id = 1`, gid).Scan(&n); err != nil {
		t.Fatalf("count embeddings: %v", err)
	}
	if n != 1 {
		t.Errorf("embeddings count = %d, want 1", n)
	}

	// vectors_vec_d768 row exists
	if err := b.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM vectors_vec_d768 WHERE generation_id = ? AND message_id = 1`, gid).Scan(&n); err != nil {
		t.Fatalf("count vectors_vec_d768: %v", err)
	}
	if n != 1 {
		t.Errorf("vectors_vec_d768 count = %d, want 1", n)
	}

	// pending row gone
	if err := b.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pending_embeddings WHERE generation_id = ? AND message_id = 1`, gid).Scan(&n); err != nil {
		t.Fatalf("count pending: %v", err)
	}
	if n != 0 {
		t.Errorf("pending count = %d, want 0 after upsert", n)
	}
}

func TestBackend_Upsert_DimensionMismatch(t *testing.T) {
	b, ctx := newBackendForTest(t)
	gid, err := b.CreateGeneration(ctx, "m", 768)
	if err != nil {
		t.Fatalf("CreateGeneration: %v", err)
	}

	short := make([]float32, 64) // wrong dim
	err = b.Upsert(ctx, gid, []vector.Chunk{{MessageID: 1, Vector: short}})
	if !errors.Is(err, vector.ErrDimensionMismatch) {
		t.Errorf("err = %v, want ErrDimensionMismatch", err)
	}
}

func TestBackend_Upsert_EmptyChunks(t *testing.T) {
	b, ctx := newBackendForTest(t)
	gid, err := b.CreateGeneration(ctx, "m", 768)
	if err != nil {
		t.Fatalf("CreateGeneration: %v", err)
	}

	if err := b.Upsert(ctx, gid, nil); err != nil {
		t.Fatalf("Upsert(nil): %v", err)
	}
	if err := b.Upsert(ctx, gid, []vector.Chunk{}); err != nil {
		t.Fatalf("Upsert(empty): %v", err)
	}

	var n int
	if err := b.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM embeddings WHERE generation_id = ?`, gid).Scan(&n); err != nil {
		t.Fatalf("count embeddings: %v", err)
	}
	if n != 0 {
		t.Errorf("embeddings count = %d, want 0", n)
	}
}

func TestBackend_Upsert_UnknownGeneration(t *testing.T) {
	b, ctx := newBackendForTest(t)

	vec := make([]float32, 768)
	err := b.Upsert(ctx, vector.GenerationID(9999), []vector.Chunk{{MessageID: 1, Vector: vec}})
	if !errors.Is(err, vector.ErrUnknownGeneration) {
		t.Errorf("err = %v, want ErrUnknownGeneration", err)
	}
}

func TestBackend_Upsert_MultiChunkAndTruncated(t *testing.T) {
	b, ctx := newBackendForTest(t)
	gid, err := b.CreateGeneration(ctx, "m", 768)
	if err != nil {
		t.Fatalf("CreateGeneration: %v", err)
	}

	vec1 := make([]float32, 768)
	vec2 := make([]float32, 768)
	for i := range vec1 {
		vec1[i] = 0.1
		vec2[i] = 0.2
	}
	chunks := []vector.Chunk{
		{MessageID: 1, Vector: vec1, SourceCharLen: 10, Truncated: true},
		{MessageID: 2, Vector: vec2, SourceCharLen: 20, Truncated: false},
	}
	if err := b.Upsert(ctx, gid, chunks); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	var n int
	if err := b.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM embeddings WHERE generation_id = ?`, gid).Scan(&n); err != nil {
		t.Fatalf("count embeddings: %v", err)
	}
	if n != 2 {
		t.Errorf("embeddings count = %d, want 2", n)
	}

	var trunc int
	if err := b.db.QueryRowContext(ctx,
		`SELECT truncated FROM embeddings WHERE generation_id = ? AND message_id = 1`, gid).Scan(&trunc); err != nil {
		t.Fatalf("scan truncated msg 1: %v", err)
	}
	if trunc != 1 {
		t.Errorf("truncated for msg 1 = %d, want 1", trunc)
	}
	if err := b.db.QueryRowContext(ctx,
		`SELECT truncated FROM embeddings WHERE generation_id = ? AND message_id = 2`, gid).Scan(&trunc); err != nil {
		t.Fatalf("scan truncated msg 2: %v", err)
	}
	if trunc != 0 {
		t.Errorf("truncated for msg 2 = %d, want 0", trunc)
	}

	if err := b.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM vectors_vec_d768 WHERE generation_id = ?`, gid).Scan(&n); err != nil {
		t.Fatalf("count vectors_vec_d768: %v", err)
	}
	if n != 2 {
		t.Errorf("vectors_vec_d768 count = %d, want 2", n)
	}
}

func TestBackend_Upsert_ReplacesExisting(t *testing.T) {
	b, ctx := newBackendForTest(t)
	gid, err := b.CreateGeneration(ctx, "m", 768)
	if err != nil {
		t.Fatalf("CreateGeneration: %v", err)
	}

	vec1 := make([]float32, 768)
	for i := range vec1 {
		vec1[i] = 0.1
	}
	if err := b.Upsert(ctx, gid, []vector.Chunk{{MessageID: 1, Vector: vec1, SourceCharLen: 10}}); err != nil {
		t.Fatalf("first Upsert: %v", err)
	}

	vec2 := make([]float32, 768)
	for i := range vec2 {
		vec2[i] = 0.9
	}
	if err := b.Upsert(ctx, gid, []vector.Chunk{{MessageID: 1, Vector: vec2, SourceCharLen: 999}}); err != nil {
		t.Fatalf("second Upsert: %v", err)
	}

	var n int
	if err := b.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM embeddings WHERE generation_id = ? AND message_id = 1`, gid).Scan(&n); err != nil {
		t.Fatalf("count embeddings: %v", err)
	}
	if n != 1 {
		t.Errorf("embeddings count = %d, want 1", n)
	}

	var charLen int
	if err := b.db.QueryRowContext(ctx,
		`SELECT source_char_len FROM embeddings WHERE generation_id = ? AND message_id = 1`, gid).Scan(&charLen); err != nil {
		t.Fatalf("scan source_char_len: %v", err)
	}
	if charLen != 999 {
		t.Errorf("source_char_len = %d, want 999", charLen)
	}

	if err := b.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM vectors_vec_d768 WHERE generation_id = ? AND message_id = 1`, gid).Scan(&n); err != nil {
		t.Fatalf("count vectors_vec_d768: %v", err)
	}
	if n != 1 {
		t.Errorf("vectors_vec_d768 count = %d, want 1", n)
	}
}
