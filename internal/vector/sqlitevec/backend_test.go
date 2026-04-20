//go:build sqlite_vec

package sqlitevec

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

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

// TestBackend_CreateGeneration_ResumesBuilding confirms that calling
// CreateGeneration while a building row already exists with the same
// fingerprint returns the existing id instead of failing on the unique
// index. This makes retries after a crash idempotent.
func TestBackend_CreateGeneration_ResumesBuilding(t *testing.T) {
	b, ctx := newBackendForTest(t)

	first, err := b.CreateGeneration(ctx, "m", 768)
	if err != nil {
		t.Fatalf("first Create: %v", err)
	}

	second, err := b.CreateGeneration(ctx, "m", 768)
	if err != nil {
		t.Fatalf("second Create with matching fingerprint: %v", err)
	}
	if first != second {
		t.Errorf("Create returned new id %d, want reused %d", second, first)
	}
}

// TestBackend_CreateGeneration_MismatchedFingerprint checks that a
// second CreateGeneration call with a different fingerprint while
// another build is in progress surfaces an actionable error wrapping
// vector.ErrBuildingInProgress, instead of a raw SQLite uniqueness
// error.
func TestBackend_CreateGeneration_MismatchedFingerprint(t *testing.T) {
	b, ctx := newBackendForTest(t)

	if _, err := b.CreateGeneration(ctx, "model-a", 768); err != nil {
		t.Fatalf("first Create: %v", err)
	}

	_, err := b.CreateGeneration(ctx, "model-b", 768)
	if err == nil {
		t.Fatal("second Create with different fingerprint: want error, got nil")
	}
	if !errors.Is(err, vector.ErrBuildingInProgress) {
		t.Errorf("error = %v, want wrapping ErrBuildingInProgress", err)
	}
}

// TestBackend_CreateGeneration_SeedCommitsVisibleFirst confirms the
// new building row is committed *before* the seed pass runs, so a
// concurrent Enqueuer can see the generation and dual-enqueue
// newly-synced messages. Without this ordering there is a window
// during which sync-side enqueues would be scoped only to the active
// generation and the new build would be missing messages.
func TestBackend_CreateGeneration_SeedCommitsVisibleFirst(t *testing.T) {
	b, ctx := newBackendForTest(t)

	// Start a CreateGeneration in the background. Before it returns,
	// any observer should be able to see the building row — this is
	// the pre-condition the Enqueuer relies on for dual-enqueue.
	done := make(chan error, 1)
	go func() {
		_, err := b.CreateGeneration(ctx, "m", 768)
		done <- err
	}()

	// Poll for visibility of the building row. A generous deadline
	// guards against CI slowness; the assertion we're making is that
	// the row becomes visible some time before CreateGeneration
	// returns (i.e. it was committed before the seed pass).
	deadline := time.Now().Add(2 * time.Second)
	var visible bool
	for time.Now().Before(deadline) {
		var id int64
		err := b.db.QueryRowContext(ctx,
			`SELECT id FROM index_generations WHERE state = 'building'`).Scan(&id)
		if err == nil && id > 0 {
			visible = true
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	if err := <-done; err != nil {
		t.Fatalf("CreateGeneration: %v", err)
	}
	if !visible {
		t.Error("building generation was never visible while CreateGeneration was in flight")
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

func TestBackend_Search_ReturnsRankedHits(t *testing.T) {
	b, ctx := newBackendForTest(t)
	gid := seedAndEmbed(t, b, map[int64][]float32{
		10: unitVec(768, 0),
		11: unitVec(768, 1),
		12: unitVec(768, 2),
	})

	hits, err := b.Search(ctx, gid, unitVec(768, 1), 2, vector.Filter{})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) != 2 {
		t.Fatalf("got %d hits, want 2", len(hits))
	}
	if hits[0].MessageID != 11 {
		t.Errorf("top hit = %d, want 11", hits[0].MessageID)
	}
	if hits[0].Rank != 1 {
		t.Errorf("top rank = %d, want 1", hits[0].Rank)
	}
}

func TestBackend_Search_EmptyQueryVector(t *testing.T) {
	b, ctx := newBackendForTest(t)
	gid, err := b.CreateGeneration(ctx, "m", 768)
	if err != nil {
		t.Fatalf("CreateGeneration: %v", err)
	}
	_, err = b.Search(ctx, gid, nil, 5, vector.Filter{})
	if err == nil {
		t.Fatal("Search with nil queryVec should error")
	}
	_, err = b.Search(ctx, gid, []float32{}, 5, vector.Filter{})
	if err == nil {
		t.Fatal("Search with empty queryVec should error")
	}
}

func TestBackend_Search_UnknownGeneration(t *testing.T) {
	b, ctx := newBackendForTest(t)
	vec := unitVec(768, 0)
	_, err := b.Search(ctx, vector.GenerationID(9999), vec, 5, vector.Filter{})
	if !errors.Is(err, vector.ErrUnknownGeneration) {
		t.Errorf("err = %v, want ErrUnknownGeneration", err)
	}
}

func TestBackend_Search_DimensionMismatch(t *testing.T) {
	b, ctx := newBackendForTest(t)
	gid, err := b.CreateGeneration(ctx, "m", 768)
	if err != nil {
		t.Fatalf("CreateGeneration: %v", err)
	}
	_, err = b.Search(ctx, gid, unitVec(64, 0), 5, vector.Filter{})
	if !errors.Is(err, vector.ErrDimensionMismatch) {
		t.Errorf("err = %v, want ErrDimensionMismatch", err)
	}
}

func TestBackend_Delete_RemovesFromAllTables(t *testing.T) {
	b, ctx := newBackendForTest(t)
	gid := seedAndEmbed(t, b, map[int64][]float32{1: unitVec(768, 0)})

	if err := b.Delete(ctx, gid, []int64{1}); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	var n int
	if err := b.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM embeddings WHERE message_id = 1`).Scan(&n); err != nil {
		t.Fatalf("count embeddings: %v", err)
	}
	if n != 0 {
		t.Errorf("embeddings remaining: %d", n)
	}
	if err := b.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM vectors_vec_d768 WHERE message_id = 1`).Scan(&n); err != nil {
		t.Fatalf("count vectors: %v", err)
	}
	if n != 0 {
		t.Errorf("vectors remaining: %d", n)
	}
}

func TestBackend_Delete_EmptyIDsIsNoop(t *testing.T) {
	b, ctx := newBackendForTest(t)
	gid, err := b.CreateGeneration(ctx, "m", 768)
	if err != nil {
		t.Fatalf("CreateGeneration: %v", err)
	}
	if err := b.Delete(ctx, gid, nil); err != nil {
		t.Errorf("Delete(nil): %v", err)
	}
	if err := b.Delete(ctx, gid, []int64{}); err != nil {
		t.Errorf("Delete(empty): %v", err)
	}
}

func TestBackend_Delete_UnknownGeneration(t *testing.T) {
	b, ctx := newBackendForTest(t)
	err := b.Delete(ctx, vector.GenerationID(9999), []int64{1})
	if !errors.Is(err, vector.ErrUnknownGeneration) {
		t.Errorf("err = %v, want ErrUnknownGeneration", err)
	}
}

func TestBackend_Stats_CountsCorrectly(t *testing.T) {
	b, ctx := newBackendForTest(t)
	gid := seedAndEmbed(t, b, map[int64][]float32{1: unitVec(768, 0)})

	s, err := b.Stats(ctx, gid)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if s.EmbeddingCount != 1 {
		t.Errorf("EmbeddingCount=%d want 1", s.EmbeddingCount)
	}
	if s.PendingCount != 0 {
		t.Errorf("PendingCount=%d want 0", s.PendingCount)
	}
}

func TestBackend_Stats_PendingCountAfterCreate(t *testing.T) {
	b, ctx := newBackendForTest(t)
	gid, err := b.CreateGeneration(ctx, "m", 768)
	if err != nil {
		t.Fatalf("CreateGeneration: %v", err)
	}
	// CreateGeneration seeds 1 pending row for the one pre-seeded message.
	s, err := b.Stats(ctx, gid)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if s.EmbeddingCount != 0 {
		t.Errorf("EmbeddingCount=%d want 0", s.EmbeddingCount)
	}
	if s.PendingCount != 1 {
		t.Errorf("PendingCount=%d want 1", s.PendingCount)
	}
}

func TestBackend_Stats_AggregateAcrossGenerations(t *testing.T) {
	// When gen == 0, Stats returns counts across ALL generations.
	b, ctx := newBackendForTest(t)
	_ = seedAndEmbed(t, b, map[int64][]float32{1: unitVec(768, 0)})

	s, err := b.Stats(ctx, vector.GenerationID(0))
	if err != nil {
		t.Fatalf("Stats(0): %v", err)
	}
	if s.EmbeddingCount != 1 {
		t.Errorf("aggregate EmbeddingCount=%d want 1", s.EmbeddingCount)
	}
}

// TestBackend_Upsert_UpdatesMessageCount verifies that
// index_generations.message_count tracks the number of embedded
// messages after both the initial insert and subsequent re-upsert /
// delete. Without this, ActiveGeneration().MessageCount stays at zero
// regardless of how many chunks have been written.
func TestBackend_Upsert_UpdatesMessageCount(t *testing.T) {
	b, ctx := newBackendForTest(t)

	gid, err := b.CreateGeneration(ctx, "m", 768)
	if err != nil {
		t.Fatalf("CreateGeneration: %v", err)
	}

	// Initially zero.
	bg, err := b.BuildingGeneration(ctx)
	if err != nil {
		t.Fatalf("BuildingGeneration: %v", err)
	}
	if bg.MessageCount != 0 {
		t.Errorf("initial MessageCount=%d, want 0", bg.MessageCount)
	}

	// Upsert three chunks → count 3.
	chunks := []vector.Chunk{
		{MessageID: 1, Vector: unitVec(768, 0), SourceCharLen: 10},
		{MessageID: 2, Vector: unitVec(768, 1), SourceCharLen: 20},
		{MessageID: 3, Vector: unitVec(768, 2), SourceCharLen: 30},
	}
	if err := b.Upsert(ctx, gid, chunks); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	bg, err = b.BuildingGeneration(ctx)
	if err != nil {
		t.Fatalf("BuildingGeneration: %v", err)
	}
	if bg.MessageCount != 3 {
		t.Errorf("after initial Upsert: MessageCount=%d, want 3", bg.MessageCount)
	}

	// Re-upsert the same messages (update, not insert) → count stays 3.
	if err := b.Upsert(ctx, gid, chunks[:2]); err != nil {
		t.Fatalf("re-Upsert: %v", err)
	}
	bg, err = b.BuildingGeneration(ctx)
	if err != nil {
		t.Fatalf("BuildingGeneration: %v", err)
	}
	if bg.MessageCount != 3 {
		t.Errorf("after re-Upsert: MessageCount=%d, want 3", bg.MessageCount)
	}

	// Delete one → count drops to 2.
	if err := b.Delete(ctx, gid, []int64{2}); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	bg, err = b.BuildingGeneration(ctx)
	if err != nil {
		t.Fatalf("BuildingGeneration: %v", err)
	}
	if bg.MessageCount != 2 {
		t.Errorf("after Delete: MessageCount=%d, want 2", bg.MessageCount)
	}
}

// TestBackend_Stats_UnknownGeneration confirms that passing a non-zero
// generation id that doesn't exist returns an error wrapping
// vector.ErrUnknownGeneration, rather than silently reporting 0 counts
// (which would be indistinguishable from a valid-but-empty generation).
func TestBackend_Stats_UnknownGeneration(t *testing.T) {
	b, ctx := newBackendForTest(t)

	_, err := b.Stats(ctx, vector.GenerationID(9999))
	if err == nil {
		t.Fatal("Stats on unknown generation: want error, got nil")
	}
	if !errors.Is(err, vector.ErrUnknownGeneration) {
		t.Errorf("error = %v, want wrapping ErrUnknownGeneration", err)
	}
}

func TestBackend_LoadVector(t *testing.T) {
	b, ctx := newBackendForTest(t)
	gid, err := b.CreateGeneration(ctx, "m", 768)
	if err != nil {
		t.Fatalf("CreateGeneration: %v", err)
	}

	vec := make([]float32, 768)
	for i := range vec {
		vec[i] = float32(i) * 0.01
	}
	chunks := []vector.Chunk{{MessageID: 1, Vector: vec, SourceCharLen: 42}}
	if err := b.Upsert(ctx, gid, chunks); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := b.ActivateGeneration(ctx, gid); err != nil {
		t.Fatalf("ActivateGeneration: %v", err)
	}

	got, err := b.LoadVector(ctx, 1)
	if err != nil {
		t.Fatalf("LoadVector: %v", err)
	}
	if len(got) != 768 {
		t.Fatalf("len=%d, want 768", len(got))
	}
	for i, v := range got {
		if v != vec[i] {
			t.Fatalf("mismatch at i=%d: got %f, want %f", i, v, vec[i])
		}
	}
}

func TestBackend_LoadVector_NotEmbedded(t *testing.T) {
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
	if err := b.ActivateGeneration(ctx, gid); err != nil {
		t.Fatalf("ActivateGeneration: %v", err)
	}

	_, err = b.LoadVector(ctx, 999)
	if err == nil {
		t.Fatal("LoadVector for missing message should error")
	}
}

func TestBackend_LoadVector_NoActive(t *testing.T) {
	b, ctx := newBackendForTest(t)
	_, err := b.LoadVector(ctx, 1)
	if err == nil || !errors.Is(err, vector.ErrNoActiveGeneration) {
		t.Fatalf("want ErrNoActiveGeneration, got %v", err)
	}
}
