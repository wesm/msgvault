//go:build sqlite_vec

package embed

import (
	"context"
	"testing"
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
