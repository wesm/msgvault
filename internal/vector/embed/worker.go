package embed

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/wesm/msgvault/internal/vector"
)

// EmbeddingClient is the subset of *Client used by Worker; allowing tests
// to inject a fake.
type EmbeddingClient interface {
	Embed(ctx context.Context, inputs []string) ([][]float32, error)
}

// WorkerDeps bundles the collaborators a Worker needs. Backend, VectorsDB,
// MainDB, and Client are required; the remaining fields have sensible
// defaults when zero: BatchSize defaults to 32, StaleThreshold defaults to
// 10 minutes, Log defaults to slog.Default().
type WorkerDeps struct {
	Backend        vector.Backend
	VectorsDB      *sql.DB
	MainDB         *sql.DB
	Client         EmbeddingClient
	Preprocess     PreprocessConfig
	MaxInputChars  int
	BatchSize      int
	StaleThreshold time.Duration // default 10 minutes
	Log            *slog.Logger
}

// Worker drives one building generation from claimed pending rows to
// persisted embeddings. A single Worker is safe for sequential use; to
// parallelize, construct multiple workers that share the same Backend
// and DB handles.
type Worker struct {
	deps WorkerDeps
	q    *Queue
}

// NewWorker constructs a Worker, applying defaults for BatchSize (32),
// StaleThreshold (10 minutes), and Log (slog.Default()).
func NewWorker(d WorkerDeps) *Worker {
	if d.Log == nil {
		d.Log = slog.Default()
	}
	if d.BatchSize == 0 {
		d.BatchSize = 32
	}
	if d.StaleThreshold == 0 {
		d.StaleThreshold = 10 * time.Minute
	}
	return &Worker{deps: d, q: NewQueue(d.VectorsDB)}
}

// RunResult summarizes the outcome of RunOnce.
type RunResult struct {
	Claimed, Succeeded, Failed, Truncated int
}

// msgText is the per-message preprocessed input to the embedder, carried
// from fetch through to Chunk construction.
type msgText struct {
	ID    int64
	Text  string
	Chars int
	Trunc bool
}

// ReclaimStale releases claims older than StaleThreshold so crashed
// workers don't leave rows stuck. Call at startup before RunOnce.
// Returns the number of rows reclaimed.
func (w *Worker) ReclaimStale(ctx context.Context) (int, error) {
	n, err := w.q.ReclaimStale(ctx, w.deps.StaleThreshold)
	if err != nil {
		return 0, fmt.Errorf("reclaim stale: %w", err)
	}
	return n, nil
}

// RunOnce drains the queue for the given generation until empty,
// releasing claimed rows on embed or upsert error so another worker can
// retry them. Returns when pending is empty or ctx is cancelled.
func (w *Worker) RunOnce(ctx context.Context, gen vector.GenerationID) (RunResult, error) {
	var res RunResult
	for {
		if err := ctx.Err(); err != nil {
			return res, fmt.Errorf("RunOnce: %w", err)
		}
		ids, token, err := w.q.Claim(ctx, gen, w.deps.BatchSize)
		if err != nil {
			return res, fmt.Errorf("claim: %w", err)
		}
		if len(ids) == 0 {
			return res, nil
		}
		res.Claimed += len(ids)

		chunks, truncated, err := w.embedBatch(ctx, ids)
		if err != nil {
			res.Failed += len(ids)
			if rerr := w.q.Release(ctx, gen, token, ids); rerr != nil {
				w.deps.Log.Error("release after embed failure", "error", rerr)
			}
			w.deps.Log.Warn("embed batch failed", "gen", gen, "ids", len(ids), "error", err)
			continue
		}
		res.Truncated += truncated

		if err := w.deps.Backend.Upsert(ctx, gen, chunks); err != nil {
			res.Failed += len(ids)
			if rerr := w.q.Release(ctx, gen, token, ids); rerr != nil {
				w.deps.Log.Error("release after upsert failure", "error", rerr)
			}
			w.deps.Log.Error("upsert failed", "gen", gen, "ids", len(ids), "error", err)
			continue
		}
		// Complete acknowledges work via (gen, msg, claim_token) so a
		// stale worker whose claim was already reclaimed cannot wipe
		// the queue row belonging to the newer worker. A row count
		// mismatch just means the stale-completion case hit us — the
		// newer worker's claim will still drive a later success.
		if cerr := w.q.Complete(ctx, gen, token, ids); cerr != nil {
			w.deps.Log.Error("complete failed", "error", cerr, "gen", gen, "ids", len(ids))
		}
		res.Succeeded += len(ids)
	}
}

// embedBatch fetches subject/body for ids, preprocesses each, calls the
// embedding client, and assembles the resulting chunks.
func (w *Worker) embedBatch(ctx context.Context, ids []int64) ([]vector.Chunk, int, error) {
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}
	query := fmt.Sprintf(`
        SELECT m.id, COALESCE(m.subject, ''), COALESCE(mb.body_text, '')
          FROM messages m
          LEFT JOIN message_bodies mb ON mb.message_id = m.id
         WHERE m.id IN (%s)`, strings.Join(placeholders, ","))

	rows, err := w.deps.MainDB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("fetch bodies: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var msgs []msgText
	var inputs []string
	for rows.Next() {
		var id int64
		var subject, body string
		if err := rows.Scan(&id, &subject, &body); err != nil {
			return nil, 0, fmt.Errorf("scan message row: %w", err)
		}
		txt, trunc := Preprocess(subject, body, w.deps.MaxInputChars, w.deps.Preprocess)
		msgs = append(msgs, msgText{ID: id, Text: txt, Chars: len(txt), Trunc: trunc})
		inputs = append(inputs, txt)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate message rows: %w", err)
	}
	if len(msgs) == 0 {
		return nil, 0, fmt.Errorf("no messages found for ids %v", ids)
	}

	start := time.Now()
	vecs, err := w.deps.Client.Embed(ctx, inputs)
	if err != nil {
		return nil, 0, fmt.Errorf("embed: %w", err)
	}
	w.deps.Log.Debug("embed batch",
		"count", len(vecs), "chars", totalChars(msgs), "duration_ms", time.Since(start).Milliseconds())

	if len(vecs) != len(msgs) {
		return nil, 0, fmt.Errorf("embedder returned %d vectors for %d inputs", len(vecs), len(msgs))
	}

	truncated := 0
	chunks := make([]vector.Chunk, 0, len(vecs))
	for i, m := range msgs {
		if m.Trunc {
			truncated++
		}
		chunks = append(chunks, vector.Chunk{
			MessageID:     m.ID,
			Vector:        vecs[i],
			SourceCharLen: m.Chars,
			Truncated:     m.Trunc,
		})
	}
	return chunks, truncated, nil
}

func totalChars(ms []msgText) int {
	n := 0
	for _, m := range ms {
		n += m.Chars
	}
	return n
}
