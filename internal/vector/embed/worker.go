package embed

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/wesm/msgvault/internal/mime"
	"github.com/wesm/msgvault/internal/vector"
)

// EmbeddingClient is the subset of *Client used by Worker; allowing tests
// to inject a fake.
type EmbeddingClient interface {
	Embed(ctx context.Context, inputs []string) ([][]float32, error)
}

// WorkerDeps bundles the collaborators a Worker needs. Backend, VectorsDB,
// MainDB, and Client are required; the remaining fields have sensible
// defaults when zero: BatchSize defaults to 32, StaleThreshold is
// auto-derived from EmbedTimeout × EmbedMaxRetries with a 10-minute
// floor (see NewWorker), MaxConsecutiveFailures defaults to 5, Log
// defaults to slog.Default().
type WorkerDeps struct {
	Backend        vector.Backend
	VectorsDB      *sql.DB
	MainDB         *sql.DB
	Client         EmbeddingClient
	Preprocess     PreprocessConfig
	MaxInputChars  int
	BatchSize      int
	StaleThreshold time.Duration
	// EmbedTimeout and EmbedMaxRetries inform the StaleThreshold
	// auto-derivation: a single batch can legitimately stay claimed for
	// up to Timeout × MaxRetries (MaxRetries is the embed.Client's
	// total-attempts count, not retries-after-the-first) before the
	// worker gives up, so reclaim must wait longer than that to avoid
	// reclaiming live work. Both are read only when StaleThreshold is
	// zero; EmbedMaxRetries=0 is normalized to 3 to match
	// embed.NewClient's default — see derivedStaleThreshold.
	EmbedTimeout    time.Duration
	EmbedMaxRetries int
	// MaxConsecutiveFailures caps the number of consecutive batch
	// failures (embed error or upsert error) before RunOnce gives up
	// and returns an error. A successful batch resets the counter.
	// Default 5.
	MaxConsecutiveFailures int
	Log                    *slog.Logger
	// TotalPending is the queue depth at run start, used by a Progress
	// callback (if any) to report percent done and ETA. Zero disables
	// the denominator — Progress still fires but leaves ETA empty.
	TotalPending int
	// Progress, if non-nil, is called once after each fully-successful
	// batch (upsert + complete both ok) with cumulative and per-batch
	// stats. Failed or partially-drained batches do not fire Progress,
	// keeping the semantics of "this many messages are now embedded"
	// unambiguous. Callbacks run on the worker goroutine; rate-limit
	// inside the callback if output is expensive.
	Progress func(ProgressReport)
}

// ProgressReport captures the state of a RunOnce at the completion of
// one successful batch. BatchElapsed is end-to-end for the batch
// (fetch + preprocess + embed + upsert + complete), matching what
// RunElapsed accumulates — so the two ratios agree.
type ProgressReport struct {
	Done         int
	TotalPending int
	BatchMsgs    int
	BatchChars   int
	BatchElapsed time.Duration
	RunElapsed   time.Duration
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
// StaleThreshold (auto-derived; see derivedStaleThreshold),
// MaxConsecutiveFailures (5), and Log (slog.Default()).
func NewWorker(d WorkerDeps) *Worker {
	if d.Log == nil {
		d.Log = slog.Default()
	}
	if d.BatchSize == 0 {
		d.BatchSize = 32
	}
	if d.StaleThreshold == 0 {
		d.StaleThreshold = derivedStaleThreshold(d.EmbedTimeout, d.EmbedMaxRetries)
	}
	if d.MaxConsecutiveFailures == 0 {
		d.MaxConsecutiveFailures = 5
	}
	return &Worker{deps: d, q: NewQueue(d.VectorsDB)}
}

// derivedStaleThreshold picks a default StaleThreshold from the
// embedder's per-request timeout and retry count, with a 10-minute
// floor. A claim must outlive at least one full retry budget
// (timeout × attempts) — anything less risks ReclaimStale pulling
// rows out from under a still-running embed call, which would then
// race a concurrent worker on the same batch and leave stale
// Complete tokens. The 2× safety factor absorbs scheduler jitter
// and pre/post-call overhead. The floor preserves the historical
// default for the common case (Timeout=30s × 3 attempts = 3 minutes
// derived; floor wins).
//
// maxRetries here matches embed.Client's MaxRetries semantics: it is
// the TOTAL number of HTTP attempts (not retries-after-the-first).
// A zero value is normalized to 3 to mirror embed.NewClient's
// default. Without this, callers that set EmbedTimeout but leave
// EmbedMaxRetries at its zero value would derive a budget for a
// single attempt, while the client would actually try up to three
// times — and ReclaimStale could pull live claims out from under a
// retrying embed call.
func derivedStaleThreshold(timeout time.Duration, maxRetries int) time.Duration {
	const floor = 10 * time.Minute
	if timeout <= 0 {
		return floor
	}
	attempts := maxRetries
	if attempts == 0 {
		attempts = 3
	}
	if attempts < 1 {
		attempts = 1
	}
	derived := 2 * timeout * time.Duration(attempts)
	if derived < floor {
		return floor
	}
	return derived
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
//
// Returns an error when consecutive batch failures reach
// MaxConsecutiveFailures, so a persistently misconfigured embedder
// (bad credentials, unreachable endpoint) surfaces quickly instead of
// looping forever. A successful batch resets the failure counter.
func (w *Worker) RunOnce(ctx context.Context, gen vector.GenerationID) (RunResult, error) {
	var res RunResult
	consecutiveFailures := 0
	var lastErr error
	runStart := time.Now()
	// orphanDrainErr/orphanDrainCount preserve the latest orphan-drain
	// failure across iterations so we can surface it on the empty-claim
	// exit. Without this, a Complete() failure on orphan rows would be
	// logged but invisible to the caller — and if those orphans were
	// the last queue rows, the next Claim returns empty and RunOnce
	// would falsely report a clean drain even though stuck claimed
	// rows persist until ReclaimStale (~10 min later).
	var orphanDrainErr error
	var orphanDrainCount int
	for {
		if err := ctx.Err(); err != nil {
			return res, fmt.Errorf("RunOnce: %w", err)
		}
		batchStart := time.Now()
		ids, token, err := w.q.Claim(ctx, gen, w.deps.BatchSize)
		if err != nil {
			return res, fmt.Errorf("claim: %w", err)
		}
		if len(ids) == 0 {
			if orphanDrainErr != nil {
				return res, fmt.Errorf(
					"orphan-drain failed for %d row(s); they remain claimed and will be recovered by ReclaimStale on the next run: %w",
					orphanDrainCount, orphanDrainErr)
			}
			return res, nil
		}
		res.Claimed += len(ids)

		eb, err := w.embedBatch(ctx, ids)
		if err != nil {
			res.Failed += len(ids)
			if rerr := w.q.Release(ctx, gen, token, ids); rerr != nil {
				w.deps.Log.Error("release after embed failure", "error", rerr)
			}
			w.deps.Log.Warn("embed batch failed", "gen", gen, "ids", len(ids), "error", err)
			consecutiveFailures++
			lastErr = err
			if consecutiveFailures >= w.deps.MaxConsecutiveFailures {
				return res, fmt.Errorf("embed worker aborting after %d consecutive failures: %w",
					consecutiveFailures, lastErr)
			}
			continue
		}
		res.Truncated += eb.truncated

		if len(eb.chunks) == 0 {
			// Nothing fetched (all ids in this batch were missing
			// from main DB). Drop the orphans and move on; failure
			// here counts toward MaxConsecutiveFailures because the
			// loop would otherwise busy-spin on a stuck claim until
			// ReclaimStale runs (10 min default).
			dropIDs := append(append([]int64(nil), eb.missing...), eb.empty...)
			if len(dropIDs) > 0 {
				if len(eb.missing) > 0 {
					w.deps.Log.Warn("pending messages missing from main DB",
						"gen", gen, "ids", eb.missing)
				}
				if len(eb.empty) > 0 {
					w.deps.Log.Warn("pending messages empty after preprocess",
						"gen", gen, "ids", eb.empty)
				}
				if cerr := w.q.Complete(ctx, gen, token, dropIDs); cerr != nil {
					res.Failed += len(dropIDs)
					w.deps.Log.Error("complete drop failed", "error", cerr,
						"gen", gen, "ids", len(dropIDs))
					consecutiveFailures++
					lastErr = cerr
					orphanDrainErr = cerr
					orphanDrainCount += len(dropIDs)
					if consecutiveFailures >= w.deps.MaxConsecutiveFailures {
						return res, fmt.Errorf("embed worker aborting after %d consecutive failures: %w",
							consecutiveFailures, lastErr)
					}
				}
			}
			continue
		}

		if err := w.deps.Backend.Upsert(ctx, gen, eb.chunks); err != nil {
			res.Failed += len(eb.embeddedIDs)
			if rerr := w.q.Release(ctx, gen, token, eb.embeddedIDs); rerr != nil {
				w.deps.Log.Error("release after upsert failure", "error", rerr)
			}
			w.deps.Log.Error("upsert failed", "gen", gen, "ids", len(eb.embeddedIDs), "error", err)
			consecutiveFailures++
			lastErr = err
			if consecutiveFailures >= w.deps.MaxConsecutiveFailures {
				return res, fmt.Errorf("embed worker aborting after %d consecutive failures: %w",
					consecutiveFailures, lastErr)
			}
			continue
		}
		// Complete acknowledges work via (gen, msg, claim_token) so a
		// stale worker whose claim was already reclaimed cannot wipe
		// the queue row belonging to the newer worker. Failure here
		// means the embedded rows stay claimed; ReclaimStale will
		// rescue them eventually but the next RunOnce would falsely
		// report a clean drain in the meantime — count the batch as
		// failed so the failure cap can short-circuit the loop.
		if cerr := w.q.Complete(ctx, gen, token, eb.embeddedIDs); cerr != nil {
			res.Failed += len(eb.embeddedIDs)
			w.deps.Log.Error("complete failed", "error", cerr,
				"gen", gen, "ids", len(eb.embeddedIDs))
			consecutiveFailures++
			lastErr = cerr
			if consecutiveFailures >= w.deps.MaxConsecutiveFailures {
				return res, fmt.Errorf("embed worker aborting after %d consecutive failures: %w",
					consecutiveFailures, lastErr)
			}
			continue
		}

		// Drop queue rows for messages that disappeared between
		// enqueue and claim. We do this AFTER embedded rows are
		// safely upserted and acknowledged so a Complete failure on
		// the orphans does not strand the valid embedded rows in a
		// claimed-but-unembedded state. Using Complete with our claim
		// token makes this a token-aware delete: we only remove rows
		// we still own. Failure here still counts as a batch failure
		// because the orphan rows would stay claimed until
		// ReclaimStale runs and falsely block the queue.
		dropIDs := append(append([]int64(nil), eb.missing...), eb.empty...)
		if len(dropIDs) > 0 {
			if len(eb.missing) > 0 {
				w.deps.Log.Warn("pending messages missing from main DB",
					"gen", gen, "ids", eb.missing)
			}
			if len(eb.empty) > 0 {
				w.deps.Log.Warn("pending messages empty after preprocess",
					"gen", gen, "ids", eb.empty)
			}
			if cerr := w.q.Complete(ctx, gen, token, dropIDs); cerr != nil {
				res.Failed += len(dropIDs)
				w.deps.Log.Error("complete drop failed", "error", cerr,
					"gen", gen, "ids", len(dropIDs))
				consecutiveFailures++
				lastErr = cerr
				orphanDrainErr = cerr
				orphanDrainCount += len(dropIDs)
				if consecutiveFailures >= w.deps.MaxConsecutiveFailures {
					// Embedded rows were already counted into
					// res.Succeeded above; record the orphan-drain
					// failure and abort.
					res.Succeeded += len(eb.embeddedIDs)
					return res, fmt.Errorf("embed worker aborting after %d consecutive failures: %w",
						consecutiveFailures, lastErr)
				}
				// Even though the orphan drain failed, the embedded
				// rows ARE done — count them. The orphan rows stay
				// claimed; orphanDrainErr is now set so the empty-
				// claim exit will surface the failure to the caller
				// instead of falsely reporting a clean drain.
				res.Succeeded += len(eb.embeddedIDs)
				continue
			}
		}
		res.Succeeded += len(eb.embeddedIDs)
		consecutiveFailures = 0
		if w.deps.Progress != nil {
			batchChars := 0
			for _, c := range eb.chunks {
				batchChars += c.SourceCharLen
			}
			w.deps.Progress(ProgressReport{
				Done:         res.Succeeded,
				TotalPending: w.deps.TotalPending,
				BatchMsgs:    len(eb.embeddedIDs),
				BatchChars:   batchChars,
				BatchElapsed: time.Since(batchStart),
				RunElapsed:   time.Since(runStart),
			})
		}
	}
}

// embedBatchResult carries the output of embedBatch. chunks and
// embeddedIDs are aligned by position and correspond to messages that
// were actually fetched and embedded. missing lists ids from the
// input that had no row in the messages table; empty lists ids whose
// content preprocessed to empty and therefore should not be sent to
// embedders that reject blank strings.
type embedBatchResult struct {
	chunks      []vector.Chunk
	embeddedIDs []int64
	missing     []int64
	empty       []int64
	truncated   int
}

// embedBatch fetches subject/body for ids, preprocesses each, calls the
// embedding client, and assembles the resulting chunks. Messages that
// vanished between enqueue and claim (e.g. the sync deleted them) are
// reported in the returned result's missing slice rather than causing
// a failure — the caller decides how to drain them from the queue.
func (w *Worker) embedBatch(ctx context.Context, ids []int64) (embedBatchResult, error) {
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}
	query := fmt.Sprintf(`
        SELECT m.id, COALESCE(m.subject, ''), COALESCE(mb.body_text, ''), COALESCE(mb.body_html, '')
          FROM messages m
          LEFT JOIN message_bodies mb ON mb.message_id = m.id
         WHERE m.id IN (%s)`, strings.Join(placeholders, ","))

	rows, err := w.deps.MainDB.QueryContext(ctx, query, args...)
	if err != nil {
		return embedBatchResult{}, fmt.Errorf("fetch bodies: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var msgs []msgText
	var inputs []string
	var empty []int64
	fetched := make(map[int64]struct{}, len(ids))
	for rows.Next() {
		var id int64
		var subject, bodyText, bodyHTML string
		if err := rows.Scan(&id, &subject, &bodyText, &bodyHTML); err != nil {
			return embedBatchResult{}, fmt.Errorf("scan message row: %w", err)
		}
		// Fall back to HTML-to-text when the plaintext body is empty —
		// HTML-only messages would otherwise get subject-only embeddings
		// and have materially worse semantic recall.
		body := bodyText
		if body == "" && bodyHTML != "" {
			body = mime.StripHTML(bodyHTML)
		}
		txt, trunc := Preprocess(subject, body, w.deps.MaxInputChars, w.deps.Preprocess)
		fetched[id] = struct{}{}
		if strings.TrimSpace(txt) == "" {
			empty = append(empty, id)
			continue
		}
		// Preprocess truncates by runes, so the recorded length must
		// also be a rune count. Using len(txt) (bytes) inflates
		// SourceCharLen by 2-4x for CJK / emoji / accented text and
		// breaks any downstream "did we truncate?" / "how big was the
		// input?" reasoning.
		msgs = append(msgs, msgText{ID: id, Text: txt, Chars: utf8.RuneCountInString(txt), Trunc: trunc})
		inputs = append(inputs, txt)
	}
	if err := rows.Err(); err != nil {
		return embedBatchResult{}, fmt.Errorf("iterate message rows: %w", err)
	}

	// Identify claimed ids that had no row in messages; we'll report
	// them back so the caller can drop them from the queue.
	var missing []int64
	for _, id := range ids {
		if _, ok := fetched[id]; !ok {
			missing = append(missing, id)
		}
	}

	if len(msgs) == 0 {
		// All claimed ids are missing — return an empty result (no
		// chunks, no error). Caller handles the drop.
		return embedBatchResult{missing: missing, empty: empty}, nil
	}

	start := time.Now()
	vecs, err := w.deps.Client.Embed(ctx, inputs)
	if err != nil {
		return embedBatchResult{}, fmt.Errorf("embed: %w", err)
	}
	w.deps.Log.Debug("embed batch",
		"count", len(vecs), "chars", totalChars(msgs), "duration_ms", time.Since(start).Milliseconds())

	if len(vecs) != len(msgs) {
		return embedBatchResult{}, fmt.Errorf("embedder returned %d vectors for %d inputs", len(vecs), len(msgs))
	}

	truncated := 0
	chunks := make([]vector.Chunk, 0, len(vecs))
	embeddedIDs := make([]int64, 0, len(vecs))
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
		embeddedIDs = append(embeddedIDs, m.ID)
	}
	return embedBatchResult{
		chunks:      chunks,
		embeddedIDs: embeddedIDs,
		missing:     missing,
		empty:       empty,
		truncated:   truncated,
	}, nil
}

func totalChars(ms []msgText) int {
	n := 0
	for _, m := range ms {
		n += m.Chars
	}
	return n
}
