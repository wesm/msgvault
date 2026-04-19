# Vector Search Design

**Date:** 2026-04-19
**Status:** Draft — awaiting approval before implementation plan
**Scope:** Add hybrid (BM25 + vector) semantic search to msgvault, exposed via CLI, HTTP API, and MCP server. TUI behavior is unchanged. RAG / question-answering is out of scope for this design and will get its own spec.

---

## 1. Goal

Enable semantic and hybrid search over a user's email corpus (~2M messages) while keeping msgvault a single-binary Go application. Embedding generation and LLM inference are externalized to user-operated infrastructure (homelab, Mac Studio, workstation GPU) via OpenAI-compatible HTTP APIs. msgvault provides:

- Chunking and pre-processing of messages
- Vector storage and ANN search over the local corpus
- Hybrid fusion with the existing FTS5 index
- A stable query surface (CLI, HTTP API, MCP server) that downstream UIs can depend on

## 2. Non-Goals

- **Not shipping an embedded inference engine.** No llama.cpp, ONNX runtime, or model weights in the binary. All embedding generation is an HTTP call.
- **Not replacing TUI search.** The existing `/` search (fast + deep modes) stays unchanged. Hybrid search is additive.
- **Not implementing RAG / generation.** The retrieval surface is designed so a RAG layer can be added later, but answer generation (prompt templates, streaming, citations) is deferred to a future design.
- **Not implementing a reranker in MVP.** The architecture reserves an interface slot for a cross-encoder reranker (e.g. Qwen3-Reranker via the same endpoint) but does not implement one in this cycle.
- **Not implementing multi-chunk-per-message indexing.** One embedding per message for MVP; revisit based on measured truncation rates.
- **Not building a web UI.** A separate project will consume these APIs.

## 3. Design Principles

1. **Local-first, no cloud dependency.** Embedding endpoint runs on user hardware by default.
2. **Additive, not disruptive.** New code paths coexist with existing search; nothing regresses.
3. **Backend-agnostic.** sqlite-vec is the MVP backend, but the interface supports lance, duckdb-vss, and remote backends without rewrites.
4. **Separate storage.** Vectors live in their own `~/.msgvault/vectors.db` to avoid bloating the system-of-record database.
5. **Flexible models.** The embedding model is configurable; schema records model name and dimension so model rotation is auditable.
6. **Hardening before release.** The feature will be exercised end-to-end against a Mac Studio endpoint on the user's tailnet before being merged to main.

## 4. High-Level Architecture

```
┌─────────────────────────────────────────────────────────┐
│                        msgvault                          │
│                                                          │
│   CLI           HTTP API           MCP (stdio)           │
│    │                │                   │                │
│    └────────┬───────┴─────────┬─────────┘                │
│             │                 │                          │
│     ┌───────▼───────┐   ┌─────▼──────┐                   │
│     │ search.Engine │   │ embed      │                   │
│     │  (hybrid RRF) │   │  .Worker   │                   │
│     └───┬───────┬───┘   └──────┬─────┘                   │
│         │       │              │                         │
│   ┌─────▼──┐ ┌──▼──────────────▼─────┐  ┌─────────────┐  │
│   │ FTS5   │ │  vector.Backend       │  │ embeddings  │  │
│   │ (main) │ │   sqlitevec (MVP)     │◄─┤   HTTP      │  │
│   │        │ │   lance (future)      │  │   client    │  │
│   └────────┘ └───────────────────────┘  └──────┬──────┘  │
│       msgvault.db   vectors.db                 │         │
└────────────────────────────────────────────────┼─────────┘
                                                 │
                                     OpenAI-compatible
                                      /v1/embeddings
                                     (Ollama, llama.cpp-
                                      server, LM Studio,
                                      vLLM, OpenAI, ...)
```

### Package layout

```
internal/
  vector/
    backend.go            # Backend interface, Chunk, Hit, Filter types
    config.go             # Config shape and validation
    sqlitevec/
      backend.go          # sqlite-vec implementation
      schema.sql          # vectors.db schema
      migrate.go          # schema migrations for vectors.db
    embed/
      worker.go           # background embedding worker (called by scheduler)
      client.go           # OpenAI-compatible HTTP client
      preprocess.go       # quote/signature stripping, subject prepending
      batch.go            # batch claim + completion
    search/
      engine.go           # hybrid search orchestrator
      rrf.go              # reciprocal-rank fusion
      explain.go          # score breakdown for ?explain=1

cmd/msgvault/cmd/
  search.go               # existing — extended with --mode and --explain flags
  embed.go                # new — `msgvault embed` CLI (backfill + rebuild)
  # mcp already exists at internal/mcp/; command wiring stays in cmd root

internal/mcp/
  server.go               # existing — search_messages tool gains mode/explain args,
                          #   plus a new find_similar_messages tool and extended
                          #   get_stats fields (§9.3)

internal/api/
  handlers.go             # existing — the /api/v1/search handler gains a mode
                          #   query parameter; /api/v1/stats gains optional embedding
                          #   fields. No new route files; no /hybrid or /vector
                          #   endpoints.
```

## 5. Data Model

### 5.1 Changes to main database (`~/.msgvault/msgvault.db`)

None required. The authoritative pending queue is `vec.pending_embeddings` (§5.2), which is populated per generation. At `CreateGeneration` time, the builder seeds the queue with

```sql
SELECT id FROM messages WHERE deleted_from_source_at IS NULL
```

— the same deletion predicate that search uses (§5.3, `internal/query/sqlite.go`, `internal/search/parser.go`). Sync calls into the vector package to enqueue new messages for every non-retired generation; see §6.7.

Messages whose `deleted_from_source_at` is set *after* they were enqueued or embedded are not actively purged from `vectors.db` in MVP. Search excludes them via the deletion predicate on `messages`, so they are invisible anyway; the storage cost of carrying a few stale vectors is negligible and the next `--full-rebuild` drops them naturally (because the new generation re-seeds from `messages` with the predicate applied). Active purging on delete can be added later if needed; it is not a correctness issue, only a storage-hygiene one.

(Earlier drafts of this spec introduced a `needs_embedding` flag on `messages`. Dropped: it duplicated state, complicated crash recovery, and required an invariant linking flag state to queue contents that broke under dual-enqueue during rebuilds.)

### 5.2 New database (`~/.msgvault/vectors.db`)

Attached to the main SQLite connection as `vec` when vector search is enabled:

```sql
-- Index generations: each is a complete corpus re-embedding with a specific
-- model + dimension. Exactly one can be active at a time; at most one more
-- may be in progress (being built). Search always filters by the active
-- generation; writes target the building generation.
CREATE TABLE index_generations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    model           TEXT NOT NULL,
    dimension       INTEGER NOT NULL,
    fingerprint     TEXT NOT NULL,   -- "<model>:<dim>"; compared to config at search time
    started_at      INTEGER NOT NULL,
    completed_at    INTEGER,         -- NULL while building
    activated_at    INTEGER,         -- NULL until it becomes the active generation
    state           TEXT NOT NULL,   -- "building" | "ready" | "active" | "retired"
    message_count   INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX idx_active_generation ON index_generations(state) WHERE state = 'active';
CREATE UNIQUE INDEX idx_building_generation ON index_generations(state) WHERE state = 'building';

-- Per-message embedding metadata, scoped to a generation.
CREATE TABLE embeddings (
    generation_id    INTEGER NOT NULL REFERENCES index_generations(id) ON DELETE CASCADE,
    message_id       INTEGER NOT NULL,
    embedded_at      INTEGER NOT NULL,
    source_char_len  INTEGER NOT NULL,
    truncated        INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (generation_id, message_id)
);
CREATE INDEX idx_embeddings_msg ON embeddings(message_id);

-- One sqlite-vec virtual table per dimension, partitioned by generation_id.
-- Partition keys let us hold multiple generations at once without cross-
-- contamination during rebuilds. Table name is versioned by dimension so
-- generations with different dims can coexist during transition.
CREATE VIRTUAL TABLE vectors_vec_d768 USING vec0(
    generation_id INTEGER PARTITION KEY,
    message_id    INTEGER PRIMARY KEY,
    embedding     FLOAT[768]
);
-- A vectors_vec_d1024, etc. is created on demand when a generation requests it.

-- Embedding job history, mirrors sync_runs pattern
CREATE TABLE embed_runs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    generation_id INTEGER NOT NULL REFERENCES index_generations(id),
    started_at   INTEGER NOT NULL,
    ended_at     INTEGER,
    claimed      INTEGER NOT NULL DEFAULT 0,
    succeeded    INTEGER NOT NULL DEFAULT 0,
    failed       INTEGER NOT NULL DEFAULT 0,
    truncated    INTEGER NOT NULL DEFAULT 0,
    error        TEXT
);

-- Authoritative work queue: one row per (generation, message) pending
-- embedding. claimed_at is NULL when the row is available to any worker;
-- a timestamp means a worker has taken it but not yet committed the
-- resulting embedding. On crash, a recovery pass on startup releases
-- stale claims back to the pool (claimed_at older than stale_threshold).
CREATE TABLE pending_embeddings (
    generation_id INTEGER NOT NULL REFERENCES index_generations(id) ON DELETE CASCADE,
    message_id    INTEGER NOT NULL,
    enqueued_at   INTEGER NOT NULL,
    claimed_at    INTEGER,
    claim_token   TEXT,
    PRIMARY KEY (generation_id, message_id)
);
CREATE INDEX idx_pending_available
  ON pending_embeddings(generation_id, message_id)
  WHERE claimed_at IS NULL;
CREATE INDEX idx_pending_claims
  ON pending_embeddings(claimed_at)
  WHERE claimed_at IS NOT NULL;
```

Notes on design:
- **One active generation at a time.** Search resolves the active generation on every request and refuses to run vector/hybrid mode if the configured fingerprint does not match the active generation's fingerprint (see §6.7).
- **`vec0` partition keys** are the supported sqlite-vec pattern for multi-tenant / generational vectors; searches pass `WHERE generation_id = ?` as a cheap pre-filter.
- **Dimension-specific virtual tables** (`vectors_vec_d768`, `vectors_vec_d1024`, ...) avoid the single-dim-per-table constraint. Created on demand by the generation builder; dropped when no generations reference them.
- **`vectors.db` is fully derived.** It can be deleted any time; a subsequent `msgvault embed --full-rebuild` restores it from the system of record.

### 5.3 ATTACH pattern and fused query

When vector search is enabled, the main SQLite connection runs `ATTACH DATABASE '<vectors.db>' AS vec` on open. Hybrid queries fuse BM25 and ANN results in a single statement.

**Filters are pushed into both signal branches** so that a narrow filter (e.g. `label:Taxes` on 0.5% of the corpus) doesn't starve either signal's candidate pool. The pattern is: resolve the filtered message-ID set first, then constrain both signal CTEs to it.

**Filter values are resolved to IDs at the Go layer, not in SQL.** `--account you@gmail.com` becomes a `source_id` list looked up via `sources.identifier`; `from:alice@example.com` becomes a `participant_id` list looked up via `participants.email_address`; `label:INBOX` becomes a `label_id` list via `labels.name`. The SQL below then takes only pre-resolved ID lists, matching how `internal/query` already works today.

Concrete SQL against the real schema (`messages`, `message_labels`, `participants`, `sources`):

```sql
WITH
  -- Resolve all filters once into a filtered candidate set.
  -- List parameters (:source_ids, :sender_ids, :label_ids) are JSON arrays;
  -- SQLite's json_each unpacks them cheaply when non-empty, or the filter
  -- is skipped when the parameter is NULL.
  filtered AS (
    SELECT m.id
    FROM messages m
    WHERE (:source_ids IS NULL OR m.source_id IN (SELECT value FROM json_each(:source_ids)))
      AND (:sender_ids IS NULL OR m.sender_id IN (SELECT value FROM json_each(:sender_ids)))
      AND (:has_attachment IS NULL OR m.has_attachments = :has_attachment)
      AND (:after  IS NULL OR m.sent_at >= :after)
      AND (:before IS NULL OR m.sent_at <  :before)
      AND (:label_ids IS NULL OR EXISTS (
            SELECT 1 FROM message_labels ml
            WHERE ml.message_id = m.id
              AND ml.label_id IN (SELECT value FROM json_each(:label_ids))))
      AND m.deleted_from_source_at IS NULL
  ),

  -- BM25 over the filtered set. MATCH is pushed through the join.
  -- NOTE: SQLite's bm25() function cannot be used inside subqueries/joins,
  -- so we read fts.rank (the FTS5 built-in column that aliases to the
  -- default bm25 score). Lower rank = better match.
  bm25 AS (
    SELECT fts.rowid AS message_id,
           fts.rank AS bm25_raw,
           ROW_NUMBER() OVER (ORDER BY fts.rank) AS rnk
    FROM messages_fts fts
    JOIN filtered f ON f.id = fts.rowid
    WHERE messages_fts MATCH :fts_query
    LIMIT :k_per_signal
  ),

  -- ANN over the filtered set in the active generation.
  -- vec0 partition keys + rowid restriction give us filtered kNN.
  vec AS (
    SELECT v.message_id,
           v.distance AS vec_dist,
           ROW_NUMBER() OVER (ORDER BY v.distance) AS rnk
    FROM vec.vectors_vec_d768 v
    WHERE v.generation_id = :active_gen
      AND v.message_id IN (SELECT id FROM filtered)
      AND v.embedding MATCH :query_vec
      AND k = :k_per_signal
  ),

  fused AS (
    SELECT COALESCE(b.message_id, v.message_id) AS message_id,
           COALESCE(1.0 / (:rrf_k + b.rnk), 0.0) +
           COALESCE(1.0 / (:rrf_k + v.rnk), 0.0) AS rrf_score,
           b.bm25_raw AS bm25_score,
           CASE WHEN v.vec_dist IS NULL THEN NULL ELSE 1.0 - v.vec_dist END AS vector_score
    FROM bm25 b
    FULL OUTER JOIN vec v USING (message_id)
  )

SELECT m.id, m.subject, m.snippet, m.sent_at,
       f.rrf_score, f.bm25_score, f.vector_score
FROM fused f
JOIN messages m ON m.id = f.message_id
ORDER BY f.rrf_score DESC
LIMIT :limit;
```

Notes:
- **Filter pushdown is mandatory.** The previous draft of this spec applied filters after fusion, which silently starved recall on narrow filters (the global top-k signal candidates often contained zero filtered rows).
- **Pre-filter kNN via `message_id IN (SELECT id FROM filtered)`** is the sqlite-vec pattern for constrained ANN. For very narrow filtered sets this degenerates to brute force, which is cheap at those sizes anyway.
- **Subject boost** is applied as a post-fusion adjustment in application code (not the SQL), so it can depend on tokenized subject matching without further complicating the CTE.
- **`deleted_from_source_at IS NULL`** excludes messages deleted from their upstream source (Gmail, etc.) from search, matching existing `internal/query` behavior (see `internal/query/sqlite.go` and `internal/search/parser.go` `HideDeleted`).
- **Backends that do not live in SQLite** (lance, remote) perform two independent filtered queries and fuse in Go via `rrf.Fuse`.

## 6. Embedding Pipeline

### 6.1 Pre-processing (`internal/vector/embed/preprocess.go`)

For each message, produce the input string fed to the embedder:

1. Read `body_text` from `message_bodies`.
2. Strip quoted reply blocks: lines matching `^>` and the trailing `On <date>, <name> wrote:` preamble.
3. Strip signatures: everything after a standalone `-- \n` (RFC 3676 signature delimiter) or common patterns (`Sent from my iPhone`, etc. — small curated list).
4. Collapse whitespace.
5. Prepend `Subject: <subject>\n\n`.
6. Truncate to a configurable character limit (default 32 KB ≈ 8k tokens — safe for nomic-embed-text; models with larger context can go higher). Mark `truncated = 1` if cut.

The stripper is a plain-regex implementation in `preprocess.go`. It is intentionally simple; upgrading to a Mailgun-talon-style parser is future work.

### 6.2 HTTP client (`internal/vector/embed/client.go`)

A small OpenAI-compatible client supporting:
- `POST /v1/embeddings` with `{ "model": "...", "input": [<strings>] }`
- Optional `Authorization: Bearer <key>` header
- Configurable timeout, retries with exponential backoff on 5xx and network errors
- Dimension verification on first call (compared against configured dimension); error clearly if mismatch

### 6.3 Batch processing (`internal/vector/embed/batch.go`)

The worker operates on a single generation at a time — either the active one (incremental fill) or the building one (full rebuild). It uses a mark-then-complete pattern so that a crash between the HTTP call and the commit cannot silently lose work.

1. **Claim** up to `batch_size` message IDs atomically:
   ```sql
   UPDATE pending_embeddings
      SET claimed_at = :now, claim_token = :worker_token
    WHERE (generation_id, message_id) IN (
          SELECT generation_id, message_id FROM pending_embeddings
           WHERE generation_id = :g AND claimed_at IS NULL
           ORDER BY message_id
           LIMIT :batch_size)
    RETURNING message_id;
   ```
   Rows stay in `pending_embeddings`; only `claimed_at` flips. Commit.
2. **Load** `body_text` + `subject` from the main DB for each claimed ID; run preprocessing.
3. **Embed** via HTTP. This step is outside any DB transaction — can take seconds.
4. **Complete** in a single transaction on `vectors.db`:
   ```sql
   INSERT INTO embeddings (generation_id, message_id, embedded_at, source_char_len, truncated) VALUES ...;
   INSERT INTO vectors_vec_dN (generation_id, message_id, embedding) VALUES ...;
   DELETE FROM pending_embeddings
     WHERE generation_id = :g AND message_id IN :ids AND claim_token = :worker_token;
   ```
   Commit.
5. **Release on failure.** If step 3 or 4 errors, release the claims so another pass picks them up:
   ```sql
   UPDATE pending_embeddings SET claimed_at = NULL, claim_token = NULL
     WHERE generation_id = :g AND message_id IN :ids AND claim_token = :worker_token;
   ```
6. Record the batch result in `embed_runs`.

**Crash recovery at startup** — a single reclaim pass on every msgvault startup, before the worker runs:
```sql
UPDATE pending_embeddings
   SET claimed_at = NULL, claim_token = NULL
 WHERE claimed_at IS NOT NULL AND claimed_at < :now - :stale_threshold;
```
`stale_threshold` is conservative (default 10 minutes — much longer than any reasonable embed batch). Any claim older than that is assumed to belong to a crashed worker and is returned to the pool.

**Why claim-mark-not-delete.** The previous draft of this spec used `DELETE ... RETURNING` to claim rows and relied on a recovery scan of `needs_embedding = 1 MINUS existing embeddings` to reclaim on crash. That scan was unsafe under dual-enqueue: if the *active* worker had already completed a message while the *building* worker was mid-batch, the message's `needs_embedding` flag was clear but no building-generation embedding existed, so the scan would not requeue it. The mark-then-complete pattern removes that invariant entirely — the only source of truth for "is this pending" is `pending_embeddings.claimed_at IS NULL`, per generation.

**Duplicate-safety.** Step 4's INSERTs use `ON CONFLICT (generation_id, message_id) DO NOTHING` (or equivalent). In the narrow window where two workers both finish step 4 for the same message (impossible under single-process operation, but belt + suspenders for future multi-worker), the second wins silently and the DELETE is still correct.

### 6.4 Daemon integration

`msgvault serve` gains a new scheduled job alongside sync:

```toml
[vector.embed.schedule]
cron = "*/5 * * * *"   # every 5 minutes, or set to "" to disable
```

The scheduler runs the worker whenever the cron fires, and also after each successful sync completion (since sync likely enqueued new rows in `pending_embeddings`). The job is a no-op if nothing is pending.

### 6.5 CLI (`msgvault embed`)

```
msgvault embed [flags]
  --account EMAIL         # incremental only: prioritize draining this account's queue (repeatable)
  --limit N               # cap this run at N messages (for testing; partial runs resume automatically)
  --full-rebuild          # create a new generation and rebuild from scratch across ALL accounts
  --progress              # live progress bar (default when stderr is a tty)
  --yes                   # skip confirmation prompts
```

Run modes:

- **Default (incremental).** Fills in missing embeddings for the currently active generation by consuming the `pending_embeddings` queue. Used by the daemon and for manual catch-up. Accepts `--account` as a prioritization/narrowing filter on the queue — it does not change what has already been enqueued, only which rows this run drains.
- **`--full-rebuild`.** Starts a new generation using the currently-configured model+dimension. Builds into shadow tables in parallel with the active generation. When complete, atomically flips `state = 'active'` on the new generation and `'retired'` on the old. Old generation rows are purged after a brief grace period (configurable, default immediate). Prompts for confirmation unless `--yes` is passed.

**`--full-rebuild` is always corpus-wide.** A generation must cover every message returned by the search deletion predicate — i.e. `SELECT id FROM messages WHERE deleted_from_source_at IS NULL` — because it atomically replaces the active index, and anything missing becomes a silent gap at query time. Combining `--full-rebuild` with `--account` is therefore rejected with an explicit CLI error (`--account cannot be combined with --full-rebuild; generations are corpus-wide`). If a user wants to re-embed one account's messages after a preprocessing change short of a full rebuild, they can do that via an out-of-band `Delete` + re-enqueue in the active generation — exposed as `msgvault embed --reembed-account EMAIL` in a future revision, not MVP.

There is **no `--model` or `--endpoint` override flag**. Model rotation goes through config: the user updates `[vector.embeddings] model` / `dimension` in `config.toml`, then runs `msgvault embed --full-rebuild` to materialize a generation under the new model. Attempting `--full-rebuild` against config that disagrees with the existing active generation is expected and valid; attempting incremental embedding or search with such config is rejected (§6.7).

### 6.6 Error surfaces

- Endpoint unreachable → retry with backoff, ultimately leave rows in `pending_embeddings`, log at error level, record in `embed_runs`. Sync is not affected. Search continues to work against the prior active generation.
- Endpoint returns unexpected dimension → hard-fail the run, surface the error to `embed_runs.error` and the CLI output. The building generation is marked `retired` rather than activated; the prior active generation remains in service.
- Model not found → 404 from endpoint → hard-fail with a clear message pointing at the config key.

### 6.7 Model change / index staleness semantics

This is the invariant that keeps queries and corpus vectors aligned.

**Fingerprint:** `<model_name>:<dimension>`. Stored on every `index_generations` row and verified at query time against the running configuration.

**On msgvault startup (or config reload):**
1. Read `[vector.embeddings] model` and `dimension` from config.
2. Look up the active generation from `index_generations WHERE state = 'active'`.
3. If configured fingerprint matches active generation → vector/hybrid search is enabled.
4. If they differ, or no generation is active:
   - Log a clear warning at startup.
   - `/api/v1/search?mode=vector|hybrid` and MCP equivalents return `index_stale` (or `index_building` / `vector_not_enabled` as appropriate).
   - `fts` mode and the existing TUI search paths continue to work normally.
   - The background embedding worker (if scheduled) does not touch the old generation; it sits idle until a `--full-rebuild` is initiated.
5. If a generation is in `state = 'building'` **and** no active generation exists (first-ever build), search returns `index_building` for vector/hybrid. A rebuild with an existing active generation is transparent to search (see "Rebuild swap" and "Guarantee" below).

**Explicit re-embed required to change models.** There is no auto-invalidation behavior that silently drops old embeddings. The user must run `msgvault embed --full-rebuild`, which is explicit and observable.

**Sync-side interaction — dual-enqueue during rebuilds.** When the sync pipeline writes a new message *and* that message satisfies the deletion predicate (`deleted_from_source_at IS NULL`), it inserts a row into `pending_embeddings` for *every non-retired generation*. Messages imported with `deleted_from_source_at` already set (e.g. some Apple Mail .emlx imports) are not enqueued, matching search behavior. The predicate is checked once at enqueue time; if it flips later, see the "stale vectors" note in §5.1.

- If only `state = 'active'` exists: one row, to the active generation. Normal steady state.
- If both `'active'` and `'building'` exist: two rows, one per generation. The building generation therefore stays current with ingest while it backfills, so the swap does not drop messages that arrived during the rebuild.
- If only `'building'` exists (first-ever rebuild, no prior active): one row, to the building generation.
- If nothing is non-retired: no rows enqueued; new messages are simply part of the `messages` table and are picked up automatically the next time `--full-rebuild` seeds a generation's initial queue from `SELECT id FROM messages WHERE deleted_from_source_at IS NULL`.

Dual-enqueue costs one extra embedding API call per new message during rebuild. New-message rate is negligible compared to the 2M-message backfill, so this is acceptable. Optimizing this (single call, write to both generations) is possible only when the two generations share model+dimension; deferred to a future pass.

**Rebuild swap.** When the building generation's `pending_embeddings` queue for that generation is empty *and* the embedding worker has no in-flight batches for it, the builder runs one final drain pass (re-enqueueing any messages that were created in the last few seconds, just to be safe) and then atomically:

```sql
BEGIN;
UPDATE index_generations SET state = 'retired', completed_at = :now WHERE state = 'active';
UPDATE index_generations SET state = 'active', activated_at = :now, completed_at = :now WHERE state = 'building';
COMMIT;
-- Then, outside the transaction: purge retired rows (or delay per §14.4 grace-period option).
```

**Rebuild abort.** If `msgvault embed --full-rebuild` is cancelled or crashes, the builder marks `state = 'building'` → `'retired'`, purges its rows, and leaves the active generation untouched. A subsequent `--full-rebuild` starts clean.

**Guarantee.** At all times, at most one generation is `'active'` and at most one is `'building'`. Vector/hybrid search runs against the active generation. `index_building` (§9.2) is returned only when there is no active generation — i.e. a first-ever rebuild that has not yet completed. A rebuild in progress while an active generation exists is invisible to search.

## 7. Search Pipeline

### 7.1 Modes

- **`fts`** — existing behavior (FTS5-only). Default on every public surface for backwards compatibility.
- **`vector`** — pure ANN query over the active-generation vectors (`vectors_vec_dN`), ordered by cosine distance.
- **`hybrid`** — RRF fusion of BM25 and vector results. Opt-in per request.

**Default mode is `fts` everywhere.** The CLI, HTTP, and MCP surfaces do not honor a config-level default override in MVP: every client that wants `hybrid` or `vector` specifies it explicitly per call. This keeps existing clients (TUI, already-deployed scripts) untouched and avoids drift between "what the server thinks is the default" and "what the client sees". If a persistent personal preference is desired later, a shell alias for `msgvault search` is a better home for it than a server-side config.

### 7.2 RRF fusion

- Fetch top `k_per_signal` (default 100) from each signal with filters applied.
- Rank each list by its native score (BM25 ascending, cosine distance ascending).
- RRF score: `Σ 1 / (k + rank)` with `k = 60` (configurable via `[vector.search] rrf_k`).
- Optional subject boost: if any query token appears literally in `messages.subject`, multiply that message's BM25 rank contribution by `subject_boost` (default 2.0). Applied to the fused score, not a separate pass.
- Return top `limit` after fusion.

### 7.3 Filters

Supported at the SQL level (applied *before* RRF in the fused CTE):
- `account` (sources.email) — one or many
- `from` (sender email)
- `label` (message_labels EXISTS) — one or many
- `has_attachment` (boolean)
- `after` / `before` (ISO-8601 dates, match against `sent_at`)

Filters are applied to both signals symmetrically to avoid lopsided candidate pools.

### 7.4 Response shape and pagination

**Pagination rules (one explicit rule per mode):**

- `mode=fts` — `page` + `page_size` parameters work exactly as today (`internal/api/handlers.go`, `internal/remote/store.go`). No change to the existing endpoint contract. Response includes `total` (exact, from BM25).
- `mode=vector` and `mode=hybrid` — **no pagination in MVP.** Only `page_size` is honored, capped at `max_page_size_hybrid` (default 50, hard max 100). Explicitly **`page > 1` is rejected with HTTP 400 `pagination_unsupported`**. Response includes neither `total` nor `has_more`; it reports `returned` (count of results in the payload) and — if the candidate pool was saturated — a boolean `pool_saturated` that tells the client the pool filled up and they should refine their query rather than ask for more.

Rationale for the stricter vector/hybrid rule: the algorithm pulls a fixed-size candidate pool (`k_per_signal`) from each signal. Deep paging into ANN results is rarely meaningful; if a client wants more results, refining the query is a better answer than paging. Keeping the contract tight in MVP avoids implicit "paginate by re-querying with a bigger pool" hacks that surface inconsistent results as the pool grows.

CLI mapping:
- `--limit N` maps to `page_size=N` in all modes (via the existing CLI contract).
- `--offset` is supported for `fts` only (maps to `page = offset/page_size + 1`, as today).
- Passing `--offset > 0` with `--mode=vector|hybrid` errors at the CLI before any HTTP round-trip.

**Response bodies:**

For `mode=fts` — unchanged from today. Existing clients see identical output.

For `mode=vector` / `mode=hybrid`:
```json
{
  "query": "...", "mode": "hybrid", "took_ms": 82,
  "returned": 20,
  "pool_saturated": false,
  "generation": { "id": 3, "model": "nomic-embed-text-v1.5", "dimension": 768 },
  "results": [{
    "message_id": 12345,
    "subject": "...",
    "from": { "email": "...", "name": "..." },
    "snippet": "...",
    "sent_at": "2024-...",
    "labels": ["INBOX"],
    "account": "you@gmail.com",
    "has_attachments": false,
    "size_bytes": 2048
  }]
}
```

With `explain=1`, each result gains:
```json
"score": { "rrf": 0.0234, "bm25": 12.3, "vector": 0.87, "subject_boosted": true }
```

The `generation` object documents which index answered the query; useful for debugging and for clients that cache results across model rotations.

## 8. Backend Abstraction

### 8.1 Interface

```go
package vector

// GenerationID is the opaque identity of an index generation. Every write
// and read goes to exactly one generation. Callers resolve the target
// generation before calling the backend: the embedding worker writes to
// the generation it is filling; the search engine reads from the active
// generation.
type GenerationID int64

type Chunk struct {
    MessageID      int64
    Vector         []float32
    SourceCharLen  int
    Truncated      bool
}

type Filter struct {
    Accounts      []string
    From          string
    Labels        []string
    HasAttachment *bool
    After, Before *time.Time
}

type Hit struct {
    MessageID  int64
    Score      float64   // native to the backend (cosine distance, dot product, etc.)
    Rank       int
}

type Backend interface {
    // CreateGeneration allocates a new generation for model+dimension and
    // returns its ID. The backend creates dimension-specific storage as
    // needed (e.g. a new vectors_vec_dN virtual table). Idempotent:
    // returns an existing building generation for the same params if one
    // is open.
    CreateGeneration(ctx context.Context, model string, dimension int) (GenerationID, error)

    // ActivateGeneration atomically swaps the currently-active generation
    // with the given generation. Previously-active generation is marked
    // retired in the same transaction. Purges retired rows unless the
    // caller passes a grace period (§14.4).
    ActivateGeneration(ctx context.Context, gen GenerationID) error

    // RetireGeneration marks a generation retired (aborted rebuild or
    // post-swap cleanup) and deletes its rows.
    RetireGeneration(ctx context.Context, gen GenerationID) error

    // Upsert writes chunks to the given generation. Transactional.
    Upsert(ctx context.Context, gen GenerationID, chunks []Chunk) error

    // Search returns top-k message hits for the query vector within the
    // given generation, after applying filter.
    Search(ctx context.Context, gen GenerationID, queryVec []float32, k int, filter Filter) ([]Hit, error)

    // Delete removes embeddings for the given messages within the given
    // generation. Used to requeue on failure, not for user-facing deletes.
    Delete(ctx context.Context, gen GenerationID, messageIDs []int64) error

    // Stats returns counts and storage size for the given generation.
    // Pass 0 to get totals across all generations.
    Stats(ctx context.Context, gen GenerationID) (Stats, error)
}

// FusingBackend is an optional capability implemented by backends that can
// fuse with the main-database FTS5 index in a single SQL query. The search
// engine checks for this via type assertion. sqlite-vec implements it;
// lance/remote backends do not.
type FusingBackend interface {
    Backend
    FusedSearch(ctx context.Context, req FusedRequest) ([]FusedHit, error)
}

type FusedRequest struct {
    FTSQuery        string         // tokenized FTS5 MATCH expression
    QueryVec        []float32      // query embedding
    Generation      GenerationID   // active index generation
    KPerSignal      int            // candidate pool size per signal
    Limit           int            // page_size for this request; no Offset (vector/hybrid is single-page in MVP)
    RRFK            int            // reciprocal-rank-fusion k
    Filter          Filter
}

type FusedHit struct {
    MessageID               int64
    RRFScore                float64
    BM25Score, VectorScore  float64  // NaN if the message did not appear in that signal
    SubjectBoosted          bool
}
```

### 8.2 MVP implementation: `sqlitevec.Backend`

- Owns the `vectors.db` file.
- Connects via `mattn/go-sqlite3` with the `sqlite-vec` extension available (build-flag-gated; see open question §14.5).
- Runs migrations on open.
- Implements both `Backend` and `FusingBackend`. The fused path is taken automatically by the search engine when the active backend satisfies `FusingBackend`; otherwise the engine falls back to two independent queries + Go-side RRF.

### 8.3 Future backends (not implemented in this cycle)

- `lance.Backend` — Go SDK at `github.com/lancedb/lancedb-go`; storage at `~/.msgvault/lance/`. Fusion is Go-side.
- `duckdbvss.Backend` — reuses existing DuckDB dependency; noted risks around experimental persistence.
- `remote.Backend` — generic HTTP client for Qdrant / Weaviate / hosted services; primarily for users who already run a vector DB.

## 9. API Surface

### 9.1 CLI — extends existing `msgvault search`

The existing `msgvault search` command already supports Gmail-style query syntax (`from:`, `to:`, `subject:`, `label:`, `has:attachment`, `before:`, `after:`, `older_than:`, `newer_than:`, `larger:`, `smaller:`) and the `--limit / --offset / --json / --account` flags (see `cmd/msgvault/cmd/search.go`). **We extend it, not replace it:**

New flags:
```
--mode=fts|vector|hybrid      # default: fts (current behavior, unchanged)
--explain                     # include per-signal scores in output
```

Semantics of `--mode` when the Gmail-syntax query is parsed via `internal/search`:
- Free-text terms → used as the FTS query *and* as the text embedded for the vector query (subject/body agnostic; the user's raw intent goes through).
- Operators (`from:`, `label:`, `after:`, etc.) → structured filters applied equally to both signals in hybrid mode.
- In `vector` mode, operators still apply as filters; free text becomes the query vector.
- In `fts` mode, behavior is bit-for-bit identical to today.

Guardrails:
- `--mode=hybrid|vector` fails clearly if `[vector]` is not configured, or if the active generation's fingerprint disagrees with the configured model (see §6.7).
- No `--model` override on `search`; querying with a different model than the active index is a footgun (silent cross-model comparison) and is disallowed. Model rotation goes through `msgvault embed` only.
- `--explain` adds a score column to the table output and populates a `score` sub-object in `--json` mode.

The existing remote-mode path (`IsRemoteMode()` in `search.go`) is extended the same way: `--mode` is forwarded as a query parameter to the remote server.

### 9.2 HTTP — extends existing `/api/v1/search`

The existing `/api/v1/search?q=...` endpoint already accepts Gmail-style queries and `page` / `page_size` pagination (see `internal/api/handlers.go` and `internal/remote/store.go`). We extend it with `mode` and `explain`; we do not add a new endpoint or new pagination parameters.

```
GET /api/v1/search?q=...&mode=fts|vector|hybrid&page=1&page_size=20[&explain=1]
```

- `mode` defaults to `fts` — existing response shape, existing semantics, no change for current clients including the TUI.
- `page` / `page_size` apply fully to `mode=fts` (unchanged). For `mode=vector|hybrid`, only `page=1` is permitted; any other value returns HTTP 400 with `pagination_unsupported`. `page_size` is capped at `max_page_size_hybrid` (default 50).
- `/api/v1/search/fast` and `/api/v1/search/deep` (used by the TUI) are unchanged.
- `/api/v1/stats` is extended with optional fields (see §11.5); existing clients are unaffected.

New error codes:
| Code | HTTP | Description |
|------|------|-------------|
| `pagination_unsupported` | 400 | `page > 1` requested with `mode=vector` or `mode=hybrid` — refine the query instead |
| `embedding_endpoint_unavailable` | 503 | Could not reach the configured embedding endpoint at query time |
| `vector_not_enabled` | 503 | `mode=vector` or `hybrid` requested but `[vector]` is not configured |
| `index_stale` | 503 | Configured model/dimension does not match active index generation — a rebuild is required |
| `index_building` | 503 | First-ever rebuild is in progress and no active generation exists yet. Not returned while an active generation exists (rebuilds are transparent to search) |
| `dimension_mismatch` | 500 | Query embedding dimension disagrees with the active index (should be caught earlier; indicates a bug) |

### 9.3 MCP server — extends existing `msgvault mcp`

An MCP server already exists at `internal/mcp/server.go`, exposing:
`search_messages`, `get_message`, `get_attachment`, `export_attachment`, `list_messages`, `get_stats`, `aggregate`, `stage_deletion`. It runs over stdio via `github.com/mark3labs/mcp-go`. **All existing tool names, arguments, and behaviors are preserved.** The only changes:

1. `search_messages` gains an optional `mode` argument (enum: `fts` | `vector` | `hybrid`; default `fts`). Existing callers are unaffected. When `mode != fts`, the tool validates against the active index generation and surfaces the same errors as the HTTP API.

2. `search_messages` gains an optional `explain` boolean; when true, each result object includes a `score` sub-object.

3. A new tool `find_similar_messages(message_id, limit?, filters?)` returns messages closest to the embedding of a given message in the active generation. This is a capability unique to vector search and doesn't map onto the existing Gmail-syntax model, so it warrants its own tool rather than overloading `search_messages`.

4. `get_stats` response is extended with the same optional embedding fields as `/api/v1/stats` (§11.5).

No tools are renamed or removed. The MCP server version advertised in `server.NewMCPServer(...)` stays at `1.0.0` because the changes are backwards-compatible extensions. Add capability negotiation later if that becomes insufficient.

## 10. Configuration

All keys live under a new `[vector]` table in `~/.msgvault/config.toml`. The feature is off until `[vector]` is present.

```toml
[vector]
enabled = true
backend = "sqlite-vec"                       # only "sqlite-vec" in MVP
db_path = "~/.msgvault/vectors.db"           # backend-specific

[vector.embeddings]
endpoint = "http://mac-studio.tailnet:8080/v1"
api_key_env = "MSGVAULT_EMBED_KEY"           # env var name; empty string = no auth
model = "nomic-embed-text-v1.5"
dimension = 768                              # must match the model; verified on first call
batch_size = 64
timeout = "30s"
max_retries = 3
max_input_chars = 32768

[vector.preprocess]
strip_quotes = true
strip_signatures = true

[vector.search]
rrf_k = 60
k_per_signal = 100
subject_boost = 2.0
max_page_size_hybrid = 50       # per-request cap for mode=vector|hybrid
# Note: no default_mode. Every request specifies mode explicitly; default is fts everywhere.

[vector.embed.schedule]
cron = "*/5 * * * *"                         # "" disables scheduled embedding
run_after_sync = true
```

Validation at startup:
- `endpoint` must parse as a URL.
- `dimension` must be positive.
- If `backend = "sqlite-vec"`, the binary must be built with the sqlite-vec extension available; otherwise startup fails with a clear message (similar to how `-tags fts5` failures surface today).

## 11. Operational Concerns

### 11.1 Scale targets

- Corpus: up to 2M messages
- Index size: ~1.5 GB at 768 dim with int8 quantization (scalar or PQ — choice deferred to implementation); ~6 GB uncompressed float32. Document both in the README.
- Embedding throughput target: bounded by the endpoint, not by msgvault. The worker should sustain `batch_size * endpoint_rps` without internal bottlenecks.
- Query latency target: p95 < 300 ms for `hybrid` at 2M messages on a Mac Studio-class machine with a warm cache. Measured during hardening.

### 11.2 Performance sanity checks

Before merging:
- Load a representative corpus (target user's actual data on his Mac Studio).
- Measure: full backfill wall time, incremental embedding throughput, p50/p95/p99 for hybrid/vector/fts across a scripted query set.
- Record findings in the PR description.

### 11.3 Concurrency

- `vectors.db` runs in WAL mode; one writer at a time is sufficient (only the embedding worker writes).
- Read queries share the SQLite handle pool used elsewhere in msgvault.
- The worker takes a file lock on `vectors.db`-level to prevent two `msgvault embed` processes from running in parallel.

### 11.4 Migration / rollout

Enabling the feature on an existing install:
1. Add `[vector]` section to `config.toml` pointing at the homelab endpoint, specifying `model` and `dimension`.
2. Restart `msgvault serve` (or run the binary once) to create `vectors.db` with the schema from §5.2. No changes to the main `msgvault.db`.
3. Run `msgvault embed --full-rebuild --progress`. This creates the first `index_generations` row, fills `pending_embeddings`, drains it via the embedding endpoint, and activates the new generation on completion. On a 2M-message corpus this is the long step (hours to days depending on endpoint throughput).
4. From that point on, the daemon keeps the active generation current as sync ingests new messages.

Changing models later (e.g. from `nomic-embed-text-v1.5` 768-dim to `mxbai-embed-large` 1024-dim):
1. Update `[vector.embeddings]` in `config.toml`.
2. On the next startup, vector search returns `index_stale` until the user runs `msgvault embed --full-rebuild`. FTS search is unaffected.
3. The rebuild runs alongside the existing active generation; users can keep searching on the old generation throughout. Atomic swap happens on completion.

Disabling:
1. Remove `[vector]` from `config.toml`.
2. Optionally `trash ~/.msgvault/vectors.db`. The main `msgvault.db` is unchanged by this feature, so nothing to roll back there.

### 11.5 Compatibility and versioning

This feature is strictly additive on every public surface:

| Surface | Existing | Change |
|---|---|---|
| `msgvault search` CLI | Gmail syntax, `--limit/--offset/--json/--account` | Adds `--mode`, `--explain` flags; `--mode=fts` (default) is byte-identical to today |
| `/api/v1/search` HTTP | Current response shape | Adds optional `mode` and `explain` query params; default behavior unchanged |
| `/api/v1/search/fast`, `/deep`, `/messages/*`, `/stats`, `/aggregates*` | TUI support endpoints | Unchanged |
| `/api/v1/stats` response | Existing fields | Adds one optional `vector_search` sub-object (see §11.6) — present only when vector search is configured. Existing top-level fields and their shapes are unchanged. |
| MCP tools (8 existing) | As-is | `search_messages` gains optional `mode` + `explain` args; `get_stats` gains the same optional fields as above |
| MCP tools (new) | — | `find_similar_messages(message_id, limit?, filters?)` |
| `config.toml` | Existing tables | New `[vector]`, `[vector.embeddings]`, `[vector.preprocess]`, `[vector.search]`, `[vector.embed.schedule]` tables |
| `msgvault.db` schema | Existing tables | No changes. All new state lives in `vectors.db` |
| Binary build | Existing `-tags fts5` | Adds one more build tag for sqlite-vec (final name TBD during implementation; see §14.5) |

Old binaries are unaffected by the feature — `msgvault.db` is unchanged, and `vectors.db` is a separate file that old binaries simply don't open. New binaries work against databases that have never enabled vector search: with no `[vector]` config and no `vectors.db`, `mode=vector|hybrid` requests return `vector_not_enabled`, and every other surface behaves exactly as before.

### 11.5 Observability

- Structured log entries at start/end of each embedding batch with duration, count, truncation count.
- `/api/v1/stats` response gains one optional `vector_search` sub-object, present only when `[vector]` is configured:

  ```json
  "vector_search": {
    "enabled": true,
    "active_generation": {
      "id": 3,
      "model": "nomic-embed-text-v1.5",
      "dimension": 768,
      "activated_at": "2026-04-12T09:14:00Z",
      "message_count": 2048121
    },
    "building_generation": {
      "id": 4,
      "model": "mxbai-embed-large-v1",
      "dimension": 1024,
      "started_at": "2026-04-18T22:10:00Z",
      "progress": { "done": 812400, "total": 2048121 }
    },
    "pending_embeddings_total": 147
  }
  ```

  `active_generation` is `null` iff no generation has been activated yet (first-ever build). `building_generation` is `null` when no rebuild is in progress. `pending_embeddings_total` sums pending rows across all non-retired generations. The MCP `get_stats` tool returns the same sub-object.
- The existing per-run logger (see `09d18df Add structured file logging with per-run correlation`) is reused for embedding runs.

## 12. Testing Strategy

### 12.1 Unit tests

- `preprocess.go`: table-driven tests for quote stripping, signature stripping, subject prepending, truncation. Fixtures include real-world-ish ugly emails (nested replies, top-posting, HTML-quoted, DKIM signature blocks mistaken for separators).
- `rrf.go`: property-style tests that the fusion is rank-order preserving when one signal is empty, and that subject boost never inverts order of messages with matching subjects vs. unboosted.
- `client.go`: `httptest` server returns canned OpenAI-compatible responses; dimension-mismatch, retry, timeout, and auth header paths all covered.
- `batch.go`: in-memory SQLite with a fake backend; verifies idempotency under simulated crash between insert and flag-clear.

### 12.2 Integration tests

- `sqlitevec` backend against a real `vectors.db` with a tiny 20-message corpus. Covers upsert, delete, search, stats.
- Hybrid search end-to-end with the fake embedding endpoint: verifies RRF matches a hand-computed expected ordering on a crafted dataset.
- CLI and HTTP handlers tested via `httptest` and cobra's test harness.

### 12.3 No real model in CI

All tests use a deterministic fake endpoint (returns pre-computed embeddings for known inputs). No ollama/llama.cpp in CI. The user-facing hardening pass on their Mac Studio covers the real-model integration.

## 13. Future Work

Each of these is explicitly out of scope for this design:

1. **Reranker stage.** Add a `Reranker` interface called after RRF. Obvious fit: Qwen3-Reranker 0.6B served by the same endpoint. Config at `[vector.rerank]`.
2. **RAG / `answer_question`.** Retrieval + prompt + streaming generation + citations. Own design doc.
3. **Multi-chunk-per-message indexing.** Triggered by observed truncation rate. Schema adds `chunks` table; `vectors_vec` keyed by chunk_id; search rolls up to message.
4. **Additional backends.** lance, duckdb-vss, remote (Qdrant).
5. **HTTP MCP transport.** For remote web-frontend integration.
6. **Embedding for attachments.** Text extracted from PDFs, docs, images (via OCR). Likely a separate pipeline.
7. **Query expansion.** LLM-generated query variants (lex/vec/hyde) à la qmd.
8. **Clustering / topic modeling.** Offline periodic job producing browsable topic views.

## 14. Open Questions

Items the user should sanity-check during review:

1. **Config key namespace.** Is `[vector]` the right top-level key? Alternatives: `[search.vector]`, `[semantic]`. `[vector]` is shortest and unambiguous.
2. **Default `rrf_k` of 60.** Widely cited but not sacred. Pin at 60 for MVP; revisit after real-query tuning on the user's corpus.
3. **Subject boost default.** `2.0` is a guess; `--explain` output during hardening will inform the real value.
4. **Grace period on old generation rows after rebuild.** MVP default is "immediate purge on swap" to reclaim disk. An alternative is a short grace window (e.g. 10 minutes) to let in-flight queries complete cleanly. Worth a call during implementation — not a spec-level decision.
5. **sqlite-vec extension build story.** Needs a build tag analogous to `-tags fts5`. Final flag name (`-tags vec`? `-tags sqlite_vec`?) is an implementation detail.

Resolved by review (previously open, now decided):
- **CLI extension vs. new command.** Extends existing `msgvault search` with `--mode` and `--explain` (§9.1). Gmail-style syntax is preserved.
- **Staleness tracking.** `index_generations` table with `fingerprint = model:dimension`; search refuses on mismatch (§6.7).
- **MCP tool surface.** All 8 existing tools preserved; `search_messages` gains `mode`/`explain`; new `find_similar_messages` added (§9.3).
- **Response totals for ANN.** Drop `total` for `vector`/`hybrid`; use `returned` + optional `pool_saturated`. Keep `total` for `fts` (§7.4).
- **Pagination for ANN.** No deep paging in MVP: `mode=vector|hybrid` accepts only `page=1` and caps `page_size` at `max_page_size_hybrid` (§7.4, §9.2).
- **Default mode at the public API.** Always `fts` on CLI, HTTP, and MCP. No config-level `default_mode`; clients opt into `hybrid`/`vector` explicitly per call (§7.1).
- **Freshness during rebuild.** Dual-enqueue: sync writes `pending_embeddings` rows for every non-retired generation, so the building generation stays current with ingest and the swap doesn't drop messages (§6.7).
- **`index_building` error scope.** Only returned on the first-ever rebuild when no active generation exists. Rebuilds with an active generation are transparent to search (§6.7, §9.2).
- **Backend contract carries generation.** `Upsert`, `Search`, `Delete`, `Stats` all take an explicit `GenerationID` so two same-dim generations can coexist during rebuild without ambiguity (§8.1).

---

## Appendix A: Sample hybrid query SQL

See §5.3 for the fused CTE. For the non-fusing case (lance or remote backend), the Go side does:

```go
req := vector.FusedRequest{
    FTSQuery: ftsMatch, QueryVec: queryVec,
    Generation: activeGen.ID, KPerSignal: kPerSignal,
    Limit: pageSize, RRFK: rrfK,          // no Offset: vector/hybrid is single-page
    Filter: filter,
}
if fb, ok := backend.(vector.FusingBackend); ok {
    return fb.FusedSearch(ctx, req)       // single-query path for sqlite-vec
}
// Fallback for non-fusing backends (lance, remote):
// two filtered queries against the active generation + Go-side RRF.
vecHits, _ := backend.Search(ctx, activeGen.ID, queryVec, kPerSignal, filter)
ftsHits, _ := sqliteFTS.Search(ctx, ftsMatch, kPerSignal, filter)
fused := rrf.Fuse(vecHits, ftsHits, rrfK, subjectBoost, subjectTokens)
if len(fused) > pageSize {
    fused = fused[:pageSize]
}
return fused
```

## Appendix B: Example workflow

```bash
# One-time setup: append [vector] config
cat >> ~/.msgvault/config.toml <<EOF
[vector]
enabled = true
backend = "sqlite-vec"

[vector.embeddings]
endpoint = "http://mac-studio.tailnet:8080/v1"
model = "nomic-embed-text-v1.5"
dimension = 768
EOF

# Create the first generation and backfill ~2M messages
# (run on the same host as the inference server for LAN-local traffic)
msgvault embed --full-rebuild --progress

# From then on, incremental embedding happens automatically as syncs add messages
msgvault serve

# Query: extends the existing `msgvault search` — Gmail syntax still works
msgvault search "onboarding plan for the data platform team" --mode hybrid --limit 10
msgvault search "from:alice@example.com billing incident after:2024-01-01" --mode hybrid

# HTTP: extends the existing /api/v1/search with a mode parameter
curl -H "Authorization: Bearer $KEY" \
  "http://localhost:8080/api/v1/search?q=billing+incident&mode=hybrid&explain=1"

# Switching models later (same dim or different):
# 1. Edit [vector.embeddings] in config.toml (new model, new dimension if needed)
# 2. Rebuild into a fresh generation; old one stays active until swap
msgvault embed --full-rebuild --yes
```
