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
  search.go               # `msgvault search` CLI
  embed.go                # `msgvault embed` CLI (backfill + rebuild)
  mcp.go                  # `msgvault mcp` stdio MCP server

internal/api/
  handlers_search.go      # adds /api/v1/search/hybrid, /vector to existing routes
```

## 5. Data Model

### 5.1 Changes to main database (`~/.msgvault/msgvault.db`)

Add a single column and index:

```sql
ALTER TABLE messages ADD COLUMN needs_embedding INTEGER NOT NULL DEFAULT 1;
CREATE INDEX idx_messages_needs_embedding ON messages(needs_embedding, id)
  WHERE needs_embedding = 1;
```

A partial index keeps the flag cheap even at 2M rows: only rows pending embedding are in the index. The column is set to 1 by sync-time inserts (new messages, re-parsed messages) and cleared to 0 after a successful embedding.

### 5.2 New database (`~/.msgvault/vectors.db`)

Attached to the main SQLite connection as `vec` when vector search is enabled:

```sql
-- Model registry: which models have been used, for auditability and rotation
CREATE TABLE models (
    name       TEXT PRIMARY KEY,
    dimension  INTEGER NOT NULL,
    endpoint   TEXT NOT NULL,
    first_seen INTEGER NOT NULL,
    last_used  INTEGER NOT NULL
);

-- Per-message embedding metadata (the source of truth for what's indexed)
CREATE TABLE embeddings (
    message_id       INTEGER PRIMARY KEY,
    model            TEXT NOT NULL REFERENCES models(name),
    embedded_at      INTEGER NOT NULL,
    source_char_len  INTEGER NOT NULL,  -- chars fed to the embedder after preprocessing
    truncated        INTEGER NOT NULL DEFAULT 0  -- 1 if input exceeded model context
);
CREATE INDEX idx_embeddings_model ON embeddings(model);

-- sqlite-vec virtual table; one row per message for the active model
-- Dimension is fixed per-table; on model dimension change we rebuild
CREATE VIRTUAL TABLE vectors_vec USING vec0(
    message_id INTEGER PRIMARY KEY,
    embedding  FLOAT[768]  -- templated from config at DB creation time
);

-- Embedding job history, mirrors sync_runs pattern
CREATE TABLE embed_runs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at   INTEGER NOT NULL,
    ended_at     INTEGER,
    model        TEXT NOT NULL,
    claimed      INTEGER NOT NULL DEFAULT 0,
    succeeded    INTEGER NOT NULL DEFAULT 0,
    failed       INTEGER NOT NULL DEFAULT 0,
    truncated    INTEGER NOT NULL DEFAULT 0,
    error        TEXT
);
```

Notes:
- `vectors_vec` dimension is templated at table creation and cannot be altered. Model rotation that changes dimension requires dropping and recreating `vectors_vec` and re-embedding everything. This is by design; the `embed --full-rebuild` command handles it.
- Model rotation at the *same* dimension (e.g. fine-tune swap) only requires re-embedding (new rows replace old), not schema changes.
- `vectors.db` can be deleted at any time with no data loss in the system of record — it's a derived cache. The `needs_embedding` flag on `messages` ensures a full rebuild is straightforward.

### 5.3 ATTACH pattern

When vector search is enabled, the main SQLite connection runs `ATTACH DATABASE '<vectors.db>' AS vec` on open. Hybrid queries can then fuse BM25 and ANN results in a single statement:

```sql
WITH bm25 AS (
    SELECT rowid AS message_id, -bm25(messages_fts) AS score, ROW_NUMBER() OVER (ORDER BY bm25(messages_fts)) AS rank
    FROM messages_fts WHERE messages_fts MATCH ? LIMIT 100
),
vec AS (
    SELECT v.message_id, 1.0 - v.distance AS score, ROW_NUMBER() OVER (ORDER BY v.distance) AS rank
    FROM vec.vectors_vec v WHERE v.embedding MATCH ? AND k = 100
),
fused AS (
    SELECT COALESCE(b.message_id, v.message_id) AS message_id,
           COALESCE(1.0 / (60 + b.rank), 0) + COALESCE(1.0 / (60 + v.rank), 0) AS rrf_score,
           b.score AS bm25_score, v.score AS vector_score
    FROM bm25 b FULL OUTER JOIN vec v USING (message_id)
)
SELECT m.id, m.subject, m.snippet, m.sent_at, f.rrf_score, f.bm25_score, f.vector_score
FROM fused f JOIN messages m ON m.id = f.message_id
WHERE <additional filters: account, date, label EXISTS, has_attachment>
ORDER BY f.rrf_score DESC LIMIT ?;
```

For backends that do not live in SQLite (lance, remote), the same fusion is done in Go after two independent queries.

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

The worker repeatedly:
1. Claims up to `batch_size` message IDs from `messages WHERE needs_embedding = 1 ORDER BY id LIMIT N`.
2. For each, loads `body_text` + subject and runs pre-processing.
3. Calls the embedding endpoint with all inputs in one batch.
4. Inside a transaction: inserts `vectors_vec` rows, inserts `embeddings` rows, sets `needs_embedding = 0` on the source messages.
5. Records batch result in `embed_runs`.
6. On error: leaves `needs_embedding = 1`, logs the error, records failure in `embed_runs`. Retries on next run.

Crash safety: because the flag is only cleared after commit, an interrupted batch is simply retried. No idempotency issues.

### 6.4 Daemon integration

`msgvault serve` gains a new scheduled job alongside sync:

```toml
[vector.embed.schedule]
cron = "*/5 * * * *"   # every 5 minutes, or set to "" to disable
```

The scheduler runs the worker whenever the cron fires, and also after each successful sync completion (since sync likely created new `needs_embedding = 1` rows). The job is a no-op if nothing is pending.

### 6.5 CLI (`msgvault embed`)

```
msgvault embed [flags]
  --account EMAIL         # limit to one account (repeatable)
  --limit N               # max messages to embed this run
  --full-rebuild          # drop vectors.db and re-embed everything
  --model NAME            # override the configured model (one-shot)
  --endpoint URL          # override the configured endpoint (one-shot)
  --progress              # live progress bar (default when stderr is a tty)
```

`--full-rebuild` prompts for confirmation unless `--yes` is passed. It recreates `vectors.db`, sets `needs_embedding = 1` on all messages, and runs the worker to completion.

### 6.6 Error surfaces

- Endpoint unreachable → retry with backoff, ultimately leave flag set, log at error level, record in `embed_runs`. Sync is not affected.
- Endpoint returns unexpected dimension → hard-fail the run, surface the error to `embed_runs.error` and the CLI output. Schema stays consistent.
- Model not found → 404 from endpoint → hard-fail with a clear message pointing at the config key.

## 7. Search Pipeline

### 7.1 Modes

- **`fts`** — existing behavior (FTS5-only). Included in the new API surface for uniformity but routes to the same backend as `/api/v1/search/deep`.
- **`vector`** — pure ANN query over `vectors_vec`, ordered by cosine distance.
- **`hybrid`** — RRF fusion of BM25 and vector results. Default.

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

### 7.4 Response shape

Lean default; `?explain=1` opts into score details.

```json
{
  "query": "...", "mode": "hybrid", "took_ms": 82,
  "total": 47,
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

## 8. Backend Abstraction

### 8.1 Interface

```go
package vector

type Chunk struct {
    MessageID      int64
    Model          string
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
    // Upsert writes chunks. Must be transactional: either all chunks land or none do.
    Upsert(ctx context.Context, chunks []Chunk) error

    // Search returns top-k message hits for the query vector, after applying filter.
    Search(ctx context.Context, queryVec []float32, k int, filter Filter) ([]Hit, error)

    // Delete removes embeddings for the given messages.
    Delete(ctx context.Context, messageIDs []int64) error

    // Stats returns counts and storage size.
    Stats(ctx context.Context) (Stats, error)
}

// FusingBackend is an optional capability implemented by backends that can
// fuse with the main-database FTS5 index in a single SQL query. The search
// engine checks for this via type assertion. sqlite-vec implements it;
// lance/remote backends do not.
type FusingBackend interface {
    Backend
    FusedSearch(ctx context.Context, fts string, queryVec []float32,
        k int, filter Filter, rrfK int, subjectBoost float64) ([]FusedHit, error)
}

type FusedHit struct {
    MessageID               int64
    RRFScore                float64
    BM25Score, VectorScore  float64  // zero if the message didn't appear in that signal
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

### 9.1 CLI

```
msgvault search "query" [flags]
  --mode=hybrid|vector|fts   # default: hybrid
  --account EMAIL            # repeatable
  --from EMAIL
  --label LABEL              # repeatable
  --has-attachment
  --after DATE --before DATE
  --limit N                  # default 20
  --json                     # machine-readable output
  --explain                  # include per-signal scores
```

Human-readable output: subject, sender, date, snippet, and a small score bar. JSON output matches the HTTP response.

### 9.2 HTTP (`/api/v1/search/...`)

Additive to the existing routes. Auth, rate limiting, and error format all follow `docs/api.md` conventions.

```
GET /api/v1/search/hybrid?q=...&mode=hybrid&account=x@y.com&label=inbox&limit=20[&explain=1]
GET /api/v1/search/vector?q=...&limit=20
```

The existing `/api/v1/search`, `/api/v1/search/fast`, `/api/v1/search/deep` endpoints used by the TUI are unchanged. A follow-up may unify them but that is not in scope here.

New error codes:
| Code | HTTP | Description |
|------|------|-------------|
| `embedding_endpoint_unavailable` | 503 | Could not reach the configured embedding endpoint |
| `no_embeddings` | 503 | Vector search requested but `vectors.db` is empty or disabled |
| `dimension_mismatch` | 500 | Query vector dimension does not match index |

### 9.3 MCP server (`msgvault mcp`)

- Transport: stdio (standard for Claude Desktop, editor integrations). HTTP transport is future work when the web frontend needs it.
- Uses the official Go MCP SDK (`github.com/modelcontextprotocol/go-sdk`).
- Tools exposed in MVP:
  - `search_emails(query, mode?, filters?, limit?)` — returns the same payload shape as the HTTP API.
  - `get_message(message_id)` — returns subject, sender/recipients, body (plain text), label list, attachment metadata.
  - `list_recent(account?, limit?)` — wraps the existing `/messages/filter` by date.
  - `get_attachment_metadata(message_id)` — names, sizes, content-types; no binary download yet.
- Authentication: the MCP client is trusted (same machine, stdio transport). No token required.
- Server reuses the same `search.Engine` and `store.Store` as the HTTP API.

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
default_mode = "hybrid"
rrf_k = 60
k_per_signal = 100
subject_boost = 2.0

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
1. Add `[vector]` table to config.toml pointing at the homelab endpoint.
2. Restart `msgvault serve` (or run the binary once) to run the migrations: adds `needs_embedding` column, creates `vectors.db`.
3. Run `msgvault embed --progress` to backfill. The daemon will handle all future syncs automatically.

Disabling:
1. Remove `[vector]` from config.toml.
2. Optionally `trash ~/.msgvault/vectors.db`. The `needs_embedding` column in `msgvault.db` is harmless when unused; no migration needed to remove it.

### 11.5 Observability

- Structured log entries at start/end of each embedding batch with duration, count, truncation count.
- `/api/v1/stats` response gains optional fields: `embeddings_total`, `embeddings_pending`, `embeddings_model`.
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
2. **Default `rrf_k` of 60.** Widely cited but not sacred. Pin at 60 for MVP; revisit after real-query tuning.
3. **Subject boost default.** `2.0` is a guess; `--explain` output during hardening will inform the real value.
4. **`msgvault mcp` vs. reusing `serve`.** Current plan: separate command for stdio. An alternative is `serve --mcp-stdio` on top of the daemon. Separate command is cleaner for the typical MCP-client launch pattern.
5. **sqlite-vec extension build story.** Plan: vendored extension or compile-time registration. Needs a build flag analogous to `-tags fts5`. Final choice is implementation detail, but worth confirming the flag name convention (`-tags vec`? `-tags sqlite_vec`?).
6. **Model file management.** Out of msgvault's scope — the user manages this on their inference server. Confirm that's correct.

---

## Appendix A: Sample hybrid query SQL

See §5.3 for the fused CTE. For the non-fusing case (lance or remote backend), the Go side does:

```go
if fb, ok := backend.(vector.FusingBackend); ok {
    return fb.FusedSearch(ctx, q, queryVec, k, filter, rrfK, subjectBoost)
}
vecHits, _ := backend.Search(ctx, queryVec, 100, filter)
ftsHits, _ := sqliteFTS.Search(ctx, q, 100, filter)
fused := rrf.Fuse(vecHits, ftsHits, rrfK, subjectBoost, subjectTokens)
return fused[:limit]
```

## Appendix B: Example workflow

```bash
# One-time setup
cat >> ~/.msgvault/config.toml <<EOF
[vector]
enabled = true
backend = "sqlite-vec"

[vector.embeddings]
endpoint = "http://mac-studio.tailnet:8080/v1"
model = "nomic-embed-text-v1.5"
dimension = 768
EOF

# Backfill 2M messages (run on the same host as the inference server for LAN-local traffic)
msgvault embed --progress

# From then on, incremental embedding happens automatically as syncs add messages
msgvault serve

# Query
msgvault search "onboarding plan for the data platform team" --mode hybrid --limit 10
msgvault search "from Alice about the billing incident" --from alice@example.com --after 2024-01-01
curl -H "Authorization: Bearer $KEY" \
  "http://localhost:8080/api/v1/search/hybrid?q=billing+incident&explain=1"
```
