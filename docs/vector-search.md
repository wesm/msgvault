# Vector Search

msgvault can search your archive semantically using vector embeddings
in addition to the default keyword (FTS5) search. When enabled, the
`search` command, the HTTP `/api/v1/search` endpoint, and the MCP
`search_messages` tool all accept `mode=vector` (pure semantic) and
`mode=hybrid` (BM25 + vector fused with Reciprocal Rank Fusion). A
separate MCP tool, `find_similar_messages`, returns nearest-neighbor
messages for a given seed.

## Prerequisites

1. **A running OpenAI-compatible embedding endpoint.** msgvault does
   not host a model. Point it at a self-hosted server on your
   network — tested options include [Ollama](https://ollama.com),
   [llama.cpp's `server`](https://github.com/ggerganov/llama.cpp/tree/master/examples/server),
   and [LM Studio](https://lmstudio.ai). The endpoint must accept
   `POST /embeddings` with an OpenAI-style JSON body and return
   `{"data": [{"embedding": [...]}]}`.

2. **A build with `sqlite_vec` support.** The standard `make build`
   target already passes `-tags "fts5 sqlite_vec"`. If you see errors
   mentioning "binary was built without -tags sqlite_vec", rebuild
   via `make build` (or `go build -tags "fts5 sqlite_vec"` if you are
   invoking `go build` directly).

## Enable

Add a `[vector]` block to `~/.msgvault/config.toml`:

```toml
[vector]
enabled = true
backend = "sqlite-vec"
# db_path defaults to <data_dir>/vectors.db when empty.
# db_path = "/path/to/vectors.db"

[vector.embeddings]
endpoint = "http://tailnet-host:11434/v1"
api_key_env = "OLLAMA_API_KEY"           # optional; omit for anonymous endpoints
model = "nomic-embed-text"
dimension = 768
batch_size = 32                          # embeddings per HTTP call
timeout = "30s"
max_retries = 3
max_input_chars = 2000                   # Ollama nomic-embed-text caps context at 2048 tokens; see sizing guidance below for other models

[vector.preprocess]
strip_quotes = true                      # drop quoted reply blocks before embedding
strip_signatures = true                  # drop common `-- ` signature blocks

[vector.search]
rrf_k = 60                               # RRF constant; higher flattens score differences
k_per_signal = 100                       # candidate pool size per signal (BM25 or vector)
subject_boost = 2.0                      # score boost when a query term hits the subject
max_page_size_hybrid = 50                # hard cap on vector/hybrid page_size

[vector.embed.schedule]
cron = "*/5 * * * *"                     # embed worker cron (5-field); empty disables cron
run_after_sync = true                    # run a pass after every successful scheduled sync
```

The `[vector]` section only takes effect when `enabled = true` **and**
the binary was built with `sqlite_vec`. If either is missing, msgvault
behaves as before and the search features described here return
`vector_not_enabled`.

### Matching `max_input_chars` to your embedder's context window

`max_input_chars` is an upper bound in characters; the embedder converts
this to tokens on its own. Set it below the embedder's maximum context
or full-length messages will fail with HTTP 400 during `embed`. Ollama
in particular ships several embedding models with GGUF-declared context
of 2048 tokens even when the upstream model supports more, and adding
`PARAMETER num_ctx 8192` to a Modelfile does not raise the hard cap.

Practical guidance:

- **Ollama `nomic-embed-text` (2048 tokens):** use `max_input_chars =
  2000`. Expect ~20% of messages to be truncated.
- **OpenAI `text-embedding-3-small` (8192 tokens):** `max_input_chars =
  24000` is a safe cap.
- **Self-hosted llama.cpp with an 8k-context GGUF:** match the context
  window; 24000 is a reasonable default.

If `msgvault embed` fails with repeated `HTTP 400` warnings, check the
embedder's logs — `the input length exceeds the context length`
confirms you need to lower `max_input_chars`.

## Backfill

Once enabled, embed your archive:

```bash
msgvault embed --full-rebuild --yes
```

This creates a new **building generation**, drains the pending queue
in batches, and activates the generation once every message has been
embedded. You can interrupt and resume — each invocation of
`msgvault embed` processes whatever is currently pending and
activates the generation if the queue reaches zero.

For daemon-mode deployments (`msgvault serve`), the scheduler will
also run the embed worker on its own cron and can optionally fire
after each scheduled sync (see `[vector.embed.schedule]` above).

## Search

**CLI:**
```bash
msgvault search "planning offsite agenda" --mode hybrid
msgvault search "planning offsite agenda" --mode vector --explain
msgvault search "..." --json --mode hybrid    # JSON output with scores
```

**HTTP:**
```bash
curl "http://localhost:8080/api/v1/search?q=planning+offsite&mode=hybrid"
curl "http://localhost:8080/api/v1/search?q=planning+offsite&mode=vector&explain=1"
```

Response shape differs from the FTS path — see `docs/api.md` for
details. Pagination is not supported for vector/hybrid responses;
bump `page_size` (capped at `max_page_size_hybrid`) instead.

`mode=vector` and `mode=hybrid` require at least one free-text term —
the free text is what gets embedded as the query vector. A query that
is purely operators (e.g. `from:alice label:IMPORTANT`) returns
`missing_free_text`; use `mode=fts` for those.

**MCP tools:**
- `search_messages` accepts `mode` (`fts`/`vector`/`hybrid`) and
  `explain` arguments.
- `find_similar_messages` takes a seed `message_id` and returns
  nearest neighbors (excluding the seed itself). Optional `account`,
  `after`, `before`, `has_attachment` filters.

## Model rotation

To switch models or dimensions, update `[vector.embeddings].model`
and/or `.dimension` in your config, then run:

```bash
msgvault embed --full-rebuild --yes
```

This builds a new generation with the new fingerprint and activates
it atomically when the build completes. While the rebuild is in
flight, `mode=vector` and `mode=hybrid` return `index_stale` (the
previously-active generation no longer matches the configured
fingerprint, so the server refuses to serve potentially-mismatched
results). Use `mode=fts` until the new generation activates — it does
not depend on the vector index. Once `msgvault embed` reports the
new generation activated, vector and hybrid modes resume.

## Troubleshooting

Common error codes and fixes:

| Error | Meaning | Recovery |
|---|---|---|
| `vector_not_enabled` | `[vector] enabled = false`, or server wasn't started with the binary built with `sqlite_vec`. | Set `enabled = true` and rebuild with `make build`. |
| `index_stale` | Active generation's model/dimension doesn't match the configured `[vector.embeddings]` fingerprint. | Run `msgvault embed --full-rebuild --yes`. |
| `index_building` | No active generation yet; one is being built. | Finish running `msgvault embed` or wait for the scheduler. Use `mode=fts` for the interim. |
| `missing_free_text` | `mode=vector` or `mode=hybrid` used with a filter-only query (no free text to embed). | Add free-text terms to `q`, or switch to `mode=fts`. |
| `pagination_unsupported` | Request asked for `page>1` with `mode=vector|hybrid`. | Use `page=1` with a larger `page_size` instead. |
| `invalid_mode` | `mode=` value other than `fts`, `vector`, `hybrid`. | Pick one of those. |

`msgvault embed` repeatedly logs `embed batch failed ... HTTP 400` and
aborts after 5 consecutive failures: check the embedder's logs. If
they say `the input length exceeds the context length` (Ollama) or an
equivalent token-limit error, lower `max_input_chars` to match the
model's context window — see the sizing guidance in [Matching
`max_input_chars` to your embedder's context window](#matching-max_input_chars-to-your-embedders-context-window).

To confirm the binary was built with vector support:

```bash
msgvault search "probe" --mode vector
```

A clear "rebuild with sqlite_vec" error indicates the tag is
missing. A different error (`vector_not_enabled`,
`index_stale`, etc.) indicates the tag is present and the backend
is reachable — you're just waiting on config or backfill.

Check index health via the stats endpoint:

```bash
curl -H "X-API-Key: ..." http://localhost:8080/api/v1/stats | jq .vector_search
```

The `active_generation.message_count` should roughly match
`total_messages`. `pending_embeddings_total` shows how many rows
still need embedding (either because a rebuild is in flight or
because recent syncs have not yet been drained).

## What gets embedded

The embedder processes one vector per message. Per-message input is
assembled from `subject` and `body_text` after preprocessing
(configurable in `[vector.preprocess]`):

- Optional stripping of quoted-reply blocks (`> ...` lines and common
  reply-preamble markers).
- Optional stripping of trailing signatures (lines after `-- `).
- Truncation at `max_input_chars` at a UTF-8 rune boundary.

Messages deleted at the source (`deleted_from_source_at IS NOT NULL`)
are skipped entirely. Messages without a `body_text` fall back to
HTML-to-text conversion of `body_html` so HTML-only messages still
contribute full-body embeddings. Messages with neither fall back to
subject-only embeddings.

## See also

- [`docs/api.md`](api.md) — HTTP API reference (search, stats).
