-- vectors.db schema. See spec §5.2.

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);
INSERT OR IGNORE INTO schema_version VALUES (1);

CREATE TABLE IF NOT EXISTS index_generations (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    model         TEXT NOT NULL,
    dimension     INTEGER NOT NULL,
    fingerprint   TEXT NOT NULL,
    started_at    INTEGER NOT NULL,
    completed_at  INTEGER,
    activated_at  INTEGER,
    state         TEXT NOT NULL,
    message_count INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_generations_active
    ON index_generations(state) WHERE state = 'active';
CREATE UNIQUE INDEX IF NOT EXISTS idx_generations_building
    ON index_generations(state) WHERE state = 'building';

CREATE TABLE IF NOT EXISTS embeddings (
    generation_id    INTEGER NOT NULL REFERENCES index_generations(id) ON DELETE CASCADE,
    message_id       INTEGER NOT NULL,
    embedded_at      INTEGER NOT NULL,
    source_char_len  INTEGER NOT NULL,
    truncated        INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (generation_id, message_id)
);
CREATE INDEX IF NOT EXISTS idx_embeddings_msg ON embeddings(message_id);

CREATE TABLE IF NOT EXISTS pending_embeddings (
    generation_id INTEGER NOT NULL REFERENCES index_generations(id) ON DELETE CASCADE,
    message_id    INTEGER NOT NULL,
    enqueued_at   INTEGER NOT NULL,
    claimed_at    INTEGER,
    claim_token   TEXT,
    PRIMARY KEY (generation_id, message_id)
);
CREATE INDEX IF NOT EXISTS idx_pending_available
    ON pending_embeddings(generation_id, message_id) WHERE claimed_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_pending_claims
    ON pending_embeddings(claimed_at) WHERE claimed_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS embed_runs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    generation_id INTEGER NOT NULL REFERENCES index_generations(id),
    started_at    INTEGER NOT NULL,
    ended_at      INTEGER,
    claimed       INTEGER NOT NULL DEFAULT 0,
    succeeded     INTEGER NOT NULL DEFAULT 0,
    failed        INTEGER NOT NULL DEFAULT 0,
    truncated     INTEGER NOT NULL DEFAULT 0,
    error         TEXT
);
