-- PostgreSQL-specific extensions (tsvector for full-text search)

-- Full-text search column and GIN index on messages.
-- Unlike SQLite's FTS5 virtual table, PostgreSQL stores the tsvector
-- directly on the messages table. Updates are managed via Store.UpsertFTS().
ALTER TABLE messages ADD COLUMN IF NOT EXISTS search_fts TSVECTOR;
CREATE INDEX IF NOT EXISTS messages_search_fts_idx ON messages USING GIN (search_fts);
