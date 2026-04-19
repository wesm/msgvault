//go:build sqlite_vec

package sqlitevec

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/wesm/msgvault/internal/vector"
)

// Compile-time check that *Backend satisfies vector.FusingBackend.
var _ vector.FusingBackend = (*Backend)(nil)

// FusedSearch runs the single-query hybrid CTE (spec §5.3) by opening a
// fresh connection to the main msgvault.db with vectors.db ATTACHed.
// Filters resolve at the Go layer.
func (b *Backend) FusedSearch(ctx context.Context, req vector.FusedRequest) ([]vector.FusedHit, error) {
	if req.QueryVec == nil && req.FTSQuery == "" {
		return nil, fmt.Errorf("FusedSearch: neither vector nor FTS query provided")
	}

	var dim int
	err := b.db.QueryRowContext(ctx,
		`SELECT dimension FROM index_generations WHERE id = ?`, int64(req.Generation)).Scan(&dim)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: %d", vector.ErrUnknownGeneration, req.Generation)
	}
	if err != nil {
		return nil, fmt.Errorf("lookup generation %d: %w", req.Generation, err)
	}
	if req.QueryVec != nil && len(req.QueryVec) != dim {
		return nil, fmt.Errorf("%w: query has %d dims, gen has %d",
			vector.ErrDimensionMismatch, len(req.QueryVec), dim)
	}

	conn, err := b.openFusedConn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()

	sourceIDs, err := idsToJSON(req.Filter.SourceIDs)
	if err != nil {
		return nil, fmt.Errorf("encode source_ids: %w", err)
	}
	senderIDs, err := idsToJSON(req.Filter.SenderIDs)
	if err != nil {
		return nil, fmt.Errorf("encode sender_ids: %w", err)
	}
	labelIDs, err := idsToJSON(req.Filter.LabelIDs)
	if err != nil {
		return nil, fmt.Errorf("encode label_ids: %w", err)
	}
	var hasAttachment sql.NullBool
	if req.Filter.HasAttachment != nil {
		hasAttachment = sql.NullBool{Valid: true, Bool: *req.Filter.HasAttachment}
	}
	var after, before sql.NullTime
	if req.Filter.After != nil {
		after = sql.NullTime{Valid: true, Time: *req.Filter.After}
	}
	if req.Filter.Before != nil {
		before = sql.NullTime{Valid: true, Time: *req.Filter.Before}
	}

	vecTable := "vec." + VectorTableName(dim)

	// The sqlite-vec virtual table requires `k = <int literal>` rather
	// than a bound parameter; we interpolate KPerSignal directly (it is
	// a trusted integer from the caller's request).
	query := fmt.Sprintf(`
WITH
  filtered AS (
    SELECT m.id
      FROM messages m
     WHERE m.deleted_from_source_at IS NULL
       AND (:source_ids IS NULL OR m.source_id IN (SELECT value FROM json_each(:source_ids)))
       AND (:sender_ids IS NULL OR m.sender_id IN (SELECT value FROM json_each(:sender_ids)))
       AND (:has_attachment IS NULL OR m.has_attachments = :has_attachment)
       AND (:after IS NULL OR m.sent_at >= :after)
       AND (:before IS NULL OR m.sent_at < :before)
       AND (:label_ids IS NULL OR EXISTS (
             SELECT 1 FROM message_labels ml
              WHERE ml.message_id = m.id
                AND ml.label_id IN (SELECT value FROM json_each(:label_ids))))
  ),
  bm25 AS (
    SELECT fts.rowid AS message_id,
           fts.rank AS bm25_raw,
           ROW_NUMBER() OVER (ORDER BY fts.rank) AS rnk
      FROM messages_fts fts
      JOIN filtered f ON f.id = fts.rowid
     WHERE :fts_query IS NOT NULL AND messages_fts MATCH :fts_query
     LIMIT %d
  ),
  ann AS (
    SELECT v.message_id,
           v.distance AS vec_dist,
           ROW_NUMBER() OVER (ORDER BY v.distance) AS rnk
      FROM %s v
     WHERE :query_vec IS NOT NULL
       AND v.generation_id = :gen
       AND v.message_id IN (SELECT id FROM filtered)
       AND v.embedding MATCH :query_vec
       AND k = %d
  ),
  fused AS (
    SELECT COALESCE(b.message_id, v.message_id) AS message_id,
           COALESCE(1.0 / (:rrf_k + b.rnk), 0.0) +
           COALESCE(1.0 / (:rrf_k + v.rnk), 0.0) AS rrf_score,
           b.bm25_raw AS bm25_score,
           CASE WHEN v.vec_dist IS NULL THEN NULL ELSE 1.0 - v.vec_dist END AS vector_score
      FROM bm25 b
      FULL OUTER JOIN ann v USING (message_id)
  )
SELECT message_id, rrf_score, bm25_score, vector_score
  FROM fused
 ORDER BY rrf_score DESC
 LIMIT :limit
`, req.KPerSignal, vecTable, req.KPerSignal)

	var queryVecArg any
	if req.QueryVec != nil {
		queryVecArg = float32SliceBlob(req.QueryVec)
	}
	var ftsArg any
	if req.FTSQuery != "" {
		ftsArg = req.FTSQuery
	}

	rows, err := conn.QueryContext(ctx, query,
		sql.Named("source_ids", sourceIDs),
		sql.Named("sender_ids", senderIDs),
		sql.Named("has_attachment", hasAttachment),
		sql.Named("after", after),
		sql.Named("before", before),
		sql.Named("label_ids", labelIDs),
		sql.Named("fts_query", ftsArg),
		sql.Named("query_vec", queryVecArg),
		sql.Named("gen", int64(req.Generation)),
		sql.Named("rrf_k", req.RRFK),
		sql.Named("limit", req.Limit),
	)
	if err != nil {
		return nil, fmt.Errorf("fused query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var hits []vector.FusedHit
	for rows.Next() {
		var h vector.FusedHit
		var bm, vec sql.NullFloat64
		if err := rows.Scan(&h.MessageID, &h.RRFScore, &bm, &vec); err != nil {
			return nil, fmt.Errorf("scan fused hit: %w", err)
		}
		h.BM25Score = math.NaN()
		if bm.Valid {
			h.BM25Score = bm.Float64
		}
		h.VectorScore = math.NaN()
		if vec.Valid {
			h.VectorScore = vec.Float64
		}
		hits = append(hits, h)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate fused hits: %w", err)
	}

	if req.SubjectBoost > 1.0 && len(req.SubjectTerms) > 0 {
		b.applySubjectBoost(ctx, hits, req.SubjectTerms, req.SubjectBoost)
	}
	return hits, nil
}

// openFusedConn opens a fresh connection to the main msgvault.db with
// vectors.db ATTACHed under the alias "vec". Caller must Close it.
func (b *Backend) openFusedConn(ctx context.Context) (*sql.DB, error) {
	if b.mainPath == "" {
		return nil, fmt.Errorf("FusedSearch requires MainPath in Options")
	}
	conn, err := sql.Open(DriverName(), b.mainPath)
	if err != nil {
		return nil, fmt.Errorf("open main for fused: %w", err)
	}
	// SQLite ATTACH is per-connection; pin the pool to 1 so every
	// subsequent query reuses the attached connection.
	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(1)
	if _, err := conn.ExecContext(ctx, `ATTACH DATABASE ? AS vec`, b.path); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("attach vectors.db: %w", err)
	}
	return conn, nil
}

// idsToJSON encodes an int64 slice as a JSON array for the json_each
// pattern used in the fused CTE. Returns a NULL NullString when the
// slice is empty, so the SQL can short-circuit with IS NULL.
func idsToJSON(ids []int64) (sql.NullString, error) {
	if len(ids) == 0 {
		return sql.NullString{}, nil
	}
	buf, err := json.Marshal(ids)
	if err != nil {
		return sql.NullString{}, fmt.Errorf("marshal ids: %w", err)
	}
	return sql.NullString{Valid: true, String: string(buf)}, nil
}

// applySubjectBoost is a stub. Subject boosting lives in Task 11.
func (b *Backend) applySubjectBoost(_ context.Context, _ []vector.FusedHit, _ []string, _ float64) {
}
