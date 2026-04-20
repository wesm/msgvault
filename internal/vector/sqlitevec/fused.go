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
	toIDs, err := idsToJSON(req.Filter.ToIDs)
	if err != nil {
		return nil, fmt.Errorf("encode to_ids: %w", err)
	}
	ccIDs, err := idsToJSON(req.Filter.CcIDs)
	if err != nil {
		return nil, fmt.Errorf("encode cc_ids: %w", err)
	}
	bccIDs, err := idsToJSON(req.Filter.BccIDs)
	if err != nil {
		return nil, fmt.Errorf("encode bcc_ids: %w", err)
	}
	labelIDs, err := idsToJSON(req.Filter.LabelIDs)
	if err != nil {
		return nil, fmt.Errorf("encode label_ids: %w", err)
	}
	var hasAttachment sql.NullBool
	if req.Filter.HasAttachment != nil {
		hasAttachment = sql.NullBool{Valid: true, Bool: *req.Filter.HasAttachment}
	}
	// Format date bounds with the canonical SQLite datetime layout used
	// elsewhere in the repo so boundary comparisons on the text sent_at
	// column agree with the existing SQLite query paths.
	var after, before sql.NullString
	if req.Filter.After != nil {
		after = sql.NullString{Valid: true, String: req.Filter.After.Format(sqliteDatetimeFormat)}
	}
	if req.Filter.Before != nil {
		before = sql.NullString{Valid: true, String: req.Filter.Before.Format(sqliteDatetimeFormat)}
	}
	var largerThan, smallerThan sql.NullInt64
	if req.Filter.LargerThan != nil {
		largerThan = sql.NullInt64{Valid: true, Int64: *req.Filter.LargerThan}
	}
	if req.Filter.SmallerThan != nil {
		smallerThan = sql.NullInt64{Valid: true, Int64: *req.Filter.SmallerThan}
	}
	// SubjectSubstrings all get AND-combined; encode as JSON so the CTE
	// can iterate via json_each and apply one LIKE per term without
	// blowing past the bound-parameter cap.
	var subjectPatterns sql.NullString
	if len(req.Filter.SubjectSubstrings) > 0 {
		patterns := make([]string, len(req.Filter.SubjectSubstrings))
		for i, term := range req.Filter.SubjectSubstrings {
			patterns[i] = "%" + escapeLikeSubject(term) + "%"
		}
		buf, err := json.Marshal(patterns)
		if err != nil {
			return nil, fmt.Errorf("encode subject patterns: %w", err)
		}
		subjectPatterns = sql.NullString{Valid: true, String: string(buf)}
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
       AND (:sender_ids IS NULL OR m.sender_id IN (SELECT value FROM json_each(:sender_ids))
            OR EXISTS (
                 SELECT 1 FROM message_recipients mr
                  WHERE mr.message_id = m.id
                    AND mr.recipient_type = 'from'
                    AND mr.participant_id IN (SELECT value FROM json_each(:sender_ids))))
       AND (:to_ids IS NULL OR EXISTS (
             SELECT 1 FROM message_recipients mr
              WHERE mr.message_id = m.id
                AND mr.recipient_type = 'to'
                AND mr.participant_id IN (SELECT value FROM json_each(:to_ids))))
       AND (:cc_ids IS NULL OR EXISTS (
             SELECT 1 FROM message_recipients mr
              WHERE mr.message_id = m.id
                AND mr.recipient_type = 'cc'
                AND mr.participant_id IN (SELECT value FROM json_each(:cc_ids))))
       AND (:bcc_ids IS NULL OR EXISTS (
             SELECT 1 FROM message_recipients mr
              WHERE mr.message_id = m.id
                AND mr.recipient_type = 'bcc'
                AND mr.participant_id IN (SELECT value FROM json_each(:bcc_ids))))
       AND (:has_attachment IS NULL OR m.has_attachments = :has_attachment)
       AND (:after IS NULL OR m.sent_at >= :after)
       AND (:before IS NULL OR m.sent_at < :before)
       AND (:larger_than IS NULL OR m.size_estimate > :larger_than)
       AND (:smaller_than IS NULL OR m.size_estimate < :smaller_than)
       AND (:subject_patterns IS NULL OR NOT EXISTS (
             SELECT 1 FROM json_each(:subject_patterns) sp
              WHERE m.subject IS NULL OR m.subject NOT LIKE sp.value ESCAPE '\'))
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
		sql.Named("to_ids", toIDs),
		sql.Named("cc_ids", ccIDs),
		sql.Named("bcc_ids", bccIDs),
		sql.Named("has_attachment", hasAttachment),
		sql.Named("after", after),
		sql.Named("before", before),
		sql.Named("larger_than", largerThan),
		sql.Named("smaller_than", smallerThan),
		sql.Named("subject_patterns", subjectPatterns),
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
