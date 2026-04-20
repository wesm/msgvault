//go:build sqlite_vec

package sqlitevec

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/wesm/msgvault/internal/vector"
)

// Compile-time check that *Backend satisfies the vector.Backend interface.
var _ vector.Backend = (*Backend)(nil)

// Options configures how Open establishes a Backend.
type Options struct {
	Path      string
	MainPath  string  // filesystem path to msgvault.db; required for FusedSearch
	Dimension int     // default dimension for EnsureVectorTable at open
	MainDB    *sql.DB // handle to the main msgvault.db
}

// Backend implements vector.Backend and vector.FusingBackend against a
// SQLite database with the sqlite-vec extension.
type Backend struct {
	db       *sql.DB // handle to vectors.db
	mainDB   *sql.DB // handle to msgvault.db
	path     string  // filesystem path to vectors.db
	mainPath string  // filesystem path to msgvault.db (for ATTACH)
	dim      int
}

// Open opens vectors.db, runs migrations, and retains the main database
// handle for seed queries. Caller must call Close.
func Open(ctx context.Context, opts Options) (*Backend, error) {
	if err := RegisterExtension(); err != nil {
		return nil, err
	}
	db, err := sql.Open(DriverName(), opts.Path)
	if err != nil {
		return nil, fmt.Errorf("open vectors.db: %w", err)
	}
	if err := Migrate(ctx, db, opts.Dimension); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate vectors.db: %w", err)
	}
	return &Backend{
		db:       db,
		mainDB:   opts.MainDB,
		path:     opts.Path,
		mainPath: opts.MainPath,
		dim:      opts.Dimension,
	}, nil
}

// Close releases the vectors.db handle.
func (b *Backend) Close() error { return b.db.Close() }

// DB returns the underlying *sql.DB for vectors.db. Exposed for callers
// that need to share the pool (e.g. the embed worker's VectorsDB field).
func (b *Backend) DB() *sql.DB { return b.db }

// Path returns the filesystem path of vectors.db.
func (b *Backend) Path() string { return b.path }

// CreateGeneration allocates a new building generation and seeds
// pending_embeddings in the same transaction (§5.1 of the spec).
func (b *Backend) CreateGeneration(ctx context.Context, model string, dim int) (vector.GenerationID, error) {
	if err := EnsureVectorTable(ctx, b.db, dim); err != nil {
		return 0, err
	}
	fp := fmt.Sprintf("%s:%d", model, dim)
	now := time.Now().Unix()

	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	res, err := tx.ExecContext(ctx,
		`INSERT INTO index_generations
		 (model, dimension, fingerprint, started_at, state)
		 VALUES (?, ?, ?, ?, 'building')`,
		model, dim, fp, now)
	if err != nil {
		return 0, fmt.Errorf("insert generation: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}

	if err := b.seedPending(ctx, tx, vector.GenerationID(id), now); err != nil {
		return 0, err
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return vector.GenerationID(id), nil
}

// seedPending inserts one pending_embeddings row per non-deleted
// message in the main DB.
func (b *Backend) seedPending(ctx context.Context, tx *sql.Tx, gen vector.GenerationID, now int64) error {
	rows, err := b.mainDB.QueryContext(ctx,
		`SELECT id FROM messages WHERE deleted_from_source_at IS NULL`)
	if err != nil {
		return fmt.Errorf("select messages: %w", err)
	}
	defer func() { _ = rows.Close() }()

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO pending_embeddings (generation_id, message_id, enqueued_at)
		 VALUES (?, ?, ?)`)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()

	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return err
		}
		if _, err := stmt.ExecContext(ctx, int64(gen), id, now); err != nil {
			return fmt.Errorf("insert pending: %w", err)
		}
	}
	return rows.Err()
}

// ActivateGeneration atomically retires the current active generation
// (if any) and promotes `gen` to active.
func (b *Backend) ActivateGeneration(ctx context.Context, gen vector.GenerationID) error {
	now := time.Now().Unix()
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx,
		`UPDATE index_generations
		 SET state = 'retired', completed_at = COALESCE(completed_at, ?)
		 WHERE state = 'active'`, now); err != nil {
		return fmt.Errorf("retire previous active: %w", err)
	}
	res, err := tx.ExecContext(ctx,
		`UPDATE index_generations
		 SET state = 'active', activated_at = ?, completed_at = COALESCE(completed_at, ?)
		 WHERE id = ? AND state = 'building'`, now, now, int64(gen))
	if err != nil {
		return fmt.Errorf("activate: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("generation %d not in 'building' state", gen)
	}
	return tx.Commit()
}

// RetireGeneration marks the given generation as retired.
func (b *Backend) RetireGeneration(ctx context.Context, gen vector.GenerationID) error {
	_, err := b.db.ExecContext(ctx,
		`UPDATE index_generations SET state = 'retired' WHERE id = ?`, int64(gen))
	return err
}

// ActiveGeneration returns the current active generation, or
// vector.ErrNoActiveGeneration if none exists.
func (b *Backend) ActiveGeneration(ctx context.Context) (vector.Generation, error) {
	return b.generationByState(ctx, vector.GenerationActive)
}

// BuildingGeneration returns the current building generation, or nil if
// none exists.
func (b *Backend) BuildingGeneration(ctx context.Context) (*vector.Generation, error) {
	g, err := b.generationByState(ctx, vector.GenerationBuilding)
	if errors.Is(err, vector.ErrNoActiveGeneration) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &g, nil
}

func (b *Backend) generationByState(ctx context.Context, state vector.GenerationState) (vector.Generation, error) {
	var g vector.Generation
	var startedAt int64
	var completedAt, activatedAt sql.NullInt64
	err := b.db.QueryRowContext(ctx,
		`SELECT id, model, dimension, fingerprint, state,
		        started_at, completed_at, activated_at, message_count
		 FROM index_generations WHERE state = ?`, string(state)).Scan(
		&g.ID, &g.Model, &g.Dimension, &g.Fingerprint, &g.State,
		&startedAt, &completedAt, &activatedAt, &g.MessageCount)
	if errors.Is(err, sql.ErrNoRows) {
		return vector.Generation{}, vector.ErrNoActiveGeneration
	}
	if err != nil {
		return vector.Generation{}, err
	}
	g.StartedAt = time.Unix(startedAt, 0)
	if completedAt.Valid {
		t := time.Unix(completedAt.Int64, 0)
		g.CompletedAt = &t
	}
	if activatedAt.Valid {
		t := time.Unix(activatedAt.Int64, 0)
		g.ActivatedAt = &t
	}
	return g, nil
}

// Upsert writes chunks to the given generation. Transactional. Dimension
// is verified per-chunk against the generation's recorded dimension.
// Returns an error wrapping vector.ErrUnknownGeneration if gen does not
// exist, and an error wrapping vector.ErrDimensionMismatch if any chunk's
// vector length does not match the generation's recorded dimension.
func (b *Backend) Upsert(ctx context.Context, gen vector.GenerationID, chunks []vector.Chunk) error {
	if len(chunks) == 0 {
		return nil
	}

	var dim int
	err := b.db.QueryRowContext(ctx,
		`SELECT dimension FROM index_generations WHERE id = ?`, int64(gen)).Scan(&dim)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %d", vector.ErrUnknownGeneration, gen)
	}
	if err != nil {
		return fmt.Errorf("lookup generation %d: %w", gen, err)
	}
	for _, c := range chunks {
		if len(c.Vector) != dim {
			return fmt.Errorf("%w: chunk for msg %d has %d dims, gen has %d",
				vector.ErrDimensionMismatch, c.MessageID, len(c.Vector), dim)
		}
	}

	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin upsert tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().Unix()
	vecTable := VectorTableName(dim)

	embedStmt, err := tx.PrepareContext(ctx, `INSERT OR REPLACE INTO embeddings
		(generation_id, message_id, embedded_at, source_char_len, truncated)
		VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare embeddings insert: %w", err)
	}
	defer func() { _ = embedStmt.Close() }()

	// vecTable name comes from VectorTableName(dim) where dim is sourced from index_generations; safe to interpolate.
	vecDeleteStmt, err := tx.PrepareContext(ctx, fmt.Sprintf(
		`DELETE FROM %s WHERE generation_id = ? AND message_id = ?`, vecTable))
	if err != nil {
		return fmt.Errorf("prepare vec delete: %w", err)
	}
	defer func() { _ = vecDeleteStmt.Close() }()

	vecStmt, err := tx.PrepareContext(ctx, fmt.Sprintf(
		`INSERT INTO %s (generation_id, message_id, embedding) VALUES (?, ?, ?)`, vecTable))
	if err != nil {
		return fmt.Errorf("prepare vec insert: %w", err)
	}
	defer func() { _ = vecStmt.Close() }()

	pendingStmt, err := tx.PrepareContext(ctx,
		`DELETE FROM pending_embeddings WHERE generation_id = ? AND message_id = ?`)
	if err != nil {
		return fmt.Errorf("prepare pending delete: %w", err)
	}
	defer func() { _ = pendingStmt.Close() }()

	for _, c := range chunks {
		truncFlag := 0
		if c.Truncated {
			truncFlag = 1
		}
		if _, err := embedStmt.ExecContext(ctx, int64(gen), c.MessageID, now, c.SourceCharLen, truncFlag); err != nil {
			return fmt.Errorf("insert embedding: %w", err)
		}
		// vec0 virtual tables do not support INSERT OR REPLACE for updates,
		// so delete any existing row first, then insert.
		if _, err := vecDeleteStmt.ExecContext(ctx, int64(gen), c.MessageID); err != nil {
			return fmt.Errorf("delete existing vector: %w", err)
		}
		if _, err := vecStmt.ExecContext(ctx, int64(gen), c.MessageID, float32SliceBlob(c.Vector)); err != nil {
			return fmt.Errorf("insert vector: %w", err)
		}
		if _, err := pendingStmt.ExecContext(ctx, int64(gen), c.MessageID); err != nil {
			return fmt.Errorf("clear pending: %w", err)
		}
	}

	if err := recomputeMessageCount(ctx, tx, gen); err != nil {
		return err
	}
	return tx.Commit()
}

// recomputeMessageCount refreshes index_generations.message_count from
// the current embeddings table contents so upserts and deletes keep the
// generation metadata in sync. Runs inside the caller's transaction.
func recomputeMessageCount(ctx context.Context, tx *sql.Tx, gen vector.GenerationID) error {
	_, err := tx.ExecContext(ctx,
		`UPDATE index_generations
		    SET message_count = (
		        SELECT COUNT(*) FROM embeddings WHERE generation_id = ?
		    )
		  WHERE id = ?`, int64(gen), int64(gen))
	if err != nil {
		return fmt.Errorf("update message_count: %w", err)
	}
	return nil
}

// float32SliceBlob converts a float32 slice to the little-endian byte
// representation that sqlite-vec expects.
func float32SliceBlob(v []float32) []byte {
	buf := make([]byte, 4*len(v))
	for i, f := range v {
		bits := math.Float32bits(f)
		buf[4*i] = byte(bits)
		buf[4*i+1] = byte(bits >> 8)
		buf[4*i+2] = byte(bits >> 16)
		buf[4*i+3] = byte(bits >> 24)
	}
	return buf
}

// blobToFloat32 decodes the little-endian byte representation produced
// by float32SliceBlob back into a float32 slice of length dim.
func blobToFloat32(b []byte, dim int) ([]float32, error) {
	if len(b) != 4*dim {
		return nil, fmt.Errorf("blob length %d does not match dimension %d", len(b), dim)
	}
	out := make([]float32, dim)
	for i := 0; i < dim; i++ {
		bits := uint32(b[4*i]) | uint32(b[4*i+1])<<8 | uint32(b[4*i+2])<<16 | uint32(b[4*i+3])<<24
		out[i] = math.Float32frombits(bits)
	}
	return out, nil
}

// LoadVector returns the embedding for a specific message in the active
// generation. Returns vector.ErrNoActiveGeneration if no active
// generation exists, or a descriptive error if the message is not
// embedded in the active generation.
func (b *Backend) LoadVector(ctx context.Context, messageID int64) ([]float32, error) {
	active, err := b.ActiveGeneration(ctx)
	if err != nil {
		return nil, err
	}
	// vecTable name derives from VectorTableName(active.Dimension) where dimension is sourced from index_generations; safe to interpolate.
	q := fmt.Sprintf(
		`SELECT embedding FROM %s WHERE generation_id = ? AND message_id = ?`,
		VectorTableName(active.Dimension))
	var blob []byte
	err = b.db.QueryRowContext(ctx, q, int64(active.ID), messageID).Scan(&blob)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("no embedding for message %d in generation %d", messageID, active.ID)
	}
	if err != nil {
		return nil, fmt.Errorf("load vector for message %d: %w", messageID, err)
	}
	return blobToFloat32(blob, active.Dimension)
}

// Search runs an ANN query against the given generation and returns the
// top-k hits (optionally intersected with a structured filter). Hits are
// ordered by ascending distance and assigned 1-based ranks.
func (b *Backend) Search(ctx context.Context, gen vector.GenerationID, queryVec []float32, k int, filter vector.Filter) ([]vector.Hit, error) {
	if len(queryVec) == 0 {
		return nil, fmt.Errorf("search: empty query vector")
	}

	var dim int
	err := b.db.QueryRowContext(ctx,
		`SELECT dimension FROM index_generations WHERE id = ?`, int64(gen)).Scan(&dim)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: %d", vector.ErrUnknownGeneration, gen)
	}
	if err != nil {
		return nil, fmt.Errorf("lookup generation %d: %w", gen, err)
	}
	if len(queryVec) != dim {
		return nil, fmt.Errorf("%w: query has %d dims, gen has %d",
			vector.ErrDimensionMismatch, len(queryVec), dim)
	}
	vecTable := VectorTableName(dim)

	idClause, filterArgs, err := b.resolveFilter(ctx, filter)
	if err != nil {
		return nil, err
	}

	q := fmt.Sprintf(`
		SELECT message_id, distance
		  FROM %s
		 WHERE generation_id = ?
		   AND embedding MATCH ?
		   AND k = ?
		   %s
		 ORDER BY distance ASC
	`, vecTable, idClause)

	allArgs := make([]any, 0, 3+len(filterArgs))
	allArgs = append(allArgs, int64(gen), float32SliceBlob(queryVec), k)
	allArgs = append(allArgs, filterArgs...)

	rows, err := b.db.QueryContext(ctx, q, allArgs...)
	if err != nil {
		return nil, fmt.Errorf("ann query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var hits []vector.Hit
	for i := 1; rows.Next(); i++ {
		var id int64
		var dist float64
		if err := rows.Scan(&id, &dist); err != nil {
			return nil, fmt.Errorf("scan hit: %w", err)
		}
		hits = append(hits, vector.Hit{
			MessageID: id,
			Score:     1.0 - dist, // native distance → monotonic score (higher is better)
			Rank:      i,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate hits: %w", err)
	}
	return hits, nil
}

// resolveFilter returns a SQL fragment of the form
// "AND message_id IN (<id list>)" and the matching args slice, or
// ("", nil, nil) when the filter is empty. When the filter matches no
// messages, it returns a clause that forces an empty result set.
func (b *Backend) resolveFilter(ctx context.Context, filter vector.Filter) (string, []any, error) {
	if filter.IsEmpty() {
		return "", nil, nil
	}
	ids, err := b.filteredMessageIDs(ctx, filter)
	if err != nil {
		return "", nil, err
	}
	if len(ids) == 0 {
		return "AND message_id IN (SELECT NULL WHERE 0)", nil, nil
	}
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}
	return fmt.Sprintf("AND message_id IN (%s)", strings.Join(placeholders, ",")), args, nil
}

// filteredMessageIDs runs the filter against the main DB and returns
// matching message IDs. See spec §5.3.
func (b *Backend) filteredMessageIDs(ctx context.Context, f vector.Filter) ([]int64, error) {
	clauses := []string{"m.deleted_from_source_at IS NULL"}
	var args []any

	if len(f.SourceIDs) > 0 {
		clauses = append(clauses, inClause("m.source_id", f.SourceIDs))
		for _, id := range f.SourceIDs {
			args = append(args, id)
		}
	}
	if len(f.SenderIDs) > 0 {
		clauses = append(clauses, inClause("m.sender_id", f.SenderIDs))
		for _, id := range f.SenderIDs {
			args = append(args, id)
		}
	}
	if f.HasAttachment != nil {
		clauses = append(clauses, "m.has_attachments = ?")
		args = append(args, *f.HasAttachment)
	}
	if f.After != nil {
		clauses = append(clauses, "m.sent_at >= ?")
		args = append(args, *f.After)
	}
	if f.Before != nil {
		clauses = append(clauses, "m.sent_at < ?")
		args = append(args, *f.Before)
	}
	if len(f.LabelIDs) > 0 {
		clauses = append(clauses, fmt.Sprintf(
			`EXISTS (SELECT 1 FROM message_labels ml WHERE ml.message_id = m.id AND %s)`,
			inClause("ml.label_id", f.LabelIDs)))
		for _, id := range f.LabelIDs {
			args = append(args, id)
		}
	}

	query := `SELECT m.id FROM messages m WHERE ` + strings.Join(clauses, " AND ")

	rows, err := b.mainDB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("filter query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan filter id: %w", err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate filter ids: %w", err)
	}
	return out, nil
}

// inClause returns "col IN (?,?,?)" for len(ids) placeholders. Caller
// must append the ids to the args slice in the same order.
func inClause(col string, ids []int64) string {
	placeholders := make([]string, len(ids))
	for i := range ids {
		placeholders[i] = "?"
	}
	return fmt.Sprintf("%s IN (%s)", col, strings.Join(placeholders, ","))
}

// Delete removes the given messages from the specified generation in
// one transaction. Empty messageIDs is a no-op. Returns an error
// wrapping vector.ErrUnknownGeneration if gen does not exist.
func (b *Backend) Delete(ctx context.Context, gen vector.GenerationID, messageIDs []int64) error {
	if len(messageIDs) == 0 {
		return nil
	}

	var dim int
	err := b.db.QueryRowContext(ctx,
		`SELECT dimension FROM index_generations WHERE id = ?`, int64(gen)).Scan(&dim)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %d", vector.ErrUnknownGeneration, gen)
	}
	if err != nil {
		return fmt.Errorf("lookup generation %d: %w", gen, err)
	}

	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin delete tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	embedStmt, err := tx.PrepareContext(ctx,
		`DELETE FROM embeddings WHERE generation_id = ? AND message_id = ?`)
	if err != nil {
		return fmt.Errorf("prepare embeddings delete: %w", err)
	}
	defer func() { _ = embedStmt.Close() }()

	// vecTable name derives from VectorTableName(dim) where dim is sourced from index_generations; safe to interpolate.
	vecStmt, err := tx.PrepareContext(ctx, fmt.Sprintf(
		`DELETE FROM %s WHERE generation_id = ? AND message_id = ?`, VectorTableName(dim)))
	if err != nil {
		return fmt.Errorf("prepare vec delete: %w", err)
	}
	defer func() { _ = vecStmt.Close() }()

	for _, id := range messageIDs {
		if _, err := embedStmt.ExecContext(ctx, int64(gen), id); err != nil {
			return fmt.Errorf("delete embedding: %w", err)
		}
		if _, err := vecStmt.ExecContext(ctx, int64(gen), id); err != nil {
			return fmt.Errorf("delete vector: %w", err)
		}
	}
	if err := recomputeMessageCount(ctx, tx, gen); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit delete tx: %w", err)
	}
	return nil
}

// Stats returns counts for the given generation. When gen == 0, counts
// are aggregated across all generations. Returns an error wrapping
// vector.ErrUnknownGeneration if gen != 0 and the generation does not
// exist, so callers can distinguish a bad gen id from a valid-but-empty
// generation. StorageBytes is left zero here; it is derived from the
// vectors.db file size by the caller.
func (b *Backend) Stats(ctx context.Context, gen vector.GenerationID) (vector.Stats, error) {
	var s vector.Stats
	where := "WHERE generation_id = ?"
	args := []any{int64(gen)}
	if gen == 0 {
		where, args = "", nil
	} else {
		var exists int
		err := b.db.QueryRowContext(ctx,
			`SELECT 1 FROM index_generations WHERE id = ?`, int64(gen)).Scan(&exists)
		if errors.Is(err, sql.ErrNoRows) {
			return s, fmt.Errorf("%w: %d", vector.ErrUnknownGeneration, gen)
		}
		if err != nil {
			return s, fmt.Errorf("lookup generation %d: %w", gen, err)
		}
	}

	if err := b.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM embeddings `+where, args...).Scan(&s.EmbeddingCount); err != nil {
		return s, fmt.Errorf("count embeddings: %w", err)
	}
	if err := b.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pending_embeddings `+where, args...).Scan(&s.PendingCount); err != nil {
		return s, fmt.Errorf("count pending: %w", err)
	}
	return s, nil
}
