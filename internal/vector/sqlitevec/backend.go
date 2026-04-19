//go:build sqlite_vec

package sqlitevec

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/wesm/msgvault/internal/vector"
)

// Compile-time check that *Backend satisfies the vector.Backend interface.
var _ vector.Backend = (*Backend)(nil)

// Options configures how Open establishes a Backend.
type Options struct {
	Path      string
	Dimension int     // default dimension for EnsureVectorTable at open
	MainDB    *sql.DB // handle to the main msgvault.db
}

// Backend implements vector.Backend (and, once FusedSearch lands, also
// vector.FusingBackend) against a SQLite database with the sqlite-vec
// extension.
type Backend struct {
	db     *sql.DB // handle to vectors.db
	mainDB *sql.DB // handle to msgvault.db
	dim    int
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
		return nil, err
	}
	return &Backend{db: db, mainDB: opts.MainDB, dim: opts.Dimension}, nil
}

// Close releases the vectors.db handle.
func (b *Backend) Close() error { return b.db.Close() }

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

// Upsert is a stub; implemented in T6.
func (b *Backend) Upsert(ctx context.Context, gen vector.GenerationID, chunks []vector.Chunk) error {
	return fmt.Errorf("Upsert: not implemented")
}

// Search is a stub; implemented in T7.
func (b *Backend) Search(ctx context.Context, gen vector.GenerationID, queryVec []float32, k int, filter vector.Filter) ([]vector.Hit, error) {
	return nil, fmt.Errorf("Search: not implemented")
}

// Delete is a stub; implemented in T8.
func (b *Backend) Delete(ctx context.Context, gen vector.GenerationID, messageIDs []int64) error {
	return fmt.Errorf("Delete: not implemented")
}

// Stats is a stub; implemented in T8.
func (b *Backend) Stats(ctx context.Context, gen vector.GenerationID) (vector.Stats, error) {
	return vector.Stats{}, fmt.Errorf("Stats: not implemented")
}
