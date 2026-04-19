package embed

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/wesm/msgvault/internal/vector"
)

// Enqueuer inserts message IDs into pending_embeddings for every
// non-retired generation. Implements the EmbedEnqueuer interface
// expected by internal/sync.
//
// Dual-enqueue is intentional: when a rebuild is in progress there are
// two non-retired generations (active + building); every newly-synced
// message gets queued into both so the building index stays current.
type Enqueuer struct {
	db *sql.DB
}

// NewEnqueuer returns an Enqueuer backed by vectors.db.
func NewEnqueuer(db *sql.DB) *Enqueuer {
	return &Enqueuer{db: db}
}

// EnqueueMessages adds the given IDs to pending_embeddings for every
// generation not in state 'retired'. Duplicate IDs are silently ignored
// via INSERT OR IGNORE. Caller must only pass non-deleted message IDs —
// the deletion predicate is not checked here.
func (e *Enqueuer) EnqueueMessages(ctx context.Context, messageIDs []int64) error {
	if len(messageIDs) == 0 {
		return nil
	}
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin enqueue tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx,
		`SELECT id FROM index_generations WHERE state != ?`,
		string(vector.GenerationRetired))
	if err != nil {
		return fmt.Errorf("select non-retired generations: %w", err)
	}
	var gens []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan generation id: %w", err)
		}
		gens = append(gens, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("iterate generations: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close generation rows: %w", err)
	}
	if len(gens) == 0 {
		return tx.Commit()
	}

	now := time.Now().Unix()
	stmt, err := tx.PrepareContext(ctx,
		`INSERT OR IGNORE INTO pending_embeddings (generation_id, message_id, enqueued_at) VALUES (?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare enqueue stmt: %w", err)
	}
	defer func() { _ = stmt.Close() }()

	for _, g := range gens {
		for _, m := range messageIDs {
			if _, err := stmt.ExecContext(ctx, g, m, now); err != nil {
				return fmt.Errorf("insert pending (gen=%d msg=%d): %w", g, m, err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit enqueue: %w", err)
	}
	return nil
}
