package store

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Collection is a named grouping of sources that should be treated as
// a single logical archive.
type Collection struct {
	ID          int64
	Name        string
	Description string
	CreatedAt   time.Time
}

// CollectionWithSources bundles a Collection with its member source
// IDs and a message-count aggregate.
type CollectionWithSources struct {
	Collection
	SourceIDs    []int64
	MessageCount int64
}

// ErrCollectionNotFound is returned when a collection lookup has no hits.
var ErrCollectionNotFound = errors.New("collection not found")

// ensureCollectionSchema creates the collections tables on demand.
func (s *Store) ensureCollectionSchema() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS collections (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL UNIQUE,
			description TEXT NOT NULL DEFAULT '',
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS collection_sources (
			collection_id INTEGER NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
			source_id     INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
			PRIMARY KEY (collection_id, source_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_collection_sources_source_id
		 ON collection_sources(source_id)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("create collections schema: %w", err)
		}
	}
	return nil
}

// EnsureDefaultCollection creates the "All" collection if it doesn't
// exist and adds all current sources to it. Safe to call on every
// schema init.
func (s *Store) EnsureDefaultCollection() error {
	if err := s.ensureCollectionSchema(); err != nil {
		return err
	}

	var id int64
	err := s.db.QueryRow(
		`SELECT id FROM collections WHERE name = 'All'`,
	).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		res, err := s.db.Exec(
			`INSERT INTO collections (name, description)
			 VALUES ('All', 'All accounts')`,
		)
		if err != nil {
			return fmt.Errorf("create default collection: %w", err)
		}
		id, _ = res.LastInsertId()
	} else if err != nil {
		return fmt.Errorf("check default collection: %w", err)
	}

	// Add all sources not already in it.
	_, err = s.db.Exec(
		`INSERT OR IGNORE INTO collection_sources (collection_id, source_id)
		 SELECT ?, id FROM sources`,
		id,
	)
	return err
}

// CreateCollection inserts a new collection with the given name,
// description, and member source IDs.
func (s *Store) CreateCollection(
	name, description string, sourceIDs []int64,
) (*Collection, error) {
	if err := s.ensureCollectionSchema(); err != nil {
		return nil, err
	}

	name = strings.TrimSpace(name)
	if name == "" {
		return nil, fmt.Errorf("collection name is required")
	}
	if len(sourceIDs) == 0 {
		return nil, fmt.Errorf(
			"collection %q needs at least one source", name,
		)
	}

	unique := uniqueInt64s(sourceIDs)
	if err := s.validateSourceIDs(unique); err != nil {
		return nil, err
	}

	var created *Collection
	err := s.withTx(func(tx *loggedTx) error {
		res, err := tx.Exec(
			`INSERT INTO collections (name, description)
			 VALUES (?, ?)`,
			name, description,
		)
		if err != nil {
			if isSQLiteError(err, "UNIQUE constraint failed") {
				return fmt.Errorf(
					"collection %q already exists", name,
				)
			}
			return fmt.Errorf("insert collection: %w", err)
		}
		id, err := res.LastInsertId()
		if err != nil {
			return fmt.Errorf("last insert id: %w", err)
		}

		for _, sid := range unique {
			if _, err := tx.Exec(
				`INSERT INTO collection_sources
				  (collection_id, source_id)
				 VALUES (?, ?)`,
				id, sid,
			); err != nil {
				return fmt.Errorf("link source %d: %w", sid, err)
			}
		}

		row := tx.QueryRow(
			`SELECT id, name, description, created_at
			 FROM collections WHERE id = ?`, id,
		)
		c, scanErr := scanCollection(row)
		if scanErr != nil {
			return scanErr
		}
		created = c
		return nil
	})
	if err != nil {
		return nil, err
	}
	return created, nil
}

// GetCollectionByName returns the collection with the given name and
// its member source IDs.
func (s *Store) GetCollectionByName(
	name string,
) (*CollectionWithSources, error) {
	if err := s.ensureCollectionSchema(); err != nil {
		return nil, err
	}

	row := s.db.QueryRow(
		`SELECT id, name, description, created_at
		 FROM collections WHERE name = ?`, name,
	)
	c, err := scanCollection(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrCollectionNotFound
		}
		return nil, err
	}
	return s.hydrateCollection(c)
}

// ListCollections returns every collection with source IDs and
// message counts.
func (s *Store) ListCollections() ([]*CollectionWithSources, error) {
	if err := s.ensureCollectionSchema(); err != nil {
		return nil, err
	}

	rows, err := s.db.Query(
		`SELECT id, name, description, created_at
		 FROM collections ORDER BY name`,
	)
	if err != nil {
		return nil, fmt.Errorf("list collections: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var collections []*Collection
	for rows.Next() {
		c, scanErr := scanCollection(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		collections = append(collections, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	result := make([]*CollectionWithSources, 0, len(collections))
	for _, c := range collections {
		hydrated, err := s.hydrateCollection(c)
		if err != nil {
			return nil, err
		}
		result = append(result, hydrated)
	}
	return result, nil
}

// getCollectionID looks up a collection ID by name without hydrating.
func (s *Store) getCollectionID(name string) (int64, error) {
	var id int64
	err := s.db.QueryRow(
		`SELECT id FROM collections WHERE name = ?`, name,
	).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrCollectionNotFound
	}
	if err != nil {
		return 0, err
	}
	return id, nil
}

// AddSourcesToCollection attaches sources to a collection. Idempotent.
func (s *Store) AddSourcesToCollection(name string, sourceIDs []int64) error {
	if err := s.ensureCollectionSchema(); err != nil {
		return err
	}
	if err := s.validateSourceIDs(sourceIDs); err != nil {
		return err
	}
	collID, err := s.getCollectionID(name)
	if err != nil {
		return err
	}
	for _, sid := range sourceIDs {
		_, err = s.db.Exec(
			`INSERT OR IGNORE INTO collection_sources
			  (collection_id, source_id)
			 VALUES (?, ?)`,
			collID, sid,
		)
		if err != nil {
			return fmt.Errorf("add source %d: %w", sid, err)
		}
	}
	return nil
}

// RemoveSourcesFromCollection detaches sources. Idempotent.
func (s *Store) RemoveSourcesFromCollection(name string, sourceIDs []int64) error {
	if err := s.ensureCollectionSchema(); err != nil {
		return err
	}
	collID, err := s.getCollectionID(name)
	if err != nil {
		return err
	}
	for _, sid := range sourceIDs {
		_, err = s.db.Exec(
			`DELETE FROM collection_sources
			 WHERE collection_id = ? AND source_id = ?`,
			collID, sid,
		)
		if err != nil {
			return fmt.Errorf("remove source %d: %w", sid, err)
		}
	}
	return nil
}

// DeleteCollection drops the collection. Sources and messages untouched.
func (s *Store) DeleteCollection(name string) error {
	if err := s.ensureCollectionSchema(); err != nil {
		return err
	}
	res, err := s.db.Exec(
		`DELETE FROM collections WHERE name = ?`, name,
	)
	if err != nil {
		return fmt.Errorf("delete collection: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrCollectionNotFound
	}
	return nil
}

func (s *Store) hydrateCollection(
	c *Collection,
) (*CollectionWithSources, error) {
	rows, err := s.db.Query(
		`SELECT source_id FROM collection_sources
		 WHERE collection_id = ?
		 ORDER BY source_id`,
		c.ID,
	)
	if err != nil {
		return nil, fmt.Errorf("load sources for %s: %w", c.Name, err)
	}
	var sourceIDs []int64
	for rows.Next() {
		var sid int64
		if err := rows.Scan(&sid); err != nil {
			_ = rows.Close()
			return nil, err
		}
		sourceIDs = append(sourceIDs, sid)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var count int64
	if len(sourceIDs) > 0 {
		count, err = s.CountActiveMessages(sourceIDs...)
		if err != nil {
			return nil, err
		}
	}

	return &CollectionWithSources{
		Collection:   *c,
		SourceIDs:    sourceIDs,
		MessageCount: count,
	}, nil
}

func scanCollection(row interface {
	Scan(dest ...any) error
}) (*Collection, error) {
	var c Collection
	if err := row.Scan(
		&c.ID, &c.Name, &c.Description, &c.CreatedAt,
	); err != nil {
		return nil, err
	}
	return &c, nil
}

func (s *Store) validateSourceIDs(ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}
	query := "SELECT id FROM sources WHERE id IN (" +
		strings.Join(placeholders, ",") + ")"
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return fmt.Errorf("validate source IDs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	found := make(map[int64]bool, len(ids))
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return err
		}
		found[id] = true
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, id := range ids {
		if !found[id] {
			return fmt.Errorf("source %d not found", id)
		}
	}
	return nil
}

func uniqueInt64s(in []int64) []int64 {
	seen := make(map[int64]bool, len(in))
	out := make([]int64, 0, len(in))
	for _, v := range in {
		if seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	return out
}
