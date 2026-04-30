package store

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"
)

// AccountIdentity is one confirmed "me" address for one source.
type AccountIdentity struct {
	SourceID     int64
	Address      string
	SourceSignal string
	ConfirmedAt  time.Time
}

// AddAccountIdentity confirms an identifier for one source.
//
// Behavior:
//   - If (source_id, address) does not exist: insert with the given signal
//     and confirmed_at = now. An empty signal inserts an empty source_signal.
//   - If it exists and the signal is already in the row's source_signal set:
//     no-op.
//   - If it exists and the signal is not yet in the set: add it (set is kept
//     sorted alphabetically, comma-delimited). confirmed_at is NOT updated;
//     it records first confirmation.
//   - Empty signal on an existing row: no-op (no new evidence to record).
//   - All-whitespace identifier: no-op (returns nil).
//   - Comma in signal: error. Comma is reserved as the in-column delimiter.
//
// The function trims the identifier; case is preserved (the identifier
// column accommodates email, phone E.164, and synthetic identifiers like
// chat handles where case can be significant).
//
// Read-modify-write inside a transaction. The single-writer SQLite model
// serializes commits within one process; cross-process concurrency is not
// a supported deployment.
func (s *Store) AddAccountIdentity(sourceID int64, address, signal string) error {
	addr := strings.TrimSpace(address)
	if addr == "" {
		return nil
	}
	if strings.Contains(signal, ",") {
		return fmt.Errorf("signal names cannot contain commas: %q", signal)
	}

	return s.withTx(func(tx *loggedTx) error {
		var existing string
		err := tx.QueryRow(
			`SELECT source_signal FROM account_identities
			 WHERE source_id = ? AND address = ?`,
			sourceID, addr,
		).Scan(&existing)
		switch {
		case err == sql.ErrNoRows:
			_, txErr := tx.Exec(
				`INSERT INTO account_identities (source_id, address, source_signal)
				 VALUES (?, ?, ?)`,
				sourceID, addr, signal,
			)
			if txErr != nil {
				return fmt.Errorf("insert account identity: %w", txErr)
			}
			return nil
		case err != nil:
			return fmt.Errorf("read existing source_signal: %w", err)
		}

		merged := mergeSignalSet(existing, signal)
		if merged == existing {
			return nil
		}
		if _, txErr := tx.Exec(
			`UPDATE account_identities SET source_signal = ?
			 WHERE source_id = ? AND address = ?`,
			merged, sourceID, addr,
		); txErr != nil {
			return fmt.Errorf("update source_signal: %w", txErr)
		}
		return nil
	})
}

// mergeSignalSet returns the comma-joined sorted union of the existing
// signal set and the new signal. Empty strings (in either argument) are
// treated as the empty set.
func mergeSignalSet(existing, signal string) string {
	set := make(map[string]struct{})
	if existing != "" {
		for _, s := range strings.Split(existing, ",") {
			if s != "" {
				set[s] = struct{}{}
			}
		}
	}
	if signal != "" {
		set[signal] = struct{}{}
	}
	if len(set) == 0 {
		return ""
	}
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

// ListAccountIdentities returns all identities for one source, ordered by address.
func (s *Store) ListAccountIdentities(sourceID int64) ([]AccountIdentity, error) {
	rows, err := s.db.Query(`
		SELECT source_id, address, source_signal, confirmed_at
		FROM account_identities
		WHERE source_id = ?
		ORDER BY address
	`, sourceID)
	if err != nil {
		return nil, fmt.Errorf("list account identities: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []AccountIdentity
	for rows.Next() {
		var ai AccountIdentity
		if err := rows.Scan(&ai.SourceID, &ai.Address, &ai.SourceSignal, &ai.ConfirmedAt); err != nil {
			return nil, fmt.Errorf("scan account identity: %w", err)
		}
		out = append(out, ai)
	}
	return out, rows.Err()
}

// RemoveAccountIdentity deletes one (source_id, address) row.
// Returns (true, nil) if a row was removed, (false, nil) if no row matched.
func (s *Store) RemoveAccountIdentity(sourceID int64, address string) (bool, error) {
	res, err := s.db.Exec(
		`DELETE FROM account_identities WHERE source_id = ? AND address = ?`,
		sourceID, address,
	)
	if err != nil {
		return false, fmt.Errorf("remove account identity: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("rows affected: %w", err)
	}
	return n > 0, nil
}

// GetIdentitiesForScope returns the union of confirmed identifier addresses
// across the given source IDs. Empty input returns an empty map — no global
// default; an explicit empty scope means no identity matching.
//
// Identifiers are returned with the case the user stored. Callers comparing
// against email-shaped strings should lowercase both sides at compare time.
func (s *Store) GetIdentitiesForScope(sourceIDs []int64) (map[string]struct{}, error) {
	out := make(map[string]struct{})
	if len(sourceIDs) == 0 {
		return out, nil
	}

	placeholders := make([]string, len(sourceIDs))
	args := make([]any, len(sourceIDs))
	for i, id := range sourceIDs {
		placeholders[i] = "?"
		args[i] = id
	}
	query := `SELECT address FROM account_identities WHERE source_id IN (` +
		strings.Join(placeholders, ",") + `)`

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("get identities for scope: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var addr string
		if err := rows.Scan(&addr); err != nil {
			return nil, fmt.Errorf("scan identity address: %w", err)
		}
		out[addr] = struct{}{}
	}
	return out, rows.Err()
}
