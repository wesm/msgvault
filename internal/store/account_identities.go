package store

import (
	"fmt"
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

// AddAccountIdentity inserts a confirmed identity for one source.
// Lowercases and trims the address. Idempotent on (source_id, address).
func (s *Store) AddAccountIdentity(sourceID int64, address, signal string) error {
	addr := strings.ToLower(strings.TrimSpace(address))
	if addr == "" {
		return nil
	}
	_, err := s.db.Exec(
		s.dialect.InsertOrIgnore(`INSERT OR IGNORE INTO account_identities (source_id, address, source_signal) VALUES (?, ?, ?)`),
		sourceID, addr, signal,
	)
	if err != nil {
		return fmt.Errorf("add account identity: %w", err)
	}
	return nil
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

// GetIdentitiesForScope returns the union of confirmed identity addresses
// across the given source IDs, lowercased. Empty input returns an empty map —
// no global default; an explicit empty scope means no identity matching.
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
