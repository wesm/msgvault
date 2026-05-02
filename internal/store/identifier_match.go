package store

import "fmt"

// identifierMatch carries the comparison rule for one identifier in
// account_identities. It exists so the email-vs-other branch lives in
// one place: every call site that compares an identifier against
// stored rows builds an identifierMatch and consumes its SQL fragment
// + bind value.
//
// Email-shaped tokens (per looksLikeEmail) compare case-insensitively
// via LOWER() in SQL; everything else compares case-sensitively.
// Case-folding always happens in SQL, never in Go, so stored rows
// retain whatever casing the user supplied at first write.
//
// Use:
//
//	match := newIdentifierMatch(addr)
//	row := tx.QueryRow(
//	    `SELECT source_signal FROM account_identities
//	     WHERE source_id = ? AND `+match.WhereClause("address"),
//	    sourceID, match.BindValue(),
//	)
type identifierMatch struct {
	isEmailShaped bool
	raw           string
}

// newIdentifierMatch classifies raw via looksLikeEmail and packages
// the classification with the raw input. Empty input is permitted
// (it falls into the non-email branch); the caller is responsible
// for any empty-input gating that has to happen before SQL is run.
func newIdentifierMatch(raw string) identifierMatch {
	return identifierMatch{
		isEmailShaped: looksLikeEmail(raw),
		raw:           raw,
	}
}

// WhereClause renders an equality predicate of the form
// "LOWER(<column>) = LOWER(?)" for email-shaped identifiers and
// "<column> = ?" for everything else. The placeholder always binds
// to BindValue.
//
// SECURITY: column is interpolated into SQL without escaping. Every
// caller in this package supplies a hard-coded column name (today,
// "address" at all three sites). Do NOT pass user input as column
// — there is no SQL-injection guard here.
func (m identifierMatch) WhereClause(column string) string {
	if m.isEmailShaped {
		return fmt.Sprintf("LOWER(%s) = LOWER(?)", column)
	}
	return fmt.Sprintf("%s = ?", column)
}

// BindValue returns the value to bind for the placeholder in
// WhereClause. Equal to the raw input — case-folding happens in SQL
// via LOWER, not in Go, so the stored row keeps original casing.
func (m identifierMatch) BindValue() any {
	return m.raw
}
