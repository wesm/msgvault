package store

import "fmt"

// LiveMessagesWhere returns the SQL predicate that selects live
// messages: rows not hidden by dedup and not recorded as deleted from
// the source server. Pass the table alias used in the surrounding
// query (use "" if the query has no alias).
//
// This is the canonical live-message predicate. See
// docs/superpowers/specs/2026-04-29-pr-286-design-alignment.md
// "Live-Message Contract" for the product contract.
func LiveMessagesWhere(alias string) string {
	if alias == "" {
		return "deleted_at IS NULL AND deleted_from_source_at IS NULL"
	}
	return fmt.Sprintf(
		"%s.deleted_at IS NULL AND %s.deleted_from_source_at IS NULL",
		alias, alias,
	)
}
