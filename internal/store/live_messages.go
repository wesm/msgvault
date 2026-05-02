package store

import "fmt"

// LiveMessagesWhere returns the SQL predicate that selects live
// messages: rows not hidden by dedup and not recorded as deleted from
// the source server. Pass the table alias used in the surrounding
// query (use "" if the query has no alias).
//
// This is the canonical live-message predicate, introduced as the
// initial contract for the identities/collections/deduplication
// feature: a row is live iff
// deleted_at IS NULL AND deleted_from_source_at IS NULL.
// Read paths (list, search, stats, vector index, FTS) MUST apply this
// predicate; never JOIN against message_bodies in list/aggregate paths.
func LiveMessagesWhere(alias string) string {
	if alias == "" {
		return "deleted_at IS NULL AND deleted_from_source_at IS NULL"
	}
	return fmt.Sprintf(
		"%s.deleted_at IS NULL AND %s.deleted_from_source_at IS NULL",
		alias, alias,
	)
}
