package store

import "fmt"

// LiveMessagesWhere returns the SQL predicate that selects live
// messages: rows not hidden by dedup and not recorded as deleted from
// the source server. Pass the table alias used in the surrounding
// query (use "" if the query has no alias). A row is live iff
// deleted_at IS NULL AND deleted_from_source_at IS NULL.
//
// Use this from the store and vector layers, where callers expect a
// single canonical "live" view (dedup, vector indexing, integrity
// counts). The query engines (internal/query) intentionally apply
// deleted_at IS NULL unconditionally and gate
// deleted_from_source_at IS NULL behind the opt-in
// HideDeletedFromSource filter, because msgvault is an archive: rows
// deleted from the source server remain part of the archive and are
// shown by default.
func LiveMessagesWhere(alias string) string {
	if alias == "" {
		return "deleted_at IS NULL AND deleted_from_source_at IS NULL"
	}
	return fmt.Sprintf(
		"%s.deleted_at IS NULL AND %s.deleted_from_source_at IS NULL",
		alias, alias,
	)
}
