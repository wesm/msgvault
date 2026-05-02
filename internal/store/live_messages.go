package store

import "fmt"

// liveMessagesUnaliased and liveMessagesM cover the two predicates that
// hot read paths build with LiveMessagesWhere; pre-computing them
// avoids a fmt.Sprintf allocation per call on list/search/count paths.
const (
	liveMessagesUnaliased = "deleted_at IS NULL AND deleted_from_source_at IS NULL"
	liveMessagesM         = "m.deleted_at IS NULL AND m.deleted_from_source_at IS NULL"
)

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
//
// The two common aliases ("" and "m") are returned from package-level
// constants to keep this allocation-free on hot paths. Other aliases
// fall back to fmt.Sprintf.
func LiveMessagesWhere(alias string) string {
	switch alias {
	case "":
		return liveMessagesUnaliased
	case "m":
		return liveMessagesM
	}
	return fmt.Sprintf(
		"%s.deleted_at IS NULL AND %s.deleted_from_source_at IS NULL",
		alias, alias,
	)
}
