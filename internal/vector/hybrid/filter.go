package hybrid

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/wesm/msgvault/internal/search"
	"github.com/wesm/msgvault/internal/vector"
)

// BuildFilter translates a parsed Gmail-syntax query into a
// vector.Filter by resolving address/label tokens to IDs against the
// main DB. Matches the semantics of the existing SQLite search path
// (internal/store/api.go): address operators use substring LIKE
// against participants.email_address; labels are matched by exact
// name; subject terms, size bounds, attachment and date filters pass
// through unchanged.
//
// Caller is responsible for any additional filter fields that do not
// derive from the query string (e.g. a SourceID coming from an HTTP
// account parameter) — just set them on the returned Filter.
func BuildFilter(ctx context.Context, db *sql.DB, q *search.Query) (vector.Filter, error) {
	var f vector.Filter
	if q == nil {
		return f, nil
	}

	addressFilters := []struct {
		addrs []string
		dst   *[]int64
	}{
		{q.FromAddrs, &f.SenderIDs},
		{q.ToAddrs, &f.ToIDs},
		{q.CcAddrs, &f.CcIDs},
		{q.BccAddrs, &f.BccIDs},
	}
	for _, af := range addressFilters {
		if len(af.addrs) == 0 {
			continue
		}
		ids, err := resolveParticipantIDs(ctx, db, af.addrs)
		if err != nil {
			return f, err
		}
		// The operator was present in the query but matched zero
		// participants. Leaving ids nil would drop to "no filter"
		// in backend code (empty slice means unrestricted), so
		// substitute a sentinel id that can never match a real
		// row. This preserves "zero hits" for nonexistent
		// senders/recipients, mirroring the SQLite search path.
		if len(ids) == 0 {
			ids = []int64{noMatchSentinel}
		}
		*af.dst = ids
	}

	if len(q.Labels) > 0 {
		ids, err := resolveLabelIDs(ctx, db, q.Labels)
		if err != nil {
			return f, err
		}
		if len(ids) == 0 {
			ids = []int64{noMatchSentinel}
		}
		f.LabelIDs = ids
	}

	if q.HasAttachment != nil {
		v := *q.HasAttachment
		f.HasAttachment = &v
	}
	if q.AfterDate != nil {
		f.After = q.AfterDate
	}
	if q.BeforeDate != nil {
		f.Before = q.BeforeDate
	}
	if q.LargerThan != nil {
		v := *q.LargerThan
		f.LargerThan = &v
	}
	if q.SmallerThan != nil {
		v := *q.SmallerThan
		f.SmallerThan = &v
	}
	if len(q.SubjectTerms) > 0 {
		f.SubjectSubstrings = append([]string(nil), q.SubjectTerms...)
	}
	return f, nil
}

// resolveParticipantIDs returns every participant whose email_address
// contains any of the supplied tokens as a substring. Mirrors the
// `from:` / `to:` behavior in internal/store/api.go so vector/hybrid
// search agrees with the FTS path.
func resolveParticipantIDs(ctx context.Context, db *sql.DB, addrs []string) ([]int64, error) {
	if len(addrs) == 0 {
		return nil, nil
	}
	parts := make([]string, 0, len(addrs))
	args := make([]any, 0, len(addrs))
	for _, a := range addrs {
		parts = append(parts, `LOWER(email_address) LIKE ? ESCAPE '\'`)
		args = append(args, "%"+escapeLike(strings.ToLower(a))+"%")
	}
	q := fmt.Sprintf(
		`SELECT id FROM participants WHERE %s`,
		strings.Join(parts, " OR "))
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query participants: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan participant id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate participants: %w", err)
	}
	return ids, nil
}

// resolveLabelIDs returns labels whose name contains any of the
// supplied tokens as a case-insensitive substring. Mirrors the
// `label:` behavior in internal/store/api.go (LOWER(l.name) LIKE
// '%token%' ESCAPE '\') so vector/hybrid search agrees with the FTS
// path on which label matches a user-supplied token.
func resolveLabelIDs(ctx context.Context, db *sql.DB, labels []string) ([]int64, error) {
	if len(labels) == 0 {
		return nil, nil
	}
	parts := make([]string, 0, len(labels))
	args := make([]any, 0, len(labels))
	for _, l := range labels {
		parts = append(parts, `LOWER(name) LIKE ? ESCAPE '\'`)
		args = append(args, "%"+escapeLike(strings.ToLower(l))+"%")
	}
	q := fmt.Sprintf(
		`SELECT id FROM labels WHERE %s`,
		strings.Join(parts, " OR "))
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query labels: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan label id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate labels: %w", err)
	}
	return ids, nil
}

// noMatchSentinel is the id stored in a resolved-to-empty filter
// slice. SQLite auto-increment ids start at 1, so -1 is guaranteed
// not to match any real row. BuildFilter substitutes this when a
// requested operator resolves to zero participants/labels, so the
// backend IN (...) check returns zero rows instead of degrading
// back to "unrestricted".
const noMatchSentinel int64 = -1

// escapeLike escapes SQL LIKE special characters (%, _, \) so they
// are matched literally. Used with ESCAPE '\'. Mirrors escapeLike in
// internal/store/api.go.
func escapeLike(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `%`, `\%`)
	s = strings.ReplaceAll(s, `_`, `\_`)
	return s
}
