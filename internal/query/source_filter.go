package query

import (
	"fmt"
	"strings"
)

// appendSourceFilter returns conditions/args updated with a source-id
// filter drawn from either SourceIDs (multi) or SourceID (single).
// SourceIDs takes precedence when both are provided.
func appendSourceFilter(
	conditions []string, args []any,
	prefix string, singleID *int64, multiIDs []int64,
) ([]string, []any) {
	if len(multiIDs) > 0 {
		placeholders := make([]string, len(multiIDs))
		for i, id := range multiIDs {
			placeholders[i] = "?"
			args = append(args, id)
		}
		conditions = append(conditions, fmt.Sprintf(
			"%ssource_id IN (%s)",
			prefix, strings.Join(placeholders, ","),
		))
		return conditions, args
	}
	if singleID != nil {
		conditions = append(conditions, prefix+"source_id = ?")
		args = append(args, *singleID)
	}
	return conditions, args
}
