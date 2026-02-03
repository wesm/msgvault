// Package search provides Gmail-like search query parsing.
package search

import (
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Query represents a parsed search query with all supported filters.
type Query struct {
	TextTerms     []string   // Full-text search terms
	FromAddrs     []string   // from: filters
	ToAddrs       []string   // to: filters
	CcAddrs       []string   // cc: filters
	BccAddrs      []string   // bcc: filters
	SubjectTerms  []string   // subject: filters
	Labels        []string   // label: filters
	HasAttachment *bool      // has:attachment
	BeforeDate    *time.Time // before: filter
	AfterDate     *time.Time // after: filter
	LargerThan    *int64     // larger: filter (bytes)
	SmallerThan   *int64     // smaller: filter (bytes)
	AccountID     *int64     // in: account filter
}

// IsEmpty returns true if the query has no search criteria.
func (q *Query) IsEmpty() bool {
	return len(q.TextTerms) == 0 &&
		len(q.FromAddrs) == 0 &&
		len(q.ToAddrs) == 0 &&
		len(q.CcAddrs) == 0 &&
		len(q.BccAddrs) == 0 &&
		len(q.SubjectTerms) == 0 &&
		len(q.Labels) == 0 &&
		q.HasAttachment == nil &&
		q.BeforeDate == nil &&
		q.AfterDate == nil &&
		q.LargerThan == nil &&
		q.SmallerThan == nil
}

// operatorFn handles a parsed operator:value pair by applying it to the query.
type operatorFn func(q *Query, value string, now time.Time)

// operators maps operator names to their handler functions.
var operators = map[string]operatorFn{
	"from": func(q *Query, v string, _ time.Time) {
		q.FromAddrs = append(q.FromAddrs, strings.ToLower(v))
	},
	"to": func(q *Query, v string, _ time.Time) {
		q.ToAddrs = append(q.ToAddrs, strings.ToLower(v))
	},
	"cc": func(q *Query, v string, _ time.Time) {
		q.CcAddrs = append(q.CcAddrs, strings.ToLower(v))
	},
	"bcc": func(q *Query, v string, _ time.Time) {
		q.BccAddrs = append(q.BccAddrs, strings.ToLower(v))
	},
	"subject": func(q *Query, v string, _ time.Time) {
		q.SubjectTerms = append(q.SubjectTerms, v)
	},
	"label": func(q *Query, v string, _ time.Time) {
		q.Labels = append(q.Labels, v)
	},
	"l": func(q *Query, v string, _ time.Time) {
		q.Labels = append(q.Labels, v)
	},
	"has": func(q *Query, v string, _ time.Time) {
		if low := strings.ToLower(v); low == "attachment" || low == "attachments" {
			b := true
			q.HasAttachment = &b
		}
	},
	"before": func(q *Query, v string, _ time.Time) {
		if t := parseDate(v); t != nil {
			q.BeforeDate = t
		}
	},
	"after": func(q *Query, v string, _ time.Time) {
		if t := parseDate(v); t != nil {
			q.AfterDate = t
		}
	},
	"older_than": func(q *Query, v string, now time.Time) {
		if t := parseRelativeDate(v, now); t != nil {
			q.BeforeDate = t
		}
	},
	"newer_than": func(q *Query, v string, now time.Time) {
		if t := parseRelativeDate(v, now); t != nil {
			q.AfterDate = t
		}
	},
	"larger": func(q *Query, v string, _ time.Time) {
		if size := parseSize(v); size != nil {
			q.LargerThan = size
		}
	},
	"smaller": func(q *Query, v string, _ time.Time) {
		if size := parseSize(v); size != nil {
			q.SmallerThan = size
		}
	},
}

// Parser holds configuration for query parsing.
type Parser struct {
	Now func() time.Time // Time source (mockable for testing)
}

// NewParser creates a Parser with default settings.
func NewParser() *Parser {
	return &Parser{Now: func() time.Time { return time.Now().UTC() }}
}

// Parse parses a Gmail-like search query string into a Query object.
//
// Supported operators:
//   - from:, to:, cc:, bcc: - address filters
//   - subject: - subject text search
//   - label: or l: - label filter
//   - has:attachment - attachment filter
//   - before:, after: - date filters (YYYY-MM-DD)
//   - older_than:, newer_than: - relative date filters (e.g., 7d, 2w, 1m, 1y)
//   - larger:, smaller: - size filters (e.g., 5M, 100K)
//   - Bare words and "quoted phrases" - full-text search
func (p *Parser) Parse(queryStr string) *Query {
	q := &Query{}
	now := time.Now().UTC()
	if p.Now != nil {
		now = p.Now()
	}
	tokens := tokenize(queryStr)

	for _, token := range tokens {
		if isQuotedPhrase(token) {
			q.TextTerms = append(q.TextTerms, unquote(token))
			continue
		}

		if idx := strings.Index(token, ":"); idx != -1 {
			op := strings.ToLower(token[:idx])
			value := unquote(token[idx+1:])

			if handler, ok := operators[op]; ok {
				handler(q, value, now)
			} else {
				q.TextTerms = append(q.TextTerms, token)
			}
			continue
		}

		q.TextTerms = append(q.TextTerms, token)
	}

	return q
}

// Parse is a convenience function that parses using default settings.
func Parse(queryStr string) *Query {
	return NewParser().Parse(queryStr)
}

// unquote removes surrounding double quotes from a string if present.
func unquote(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}

// isQuotedPhrase returns true if the token is a double-quoted phrase.
func isQuotedPhrase(token string) bool {
	return len(token) > 2 && token[0] == '"' && token[len(token)-1] == '"'
}

// tokenize splits a query string, preserving quoted phrases and operator:value pairs.
// Handles cases like subject:"foo bar" where the operator and quoted value should stay together.
func tokenize(queryStr string) []string {
	var tokens []string
	var current strings.Builder
	inQuotes := false
	quoteChar := rune(0)
	// Track if we just saw a colon (for op:"value" handling)
	afterColon := false
	// Track if this quoted section started as op:"value" (quote immediately after colon)
	opQuoted := false

	for _, char := range queryStr {
		if (char == '"' || char == '\'') && !inQuotes {
			// Start of quoted section
			inQuotes = true
			quoteChar = char
			// If we just saw a colon, this is an op:"value" case
			opQuoted = afterColon
			// If we just saw a colon, keep building the same token (op:"value" case)
			if !afterColon && current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
			// Include the quote in the token for op:"value" case
			if afterColon {
				current.WriteRune(char)
			}
			afterColon = false
		} else if char == quoteChar && inQuotes {
			// End of quoted section
			inQuotes = false
			// Check if this was an op:"value" case (quote started after colon)
			if opQuoted {
				// Include the closing quote and save the whole token
				current.WriteRune(char)
				tokens = append(tokens, current.String())
				current.Reset()
			} else if current.Len() > 0 {
				// Standalone quoted phrase (may contain colons, but not op:"value")
				tokens = append(tokens, "\""+current.String()+"\"")
				current.Reset()
			}
			quoteChar = 0
			opQuoted = false
		} else if char == ' ' && !inQuotes {
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
			afterColon = false
		} else {
			current.WriteRune(char)
			afterColon = (char == ':')
		}
	}

	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}

	return tokens
}

// parseDate parses date strings like YYYY-MM-DD or YYYY/MM/DD.
func parseDate(value string) *time.Time {
	formats := []string{
		"2006-01-02",
		"2006/01/02",
		"01/02/2006",
		"02/01/2006",
	}

	value = strings.TrimSpace(value)
	for _, format := range formats {
		if t, err := time.Parse(format, value); err == nil {
			t = t.UTC()
			return &t
		}
	}
	return nil
}

// parseRelativeDate parses relative dates like 7d, 2w, 1m, 1y relative to now.
func parseRelativeDate(value string, now time.Time) *time.Time {
	value = strings.TrimSpace(strings.ToLower(value))
	re := regexp.MustCompile(`^(\d+)([dwmy])$`)
	match := re.FindStringSubmatch(value)
	if match == nil {
		return nil
	}

	amount, _ := strconv.Atoi(match[1])
	unit := match[2]

	var result time.Time
	switch unit {
	case "d":
		result = now.AddDate(0, 0, -amount)
	case "w":
		result = now.AddDate(0, 0, -amount*7)
	case "m":
		result = now.AddDate(0, -amount, 0)
	case "y":
		result = now.AddDate(-amount, 0, 0)
	default:
		return nil
	}

	return &result
}

// parseSize parses size strings like 5M, 100K, 1G into bytes.
func parseSize(value string) *int64 {
	value = strings.TrimSpace(strings.ToUpper(value))
	multipliers := map[string]int64{
		"K":  1024,
		"KB": 1024,
		"M":  1024 * 1024,
		"MB": 1024 * 1024,
		"G":  1024 * 1024 * 1024,
		"GB": 1024 * 1024 * 1024,
	}

	for suffix, mult := range multipliers {
		if strings.HasSuffix(value, suffix) {
			numStr := value[:len(value)-len(suffix)]
			if num, err := strconv.ParseFloat(numStr, 64); err == nil {
				result := int64(num * float64(mult))
				return &result
			}
			return nil
		}
	}

	// Plain number (bytes)
	if num, err := strconv.ParseInt(value, 10, 64); err == nil {
		return &num
	}
	return nil
}
