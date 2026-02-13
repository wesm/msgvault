package store

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

func TestEscapeLike(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"plain text", "hello", "hello"},
		{"percent", "100% done", `100\% done`},
		{"underscore", "file_name", `file\_name`},
		{"backslash", `path\to`, `path\\to`},
		{"all special", `50%_off\sale`, `50\%\_off\\sale`},
		{"empty", "", ""},
		{"multiple percents", "%%", `\%\%`},
		{"adjacent specials", `%_\`, `\%\_\\`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := escapeLike(tt.input)
			if got != tt.want {
				t.Errorf("escapeLike(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// openTestStore creates a temporary store for internal tests.
func openTestStore(t *testing.T) *Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	st, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.InitSchema(); err != nil {
		t.Fatalf("InitSchema: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

// seedMessage inserts a message with the given subject and snippet, returning its ID.
// SentAt is left NULL so COALESCE returns NULL (avoids SQLite string-vs-time scan issue).
func seedMessage(t *testing.T, st *Store, sourceID, convID int64, sourceMessageID, subject, snippet string) int64 {
	t.Helper()
	id, err := st.UpsertMessage(&Message{
		ConversationID:  convID,
		SourceID:        sourceID,
		SourceMessageID: sourceMessageID,
		MessageType:     "email",
		Subject:         sql.NullString{String: subject, Valid: true},
		Snippet:         sql.NullString{String: snippet, Valid: true},
		SizeEstimate:    100,
	})
	if err != nil {
		t.Fatalf("UpsertMessage(%q): %v", sourceMessageID, err)
	}
	return id
}

func TestParseSQLiteTime(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  time.Time
	}{
		{
			"space-separated with fractional seconds and TZ",
			"2024-06-15 10:30:45.123456-07:00",
			time.Date(2024, 6, 15, 10, 30, 45, 123456000, time.FixedZone("", -7*3600)),
		},
		{
			"T-separated with fractional seconds and TZ",
			"2024-06-15T10:30:45.123456-07:00",
			time.Date(2024, 6, 15, 10, 30, 45, 123456000, time.FixedZone("", -7*3600)),
		},
		{
			"space-separated with fractional seconds no TZ",
			"2024-06-15 10:30:45.500",
			time.Date(2024, 6, 15, 10, 30, 45, 500000000, time.UTC),
		},
		{
			"T-separated with fractional seconds no TZ",
			"2024-06-15T10:30:45.500",
			time.Date(2024, 6, 15, 10, 30, 45, 500000000, time.UTC),
		},
		{
			"space-separated basic (datetime('now') format)",
			"2024-06-15 10:30:45",
			time.Date(2024, 6, 15, 10, 30, 45, 0, time.UTC),
		},
		{
			"T-separated basic",
			"2024-06-15T10:30:45",
			time.Date(2024, 6, 15, 10, 30, 45, 0, time.UTC),
		},
		{
			"space-separated without seconds",
			"2024-06-15 10:30",
			time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			"T-separated without seconds",
			"2024-06-15T10:30",
			time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			"date only",
			"2024-06-15",
			time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			"RFC3339 with Z",
			"2024-06-15T10:30:45Z",
			time.Date(2024, 6, 15, 10, 30, 45, 0, time.UTC),
		},
		{
			"RFC3339 with offset",
			"2024-06-15T10:30:45+05:30",
			time.Date(2024, 6, 15, 10, 30, 45, 0, time.FixedZone("", 5*3600+30*60)),
		},
		{
			"RFC3339Nano",
			"2024-06-15T10:30:45.123456789Z",
			time.Date(2024, 6, 15, 10, 30, 45, 123456789, time.UTC),
		},
		{
			"empty string returns zero time",
			"",
			time.Time{},
		},
		{
			"garbage returns zero time",
			"not-a-date",
			time.Time{},
		},
		{
			"unix timestamp string returns zero time",
			"1718451045",
			time.Time{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseSQLiteTime(tt.input)
			if !got.Equal(tt.want) {
				t.Errorf(
					"parseSQLiteTime(%q) = %v, want %v",
					tt.input, got, tt.want,
				)
			}
		})
	}
}

func TestSearchMessagesLikeLiteralWildcards(t *testing.T) {
	st := openTestStore(t)

	// Create a source and conversation
	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	if err != nil {
		t.Fatalf("GetOrCreateSource: %v", err)
	}
	convID, err := st.EnsureConversation(source.ID, "thread-1", "Thread")
	if err != nil {
		t.Fatalf("EnsureConversation: %v", err)
	}

	// Seed messages: one with literal %, one with literal _, one plain,
	// plus confounding rows that would match if wildcards weren't escaped.
	seedMessage(t, st, source.ID, convID, "msg-pct", "100% off sale", "great deal")
	seedMessage(t, st, source.ID, convID, "msg-pct-confound", "100 days sale", "another deal") // would match "100%" if % is a wildcard
	seedMessage(t, st, source.ID, convID, "msg-us", "file_name.txt", "attachment info")
	seedMessage(t, st, source.ID, convID, "msg-us-confound", "fileXname.txt", "another attachment") // would match "file_name" if _ is a wildcard
	seedMessage(t, st, source.ID, convID, "msg-plain", "plain subject", "nothing special")

	tests := []struct {
		name      string
		query     string
		wantCount int64
		wantLen   int // number of result rows
	}{
		{
			name:      "literal percent matches only percent message not confounding row",
			query:     "100%",
			wantCount: 1,
			wantLen:   1,
		},
		{
			name:      "literal underscore matches only underscore message not confounding row",
			query:     "file_name",
			wantCount: 1,
			wantLen:   1,
		},
		{
			name:      "plain query still works",
			query:     "plain",
			wantCount: 1,
			wantLen:   1,
		},
		{
			name:      "no match returns zero",
			query:     "nonexistent",
			wantCount: 0,
			wantLen:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			messages, total, err := st.searchMessagesLike(tt.query, 0, 100)
			if err != nil {
				t.Fatalf("searchMessagesLike(%q): %v", tt.query, err)
			}
			if total != tt.wantCount {
				t.Errorf("total = %d, want %d", total, tt.wantCount)
			}
			if len(messages) != tt.wantLen {
				t.Errorf("len(messages) = %d, want %d", len(messages), tt.wantLen)
			}
		})
	}
}
