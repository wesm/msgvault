package importer

import (
	"context"
	"database/sql"
	"log/slog"
	"path/filepath"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/wesm/msgvault/internal/store"
	"github.com/wesm/msgvault/internal/testutil/email"
)

func TestNormalizeMessageID_InvalidUTF8(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "latin1 bytes in angle brackets",
			input: "<03f501c9a35b$add3cc60$cc22f472@\xD5\xC5\xC6\xE6\xB9\xF3>",
		},
		{
			name:  "bare invalid bytes",
			input: "msg-\x80\x81\x82@example.com",
		},
		{
			name:  "valid utf8 unchanged",
			input: "<valid@example.com>",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizeMessageID(tt.input)
			if !utf8.ValidString(result) {
				t.Errorf(
					"normalizeMessageID(%q) produced invalid UTF-8: %q",
					tt.input, result,
				)
			}
		})
	}
}

func TestNormalizeMessageID_PreservesValidContent(t *testing.T) {
	result := normalizeMessageID("<valid@example.com>")
	if result != "valid@example.com" {
		t.Errorf("got %q, want %q", result, "valid@example.com")
	}
}

func TestIngestRawMessage_SanitizesAddressFields(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	src, err := st.GetOrCreateSource("test", "test@example.com")
	if err != nil {
		t.Fatalf("get/create source: %v", err)
	}

	// Build a message with a Message-ID containing invalid UTF-8.
	// The From address has a display name with invalid bytes.
	invalidName := "User \xD5\xC5\xC6"
	invalidMsgID := "<03f501c9a35b@\xD5\xC5\xC6\xE6\xB9\xF3>"

	raw := email.NewMessage().
		From(invalidName+" <sender@example.com>").
		To("recipient@example.com").
		Subject("Test").
		Header("Message-ID", invalidMsgID).
		Body("body text").
		Bytes()

	log := slog.Default()

	err = IngestRawMessage(
		context.Background(), st,
		src.ID, "test@example.com", "",
		nil, "source-msg-1", "fakehash",
		raw, time.Time{}, log,
	)
	if err != nil {
		t.Fatalf("IngestRawMessage: %v", err)
	}

	// Verify all participant fields are valid UTF-8
	db := st.DB()
	rows, err := db.Query(
		"SELECT email_address, display_name, domain FROM participants",
	)
	if err != nil {
		t.Fatalf("query participants: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var emailAddr string
		var displayName sql.NullString
		var domain string
		if err := rows.Scan(
			&emailAddr, &displayName, &domain,
		); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if !utf8.ValidString(emailAddr) {
			t.Errorf("invalid UTF-8 in email_address: %q", emailAddr)
		}
		if displayName.Valid && !utf8.ValidString(displayName.String) {
			t.Errorf(
				"invalid UTF-8 in display_name: %q",
				displayName.String,
			)
		}
		if !utf8.ValidString(domain) {
			t.Errorf("invalid UTF-8 in domain: %q", domain)
		}
	}

	// Verify conversation source_conversation_id is valid UTF-8
	rows2, err := db.Query(
		"SELECT source_conversation_id FROM conversations",
	)
	if err != nil {
		t.Fatalf("query conversations: %v", err)
	}
	defer func() { _ = rows2.Close() }()

	for rows2.Next() {
		var srcID string
		if err := rows2.Scan(&srcID); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if !utf8.ValidString(srcID) {
			t.Errorf(
				"invalid UTF-8 in source_conversation_id: %q", srcID,
			)
		}
	}
}
