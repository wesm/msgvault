package whatsapp

import (
	"database/sql"
	"testing"
)

func TestNormalizePhone(t *testing.T) {
	tests := []struct {
		user, server string
		want         string
	}{
		{"447700900000", "s.whatsapp.net", "+447700900000"},
		{"12025551234", "s.whatsapp.net", "+12025551234"},
		{"+447700900000", "s.whatsapp.net", "+447700900000"},
		{"", "s.whatsapp.net", ""},
		{"447700900000", "g.us", "+447700900000"},
	}

	for _, tt := range tests {
		got := normalizePhone(tt.user, tt.server)
		if got != tt.want {
			t.Errorf("normalizePhone(%q, %q) = %q, want %q", tt.user, tt.server, got, tt.want)
		}
	}
}

func TestMapMediaType(t *testing.T) {
	tests := []struct {
		waType int
		want   string
	}{
		{0, ""}, // text
		{1, "image"},
		{2, "video"},
		{3, "audio"},
		{4, "gif"},
		{5, "voice_note"},
		{13, "document"},
		{90, "sticker"},
		{7, ""},  // system (no media type)
		{15, ""}, // call
		{99, ""}, // poll
	}

	for _, tt := range tests {
		got := mapMediaType(tt.waType)
		if got != tt.want {
			t.Errorf("mapMediaType(%d) = %q, want %q", tt.waType, got, tt.want)
		}
	}
}

func TestIsMediaType(t *testing.T) {
	if !isMediaType(1) {
		t.Error("isMediaType(1) should be true (image)")
	}
	if isMediaType(0) {
		t.Error("isMediaType(0) should be false (text)")
	}
	if isMediaType(7) {
		t.Error("isMediaType(7) should be false (system)")
	}
}

func TestIsSkippedType(t *testing.T) {
	skipped := []int{7, 9, 10, 15, 64, 66, 99, 11}
	for _, typ := range skipped {
		if !isSkippedType(typ) {
			t.Errorf("isSkippedType(%d) should be true", typ)
		}
	}

	notSkipped := []int{0, 1, 2, 3, 4, 5, 13, 90}
	for _, typ := range notSkipped {
		if isSkippedType(typ) {
			t.Errorf("isSkippedType(%d) should be false", typ)
		}
	}
}

func TestMapConversation(t *testing.T) {
	// Direct chat.
	direct := waChat{
		RawString: "447700900000@s.whatsapp.net",
		GroupType: 0,
	}
	id, typ, title := mapConversation(direct)
	if id != "447700900000@s.whatsapp.net" {
		t.Errorf("direct chat sourceConvID = %q, want %q", id, "447700900000@s.whatsapp.net")
	}
	if typ != "direct_chat" {
		t.Errorf("direct chat convType = %q, want %q", typ, "direct_chat")
	}
	if title != "" {
		t.Errorf("direct chat title = %q, want empty", title)
	}

	// Group chat.
	group := waChat{
		RawString: "120363001234567890@g.us",
		GroupType: 1,
		Subject:   sql.NullString{String: "Family Group", Valid: true},
	}
	id, typ, title = mapConversation(group)
	if id != "120363001234567890@g.us" {
		t.Errorf("group chat sourceConvID = %q", id)
	}
	if typ != "group_chat" {
		t.Errorf("group chat convType = %q, want %q", typ, "group_chat")
	}
	if title != "Family Group" {
		t.Errorf("group chat title = %q, want %q", title, "Family Group")
	}
}

func TestMapMessage(t *testing.T) {
	msg := waMessage{
		RowID:       42,
		ChatRowID:   1,
		FromMe:      1,
		KeyID:       "ABC123",
		Timestamp:   1700000000000, // ms
		MessageType: 0,
		TextData:    sql.NullString{String: "Hello world", Valid: true},
	}

	senderID := sql.NullInt64{Int64: 99, Valid: true}
	result := mapMessage(msg, 10, 20, senderID)

	if result.ConversationID != 10 {
		t.Errorf("ConversationID = %d, want 10", result.ConversationID)
	}
	if result.SourceID != 20 {
		t.Errorf("SourceID = %d, want 20", result.SourceID)
	}
	if result.SourceMessageID != "ABC123" {
		t.Errorf("SourceMessageID = %q, want %q", result.SourceMessageID, "ABC123")
	}
	if result.MessageType != "whatsapp" {
		t.Errorf("MessageType = %q, want %q", result.MessageType, "whatsapp")
	}
	if !result.IsFromMe {
		t.Error("IsFromMe should be true")
	}
	if !result.SentAt.Valid {
		t.Error("SentAt should be valid")
	}
	if result.SentAt.Time.Unix() != 1700000000 {
		t.Errorf("SentAt Unix = %d, want 1700000000", result.SentAt.Time.Unix())
	}
	if !result.Snippet.Valid || result.Snippet.String != "Hello world" {
		t.Errorf("Snippet = %v, want 'Hello world'", result.Snippet)
	}
	if result.HasAttachments {
		t.Error("HasAttachments should be false for text message")
	}
}

func TestMapMessageSnippetTruncation(t *testing.T) {
	// Create a message with text longer than 100 characters.
	longText := ""
	for i := 0; i < 150; i++ {
		longText += "x"
	}

	msg := waMessage{
		KeyID:       "LONG1",
		Timestamp:   1700000000000,
		MessageType: 0,
		TextData:    sql.NullString{String: longText, Valid: true},
	}

	result := mapMessage(msg, 1, 1, sql.NullInt64{})
	if !result.Snippet.Valid {
		t.Fatal("Snippet should be valid")
	}
	if len([]rune(result.Snippet.String)) != 100 {
		t.Errorf("Snippet rune count = %d, want 100", len([]rune(result.Snippet.String)))
	}
}

func TestMapGroupRole(t *testing.T) {
	tests := []struct {
		admin int
		want  string
	}{
		{0, "member"},
		{1, "admin"},
		{2, "admin"}, // superadmin
		{3, "member"},
	}

	for _, tt := range tests {
		got := mapGroupRole(tt.admin)
		if got != tt.want {
			t.Errorf("mapGroupRole(%d) = %q, want %q", tt.admin, got, tt.want)
		}
	}
}

func TestMapReaction(t *testing.T) {
	r := waReaction{
		ReactionValue: sql.NullString{String: "❤️", Valid: true},
	}
	typ, val := mapReaction(r)
	if typ != "emoji" {
		t.Errorf("reaction type = %q, want %q", typ, "emoji")
	}
	if val != "❤️" {
		t.Errorf("reaction value = %q, want %q", val, "❤️")
	}

	// Empty reaction.
	empty := waReaction{
		ReactionValue: sql.NullString{},
	}
	_, val = mapReaction(empty)
	if val != "" {
		t.Errorf("empty reaction value = %q, want empty", val)
	}
}

func TestChatTitle(t *testing.T) {
	// Group with subject.
	group := waChat{
		Subject:   sql.NullString{String: "Work Chat", Valid: true},
		User:      "120363001234567890",
		Server:    "g.us",
		RawString: "120363001234567890@g.us",
	}
	if chatTitle(group) != "Work Chat" {
		t.Errorf("chatTitle(group) = %q, want %q", chatTitle(group), "Work Chat")
	}

	// Direct chat.
	direct := waChat{
		User:      "447700900000",
		Server:    "s.whatsapp.net",
		RawString: "447700900000@s.whatsapp.net",
	}
	if chatTitle(direct) != "+447700900000" {
		t.Errorf("chatTitle(direct) = %q, want %q", chatTitle(direct), "+447700900000")
	}
}
