package query

import (
	"context"
	"testing"
)

func TestRegisterViews_BaseViews(t *testing.T) {
	builder := NewTestDataBuilder(t)
	srcID := builder.AddSource("alice@example.com")
	partID := builder.AddParticipant(
		"bob@example.com", "example.com", "Bob",
	)
	lblID := builder.AddLabel("INBOX")
	msgID := builder.AddMessage(MessageOpt{
		Subject:  "Hello",
		SourceID: srcID,
	})
	builder.AddFrom(msgID, partID, "Bob")
	builder.AddMessageLabel(msgID, lblID)

	dir, cleanup := builder.Build()
	defer cleanup()

	engine := builder.BuildEngine()
	defer func() { _ = engine.Close() }()

	if err := RegisterViews(engine.db, dir); err != nil {
		t.Fatalf("RegisterViews: %v", err)
	}

	tables := []string{
		"messages", "participants", "message_recipients",
		"labels", "message_labels", "attachments",
		"conversations", "sources",
	}
	for _, table := range tables {
		var count int
		err := engine.db.QueryRowContext(
			context.Background(),
			"SELECT COUNT(*) FROM "+table,
		).Scan(&count)
		if err != nil {
			t.Errorf("query %s: %v", table, err)
		}
	}

	var id int64
	var subject, messageType string
	var attachmentCount int
	err := engine.db.QueryRowContext(
		context.Background(),
		"SELECT id, subject, attachment_count, message_type FROM messages LIMIT 1",
	).Scan(&id, &subject, &attachmentCount, &messageType)
	if err != nil {
		t.Fatalf("scan messages: %v", err)
	}
	if subject != "Hello" {
		t.Errorf("subject = %q, want %q", subject, "Hello")
	}
}
