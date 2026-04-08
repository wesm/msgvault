package query

import (
	"context"
	"database/sql"
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

func TestRegisterViews_ConvenienceViews(t *testing.T) {
	builder := NewTestDataBuilder(t)
	srcID := builder.AddSource("alice@example.com")
	bob := builder.AddParticipant(
		"bob@corp.com", "corp.com", "Bob Smith",
	)
	carol := builder.AddParticipant(
		"carol@corp.com", "corp.com", "Carol",
	)
	inbox := builder.AddLabel("INBOX")
	sent := builder.AddLabel("SENT")

	msg1 := builder.AddMessage(MessageOpt{
		Subject:      "First",
		SourceID:     srcID,
		SizeEstimate: 1000,
	})
	builder.AddFrom(msg1, bob, "Bob Smith")
	builder.AddTo(msg1, carol, "Carol")
	builder.AddMessageLabel(msg1, inbox)
	builder.AddAttachment(msg1, 500, "doc.pdf")

	msg2 := builder.AddMessage(MessageOpt{
		Subject:      "Second",
		SourceID:     srcID,
		SizeEstimate: 2000,
	})
	builder.AddFrom(msg2, bob, "Bob Smith")
	builder.AddMessageLabel(msg2, inbox)
	builder.AddMessageLabel(msg2, sent)

	dir, cleanup := builder.Build()
	defer cleanup()
	engine := builder.BuildEngine()
	defer func() { _ = engine.Close() }()

	if err := RegisterViews(engine.db, dir); err != nil {
		t.Fatalf("RegisterViews: %v", err)
	}
	ctx := context.Background()

	t.Run("v_messages", func(t *testing.T) {
		var fromEmail, fromDomain, labels string
		err := engine.db.QueryRowContext(ctx,
			"SELECT from_email, from_domain, labels "+
				"FROM v_messages WHERE subject = 'First'",
		).Scan(&fromEmail, &fromDomain, &labels)
		if err != nil {
			t.Fatalf("scan v_messages: %v", err)
		}
		if fromEmail != "bob@corp.com" {
			t.Errorf("from_email = %q, want %q",
				fromEmail, "bob@corp.com")
		}
		if fromDomain != "corp.com" {
			t.Errorf("from_domain = %q, want %q",
				fromDomain, "corp.com")
		}
		if labels != `["INBOX"]` {
			t.Errorf("labels = %q, want %q", labels, `["INBOX"]`)
		}
	})

	t.Run("v_messages_multi_labels", func(t *testing.T) {
		var labels string
		err := engine.db.QueryRowContext(ctx,
			"SELECT labels FROM v_messages "+
				"WHERE subject = 'Second'",
		).Scan(&labels)
		if err != nil {
			t.Fatalf("scan v_messages: %v", err)
		}
		if labels != `["INBOX","SENT"]` {
			t.Errorf("labels = %q, want %q",
				labels, `["INBOX","SENT"]`)
		}
	})

	t.Run("v_senders", func(t *testing.T) {
		var msgCount int64
		var totalSize int64
		err := engine.db.QueryRowContext(ctx,
			"SELECT message_count, total_size "+
				"FROM v_senders "+
				"WHERE email_address = 'bob@corp.com'",
		).Scan(&msgCount, &totalSize)
		if err != nil {
			t.Fatalf("scan v_senders: %v", err)
		}
		if msgCount != 2 {
			t.Errorf("message_count = %d, want 2", msgCount)
		}
		if totalSize != 3000 {
			t.Errorf("total_size = %d, want 3000", totalSize)
		}
	})

	t.Run("v_domains", func(t *testing.T) {
		var msgCount, senderCount int64
		err := engine.db.QueryRowContext(ctx,
			"SELECT message_count, sender_count "+
				"FROM v_domains "+
				"WHERE domain = 'corp.com'",
		).Scan(&msgCount, &senderCount)
		if err != nil {
			t.Fatalf("scan v_domains: %v", err)
		}
		if msgCount != 2 {
			t.Errorf("message_count = %d, want 2", msgCount)
		}
		if senderCount != 1 {
			t.Errorf("sender_count = %d, want 1", senderCount)
		}
	})

	t.Run("v_labels", func(t *testing.T) {
		var msgCount int64
		err := engine.db.QueryRowContext(ctx,
			"SELECT message_count FROM v_labels "+
				"WHERE name = 'INBOX'",
		).Scan(&msgCount)
		if err != nil {
			t.Fatalf("scan v_labels: %v", err)
		}
		if msgCount != 2 {
			t.Errorf("message_count = %d, want 2", msgCount)
		}
	})

	t.Run("v_threads", func(t *testing.T) {
		var count int
		err := engine.db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM v_threads "+
				"WHERE message_count >= 1",
		).Scan(&count)
		if err != nil {
			t.Fatalf("scan v_threads: %v", err)
		}
		if count < 1 {
			t.Errorf("expected at least 1 thread, got %d", count)
		}

		// Verify participant_emails is valid JSON
		var participantEmails sql.NullString
		err = engine.db.QueryRowContext(ctx,
			"SELECT participant_emails FROM v_threads LIMIT 1",
		).Scan(&participantEmails)
		if err != nil {
			t.Fatalf("scan v_threads participant_emails: %v", err)
		}
	})
}
