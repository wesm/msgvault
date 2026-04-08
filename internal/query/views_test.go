package query

import (
	"context"
	"database/sql"
	"testing"
)

func TestDuckDBEngine_QuerySQL(t *testing.T) {
	builder := NewTestDataBuilder(t)
	srcID := builder.AddSource("test@example.com")
	bob := builder.AddParticipant(
		"bob@example.com", "example.com", "Bob",
	)
	msgID := builder.AddMessage(MessageOpt{
		Subject: "Test", SourceID: srcID, SizeEstimate: 100,
	})
	builder.AddFrom(msgID, bob, "Bob")

	engine := builder.BuildEngine()
	defer func() { _ = engine.Close() }()

	ctx := context.Background()
	result, err := engine.QuerySQL(ctx,
		"SELECT from_email, message_count FROM v_senders")
	if err != nil {
		t.Fatalf("QuerySQL: %v", err)
	}
	if len(result.Columns) < 2 {
		t.Fatalf(
			"columns = %v, want at least 2", result.Columns,
		)
	}
	if result.Columns[0] != "from_email" {
		t.Errorf(
			"columns[0] = %q, want from_email",
			result.Columns[0],
		)
	}
	if result.RowCount != 1 {
		t.Errorf("row_count = %d, want 1", result.RowCount)
	}
}

func TestDuckDBEngine_QuerySQL_Error(t *testing.T) {
	builder := NewTestDataBuilder(t)
	builder.AddSource("test@example.com")
	engine := builder.BuildEngine()
	defer func() { _ = engine.Close() }()

	_, err := engine.QuerySQL(
		context.Background(),
		"SELECT * FROM nonexistent_table",
	)
	if err == nil {
		t.Fatal("expected error for bad SQL")
	}
}

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
		var fromName string
		var msgCount int64
		var totalSize int64
		err := engine.db.QueryRowContext(ctx,
			"SELECT from_name, message_count, total_size "+
				"FROM v_senders "+
				"WHERE from_email = 'bob@corp.com'",
		).Scan(&fromName, &msgCount, &totalSize)
		if err != nil {
			t.Fatalf("scan v_senders: %v", err)
		}
		if fromName != "Bob Smith" {
			t.Errorf("from_name = %q, want %q",
				fromName, "Bob Smith")
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
		// Both messages share the same conversation (auto-assigned),
		// so we expect exactly 2 threads (one per conversation).
		var threadCount int
		err := engine.db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM v_threads",
		).Scan(&threadCount)
		if err != nil {
			t.Fatalf("scan v_threads count: %v", err)
		}
		if threadCount != 2 {
			t.Errorf("thread count = %d, want 2", threadCount)
		}

		// Sum of message_count across all threads should be 2.
		var totalMsgCount int64
		err = engine.db.QueryRowContext(ctx,
			"SELECT SUM(message_count) FROM v_threads",
		).Scan(&totalMsgCount)
		if err != nil {
			t.Fatalf("scan v_threads sum: %v", err)
		}
		if totalMsgCount != 2 {
			t.Errorf(
				"total message_count = %d, want 2",
				totalMsgCount,
			)
		}

		// Verify participant_emails, conversation_title, conversation_type
		var participantEmails sql.NullString
		var convTitle, convType string
		err = engine.db.QueryRowContext(ctx,
			"SELECT participant_emails, conversation_title, conversation_type FROM v_threads LIMIT 1",
		).Scan(&participantEmails, &convTitle, &convType)
		if err != nil {
			t.Fatalf("scan v_threads columns: %v", err)
		}
		if convType != "email" {
			t.Errorf("conversation_type = %q, want %q",
				convType, "email")
		}
	})
}
