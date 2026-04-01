package textimport_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/store"
)

// TestIntegration exercises the full text message import pipeline:
// store methods, participant deduplication across sources,
// conversation stats recomputation, and TextEngine queries.
func TestIntegration(t *testing.T) {
	ctx := context.Background()

	// Create a temporary on-disk DB (store.Open does MkdirAll, WAL, etc.)
	dbPath := filepath.Join(t.TempDir(), "integration.db")
	s, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if err := s.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	// --- Sources ---
	src1, err := s.GetOrCreateSource("whatsapp", "whatsapp:+15550000001")
	if err != nil {
		t.Fatalf("GetOrCreateSource(whatsapp): %v", err)
	}
	src2, err := s.GetOrCreateSource("apple_messages", "apple_messages:+15550000001")
	if err != nil {
		t.Fatalf("GetOrCreateSource(apple_messages): %v", err)
	}

	// --- Participant deduplication across sources ---
	// Both sources reference the same phone +15551234567.
	// EnsureParticipantByPhone deduplicates by phone, so both calls should
	// return the same participant ID.
	participantID1, err := s.EnsureParticipantByPhone("+15551234567", "Alice", "whatsapp")
	if err != nil {
		t.Fatalf("EnsureParticipantByPhone(src1): %v", err)
	}
	participantID2, err := s.EnsureParticipantByPhone("+15551234567", "Alice", "imessage")
	if err != nil {
		t.Fatalf("EnsureParticipantByPhone(src2): %v", err)
	}
	if participantID1 != participantID2 {
		t.Errorf("same phone across sources: participant IDs differ: %d != %d", participantID1, participantID2)
	}
	phoneParticipantID := participantID1

	// --- Conversations ---
	conv1ID, err := s.EnsureConversationWithType(src1.ID, "wa-conv-1", "whatsapp", "WhatsApp Chat")
	if err != nil {
		t.Fatalf("EnsureConversationWithType(src1): %v", err)
	}
	conv2ID, err := s.EnsureConversationWithType(src2.ID, "am-conv-1", "imessage", "iMessage Chat")
	if err != nil {
		t.Fatalf("EnsureConversationWithType(src2): %v", err)
	}

	// Link participant to both conversations.
	if err := s.EnsureConversationParticipant(conv1ID, phoneParticipantID, "member"); err != nil {
		t.Fatalf("EnsureConversationParticipant(conv1): %v", err)
	}
	if err := s.EnsureConversationParticipant(conv2ID, phoneParticipantID, "member"); err != nil {
		t.Fatalf("EnsureConversationParticipant(conv2): %v", err)
	}

	// --- Messages for source 1 (whatsapp) ---
	baseTime := time.Date(2024, 6, 1, 10, 0, 0, 0, time.UTC)
	whatsappMsgs := []struct {
		srcMsgID string
		snippet  string
		sentAt   time.Time
		fromMe   bool
	}{
		{"wa-1", "Hello from WhatsApp", baseTime, false},
		{"wa-2", "Reply on WhatsApp", baseTime.Add(time.Minute), true},
		{"wa-3", "Third WhatsApp message", baseTime.Add(2 * time.Minute), false},
	}
	for _, m := range whatsappMsgs {
		msg := &store.Message{
			SourceID:        src1.ID,
			SourceMessageID: m.srcMsgID,
			ConversationID:  conv1ID,
			MessageType:     "whatsapp",
			Snippet:         sql.NullString{String: m.snippet, Valid: true},
			SentAt:          sql.NullTime{Time: m.sentAt, Valid: true},
			IsFromMe:        m.fromMe,
			SizeEstimate:    int64(len(m.snippet)),
			SenderID:        sql.NullInt64{Int64: phoneParticipantID, Valid: !m.fromMe},
		}
		msgID, err := s.UpsertMessage(msg)
		if err != nil {
			t.Fatalf("UpsertMessage(%s): %v", m.srcMsgID, err)
		}
		bodyText := sql.NullString{String: m.snippet, Valid: true}
		if err := s.UpsertMessageBody(msgID, bodyText, sql.NullString{}); err != nil {
			t.Fatalf("UpsertMessageBody(%s): %v", m.srcMsgID, err)
		}
		// Add participant as message recipient for TextAggregate to pick up
		if err := s.ReplaceMessageRecipients(
			msgID, "from",
			[]int64{phoneParticipantID}, []string{"Alice"},
		); err != nil {
			t.Fatalf("ReplaceMessageRecipients(%s): %v", m.srcMsgID, err)
		}
	}

	// --- Messages for source 2 (apple_messages) ---
	imessageMsgs := []struct {
		srcMsgID string
		snippet  string
		sentAt   time.Time
	}{
		{"am-1", "Hi from iMessage", baseTime.Add(time.Hour)},
		{"am-2", "iMessage follow-up", baseTime.Add(time.Hour + time.Minute)},
	}
	for _, m := range imessageMsgs {
		msg := &store.Message{
			SourceID:        src2.ID,
			SourceMessageID: m.srcMsgID,
			ConversationID:  conv2ID,
			MessageType:     "imessage",
			Snippet:         sql.NullString{String: m.snippet, Valid: true},
			SentAt:          sql.NullTime{Time: m.sentAt, Valid: true},
			SizeEstimate:    int64(len(m.snippet)),
			SenderID:        sql.NullInt64{Int64: phoneParticipantID, Valid: true},
		}
		msgID, err := s.UpsertMessage(msg)
		if err != nil {
			t.Fatalf("UpsertMessage(%s): %v", m.srcMsgID, err)
		}
		bodyText := sql.NullString{String: m.snippet, Valid: true}
		if err := s.UpsertMessageBody(msgID, bodyText, sql.NullString{}); err != nil {
			t.Fatalf("UpsertMessageBody(%s): %v", m.srcMsgID, err)
		}
		if err := s.ReplaceMessageRecipients(
			msgID, "from",
			[]int64{phoneParticipantID}, []string{"Alice"},
		); err != nil {
			t.Fatalf("ReplaceMessageRecipients(%s): %v", m.srcMsgID, err)
		}
	}

	// --- Labels ---
	labelID, err := s.EnsureLabel(src1.ID, "important", "Important", "user")
	if err != nil {
		t.Fatalf("EnsureLabel: %v", err)
	}
	// Fetch the first WhatsApp message ID to link a label.
	var wa1MsgID int64
	if err := s.DB().QueryRow(
		`SELECT id FROM messages WHERE source_message_id = ?`, "wa-1",
	).Scan(&wa1MsgID); err != nil {
		t.Fatalf("lookup wa-1 message: %v", err)
	}
	if err := s.LinkMessageLabel(wa1MsgID, labelID); err != nil {
		t.Fatalf("LinkMessageLabel: %v", err)
	}

	// Verify label is linked.
	var labelCount int
	if err := s.DB().QueryRow(
		`SELECT COUNT(*) FROM message_labels WHERE message_id = ?`, wa1MsgID,
	).Scan(&labelCount); err != nil {
		t.Fatalf("count labels: %v", err)
	}
	if labelCount != 1 {
		t.Errorf("label count for wa-1: got %d, want 1", labelCount)
	}

	// --- Recompute conversation stats ---
	if err := s.RecomputeConversationStats(src1.ID); err != nil {
		t.Fatalf("RecomputeConversationStats(src1): %v", err)
	}
	if err := s.RecomputeConversationStats(src2.ID); err != nil {
		t.Fatalf("RecomputeConversationStats(src2): %v", err)
	}

	// Verify conversation stats for conv1.
	var msgCount int64
	if err := s.DB().QueryRow(
		`SELECT message_count FROM conversations WHERE id = ?`, conv1ID,
	).Scan(&msgCount); err != nil {
		t.Fatalf("read conv1 stats: %v", err)
	}
	if msgCount != 3 {
		t.Errorf("conv1 message_count: got %d, want 3", msgCount)
	}

	// --- TextEngine queries ---
	eng := query.NewSQLiteEngine(s.DB())
	var te query.TextEngine = eng

	// ListConversations — should return both conversations.
	convRows, err := te.ListConversations(ctx, query.TextFilter{})
	if err != nil {
		t.Fatalf("ListConversations: %v", err)
	}
	if len(convRows) != 2 {
		t.Errorf("ListConversations: got %d rows, want 2", len(convRows))
	}
	convByID := make(map[int64]query.ConversationRow)
	for _, row := range convRows {
		convByID[row.ConversationID] = row
	}
	if row, ok := convByID[conv1ID]; !ok {
		t.Errorf("conv1 not found in ListConversations results")
	} else if row.MessageCount != 3 {
		t.Errorf("conv1 MessageCount: got %d, want 3", row.MessageCount)
	}
	if row, ok := convByID[conv2ID]; !ok {
		t.Errorf("conv2 not found in ListConversations results")
	} else if row.MessageCount != 2 {
		t.Errorf("conv2 MessageCount: got %d, want 2", row.MessageCount)
	}

	// TextAggregate by contacts — groups by phone number.
	// All 5 messages have +15551234567 as the from participant.
	aggRows, err := te.TextAggregate(ctx, query.TextViewContacts, query.TextAggregateOptions{Limit: 100})
	if err != nil {
		t.Fatalf("TextAggregate(TextViewContacts): %v", err)
	}
	if len(aggRows) == 0 {
		t.Fatal("TextAggregate(TextViewContacts): got 0 rows, want at least 1")
	}
	foundPhone := false
	for _, row := range aggRows {
		if row.Key == "+15551234567" {
			foundPhone = true
			if row.Count != 5 {
				t.Errorf("contact +15551234567: got count %d, want 5", row.Count)
			}
		}
	}
	if !foundPhone {
		t.Errorf("TextAggregate: phone +15551234567 not found in results")
	}

	// ListConversationMessages — returns messages for conv1 in chronological order.
	messages, err := te.ListConversationMessages(ctx, conv1ID, query.TextFilter{})
	if err != nil {
		t.Fatalf("ListConversationMessages(conv1): %v", err)
	}
	if len(messages) != 3 {
		t.Errorf("ListConversationMessages(conv1): got %d messages, want 3", len(messages))
	}
	// Verify chronological order (ascending by sent_at).
	for i := 1; i < len(messages); i++ {
		if messages[i].SentAt.Before(messages[i-1].SentAt) {
			t.Errorf("messages not in chronological order at index %d", i)
		}
	}
	// Verify message type is correct.
	for _, msg := range messages {
		if msg.MessageType != "whatsapp" {
			t.Errorf("expected message_type=whatsapp, got %q", msg.MessageType)
		}
	}

	// GetTextStats — should count all 5 text messages.
	stats, err := te.GetTextStats(ctx, query.TextStatsOptions{})
	if err != nil {
		t.Fatalf("GetTextStats: %v", err)
	}
	if stats.MessageCount != 5 {
		t.Errorf("GetTextStats.MessageCount: got %d, want 5", stats.MessageCount)
	}
	// Should see 2 accounts (sources).
	if stats.AccountCount != 2 {
		t.Errorf("GetTextStats.AccountCount: got %d, want 2", stats.AccountCount)
	}
	// LabelCount: 1 label linked to at least one text message.
	if stats.LabelCount != 1 {
		t.Errorf("GetTextStats.LabelCount: got %d, want 1", stats.LabelCount)
	}

	// GetTextStats filtered by source 1 only.
	statsS1, err := te.GetTextStats(ctx, query.TextStatsOptions{SourceID: &src1.ID})
	if err != nil {
		t.Fatalf("GetTextStats(src1): %v", err)
	}
	if statsS1.MessageCount != 3 {
		t.Errorf("GetTextStats(src1).MessageCount: got %d, want 3", statsS1.MessageCount)
	}
}
