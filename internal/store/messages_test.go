package store_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/wesm/msgvault/internal/store"
	"github.com/wesm/msgvault/internal/testutil"
)

func TestRecomputeConversationStats(t *testing.T) {
	st := testutil.NewTestStore(t)

	source, err := st.GetOrCreateSource("whatsapp", "+15550000001")
	if err != nil {
		t.Fatalf("GetOrCreateSource: %v", err)
	}

	convID, err := st.EnsureConversationWithType(source.ID, "conv-1", "whatsapp_dm", "Test Chat")
	if err != nil {
		t.Fatalf("EnsureConversationWithType: %v", err)
	}

	// Verify initial message_count is 0 (stats not maintained on insert).
	var initialCount int
	if err := st.DB().QueryRow(
		`SELECT message_count FROM conversations WHERE id = ?`, convID,
	).Scan(&initialCount); err != nil {
		t.Fatalf("initial message_count scan: %v", err)
	}
	if initialCount != 0 {
		t.Errorf("initial message_count = %d, want 0", initialCount)
	}

	sentAt := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	msg1 := &store.Message{
		SourceID:        source.ID,
		SourceMessageID: "msg-1",
		ConversationID:  convID,
		MessageType:     "whatsapp",
		SentAt:          sql.NullTime{Time: sentAt, Valid: true},
		Snippet:         sql.NullString{String: "hello", Valid: true},
	}
	if _, err := st.UpsertMessage(msg1); err != nil {
		t.Fatalf("UpsertMessage msg1: %v", err)
	}

	sentAt2 := sentAt.Add(time.Hour)
	msg2 := &store.Message{
		SourceID:        source.ID,
		SourceMessageID: "msg-2",
		ConversationID:  convID,
		MessageType:     "whatsapp",
		SentAt:          sql.NullTime{Time: sentAt2, Valid: true},
		Snippet:         sql.NullString{String: "world", Valid: true},
	}
	if _, err := st.UpsertMessage(msg2); err != nil {
		t.Fatalf("UpsertMessage msg2: %v", err)
	}

	// msg3 has the SAME sent_at as msg2 but a different snippet.
	// After recompute, last_message_preview must come from msg3 (higher id),
	// exercising the `id DESC` tie-breaker in the SQL.
	msg3 := &store.Message{
		SourceID:        source.ID,
		SourceMessageID: "msg-3",
		ConversationID:  convID,
		MessageType:     "whatsapp",
		SentAt:          sql.NullTime{Time: sentAt2, Valid: true},
		Snippet:         sql.NullString{String: "tie-breaker", Valid: true},
	}
	if _, err := st.UpsertMessage(msg3); err != nil {
		t.Fatalf("UpsertMessage msg3: %v", err)
	}

	// Add a conversation participant so participant_count is non-zero.
	participantID, err := st.EnsureParticipantByPhone("+15559876543", "Bob", "whatsapp")
	if err != nil {
		t.Fatalf("EnsureParticipantByPhone: %v", err)
	}
	if err := st.EnsureConversationParticipant(convID, participantID, "member"); err != nil {
		t.Fatalf("EnsureConversationParticipant: %v", err)
	}

	// Recompute and verify counts.
	if err := st.RecomputeConversationStats(source.ID); err != nil {
		t.Fatalf("RecomputeConversationStats: %v", err)
	}

	var count int
	var participantCount int
	var lastMsgAt sql.NullTime
	var preview sql.NullString
	if err := st.DB().QueryRow(
		`SELECT message_count, participant_count, last_message_at, last_message_preview
		 FROM conversations WHERE id = ?`, convID,
	).Scan(&count, &participantCount, &lastMsgAt, &preview); err != nil {
		t.Fatalf("post-recompute scan: %v", err)
	}
	if count != 3 {
		t.Errorf("message_count = %d, want 3", count)
	}
	if participantCount != 1 {
		t.Errorf("participant_count = %d, want 1", participantCount)
	}
	if !lastMsgAt.Valid {
		t.Error("last_message_at is NULL, want a timestamp")
	}
	// msg2 and msg3 share the same sent_at; msg3 has the higher id, so its
	// snippet ("tie-breaker") must win via the `id DESC` tie-breaker.
	if !preview.Valid || preview.String != "tie-breaker" {
		t.Errorf("last_message_preview = %q, want %q", preview.String, "tie-breaker")
	}

	// Idempotency: calling again should produce the same result.
	if err := st.RecomputeConversationStats(source.ID); err != nil {
		t.Fatalf("RecomputeConversationStats (second call): %v", err)
	}
	if err := st.DB().QueryRow(
		`SELECT message_count FROM conversations WHERE id = ?`, convID,
	).Scan(&count); err != nil {
		t.Fatalf("idempotency scan: %v", err)
	}
	if count != 3 {
		t.Errorf("idempotency: message_count = %d, want 3", count)
	}
}

func TestEnsureParticipantByPhone_IdentifierType(t *testing.T) {
	st := testutil.NewTestStore(t)

	// Create participant via WhatsApp
	id1, err := st.EnsureParticipantByPhone("+15551234567", "Alice", "whatsapp")
	if err != nil {
		t.Fatalf("EnsureParticipantByPhone(whatsapp): %v", err)
	}
	if id1 == 0 {
		t.Fatal("expected non-zero participant ID")
	}

	// Same phone via iMessage — should return the same participant ID
	id2, err := st.EnsureParticipantByPhone("+15551234567", "Alice", "imessage")
	if err != nil {
		t.Fatalf("EnsureParticipantByPhone(imessage): %v", err)
	}
	if id2 != id1 {
		t.Errorf("imessage call returned participant ID %d, want %d (same as whatsapp)", id2, id1)
	}

	// Both participant_identifiers rows should exist
	var count int
	err = st.DB().QueryRow(
		`SELECT COUNT(*) FROM participant_identifiers WHERE participant_id = ?`,
		id1,
	).Scan(&count)
	if err != nil {
		t.Fatalf("count participant_identifiers: %v", err)
	}
	if count != 2 {
		t.Errorf("participant_identifiers count = %d, want 2", count)
	}

	// Verify each identifier type is present
	for _, identType := range []string{"whatsapp", "imessage"} {
		var exists int
		err = st.DB().QueryRow(
			`SELECT COUNT(*) FROM participant_identifiers
			 WHERE participant_id = ? AND identifier_type = ?`,
			id1, identType,
		).Scan(&exists)
		if err != nil {
			t.Fatalf("check identifier_type %q: %v", identType, err)
		}
		if exists != 1 {
			t.Errorf("identifier_type %q count = %d, want 1", identType, exists)
		}
	}
}
