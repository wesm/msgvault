package store_test

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/wesm/msgvault/internal/mime"
	"github.com/wesm/msgvault/internal/store"
	"github.com/wesm/msgvault/internal/testutil"
)

// Helper functions

var sampleRawMessage = []byte("From: test@example.com\r\nSubject: Test\r\n\r\nBody")

func setupStore(t *testing.T) (*store.Store, *store.Source, int64) {
	t.Helper()
	st := testutil.NewTestStore(t)
	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	testutil.MustNoErr(t, err, "setup: GetOrCreateSource")
	convID, err := st.EnsureConversation(source.ID, "default-thread", "Default Thread")
	testutil.MustNoErr(t, err, "setup: EnsureConversation")
	return st, source, convID
}

func createMessage(t *testing.T, st *store.Store, sourceID, convID int64, msgID string) int64 {
	t.Helper()
	id, err := st.UpsertMessage(&store.Message{
		ConversationID:  convID,
		SourceID:        sourceID,
		SourceMessageID: msgID,
		MessageType:     "email",
		SizeEstimate:    1000,
	})
	testutil.MustNoErr(t, err, "createMessage")
	return id
}

func createMessages(t *testing.T, st *store.Store, sourceID, convID int64, count int) []int64 {
	t.Helper()
	var ids []int64
	for i := 0; i < count; i++ {
		ids = append(ids, createMessage(t, st, sourceID, convID, fmt.Sprintf("msg-%d", i)))
	}
	return ids
}

func mustEnsureLabels(t *testing.T, st *store.Store, sourceID int64, labels map[string]string, typ string) map[string]int64 {
	t.Helper()
	result := make(map[string]int64, len(labels))
	for sourceLabelID, name := range labels {
		lid, err := st.EnsureLabel(sourceID, sourceLabelID, name, typ)
		testutil.MustNoErr(t, err, "EnsureLabel "+sourceLabelID)
		result[sourceLabelID] = lid
	}
	return result
}

func assertMessageLabelsCount(t *testing.T, st *store.Store, msgID int64, want int) {
	t.Helper()
	var count int
	err := st.DB().QueryRow(st.Rebind("SELECT COUNT(*) FROM message_labels WHERE message_id = ?"), msgID).Scan(&count)
	testutil.MustNoErr(t, err, "count message_labels")
	if count != want {
		t.Errorf("message_labels count = %d, want %d", count, want)
	}
}

func assertRecipientsCount(t *testing.T, st *store.Store, msgID int64, typ string, want int) {
	t.Helper()
	var count int
	err := st.DB().QueryRow(st.Rebind("SELECT COUNT(*) FROM message_recipients WHERE message_id = ? AND recipient_type = ?"), msgID, typ).Scan(&count)
	testutil.MustNoErr(t, err, "count message_recipients")
	if count != want {
		t.Errorf("message_recipients(%s) count = %d, want %d", typ, count, want)
	}
}

func startSync(t *testing.T, st *store.Store, sourceID int64) int64 {
	t.Helper()
	syncID, err := st.StartSync(sourceID, "full")
	testutil.MustNoErr(t, err, "StartSync")
	if syncID == 0 {
		t.Fatal("sync ID should be non-zero")
	}
	return syncID
}

func assertActiveSync(t *testing.T, st *store.Store, sourceID int64, wantID int64, wantStatus string) {
	t.Helper()
	active, err := st.GetActiveSync(sourceID)
	testutil.MustNoErr(t, err, "GetActiveSync")
	if active == nil {
		t.Fatal("expected active sync, got nil")
	}
	if active.ID != wantID {
		t.Errorf("active sync ID = %d, want %d", active.ID, wantID)
	}
	if active.Status != wantStatus {
		t.Errorf("active sync status = %q, want %q", active.Status, wantStatus)
	}
}

func assertNoActiveSync(t *testing.T, st *store.Store, sourceID int64) {
	t.Helper()
	active, err := st.GetActiveSync(sourceID)
	testutil.MustNoErr(t, err, "GetActiveSync")
	if active != nil {
		t.Errorf("expected no active sync, got %+v", active)
	}
}

func mustEnsureParticipant(t *testing.T, st *store.Store, email, name, domain string) int64 {
	t.Helper()
	pid, err := st.EnsureParticipant(email, name, domain)
	testutil.MustNoErr(t, err, "EnsureParticipant "+email)
	return pid
}

func assertSyncCheckpointField(t *testing.T, field string, got, want int64) {
	t.Helper()
	if got != want {
		t.Errorf("sync %s = %d, want %d", field, got, want)
	}
}

func TestStore_Open(t *testing.T) {
	st := testutil.NewTestStore(t)

	// Store should be usable
	if st.DB() == nil {
		t.Error("DB() returned nil")
	}
}

func TestStore_GetStats_Empty(t *testing.T) {
	st := testutil.NewTestStore(t)

	stats, err := st.GetStats()
	testutil.MustNoErr(t, err, "GetStats()")

	if stats.MessageCount != 0 {
		t.Errorf("MessageCount = %d, want 0", stats.MessageCount)
	}
}

func TestStore_Source_CreateAndGet(t *testing.T) {

st := testutil.NewTestStore(t)

	// Create source
	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	testutil.MustNoErr(t, err, "GetOrCreateSource()")

	if source.ID == 0 {
		t.Error("source ID should be non-zero")
	}
	if source.SourceType != "gmail" {
		t.Errorf("SourceType = %q, want %q", source.SourceType, "gmail")
	}
	if source.Identifier != "test@example.com" {
		t.Errorf("Identifier = %q, want %q", source.Identifier, "test@example.com")
	}

	// Get same source again (should return existing)
	source2, err := st.GetOrCreateSource("gmail", "test@example.com")
	testutil.MustNoErr(t, err, "GetOrCreateSource() second call")

	if source2.ID != source.ID {
		t.Errorf("second call ID = %d, want %d", source2.ID, source.ID)
	}
}

func TestStore_Source_UpdateSyncCursor(t *testing.T) {

st, source, _ := setupStore(t)

	err := st.UpdateSourceSyncCursor(source.ID, "12345")
	testutil.MustNoErr(t, err, "UpdateSourceSyncCursor()")

	// Verify cursor was updated
	updated, err := st.GetSourceByIdentifier("test@example.com")
	testutil.MustNoErr(t, err, "GetSourceByIdentifier()")

	if !updated.SyncCursor.Valid || updated.SyncCursor.String != "12345" {
		t.Errorf("SyncCursor = %v, want 12345", updated.SyncCursor)
	}
}

func TestStore_Conversation(t *testing.T) {

st, source, _ := setupStore(t)

	// Create conversation
	convID, err := st.EnsureConversation(source.ID, "thread-123", "Test Thread")
	testutil.MustNoErr(t, err, "EnsureConversation()")

	if convID == 0 {
		t.Error("conversation ID should be non-zero")
	}

	// Get same conversation (should return existing)
	convID2, err := st.EnsureConversation(source.ID, "thread-123", "Test Thread")
	testutil.MustNoErr(t, err, "EnsureConversation() second call")

	if convID2 != convID {
		t.Errorf("second call ID = %d, want %d", convID2, convID)
	}
}

func TestStore_UpsertMessage(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name string
		msg  func(sourceID, convID int64) *store.Message
	}{
		{
			name: "AllFields",
			msg: func(sourceID, convID int64) *store.Message {
				return &store.Message{
					ConversationID:  convID,
					SourceID:        sourceID,
					SourceMessageID: "msg-all-fields",
					MessageType:     "email",
					Subject:         sql.NullString{String: "Full Subject", Valid: true},
					Snippet:         sql.NullString{String: "Preview snippet", Valid: true},
					SizeEstimate:    2048,
					SentAt:          sql.NullTime{Time: now, Valid: true},
					ReceivedAt:      sql.NullTime{Time: now.Add(time.Second), Valid: true},
					InternalDate:    sql.NullTime{Time: now, Valid: true},
					HasAttachments:  true,
					AttachmentCount: 2,
					IsFromMe:        true,
				}
			},
		},
		{
			name: "MinimalFields",
			msg: func(sourceID, convID int64) *store.Message {
				return &store.Message{
					ConversationID:  convID,
					SourceID:        sourceID,
					SourceMessageID: "msg-minimal",
					MessageType:     "email",
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st, source, convID := setupStore(t)
			msg := tt.msg(source.ID, convID)

			// Insert
			msgID, err := st.UpsertMessage(msg)
			testutil.MustNoErr(t, err, "UpsertMessage insert")
			if msgID == 0 {
				t.Error("message ID should be non-zero")
			}

			// Update (same source_message_id) should return same ID
			msgID2, err := st.UpsertMessage(msg)
			testutil.MustNoErr(t, err, "UpsertMessage update")
			if msgID2 != msgID {
				t.Errorf("update ID = %d, want %d", msgID2, msgID)
			}

			// Verify stats show exactly one message
			stats, err := st.GetStats()
			testutil.MustNoErr(t, err, "GetStats")
			if stats.MessageCount != 1 {
				t.Errorf("MessageCount = %d, want 1", stats.MessageCount)
			}
		})
	}
}

func TestStore_MessageExistsBatch(t *testing.T) {

st, source, convID := setupStore(t)

	// Insert some messages
	ids := []string{"msg-1", "msg-2", "msg-3"}
	for _, id := range ids {
		createMessage(t, st, source.ID, convID, id)
	}

	// Check which exist
	checkIDs := []string{"msg-1", "msg-2", "msg-4", "msg-5"}
	existing, err := st.MessageExistsBatch(source.ID, checkIDs)
	testutil.MustNoErr(t, err, "MessageExistsBatch()")

	if len(existing) != 2 {
		t.Errorf("len(existing) = %d, want 2", len(existing))
	}
	if _, ok := existing["msg-1"]; !ok {
		t.Error("msg-1 should exist")
	}
	if _, ok := existing["msg-2"]; !ok {
		t.Error("msg-2 should exist")
	}
	if _, ok := existing["msg-4"]; ok {
		t.Error("msg-4 should not exist")
	}
}

func TestStore_MessageRaw(t *testing.T) {

st, source, convID := setupStore(t)

	msgID := createMessage(t, st, source.ID, convID, "msg-1")

	// Store raw data
	err := st.UpsertMessageRaw(msgID, sampleRawMessage)
	testutil.MustNoErr(t, err, "UpsertMessageRaw()")

	// Retrieve raw data
	retrieved, err := st.GetMessageRaw(msgID)
	testutil.MustNoErr(t, err, "GetMessageRaw()")

	if string(retrieved) != string(sampleRawMessage) {
		t.Errorf("retrieved data = %q, want %q", retrieved, sampleRawMessage)
	}
}

func TestStore_Participant(t *testing.T) {

st := testutil.NewTestStore(t)

	// Create participant
	pid, err := st.EnsureParticipant("alice@example.com", "Alice Smith", "example.com")
	testutil.MustNoErr(t, err, "EnsureParticipant()")

	if pid == 0 {
		t.Error("participant ID should be non-zero")
	}

	// Get same participant
	pid2, err := st.EnsureParticipant("alice@example.com", "Alice", "example.com")
	testutil.MustNoErr(t, err, "EnsureParticipant() second call")

	if pid2 != pid {
		t.Errorf("second call ID = %d, want %d", pid2, pid)
	}
}

func TestStore_EnsureParticipantsBatch(t *testing.T) {

st := testutil.NewTestStore(t)

	addresses := []mime.Address{
		{Email: "alice@example.com", Name: "Alice", Domain: "example.com"},
		{Email: "bob@example.org", Name: "Bob", Domain: "example.org"},
		{Email: "", Name: "No Email", Domain: ""}, // Should be skipped
	}

	result, err := st.EnsureParticipantsBatch(addresses)
	testutil.MustNoErr(t, err, "EnsureParticipantsBatch()")

	if len(result) != 2 {
		t.Errorf("len(result) = %d, want 2", len(result))
	}
	if _, ok := result["alice@example.com"]; !ok {
		t.Error("alice@example.com should be in result")
	}
	if _, ok := result["bob@example.org"]; !ok {
		t.Error("bob@example.org should be in result")
	}
}

func TestStore_Label(t *testing.T) {

st, source, _ := setupStore(t)

	// Create label
	lid, err := st.EnsureLabel(source.ID, "INBOX", "Inbox", "system")
	testutil.MustNoErr(t, err, "EnsureLabel()")

	if lid == 0 {
		t.Error("label ID should be non-zero")
	}

	// Get same label
	lid2, err := st.EnsureLabel(source.ID, "INBOX", "Inbox", "system")
	testutil.MustNoErr(t, err, "EnsureLabel() second call")

	if lid2 != lid {
		t.Errorf("second call ID = %d, want %d", lid2, lid)
	}
}

func TestStore_EnsureLabelsBatch(t *testing.T) {

st, source, _ := setupStore(t)

	labels := map[string]string{
		"INBOX":       "Inbox",
		"SENT":        "Sent",
		"Label_12345": "My Label",
	}

	result, err := st.EnsureLabelsBatch(source.ID, labels)
	testutil.MustNoErr(t, err, "EnsureLabelsBatch()")

	if len(result) != 3 {
		t.Errorf("len(result) = %d, want 3", len(result))
	}
	for sourceLabelID := range labels {
		if _, ok := result[sourceLabelID]; !ok {
			t.Errorf("%s should be in result", sourceLabelID)
		}
	}
}

func TestStore_MessageLabels(t *testing.T) {

st, source, convID := setupStore(t)

	msgID := createMessage(t, st, source.ID, convID, "msg-1")

	labels := mustEnsureLabels(t, st, source.ID, map[string]string{
		"INBOX":   "Inbox",
		"STARRED": "Starred",
		"SENT":    "Sent",
	}, "system")

	// Set labels
	err := st.ReplaceMessageLabels(msgID, []int64{labels["INBOX"], labels["STARRED"]})
	testutil.MustNoErr(t, err, "ReplaceMessageLabels()")

	assertMessageLabelsCount(t, st, msgID, 2)

	// Replace with different labels
	err = st.ReplaceMessageLabels(msgID, []int64{labels["SENT"]})
	testutil.MustNoErr(t, err, "ReplaceMessageLabels() replace")

	assertMessageLabelsCount(t, st, msgID, 1)

	// Verify it's the right label
	var labelID int64
	err = st.DB().QueryRow(st.Rebind("SELECT label_id FROM message_labels WHERE message_id = ?"), msgID).Scan(&labelID)
	testutil.MustNoErr(t, err, "get label_id")
	if labelID != labels["SENT"] {
		t.Errorf("label_id = %d, want %d (SENT)", labelID, labels["SENT"])
	}
}

func TestStore_MessageRecipients(t *testing.T) {

st, source, convID := setupStore(t)

	msgID := createMessage(t, st, source.ID, convID, "msg-1")

	pid1 := mustEnsureParticipant(t, st, "alice@example.com", "Alice", "example.com")
	pid2 := mustEnsureParticipant(t, st, "bob@example.org", "Bob", "example.org")

	// Set recipients
	err := st.ReplaceMessageRecipients(msgID, "to", []int64{pid1, pid2}, []string{"Alice", "Bob"})
	testutil.MustNoErr(t, err, "ReplaceMessageRecipients()")

	assertRecipientsCount(t, st, msgID, "to", 2)

	// Replace recipients
	err = st.ReplaceMessageRecipients(msgID, "to", []int64{pid1}, []string{"Alice"})
	testutil.MustNoErr(t, err, "ReplaceMessageRecipients() replace")

	assertRecipientsCount(t, st, msgID, "to", 1)

	// Verify it's the right recipient
	var participantID int64
	err = st.DB().QueryRow(st.Rebind("SELECT participant_id FROM message_recipients WHERE message_id = ? AND recipient_type = 'to'"), msgID).Scan(&participantID)
	testutil.MustNoErr(t, err, "get participant_id")
	if participantID != pid1 {
		t.Errorf("participant_id = %d, want %d (alice)", participantID, pid1)
	}
}

func TestStore_MarkMessageDeleted(t *testing.T) {

st, source, convID := setupStore(t)

	msgID := createMessage(t, st, source.ID, convID, "msg-1")

	// Verify not deleted initially
	var deletedAt sql.NullTime
	err := st.DB().QueryRow(st.Rebind("SELECT deleted_from_source_at FROM messages WHERE id = ?"), msgID).Scan(&deletedAt)
	testutil.MustNoErr(t, err, "check deleted_from_source_at before")
	if deletedAt.Valid {
		t.Error("deleted_from_source_at should be NULL before MarkMessageDeleted")
	}

	err = st.MarkMessageDeleted(source.ID, "msg-1")
	testutil.MustNoErr(t, err, "MarkMessageDeleted()")

	// Verify deleted flag is now set
	err = st.DB().QueryRow(st.Rebind("SELECT deleted_from_source_at FROM messages WHERE id = ?"), msgID).Scan(&deletedAt)
	testutil.MustNoErr(t, err, "check deleted_from_source_at after")
	if !deletedAt.Valid {
		t.Error("deleted_from_source_at should be set after MarkMessageDeleted")
	}
}

func TestStore_Attachment(t *testing.T) {

st, source, convID := setupStore(t)

	msgID, err := st.UpsertMessage(&store.Message{
		ConversationID:  convID,
		SourceID:        source.ID,
		SourceMessageID: "msg-1",
		MessageType:     "email",
		HasAttachments:  true,
	})
	testutil.MustNoErr(t, err, "UpsertMessage")

	err = st.UpsertAttachment(msgID, "document.pdf", "application/pdf", "/path/to/file", "abc123hash", 1024)
	testutil.MustNoErr(t, err, "UpsertAttachment()")

	// Upsert same attachment (should not error, dedupe by content_hash)
	err = st.UpsertAttachment(msgID, "document.pdf", "application/pdf", "/path/to/file", "abc123hash", 1024)
	testutil.MustNoErr(t, err, "UpsertAttachment() duplicate")

	stats, err := st.GetStats()
	testutil.MustNoErr(t, err, "GetStats")
	if stats.AttachmentCount != 1 {
		t.Errorf("AttachmentCount = %d, want 1", stats.AttachmentCount)
	}
}

func TestStore_SyncRun(t *testing.T) {

st, source, _ := setupStore(t)

	syncID := startSync(t, st, source.ID)
	assertActiveSync(t, st, source.ID, syncID, "running")
}

func TestStore_SyncCheckpoint(t *testing.T) {

st, source, _ := setupStore(t)

	syncID := startSync(t, st, source.ID)

	cp := &store.Checkpoint{
		PageToken:         "next-page-token",
		MessagesProcessed: 100,
		MessagesAdded:     50,
		MessagesUpdated:   10,
		ErrorsCount:       2,
	}

	err := st.UpdateSyncCheckpoint(syncID, cp)
	testutil.MustNoErr(t, err, "UpdateSyncCheckpoint()")

	// Verify checkpoint was saved
	assertActiveSync(t, st, source.ID, syncID, "running")
	active, err := st.GetActiveSync(source.ID)
	testutil.MustNoErr(t, err, "GetActiveSync")
	assertSyncCheckpointField(t, "MessagesProcessed", active.MessagesProcessed, 100)
}

func TestStore_SyncComplete(t *testing.T) {

st, source, _ := setupStore(t)

	syncID := startSync(t, st, source.ID)

	err := st.CompleteSync(syncID, "history-12345")
	testutil.MustNoErr(t, err, "CompleteSync()")

	assertNoActiveSync(t, st, source.ID)

	// Should have a successful sync
	lastSync, err := st.GetLastSuccessfulSync(source.ID)
	testutil.MustNoErr(t, err, "GetLastSuccessfulSync()")
	if lastSync == nil {
		t.Fatal("expected last successful sync, got nil")
	}
	if lastSync.Status != "completed" {
		t.Errorf("status = %q, want %q", lastSync.Status, "completed")
	}
}

func TestStore_SyncFail(t *testing.T) {

st, source, _ := setupStore(t)

	syncID := startSync(t, st, source.ID)

	err := st.FailSync(syncID, "network error")
	testutil.MustNoErr(t, err, "FailSync()")

	assertNoActiveSync(t, st, source.ID)

	// Verify sync status is "failed" and error message is stored
	var status, errorMsg string
	err = st.DB().QueryRow(st.Rebind("SELECT status, error_message FROM sync_runs WHERE id = ?"), syncID).Scan(&status, &errorMsg)
	testutil.MustNoErr(t, err, "query sync status")
	if status != "failed" {
		t.Errorf("sync status = %q, want %q", status, "failed")
	}
	if errorMsg != "network error" {
		t.Errorf("error_message = %q, want %q", errorMsg, "network error")
	}
}

func TestStore_MarkMessageDeletedByGmailID(t *testing.T) {

st, source, convID := setupStore(t)

	createMessage(t, st, source.ID, convID, "gmail-msg-123")

	// Mark as deleted (trash)
	err := st.MarkMessageDeletedByGmailID(false, "gmail-msg-123")
	testutil.MustNoErr(t, err, "MarkMessageDeletedByGmailID(trash)")

	// Mark as permanently deleted
	err = st.MarkMessageDeletedByGmailID(true, "gmail-msg-123")
	testutil.MustNoErr(t, err, "MarkMessageDeletedByGmailID(permanent)")

	// Non-existent message should not error (no rows affected is OK)
	err = st.MarkMessageDeletedByGmailID(true, "nonexistent-id")
	testutil.MustNoErr(t, err, "MarkMessageDeletedByGmailID(nonexistent)")
}

func TestStore_GetMessageRaw_NotFound(t *testing.T) {

st := testutil.NewTestStore(t)

	// Try to get raw for non-existent message
	_, err := st.GetMessageRaw(99999)
	if err == nil {
		t.Error("GetMessageRaw() should error for non-existent message")
	}
}


func TestStore_UpsertMessageRaw_Update(t *testing.T) {

st, source, convID := setupStore(t)

	msgID := createMessage(t, st, source.ID, convID, "msg-raw-update")

	// Insert raw data
	rawData1 := []byte("Original raw content")
	err := st.UpsertMessageRaw(msgID, rawData1)
	testutil.MustNoErr(t, err, "UpsertMessageRaw()")

	// Update with new raw data
	rawData2 := []byte("Updated raw content that is different")
	err = st.UpsertMessageRaw(msgID, rawData2)
	testutil.MustNoErr(t, err, "UpsertMessageRaw() update")

	// Verify updated data
	retrieved, err := st.GetMessageRaw(msgID)
	testutil.MustNoErr(t, err, "GetMessageRaw")
	if string(retrieved) != string(rawData2) {
		t.Errorf("retrieved = %q, want %q", retrieved, rawData2)
	}
}

func TestStore_UpsertMessageBody(t *testing.T) {
	st, source, convID := setupStore(t)
	msgID := createMessage(t, st, source.ID, convID, "msg-body-test")

	// Insert body
	err := st.UpsertMessageBody(msgID,
		sql.NullString{String: "hello text", Valid: true},
		sql.NullString{String: "<p>hello html</p>", Valid: true},
	)
	testutil.MustNoErr(t, err, "UpsertMessageBody()")

	// Verify via direct query
	var bodyText, bodyHTML sql.NullString
	err = st.DB().QueryRow("SELECT body_text, body_html FROM message_bodies WHERE message_id = ?", msgID).Scan(&bodyText, &bodyHTML)
	testutil.MustNoErr(t, err, "query message_bodies")
	if bodyText.String != "hello text" {
		t.Errorf("body_text = %q, want %q", bodyText.String, "hello text")
	}
	if bodyHTML.String != "<p>hello html</p>" {
		t.Errorf("body_html = %q, want %q", bodyHTML.String, "<p>hello html</p>")
	}

	// Update body (upsert)
	err = st.UpsertMessageBody(msgID,
		sql.NullString{String: "updated text", Valid: true},
		sql.NullString{},
	)
	testutil.MustNoErr(t, err, "UpsertMessageBody() update")
	err = st.DB().QueryRow("SELECT body_text, body_html FROM message_bodies WHERE message_id = ?", msgID).Scan(&bodyText, &bodyHTML)
	testutil.MustNoErr(t, err, "query after update")
	if bodyText.String != "updated text" {
		t.Errorf("after update: body_text = %q, want %q", bodyText.String, "updated text")
	}
	// UpsertMessageBody overwrites both columns; invalid NullString NULLs the column
	if bodyHTML.Valid {
		t.Errorf("after update: body_html should be NULL, got %q", bodyHTML.String)
	}
}

func TestStore_MessageExistsBatch_Empty(t *testing.T) {

st, source, _ := setupStore(t)

	// Check with empty list
	result, err := st.MessageExistsBatch(source.ID, []string{})
	testutil.MustNoErr(t, err, "MessageExistsBatch(empty)")
	if len(result) != 0 {
		t.Errorf("len(result) = %d, want 0", len(result))
	}
}

func TestStore_ReplaceMessageLabels_Empty(t *testing.T) {

st, source, convID := setupStore(t)

	msgID := createMessage(t, st, source.ID, convID, "msg-labels")

	labels := mustEnsureLabels(t, st, source.ID, map[string]string{
		"INBOX":   "Inbox",
		"STARRED": "Starred",
	}, "system")

	// Add labels
	err := st.ReplaceMessageLabels(msgID, []int64{labels["INBOX"], labels["STARRED"]})
	testutil.MustNoErr(t, err, "ReplaceMessageLabels")

	assertMessageLabelsCount(t, st, msgID, 2)

	// Replace with empty list (remove all labels)
	err = st.ReplaceMessageLabels(msgID, []int64{})
	testutil.MustNoErr(t, err, "ReplaceMessageLabels(empty)")

	assertMessageLabelsCount(t, st, msgID, 0)
}

func TestStore_ReplaceMessageRecipients_Empty(t *testing.T) {

st, source, convID := setupStore(t)

	msgID := createMessage(t, st, source.ID, convID, "msg-recip")

	pid1 := mustEnsureParticipant(t, st, "alice@example.com", "Alice", "example.com")

	// Add recipient
	err := st.ReplaceMessageRecipients(msgID, "to", []int64{pid1}, []string{"Alice"})
	testutil.MustNoErr(t, err, "ReplaceMessageRecipients")

	assertRecipientsCount(t, st, msgID, "to", 1)

	// Replace with empty list
	err = st.ReplaceMessageRecipients(msgID, "to", []int64{}, []string{})
	testutil.MustNoErr(t, err, "ReplaceMessageRecipients(empty)")

	assertRecipientsCount(t, st, msgID, "to", 0)
}

func TestStore_GetActiveSync_NoSync(t *testing.T) {

st, source, _ := setupStore(t)

	assertNoActiveSync(t, st, source.ID)
}

func TestStore_GetLastSuccessfulSync_None(t *testing.T) {

st, source, _ := setupStore(t)

	// No successful sync yet
	lastSync, err := st.GetLastSuccessfulSync(source.ID)
	testutil.MustNoErr(t, err, "GetLastSuccessfulSync()")
	if lastSync != nil {
		t.Errorf("expected nil last sync, got %+v", lastSync)
	}
}

func TestStore_GetSourceByIdentifier_NotFound(t *testing.T) {

st := testutil.NewTestStore(t)

	source, err := st.GetSourceByIdentifier("nonexistent@example.com")
	testutil.MustNoErr(t, err, "GetSourceByIdentifier()")
	if source != nil {
		t.Errorf("expected nil source, got %+v", source)
	}
}

func TestStore_GetStats_WithData(t *testing.T) {

st, source, convID := setupStore(t)

	// Add multiple messages
	createMessages(t, st, source.ID, convID, 5)

	stats, err := st.GetStats()
	testutil.MustNoErr(t, err, "GetStats()")

	if stats.MessageCount != 5 {
		t.Errorf("MessageCount = %d, want 5", stats.MessageCount)
	}
	if stats.ThreadCount == 0 {
		t.Error("ThreadCount should be non-zero")
	}
}

func TestStore_CountMessagesForSource(t *testing.T) {

st, source, convID := setupStore(t)

	// Initially zero
	count, err := st.CountMessagesForSource(source.ID)
	testutil.MustNoErr(t, err, "CountMessagesForSource()")
	if count != 0 {
		t.Errorf("count = %d, want 0", count)
	}

	// Add messages
	createMessages(t, st, source.ID, convID, 3)

	count, err = st.CountMessagesForSource(source.ID)
	testutil.MustNoErr(t, err, "CountMessagesForSource()")
	if count != 3 {
		t.Errorf("count = %d, want 3", count)
	}

	// Mark one as deleted - should not be counted
	err = st.MarkMessageDeleted(source.ID, "msg-0")
	testutil.MustNoErr(t, err, "MarkMessageDeleted")

	count, err = st.CountMessagesForSource(source.ID)
	testutil.MustNoErr(t, err, "CountMessagesForSource() after delete")
	if count != 2 {
		t.Errorf("count after delete = %d, want 2", count)
	}
}

func TestStore_CountMessagesWithRaw(t *testing.T) {

st, source, convID := setupStore(t)

	// Initially zero
	count, err := st.CountMessagesWithRaw(source.ID)
	testutil.MustNoErr(t, err, "CountMessagesWithRaw()")
	if count != 0 {
		t.Errorf("count = %d, want 0", count)
	}

	// Add messages, some with raw data
	for i := 0; i < 4; i++ {
		msgID := createMessage(t, st, source.ID, convID, fmt.Sprintf("raw-count-msg-%d", i))

		// Only store raw for first 2 messages
		if i < 2 {
			err = st.UpsertMessageRaw(msgID, sampleRawMessage)
			testutil.MustNoErr(t, err, "UpsertMessageRaw")
		}
	}

	count, err = st.CountMessagesWithRaw(source.ID)
	testutil.MustNoErr(t, err, "CountMessagesWithRaw()")
	if count != 2 {
		t.Errorf("count = %d, want 2", count)
	}
}

func TestStore_GetRandomMessageIDs(t *testing.T) {

st, source, convID := setupStore(t)

	// Empty source
	ids, err := st.GetRandomMessageIDs(source.ID, 5)
	testutil.MustNoErr(t, err, "GetRandomMessageIDs(empty)")
	if len(ids) != 0 {
		t.Errorf("len(ids) = %d, want 0 for empty source", len(ids))
	}

	// Add 10 messages
	allIDs := make(map[int64]bool)
	createdIDs := createMessages(t, st, source.ID, convID, 10)
	for _, id := range createdIDs {
		allIDs[id] = true
	}

	// Sample fewer than available
	ids, err = st.GetRandomMessageIDs(source.ID, 5)
	testutil.MustNoErr(t, err, "GetRandomMessageIDs()")
	if len(ids) != 5 {
		t.Errorf("len(ids) = %d, want 5", len(ids))
	}

	// All returned IDs should be valid
	for _, id := range ids {
		if !allIDs[id] {
			t.Errorf("returned ID %d is not in allIDs", id)
		}
	}

	// All returned IDs should be unique
	seen := make(map[int64]bool)
	for _, id := range ids {
		if seen[id] {
			t.Errorf("duplicate ID %d returned", id)
		}
		seen[id] = true
	}

	// Sample more than available - should return all
	ids, err = st.GetRandomMessageIDs(source.ID, 20)
	testutil.MustNoErr(t, err, "GetRandomMessageIDs(more than available)")
	if len(ids) != 10 {
		t.Errorf("len(ids) = %d, want 10", len(ids))
	}
}

func TestStore_GetRandomMessageIDs_ExcludesDeleted(t *testing.T) {

st, source, convID := setupStore(t)

	// Add 5 messages
	createMessages(t, st, source.ID, convID, 5)

	// Delete 2 messages
	err := st.MarkMessageDeleted(source.ID, "msg-0")
	testutil.MustNoErr(t, err, "MarkMessageDeleted msg-0")
	err = st.MarkMessageDeleted(source.ID, "msg-2")
	testutil.MustNoErr(t, err, "MarkMessageDeleted msg-2")

	// Should only return 3 (non-deleted) messages
	ids, err := st.GetRandomMessageIDs(source.ID, 10)
	testutil.MustNoErr(t, err, "GetRandomMessageIDs()")
	if len(ids) != 3 {
		t.Errorf("len(ids) = %d, want 3 (5 total - 2 deleted)", len(ids))
	}
}