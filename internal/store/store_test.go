package store_test

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/wesm/msgvault/internal/mime"
	"github.com/wesm/msgvault/internal/store"
	"github.com/wesm/msgvault/internal/testutil"
	"github.com/wesm/msgvault/internal/testutil/storetest"
)

var sampleRawMessage = []byte("From: test@example.com\r\nSubject: Test\r\n\r\nBody")

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
	if stats.ThreadCount != 0 {
		t.Errorf("ThreadCount = %d, want 0", stats.ThreadCount)
	}
	if stats.SourceCount != 0 {
		t.Errorf("SourceCount = %d, want 0", stats.SourceCount)
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
	f := storetest.New(t)

	err := f.Store.UpdateSourceSyncCursor(f.Source.ID, "12345")
	testutil.MustNoErr(t, err, "UpdateSourceSyncCursor()")

	// Verify cursor was updated
	updated, err := f.Store.GetSourceByIdentifier("test@example.com")
	testutil.MustNoErr(t, err, "GetSourceByIdentifier()")

	if !updated.SyncCursor.Valid || updated.SyncCursor.String != "12345" {
		t.Errorf("SyncCursor = %v, want 12345", updated.SyncCursor)
	}
}

func TestStore_Conversation(t *testing.T) {
	f := storetest.New(t)

	// Create conversation
	convID, err := f.Store.EnsureConversation(f.Source.ID, "thread-123", "Test Thread")
	testutil.MustNoErr(t, err, "EnsureConversation()")

	if convID == 0 {
		t.Error("conversation ID should be non-zero")
	}

	// Get same conversation (should return existing)
	convID2, err := f.Store.EnsureConversation(f.Source.ID, "thread-123", "Test Thread")
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
				return storetest.NewMessage(sourceID, convID).
					WithSourceMessageID("msg-all-fields").
					WithSubject("Full Subject").
					WithSnippet("Preview snippet").
					WithSize(2048).
					WithSentAt(now).
					WithReceivedAt(now.Add(time.Second)).
					WithInternalDate(now).
					WithAttachmentCount(2).
					WithIsFromMe(true).
					Build()
			},
		},
		{
			name: "MinimalFields",
			msg: func(sourceID, convID int64) *store.Message {
				return storetest.NewMessage(sourceID, convID).
					WithSourceMessageID("msg-minimal").
					WithSize(0).
					Build()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := storetest.New(t)
			msg := tt.msg(f.Source.ID, f.ConvID)

			// Insert
			msgID, err := f.Store.UpsertMessage(msg)
			testutil.MustNoErr(t, err, "UpsertMessage insert")
			if msgID == 0 {
				t.Error("message ID should be non-zero")
			}

			// Update: mutate fields and upsert again
			msg.Subject = sql.NullString{String: "Updated Subject", Valid: true}
			msg.Snippet = sql.NullString{String: "Updated snippet", Valid: true}
			msg.HasAttachments = !msg.HasAttachments
			msgID2, err := f.Store.UpsertMessage(msg)
			testutil.MustNoErr(t, err, "UpsertMessage update")
			if msgID2 != msgID {
				t.Errorf("update ID = %d, want %d", msgID2, msgID)
			}

			// Verify updated fields are persisted
			got := f.GetMessageFields(msgID)
			if got.Subject != "Updated Subject" {
				t.Errorf("subject = %q, want %q", got.Subject, "Updated Subject")
			}
			if got.Snippet != "Updated snippet" {
				t.Errorf("snippet = %q, want %q", got.Snippet, "Updated snippet")
			}
			if got.HasAttachments != msg.HasAttachments {
				t.Errorf("has_attachments = %v, want %v", got.HasAttachments, msg.HasAttachments)
			}

			// Verify stats show exactly one message
			stats, err := f.Store.GetStats()
			testutil.MustNoErr(t, err, "GetStats")
			if stats.MessageCount != 1 {
				t.Errorf("MessageCount = %d, want 1", stats.MessageCount)
			}
		})
	}
}

func TestStore_MessageExistsBatch(t *testing.T) {
	f := storetest.New(t)

	// Insert some messages
	ids := []string{"msg-1", "msg-2", "msg-3"}
	for _, id := range ids {
		f.CreateMessage(id)
	}

	// Check which exist
	checkIDs := []string{"msg-1", "msg-2", "msg-4", "msg-5"}
	existing, err := f.Store.MessageExistsBatch(f.Source.ID, checkIDs)
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
	f := storetest.New(t)

	msgID := f.CreateMessage("msg-1")

	// Store raw data
	err := f.Store.UpsertMessageRaw(msgID, sampleRawMessage)
	testutil.MustNoErr(t, err, "UpsertMessageRaw()")

	// Retrieve raw data
	retrieved, err := f.Store.GetMessageRaw(msgID)
	testutil.MustNoErr(t, err, "GetMessageRaw()")

	if string(retrieved) != string(sampleRawMessage) {
		t.Errorf("retrieved data = %q, want %q", retrieved, sampleRawMessage)
	}
}

func TestStore_Participant(t *testing.T) {
	f := storetest.New(t)

	// Create participant
	pid := f.EnsureParticipant("alice@example.com", "Alice Smith", "example.com")
	if pid == 0 {
		t.Error("participant ID should be non-zero")
	}

	// Get same participant (should return existing)
	pid2 := f.EnsureParticipant("alice@example.com", "Alice", "example.com")
	if pid2 != pid {
		t.Errorf("second call ID = %d, want %d", pid2, pid)
	}
}

func TestStore_EnsureParticipantsBatch(t *testing.T) {
	f := storetest.New(t)

	addresses := []mime.Address{
		{Email: "alice@example.com", Name: "Alice", Domain: "example.com"},
		{Email: "bob@example.org", Name: "Bob", Domain: "example.org"},
		{Email: "", Name: "No Email", Domain: ""}, // Should be skipped
	}

	result, err := f.Store.EnsureParticipantsBatch(addresses)
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
	f := storetest.New(t)

	// Create label
	lid, err := f.Store.EnsureLabel(f.Source.ID, "INBOX", "Inbox", "system")
	testutil.MustNoErr(t, err, "EnsureLabel()")

	if lid == 0 {
		t.Error("label ID should be non-zero")
	}

	// Get same label
	lid2, err := f.Store.EnsureLabel(f.Source.ID, "INBOX", "Inbox", "system")
	testutil.MustNoErr(t, err, "EnsureLabel() second call")

	if lid2 != lid {
		t.Errorf("second call ID = %d, want %d", lid2, lid)
	}
}

func TestStore_EnsureLabelsBatch(t *testing.T) {
	f := storetest.New(t)

	labels := map[string]store.LabelInfo{
		"INBOX":       {Name: "Inbox", Type: "system"},
		"SENT":        {Name: "Sent", Type: "system"},
		"Label_12345": {Name: "My Label", Type: "user"},
	}

	result, err := f.Store.EnsureLabelsBatch(f.Source.ID, labels)
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
	f := storetest.New(t)

	msgID := f.CreateMessage("msg-1")

	labels := f.EnsureLabels(map[string]string{
		"INBOX":   "Inbox",
		"STARRED": "Starred",
		"SENT":    "Sent",
	}, "system")

	// Set labels
	err := f.Store.ReplaceMessageLabels(msgID, []int64{labels["INBOX"], labels["STARRED"]})
	testutil.MustNoErr(t, err, "ReplaceMessageLabels()")

	f.AssertLabelCount(msgID, 2)

	// Replace with different labels
	err = f.Store.ReplaceMessageLabels(msgID, []int64{labels["SENT"]})
	testutil.MustNoErr(t, err, "ReplaceMessageLabels() replace")

	f.AssertLabelCount(msgID, 1)

	// Verify it's the right label
	labelID := f.GetSingleLabelID(msgID)
	if labelID != labels["SENT"] {
		t.Errorf("label_id = %d, want %d (SENT)", labelID, labels["SENT"])
	}
}

func TestStore_MessageRecipients(t *testing.T) {
	f := storetest.New(t)

	msgID := f.CreateMessage("msg-1")

	pid1 := f.EnsureParticipant("alice@example.com", "Alice", "example.com")
	pid2 := f.EnsureParticipant("bob@example.org", "Bob", "example.org")

	// Set recipients
	err := f.Store.ReplaceMessageRecipients(msgID, "to", []int64{pid1, pid2}, []string{"Alice", "Bob"})
	testutil.MustNoErr(t, err, "ReplaceMessageRecipients()")

	f.AssertRecipientCount(msgID, "to", 2)

	// Replace recipients
	err = f.Store.ReplaceMessageRecipients(msgID, "to", []int64{pid1}, []string{"Alice"})
	testutil.MustNoErr(t, err, "ReplaceMessageRecipients() replace")

	f.AssertRecipientCount(msgID, "to", 1)

	// Verify it's the right recipient
	participantID := f.GetSingleRecipientID(msgID, "to")
	if participantID != pid1 {
		t.Errorf("participant_id = %d, want %d (alice)", participantID, pid1)
	}
}

func TestStore_MarkMessageDeleted(t *testing.T) {
	f := storetest.New(t)

	msgID := f.CreateMessage("msg-1")

	f.AssertMessageNotDeleted(msgID)

	err := f.Store.MarkMessageDeleted(f.Source.ID, "msg-1")
	testutil.MustNoErr(t, err, "MarkMessageDeleted()")

	f.AssertMessageDeleted(msgID)
}

func TestStore_Attachment(t *testing.T) {
	f := storetest.New(t)

	msgID := storetest.NewMessage(f.Source.ID, f.ConvID).
		WithSourceMessageID("msg-1").
		WithAttachmentCount(1).
		Create(t, f.Store)

	err := f.Store.UpsertAttachment(msgID, "document.pdf", "application/pdf", "/path/to/file", "abc123hash", 1024)
	testutil.MustNoErr(t, err, "UpsertAttachment()")

	// Upsert same attachment (should not error, dedupe by content_hash)
	err = f.Store.UpsertAttachment(msgID, "document.pdf", "application/pdf", "/path/to/file", "abc123hash", 1024)
	testutil.MustNoErr(t, err, "UpsertAttachment() duplicate")

	stats, err := f.Store.GetStats()
	testutil.MustNoErr(t, err, "GetStats")
	if stats.AttachmentCount != 1 {
		t.Errorf("AttachmentCount = %d, want 1", stats.AttachmentCount)
	}
}

func TestStore_SyncRun(t *testing.T) {
	f := storetest.New(t)

	syncID := f.StartSync()
	f.AssertActiveSync(syncID, "running")
}

func TestStore_SyncCheckpoint(t *testing.T) {
	f := storetest.New(t)

	syncID := f.StartSync()

	cp := &store.Checkpoint{
		PageToken:         "next-page-token",
		MessagesProcessed: 100,
		MessagesAdded:     50,
		MessagesUpdated:   10,
		ErrorsCount:       2,
	}

	err := f.Store.UpdateSyncCheckpoint(syncID, cp)
	testutil.MustNoErr(t, err, "UpdateSyncCheckpoint()")

	// Verify checkpoint was saved
	f.AssertActiveSync(syncID, "running")
	active, err := f.Store.GetActiveSync(f.Source.ID)
	testutil.MustNoErr(t, err, "GetActiveSync")
	if active.MessagesProcessed != 100 {
		t.Errorf("sync MessagesProcessed = %d, want %d", active.MessagesProcessed, 100)
	}
}

func TestStore_SyncComplete(t *testing.T) {
	f := storetest.New(t)

	syncID := f.StartSync()

	err := f.Store.CompleteSync(syncID, "history-12345")
	testutil.MustNoErr(t, err, "CompleteSync()")

	f.AssertNoActiveSync()

	// Should have a successful sync
	lastSync, err := f.Store.GetLastSuccessfulSync(f.Source.ID)
	testutil.MustNoErr(t, err, "GetLastSuccessfulSync()")
	if lastSync == nil {
		t.Fatal("expected last successful sync, got nil")
	}
	if lastSync.Status != "completed" {
		t.Errorf("status = %q, want %q", lastSync.Status, "completed")
	}
}

func TestStore_SyncFail(t *testing.T) {
	f := storetest.New(t)

	syncID := f.StartSync()

	err := f.Store.FailSync(syncID, "network error")
	testutil.MustNoErr(t, err, "FailSync()")

	f.AssertNoActiveSync()

	// Verify sync status is "failed" and error message is stored
	status, errorMsg := f.GetSyncRun(syncID)
	if status != "failed" {
		t.Errorf("sync status = %q, want %q", status, "failed")
	}
	if errorMsg != "network error" {
		t.Errorf("error_message = %q, want %q", errorMsg, "network error")
	}
}

func TestStore_MarkMessageDeletedByGmailID(t *testing.T) {
	f := storetest.New(t)

	f.CreateMessage("gmail-msg-123")

	// Mark as deleted (trash)
	err := f.Store.MarkMessageDeletedByGmailID(false, "gmail-msg-123")
	testutil.MustNoErr(t, err, "MarkMessageDeletedByGmailID(trash)")

	// Mark as permanently deleted
	err = f.Store.MarkMessageDeletedByGmailID(true, "gmail-msg-123")
	testutil.MustNoErr(t, err, "MarkMessageDeletedByGmailID(permanent)")

	// Non-existent message should not error (no rows affected is OK)
	err = f.Store.MarkMessageDeletedByGmailID(true, "nonexistent-id")
	testutil.MustNoErr(t, err, "MarkMessageDeletedByGmailID(nonexistent)")
}

func TestStore_GetMessageRaw_NotFound(t *testing.T) {
	f := storetest.New(t)

	// Try to get raw for non-existent message
	_, err := f.Store.GetMessageRaw(99999)
	if err == nil {
		t.Error("GetMessageRaw() should error for non-existent message")
	}
}

func TestStore_UpsertMessageRaw_Update(t *testing.T) {
	f := storetest.New(t)

	msgID := f.CreateMessage("msg-raw-update")

	// Insert raw data
	rawData1 := []byte("Original raw content")
	err := f.Store.UpsertMessageRaw(msgID, rawData1)
	testutil.MustNoErr(t, err, "UpsertMessageRaw()")

	// Update with new raw data
	rawData2 := []byte("Updated raw content that is different")
	err = f.Store.UpsertMessageRaw(msgID, rawData2)
	testutil.MustNoErr(t, err, "UpsertMessageRaw() update")

	// Verify updated data
	retrieved, err := f.Store.GetMessageRaw(msgID)
	testutil.MustNoErr(t, err, "GetMessageRaw")
	if string(retrieved) != string(rawData2) {
		t.Errorf("retrieved = %q, want %q", retrieved, rawData2)
	}
}

func TestStore_UpsertMessageBody(t *testing.T) {
	f := storetest.New(t)
	msgID := f.CreateMessage("msg-body-test")

	// Insert body
	err := f.Store.UpsertMessageBody(msgID,
		sql.NullString{String: "hello text", Valid: true},
		sql.NullString{String: "<p>hello html</p>", Valid: true},
	)
	testutil.MustNoErr(t, err, "UpsertMessageBody()")

	// Verify via helper
	bodyText, bodyHTML := f.GetMessageBody(msgID)
	if bodyText.String != "hello text" {
		t.Errorf("body_text = %q, want %q", bodyText.String, "hello text")
	}
	if bodyHTML.String != "<p>hello html</p>" {
		t.Errorf("body_html = %q, want %q", bodyHTML.String, "<p>hello html</p>")
	}

	// Update body (upsert)
	err = f.Store.UpsertMessageBody(msgID,
		sql.NullString{String: "updated text", Valid: true},
		sql.NullString{},
	)
	testutil.MustNoErr(t, err, "UpsertMessageBody() update")
	bodyText, bodyHTML = f.GetMessageBody(msgID)
	if bodyText.String != "updated text" {
		t.Errorf("after update: body_text = %q, want %q", bodyText.String, "updated text")
	}
	// UpsertMessageBody overwrites both columns; invalid NullString NULLs the column
	if bodyHTML.Valid {
		t.Errorf("after update: body_html should be NULL, got %q", bodyHTML.String)
	}
}

func TestStore_MessageExistsBatch_Empty(t *testing.T) {
	f := storetest.New(t)

	// Check with empty list
	result, err := f.Store.MessageExistsBatch(f.Source.ID, []string{})
	testutil.MustNoErr(t, err, "MessageExistsBatch(empty)")
	if len(result) != 0 {
		t.Errorf("len(result) = %d, want 0", len(result))
	}
}

func TestStore_ReplaceMessageLabels_Empty(t *testing.T) {
	f := storetest.New(t)

	msgID := f.CreateMessage("msg-labels")

	labels := f.EnsureLabels(map[string]string{
		"INBOX":   "Inbox",
		"STARRED": "Starred",
	}, "system")

	// Add labels
	err := f.Store.ReplaceMessageLabels(msgID, []int64{labels["INBOX"], labels["STARRED"]})
	testutil.MustNoErr(t, err, "ReplaceMessageLabels")

	f.AssertLabelCount(msgID, 2)

	// Replace with empty list (remove all labels)
	err = f.Store.ReplaceMessageLabels(msgID, []int64{})
	testutil.MustNoErr(t, err, "ReplaceMessageLabels(empty)")

	f.AssertLabelCount(msgID, 0)
}

func TestStore_ReplaceMessageRecipients_Empty(t *testing.T) {
	f := storetest.New(t)

	msgID := f.CreateMessage("msg-recip")

	pid1 := f.EnsureParticipant("alice@example.com", "Alice", "example.com")

	// Add recipient
	err := f.Store.ReplaceMessageRecipients(msgID, "to", []int64{pid1}, []string{"Alice"})
	testutil.MustNoErr(t, err, "ReplaceMessageRecipients")

	f.AssertRecipientCount(msgID, "to", 1)

	// Replace with empty list
	err = f.Store.ReplaceMessageRecipients(msgID, "to", []int64{}, []string{})
	testutil.MustNoErr(t, err, "ReplaceMessageRecipients(empty)")

	f.AssertRecipientCount(msgID, "to", 0)
}

func TestStore_GetActiveSync_NoSync(t *testing.T) {
	f := storetest.New(t)
	f.AssertNoActiveSync()
}

func TestStore_GetLastSuccessfulSync_None(t *testing.T) {
	f := storetest.New(t)

	// No successful sync yet
	lastSync, err := f.Store.GetLastSuccessfulSync(f.Source.ID)
	testutil.MustNoErr(t, err, "GetLastSuccessfulSync()")
	if lastSync != nil {
		t.Errorf("expected nil last sync, got %+v", lastSync)
	}
}

func TestStore_GetSourceByIdentifier_NotFound(t *testing.T) {
	f := storetest.New(t)

	source, err := f.Store.GetSourceByIdentifier("nonexistent@example.com")
	testutil.MustNoErr(t, err, "GetSourceByIdentifier()")
	if source != nil {
		t.Errorf("expected nil source, got %+v", source)
	}
}

func TestStore_GetStats_WithData(t *testing.T) {
	f := storetest.New(t)

	// Add multiple messages
	f.CreateMessages(5)

	stats, err := f.Store.GetStats()
	testutil.MustNoErr(t, err, "GetStats()")

	if stats.MessageCount != 5 {
		t.Errorf("MessageCount = %d, want 5", stats.MessageCount)
	}
	if stats.ThreadCount == 0 {
		t.Error("ThreadCount should be non-zero")
	}
}

func TestStore_GetStats_ClosedDB(t *testing.T) {
	st := testutil.NewTestStore(t)

	// Close the database
	err := st.Close()
	testutil.MustNoErr(t, err, "Close()")

	// GetStats should return an error for closed DB (not silently ignore)
	_, err = st.GetStats()
	if err == nil {
		t.Error("GetStats() should return error on closed DB")
	}
}

func TestStore_GetStats_MissingTable(t *testing.T) {
	st := testutil.NewTestStore(t)

	// Drop a table to simulate missing table scenario
	_, err := st.DB().Exec("DROP TABLE IF EXISTS attachments")
	testutil.MustNoErr(t, err, "DROP TABLE attachments")

	// GetStats should ignore missing tables and return partial stats
	stats, err := st.GetStats()
	testutil.MustNoErr(t, err, "GetStats() with missing table")

	// AttachmentCount should be 0 (table missing, ignored)
	if stats.AttachmentCount != 0 {
		t.Errorf("AttachmentCount = %d, want 0 for missing table", stats.AttachmentCount)
	}
}

func TestStore_CountMessagesForSource(t *testing.T) {
	f := storetest.New(t)

	// Initially zero
	count, err := f.Store.CountMessagesForSource(f.Source.ID)
	testutil.MustNoErr(t, err, "CountMessagesForSource()")
	if count != 0 {
		t.Errorf("count = %d, want 0", count)
	}

	// Add messages
	f.CreateMessages(3)

	count, err = f.Store.CountMessagesForSource(f.Source.ID)
	testutil.MustNoErr(t, err, "CountMessagesForSource()")
	if count != 3 {
		t.Errorf("count = %d, want 3", count)
	}

	// Mark one as deleted - should not be counted
	err = f.Store.MarkMessageDeleted(f.Source.ID, "msg-0")
	testutil.MustNoErr(t, err, "MarkMessageDeleted")

	count, err = f.Store.CountMessagesForSource(f.Source.ID)
	testutil.MustNoErr(t, err, "CountMessagesForSource() after delete")
	if count != 2 {
		t.Errorf("count after delete = %d, want 2", count)
	}
}

func TestStore_CountMessagesWithRaw(t *testing.T) {
	f := storetest.New(t)

	// Initially zero
	count, err := f.Store.CountMessagesWithRaw(f.Source.ID)
	testutil.MustNoErr(t, err, "CountMessagesWithRaw()")
	if count != 0 {
		t.Errorf("count = %d, want 0", count)
	}

	// Add messages, some with raw data
	for i := 0; i < 4; i++ {
		msgID := f.CreateMessage(fmt.Sprintf("raw-count-msg-%d", i))

		// Only store raw for first 2 messages
		if i < 2 {
			err = f.Store.UpsertMessageRaw(msgID, sampleRawMessage)
			testutil.MustNoErr(t, err, "UpsertMessageRaw")
		}
	}

	count, err = f.Store.CountMessagesWithRaw(f.Source.ID)
	testutil.MustNoErr(t, err, "CountMessagesWithRaw()")
	if count != 2 {
		t.Errorf("count = %d, want 2", count)
	}
}

func TestStore_GetRandomMessageIDs(t *testing.T) {
	f := storetest.New(t)

	// Empty source
	ids, err := f.Store.GetRandomMessageIDs(f.Source.ID, 5)
	testutil.MustNoErr(t, err, "GetRandomMessageIDs(empty)")
	if len(ids) != 0 {
		t.Errorf("len(ids) = %d, want 0 for empty source", len(ids))
	}

	// Add 10 messages
	allIDs := make(map[int64]bool)
	createdIDs := f.CreateMessages(10)
	for _, id := range createdIDs {
		allIDs[id] = true
	}

	// Sample fewer than available
	ids, err = f.Store.GetRandomMessageIDs(f.Source.ID, 5)
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
	ids, err = f.Store.GetRandomMessageIDs(f.Source.ID, 20)
	testutil.MustNoErr(t, err, "GetRandomMessageIDs(more than available)")
	if len(ids) != 10 {
		t.Errorf("len(ids) = %d, want 10", len(ids))
	}
}

func TestStore_GetRandomMessageIDs_ExcludesDeleted(t *testing.T) {
	f := storetest.New(t)

	// Add 5 messages
	f.CreateMessages(5)

	// Delete 2 messages
	err := f.Store.MarkMessageDeleted(f.Source.ID, "msg-0")
	testutil.MustNoErr(t, err, "MarkMessageDeleted msg-0")
	err = f.Store.MarkMessageDeleted(f.Source.ID, "msg-2")
	testutil.MustNoErr(t, err, "MarkMessageDeleted msg-2")

	// Should only return 3 (non-deleted) messages
	ids, err := f.Store.GetRandomMessageIDs(f.Source.ID, 10)
	testutil.MustNoErr(t, err, "GetRandomMessageIDs()")
	if len(ids) != 3 {
		t.Errorf("len(ids) = %d, want 3 (5 total - 2 deleted)", len(ids))
	}
}

func TestStore_ReplaceMessageRecipients_LargeBatch(t *testing.T) {
	f := storetest.New(t)

	msgID := f.CreateMessage("msg-large-recipients")

	// Create 300 participants (exceeds SQLite limit of ~249 rows with 4 params each)
	const numRecipients = 300
	participantIDs := make([]int64, numRecipients)
	displayNames := make([]string, numRecipients)
	for i := 0; i < numRecipients; i++ {
		email := fmt.Sprintf("user%d@example.com", i)
		pid := f.EnsureParticipant(email, fmt.Sprintf("User %d", i), "example.com")
		participantIDs[i] = pid
		displayNames[i] = fmt.Sprintf("User %d", i)
	}

	// This should work without hitting SQLite parameter limit
	err := f.Store.ReplaceMessageRecipients(msgID, "to", participantIDs, displayNames)
	testutil.MustNoErr(t, err, "ReplaceMessageRecipients(300 recipients)")

	f.AssertRecipientCount(msgID, "to", numRecipients)

	// Replace with a different large batch to ensure chunked delete+insert works
	err = f.Store.ReplaceMessageRecipients(msgID, "to", participantIDs[:150], displayNames[:150])
	testutil.MustNoErr(t, err, "ReplaceMessageRecipients(150 recipients)")

	f.AssertRecipientCount(msgID, "to", 150)
}

func TestStore_ReplaceMessageLabels_LargeBatch(t *testing.T) {
	f := storetest.New(t)

	msgID := f.CreateMessage("msg-large-labels")

	// Create 600 labels (exceeds SQLite limit of ~499 rows with 2 params each)
	const numLabels = 600
	labelIDs := make([]int64, numLabels)
	for i := 0; i < numLabels; i++ {
		sourceLabelID := fmt.Sprintf("Label_%d", i)
		lid, err := f.Store.EnsureLabel(f.Source.ID, sourceLabelID, fmt.Sprintf("Label %d", i), "user")
		testutil.MustNoErr(t, err, "EnsureLabel")
		labelIDs[i] = lid
	}

	// This should work without hitting SQLite parameter limit
	err := f.Store.ReplaceMessageLabels(msgID, labelIDs)
	testutil.MustNoErr(t, err, "ReplaceMessageLabels(600 labels)")

	f.AssertLabelCount(msgID, numLabels)

	// Replace with a different large batch to ensure chunked delete+insert works
	err = f.Store.ReplaceMessageLabels(msgID, labelIDs[:250])
	testutil.MustNoErr(t, err, "ReplaceMessageLabels(250 labels)")

	f.AssertLabelCount(msgID, 250)
}
