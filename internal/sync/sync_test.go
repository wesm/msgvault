package sync

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wesm/msgvault/internal/gmail"
	"github.com/wesm/msgvault/internal/store"
)

// Sample MIME message for testing
var testMIME = []byte(`From: sender@example.com
To: recipient@example.com
Subject: Test Message
Date: Mon, 01 Jan 2024 12:00:00 +0000
Content-Type: text/plain; charset="utf-8"

This is a test message body.
`)

func TestFullSync(t *testing.T) {
	env := newTestEnv(t)
	seedMessages(env, 3, 12345, "msg1", "msg2", "msg3")
	env.Mock.Messages["msg2"].LabelIDs = []string{"INBOX", "SENT"}
	env.Mock.Messages["msg3"].LabelIDs = []string{"SENT"}

	summary := runFullSync(t, env)
	assertSummary(t, summary, 3, 0, -1, -1)
	if summary.FinalHistoryID != 12345 {
		t.Errorf("expected history ID 12345, got %d", summary.FinalHistoryID)
	}

	// Verify API calls
	if env.Mock.ProfileCalls != 1 {
		t.Errorf("expected 1 profile call, got %d", env.Mock.ProfileCalls)
	}
	if env.Mock.LabelsCalls != 1 {
		t.Errorf("expected 1 labels call, got %d", env.Mock.LabelsCalls)
	}
	if len(env.Mock.GetMessageCalls) != 3 {
		t.Errorf("expected 3 message fetches, got %d", len(env.Mock.GetMessageCalls))
	}

	assertMessageCount(t, env.Store, 3)
}

func TestFullSyncResume(t *testing.T) {
	env := newTestEnv(t)

	// Create mock with pagination
	env.Mock.Profile.MessagesTotal = 4
	env.Mock.Profile.HistoryID = 12345
	env.Mock.AddMessage("msg1", testMIME, []string{"INBOX"})
	env.Mock.AddMessage("msg2", testMIME, []string{"INBOX"})
	env.Mock.AddMessage("msg3", testMIME, []string{"INBOX"})
	env.Mock.AddMessage("msg4", testMIME, []string{"INBOX"})
	env.Mock.MessagePages = [][]string{
		{"msg1", "msg2"},
		{"msg3", "msg4"},
	}

	summary1 := runFullSync(t, env)
	assertSummary(t, summary1, 4, -1, -1, -1)

	// Second sync should skip already-synced messages
	env.Mock.Reset()
	env.Mock.Profile = &gmail.Profile{
		EmailAddress:  testEmail,
		MessagesTotal: 4,
		HistoryID:     12346,
	}
	env.Mock.AddMessage("msg1", testMIME, []string{"INBOX"})
	env.Mock.AddMessage("msg2", testMIME, []string{"INBOX"})
	env.Mock.AddMessage("msg3", testMIME, []string{"INBOX"})
	env.Mock.AddMessage("msg4", testMIME, []string{"INBOX"})

	summary2 := runFullSync(t, env)
	assertSummary(t, summary2, 0, -1, -1, -1)
}

func TestFullSyncWithErrors(t *testing.T) {
	env := newTestEnv(t)
	seedMessages(env, 3, 12345, "msg1", "msg2", "msg3")

	// Make msg2 fail to fetch
	env.Mock.GetMessageError["msg2"] = &gmail.NotFoundError{Path: "/messages/msg2"}

	summary := runFullSync(t, env)
	assertSummary(t, summary, 2, 1, -1, -1)
}

func TestMIMEParsing(t *testing.T) {
	env := newTestEnv(t)

	complexMIME := []byte(`From: "John Doe" <john@example.com>
To: "Jane Smith" <jane@example.com>, bob@example.com
Cc: cc@example.com
Subject: Re: Meeting Notes
Date: Tue, 15 Jan 2024 14:30:00 -0500
Message-ID: <msg123@example.com>
In-Reply-To: <msg122@example.com>
Content-Type: multipart/mixed; boundary="boundary123"

--boundary123
Content-Type: text/plain; charset="utf-8"

Hello,

This is the message body.

Best regards,
John

--boundary123
Content-Type: application/pdf; name="document.pdf"
Content-Disposition: attachment; filename="document.pdf"
Content-Transfer-Encoding: base64

JVBERi0xLjQKJeLjz9MKMSAwIG9iago8PC9UeXBlL0NhdGFsb2cvUGFnZXMgMiAwIFI+PgplbmRv
Ymo=
--boundary123--
`)

	env.Mock.Profile.MessagesTotal = 1
	env.Mock.Profile.HistoryID = 12345
	env.Mock.AddMessage("complex1", complexMIME, []string{"INBOX"})

	opts := DefaultOptions()
	opts.AttachmentsDir = filepath.Join(env.TmpDir, "attachments")
	env.Syncer = New(env.Mock, env.Store, opts)

	summary := runFullSync(t, env)
	assertSummary(t, summary, 1, -1, -1, -1)
	assertAttachmentCount(t, env.Store, 1)
}

func TestFullSyncEmptyInbox(t *testing.T) {
	env := newTestEnv(t)
	env.Mock.Profile.MessagesTotal = 0
	env.Mock.Profile.HistoryID = 12345

	summary := runFullSync(t, env)
	assertSummary(t, summary, 0, -1, -1, 0)
}

func TestFullSyncProfileError(t *testing.T) {
	env := newTestEnv(t)
	env.Mock.ProfileError = fmt.Errorf("auth failed")

	_, err := env.Syncer.Full(env.Context, testEmail)
	if err == nil {
		t.Error("expected error when profile fails")
	}
}

func TestFullSyncAllDuplicates(t *testing.T) {
	env := newTestEnv(t)
	seedMessages(env, 3, 12345, "msg1", "msg2", "msg3")

	// First sync
	runFullSync(t, env)

	// Second sync with same messages - all should be skipped
	summary := runFullSync(t, env)
	assertSummary(t, summary, 0, -1, 3, -1)
}

func TestFullSyncNoResume(t *testing.T) {
	env := newTestEnv(t)
	seedMessages(env, 2, 12345, "msg1", "msg2")

	opts := DefaultOptions()
	opts.NoResume = true
	env.Syncer = New(env.Mock, env.Store, opts)

	summary := runFullSync(t, env)
	if summary.WasResumed {
		t.Error("expected WasResumed to be false with NoResume option")
	}
	assertSummary(t, summary, 2, -1, -1, -1)
}

func TestFullSyncAllErrors(t *testing.T) {
	env := newTestEnv(t)
	seedMessages(env, 3, 12345, "msg1", "msg2", "msg3")

	env.Mock.GetMessageError["msg1"] = &gmail.NotFoundError{Path: "/messages/msg1"}
	env.Mock.GetMessageError["msg2"] = &gmail.NotFoundError{Path: "/messages/msg2"}
	env.Mock.GetMessageError["msg3"] = &gmail.NotFoundError{Path: "/messages/msg3"}

	summary := runFullSync(t, env)
	assertSummary(t, summary, 0, 3, -1, -1)
}

func TestFullSyncWithQuery(t *testing.T) {
	env := newTestEnv(t)
	seedMessages(env, 2, 12345, "msg1", "msg2")

	opts := DefaultOptions()
	opts.Query = "before:2024/06/01"
	env.Syncer = New(env.Mock, env.Store, opts)

	summary := runFullSync(t, env)

	if env.Mock.LastQuery != "before:2024/06/01" {
		t.Errorf("expected query %q, got %q", "before:2024/06/01", env.Mock.LastQuery)
	}
	assertSummary(t, summary, 2, -1, -1, -1)
}

func TestFullSyncPagination(t *testing.T) {
	env := newTestEnv(t)
	env.Mock.Profile.MessagesTotal = 6
	env.Mock.Profile.HistoryID = 12345

	for i := 1; i <= 6; i++ {
		env.Mock.AddMessage(fmt.Sprintf("msg%d", i), testMIME, []string{"INBOX"})
	}
	env.Mock.MessagePages = [][]string{
		{"msg1", "msg2"},
		{"msg3", "msg4"},
		{"msg5", "msg6"},
	}

	summary := runFullSync(t, env)
	assertSummary(t, summary, 6, -1, -1, -1)

	if env.Mock.ListMessagesCalls != 3 {
		t.Errorf("expected 3 list calls (one per page), got %d", env.Mock.ListMessagesCalls)
	}
}

func TestSyncerWithLogger(t *testing.T) {
	env := newTestEnv(t)
	syncer := env.Syncer.WithLogger(nil)
	if syncer == nil {
		t.Error("WithLogger should return syncer for chaining")
	}
}

func TestSyncerWithProgress(t *testing.T) {
	env := newTestEnv(t)
	syncer := env.Syncer.WithProgress(gmail.NullProgress{})
	if syncer == nil {
		t.Error("WithProgress should return syncer for chaining")
	}
}

// Tests for incremental sync

func TestIncrementalSyncNoSource(t *testing.T) {
	env := newTestEnv(t)

	_, err := env.Syncer.Incremental(env.Context, testEmail)
	if err == nil {
		t.Error("expected error for incremental sync without source")
	}
}

func TestIncrementalSyncNoHistoryID(t *testing.T) {
	env := newTestEnv(t)

	env.MustCreateSource(t)

	_, err := env.Syncer.Incremental(env.Context, testEmail)
	if err == nil {
		t.Error("expected error for incremental sync without history ID")
	}
}

func TestIncrementalSyncAlreadyUpToDate(t *testing.T) {
	env := newTestEnv(t)
	env.SetupSource(t, "12345")

	env.Mock.Profile.MessagesTotal = 10
	env.Mock.Profile.HistoryID = 12345 // Same as cursor

	summary := runIncrementalSync(t, env)
	assertSummary(t, summary, 0, -1, -1, -1)
}

func TestIncrementalSyncWithChanges(t *testing.T) {
	env := newTestEnv(t)
	env.SetupSource(t, "12340")

	env.Mock.Profile.MessagesTotal = 10
	env.Mock.Profile.HistoryID = 12350
	env.Mock.AddMessage("new-msg-1", testMIME, []string{"INBOX"})
	env.Mock.AddMessage("new-msg-2", testMIME, []string{"INBOX"})

	env.Mock.HistoryRecords = []gmail.HistoryRecord{
		{
			MessagesAdded: []gmail.HistoryMessage{
				{Message: gmail.MessageID{ID: "new-msg-1", ThreadID: "thread_new-msg-1"}},
			},
		},
		{
			MessagesAdded: []gmail.HistoryMessage{
				{Message: gmail.MessageID{ID: "new-msg-2", ThreadID: "thread_new-msg-2"}},
			},
		},
	}
	env.Mock.HistoryID = 12350

	summary := runIncrementalSync(t, env)
	assertSummary(t, summary, 2, -1, -1, -1)
}

func TestIncrementalSyncWithDeletions(t *testing.T) {
	env := newTestEnv(t)
	seedMessages(env, 2, 12340, "msg1", "msg2")

	runFullSync(t, env)

	// Now simulate deletion via incremental
	env.Mock.Profile.HistoryID = 12350
	env.Mock.HistoryRecords = []gmail.HistoryRecord{
		{
			MessagesDeleted: []gmail.HistoryMessage{
				{Message: gmail.MessageID{ID: "msg1", ThreadID: "thread_msg1"}},
			},
		},
	}
	env.Mock.HistoryID = 12350

	summary := runIncrementalSync(t, env)
	assertSummary(t, summary, -1, -1, -1, 1)

	// Verify deletion was persisted
	var deletedAt sql.NullTime
	err := env.Store.DB().QueryRow(env.Store.Rebind("SELECT deleted_from_source_at FROM messages WHERE source_message_id = ?"), "msg1").Scan(&deletedAt)
	if err != nil {
		t.Fatalf("query deleted_from_source_at: %v", err)
	}
	if !deletedAt.Valid {
		t.Error("msg1 should have deleted_from_source_at set after incremental sync with deletion")
	}

	// Verify msg2 is NOT marked as deleted
	err = env.Store.DB().QueryRow(env.Store.Rebind("SELECT deleted_from_source_at FROM messages WHERE source_message_id = ?"), "msg2").Scan(&deletedAt)
	if err != nil {
		t.Fatalf("query deleted_from_source_at for msg2: %v", err)
	}
	if deletedAt.Valid {
		t.Error("msg2 should NOT have deleted_from_source_at set")
	}
}

func TestIncrementalSyncHistoryExpired(t *testing.T) {
	env := newTestEnv(t)
	env.SetupSource(t, "1000")

	env.Mock.Profile.MessagesTotal = 10
	env.Mock.Profile.HistoryID = 12350
	env.Mock.HistoryError = &gmail.NotFoundError{Path: "/history"}

	_, err := env.Syncer.Incremental(env.Context, testEmail)
	if err == nil {
		t.Error("expected error for expired history")
	}
}

func TestIncrementalSyncProfileError(t *testing.T) {
	env := newTestEnv(t)
	env.SetupSource(t, "12345")
	env.Mock.ProfileError = fmt.Errorf("auth failed")

	_, err := env.Syncer.Incremental(env.Context, testEmail)
	if err == nil {
		t.Error("expected error when profile fails")
	}
}

func TestIncrementalSyncWithLabelAdded(t *testing.T) {
	env := newTestEnv(t)
	env.Mock.Profile.MessagesTotal = 1
	env.Mock.Profile.HistoryID = 12340
	env.Mock.AddMessage("msg1", testMIME, []string{"INBOX"})

	runFullSync(t, env)

	// Now simulate label addition via incremental
	env.Mock.Profile.HistoryID = 12350
	env.Mock.HistoryRecords = []gmail.HistoryRecord{
		{
			LabelsAdded: []gmail.HistoryLabelChange{
				{
					Message:  gmail.MessageID{ID: "msg1", ThreadID: "thread_msg1"},
					LabelIDs: []string{"STARRED"},
				},
			},
		},
	}
	env.Mock.HistoryID = 12350
	env.Mock.Messages["msg1"].LabelIDs = []string{"INBOX", "STARRED"}

	summary := runIncrementalSync(t, env)
	assertSummary(t, summary, -1, -1, -1, 1)
}

func TestIncrementalSyncWithLabelRemoved(t *testing.T) {
	env := newTestEnv(t)
	env.Mock.Profile.MessagesTotal = 1
	env.Mock.Profile.HistoryID = 12340
	env.Mock.AddMessage("msg1", testMIME, []string{"INBOX", "STARRED"})

	runFullSync(t, env)

	// Now simulate label removal via incremental
	env.Mock.Profile.HistoryID = 12350
	env.Mock.HistoryRecords = []gmail.HistoryRecord{
		{
			LabelsRemoved: []gmail.HistoryLabelChange{
				{
					Message:  gmail.MessageID{ID: "msg1", ThreadID: "thread_msg1"},
					LabelIDs: []string{"STARRED"},
				},
			},
		},
	}
	env.Mock.HistoryID = 12350
	env.Mock.Messages["msg1"].LabelIDs = []string{"INBOX"}

	summary := runIncrementalSync(t, env)
	assertSummary(t, summary, -1, -1, -1, 1)
}

func TestIncrementalSyncLabelAddedToNewMessage(t *testing.T) {
	env := newTestEnv(t)
	source := env.SetupSource(t, "12340")
	if _, err := env.Store.EnsureLabel(source.ID, "INBOX", "Inbox", "system"); err != nil {
		t.Fatalf("EnsureLabel INBOX: %v", err)
	}
	if _, err := env.Store.EnsureLabel(source.ID, "STARRED", "Starred", "system"); err != nil {
		t.Fatalf("EnsureLabel STARRED: %v", err)
	}

	env.Mock.Profile.MessagesTotal = 1
	env.Mock.Profile.HistoryID = 12350
	env.Mock.AddMessage("new-msg", testMIME, []string{"INBOX", "STARRED"})

	env.Mock.HistoryRecords = []gmail.HistoryRecord{
		{
			LabelsAdded: []gmail.HistoryLabelChange{
				{
					Message:  gmail.MessageID{ID: "new-msg", ThreadID: "thread_new-msg"},
					LabelIDs: []string{"STARRED"},
				},
			},
		},
	}
	env.Mock.HistoryID = 12350

	_, err := env.Syncer.Incremental(env.Context, testEmail)
	if err != nil {
		t.Fatalf("incremental sync: %v", err)
	}

	assertMessageCount(t, env.Store, 1)
}

func TestIncrementalSyncLabelRemovedFromMissingMessage(t *testing.T) {
	env := newTestEnv(t)
	env.SetupSource(t, "12340")

	env.Mock.Profile.MessagesTotal = 1
	env.Mock.Profile.HistoryID = 12350

	env.Mock.HistoryRecords = []gmail.HistoryRecord{
		{
			LabelsRemoved: []gmail.HistoryLabelChange{
				{
					Message:  gmail.MessageID{ID: "unknown-msg", ThreadID: "thread_unknown"},
					LabelIDs: []string{"STARRED"},
				},
			},
		},
	}
	env.Mock.HistoryID = 12350

	summary := runIncrementalSync(t, env)
	assertSummary(t, summary, 0, -1, -1, -1)
}

// MIME message with attachment for testing
var testMIMEWithAttachment = []byte(`From: sender@example.com
To: recipient@example.com
Subject: Test with Attachment
Date: Mon, 01 Jan 2024 12:00:00 +0000
MIME-Version: 1.0
Content-Type: multipart/mixed; boundary="boundary123"

--boundary123
Content-Type: text/plain; charset="utf-8"

This is the message body.
--boundary123
Content-Type: application/octet-stream; name="test.bin"
Content-Disposition: attachment; filename="test.bin"
Content-Transfer-Encoding: base64

SGVsbG8gV29ybGQh
--boundary123--
`)

func TestFullSyncWithAttachment(t *testing.T) {
	env := newTestEnv(t)
	env.Mock.Profile.MessagesTotal = 1
	env.Mock.Profile.HistoryID = 12345
	env.Mock.AddMessage("msg-with-attachment", testMIMEWithAttachment, []string{"INBOX"})

	attachDir := withAttachmentsDir(t, env)

	summary := runFullSync(t, env)
	assertSummary(t, summary, 1, -1, -1, -1)

	if _, err := os.Stat(attachDir); os.IsNotExist(err) {
		t.Error("attachments directory should have been created")
	}

	assertAttachmentCount(t, env.Store, 1)
}

func TestFullSyncWithEmptyAttachment(t *testing.T) {
	env := newTestEnv(t)

	emptyAttachMIME := []byte(`From: sender@example.com
To: recipient@example.com
Subject: Empty Attachment
Date: Mon, 01 Jan 2024 12:00:00 +0000
MIME-Version: 1.0
Content-Type: multipart/mixed; boundary="boundary123"

--boundary123
Content-Type: text/plain; charset="utf-8"

Body text.
--boundary123
Content-Type: application/octet-stream; name="empty.bin"
Content-Disposition: attachment; filename="empty.bin"
Content-Transfer-Encoding: base64


--boundary123--
`)

	env.Mock.Profile.MessagesTotal = 1
	env.Mock.Profile.HistoryID = 12345
	env.Mock.AddMessage("msg-empty-attach", emptyAttachMIME, []string{"INBOX"})

	withAttachmentsDir(t, env)

	runFullSync(t, env)
	assertAttachmentCount(t, env.Store, 0)
}

func TestFullSyncAttachmentDeduplication(t *testing.T) {
	env := newTestEnv(t)
	env.Mock.Profile.MessagesTotal = 2
	env.Mock.Profile.HistoryID = 12345
	env.Mock.AddMessage("msg1-attach", testMIMEWithAttachment, []string{"INBOX"})
	env.Mock.AddMessage("msg2-attach", testMIMEWithAttachment, []string{"INBOX"})

	attachDir := withAttachmentsDir(t, env)

	runFullSync(t, env)
	assertAttachmentCount(t, env.Store, 2)

	if fileCount := countFiles(t, attachDir); fileCount != 1 {
		t.Errorf("expected 1 file in attachments dir (deduped), got %d", fileCount)
	}
}

// MIME message with no subject
var testMIMENoSubject = []byte(`From: sender@example.com
To: recipient@example.com
Date: Mon, 01 Jan 2024 12:00:00 +0000
Content-Type: text/plain; charset="utf-8"

Message with no subject line.
`)

func TestFullSyncNoSubject(t *testing.T) {
	env := newTestEnv(t)
	env.Mock.Profile.MessagesTotal = 1
	env.Mock.Profile.HistoryID = 12345
	env.Mock.AddMessage("msg-no-subject", testMIMENoSubject, []string{"INBOX"})

	summary := runFullSync(t, env)
	assertSummary(t, summary, 1, -1, -1, -1)
}

// MIME message with multiple recipients (CC and BCC)
var testMIMEMultipleRecipients = []byte(`From: sender@example.com
To: to1@example.com, to2@example.com
Cc: cc1@example.com
Bcc: bcc1@example.com
Subject: Multiple Recipients
Date: Mon, 01 Jan 2024 12:00:00 +0000
Content-Type: text/plain; charset="utf-8"

Message with multiple recipients.
`)

func TestFullSyncMultipleRecipients(t *testing.T) {
	env := newTestEnv(t)
	env.Mock.Profile.MessagesTotal = 1
	env.Mock.Profile.HistoryID = 12345
	env.Mock.AddMessage("msg-multi-recip", testMIMEMultipleRecipients, []string{"INBOX"})

	summary := runFullSync(t, env)
	assertSummary(t, summary, 1, -1, -1, -1)
}

func TestFullSyncWithMIMEParseError(t *testing.T) {
	env := newTestEnv(t)
	env.Mock.Profile.MessagesTotal = 2
	env.Mock.Profile.HistoryID = 12345
	env.Mock.AddMessage("msg-good", testMIME, []string{"INBOX"})
	env.Mock.Messages["msg-bad"] = &gmail.RawMessage{
		ID:           "msg-bad",
		ThreadID:     "thread_msg-bad",
		LabelIDs:     []string{"INBOX"},
		Raw:          []byte("not valid mime at all - just garbage"),
		Snippet:      "This is the snippet preview",
		SizeEstimate: 100,
	}

	summary := runFullSync(t, env)
	assertSummary(t, summary, 2, 0, -1, -1)

	// Verify the bad message was stored with placeholder content in message_bodies
	var bodyText string
	err := env.Store.DB().QueryRow(`
		SELECT mb.body_text FROM message_bodies mb
		JOIN messages m ON m.id = mb.message_id
		WHERE m.source_message_id = 'msg-bad'
	`).Scan(&bodyText)
	if err != nil {
		t.Fatalf("query bad message: %v", err)
	}
	if !strings.Contains(bodyText, "MIME parsing failed") {
		t.Errorf("expected placeholder body with error message, got: %s", bodyText)
	}

	// Verify raw MIME was preserved
	var rawData []byte
	err = env.Store.DB().QueryRow(`
		SELECT raw_data FROM message_raw mr
		JOIN messages m ON m.id = mr.message_id
		WHERE m.source_message_id = 'msg-bad'
	`).Scan(&rawData)
	if err != nil {
		t.Fatalf("query raw data: %v", err)
	}
	if len(rawData) == 0 {
		t.Error("expected raw MIME data to be preserved")
	}
}

func TestFullSyncMessageFetchError(t *testing.T) {
	env := newTestEnv(t)
	env.Mock.Profile.MessagesTotal = 2
	env.Mock.Profile.HistoryID = 12345
	env.Mock.AddMessage("msg-good", testMIME, []string{"INBOX"})

	env.Mock.MessagePages = [][]string{{"msg-good", "msg-missing"}}

	summary := runFullSync(t, env)
	assertSummary(t, summary, 1, -1, -1, -1)
}

func TestIncrementalSyncLabelsError(t *testing.T) {
	env := newTestEnv(t)
	env.SetupSource(t, "12340")

	env.Mock.Profile.MessagesTotal = 1
	env.Mock.Profile.HistoryID = 12350
	env.Mock.LabelsError = fmt.Errorf("labels API error")

	_, err := env.Syncer.Incremental(env.Context, testEmail)
	if err == nil {
		t.Error("expected error when labels sync fails")
	}
}

func TestFullSyncResumeWithCursor(t *testing.T) {
	env := newTestEnv(t)
	env.Mock.Profile.MessagesTotal = 4
	env.Mock.Profile.HistoryID = 12345
	env.Mock.MessagePages = [][]string{
		{"msg1", "msg2"},
		{"msg3", "msg4"},
	}
	env.Mock.AddMessage("msg1", testMIME, []string{"INBOX"})
	env.Mock.AddMessage("msg2", testMIME, []string{"INBOX"})
	env.Mock.AddMessage("msg3", testMIME, []string{"INBOX"})
	env.Mock.AddMessage("msg4", testMIME, []string{"INBOX"})

	source := env.MustCreateSource(t)

	// Process just page 1
	env.Mock.MessagePages = [][]string{{"msg1", "msg2"}}
	runFullSync(t, env)
	assertMessageCount(t, env.Store, 2)

	// Restore both pages and create an "interrupted" sync
	env.Mock.MessagePages = [][]string{
		{"msg1", "msg2"},
		{"msg3", "msg4"},
	}
	env.Mock.ListMessagesCalls = 0

	syncID, err := env.Store.StartSync(source.ID, "full")
	if err != nil {
		t.Fatalf("StartSync: %v", err)
	}

	checkpoint := &store.Checkpoint{
		PageToken:         "page_1",
		MessagesProcessed: 2,
		MessagesAdded:     2,
	}
	if err := env.Store.UpdateSyncCheckpoint(syncID, checkpoint); err != nil {
		t.Fatalf("UpdateSyncCheckpoint: %v", err)
	}

	summary := runFullSync(t, env)

	if !summary.WasResumed {
		t.Error("expected WasResumed = true")
	}
	if summary.ResumedFromToken != "page_1" {
		t.Errorf("expected ResumedFromToken = 'page_1', got %q", summary.ResumedFromToken)
	}
	assertSummary(t, summary, 4, -1, -1, -1)

	if env.Mock.ListMessagesCalls != 1 {
		t.Errorf("expected 1 ListMessages call (resumed from page_1), got %d", env.Mock.ListMessagesCalls)
	}
	assertMessageCount(t, env.Store, 4)
}

func TestFullSyncHTMLOnlyMessage(t *testing.T) {
	env := newTestEnv(t)

	htmlOnlyMIME := []byte(`From: sender@example.com
To: recipient@example.com
Subject: HTML Only
Date: Mon, 01 Jan 2024 12:00:00 +0000
MIME-Version: 1.0
Content-Type: text/html; charset="utf-8"

<html><body><p>This is HTML only content.</p></body></html>
`)

	env.Mock.Profile.MessagesTotal = 1
	env.Mock.Profile.HistoryID = 12345
	env.Mock.AddMessage("msg-html-only", htmlOnlyMIME, []string{"INBOX"})

	summary := runFullSync(t, env)
	assertSummary(t, summary, 1, -1, -1, -1)
}

// MIME message with duplicate recipients across To/Cc/Bcc
var testMIMEDuplicateRecipients = []byte(`From: sender@example.com
To: duplicate@example.com, other@example.com, "Duplicate Person" <duplicate@example.com>
Cc: cc-dup@example.com, "CC Duplicate" <cc-dup@example.com>
Bcc: bcc-dup@example.com, bcc-dup@example.com
Subject: Duplicate Recipients
Date: Mon, 01 Jan 2024 12:00:00 +0000
Content-Type: text/plain; charset="utf-8"

Message with duplicate recipients in To, Cc, and Bcc fields.
`)

func TestFullSyncDuplicateRecipients(t *testing.T) {
	env := newTestEnv(t)
	env.Mock.Profile.MessagesTotal = 1
	env.Mock.Profile.HistoryID = 12345
	env.Mock.AddMessage("msg-dup-recip", testMIMEDuplicateRecipients, []string{"INBOX"})

	summary := runFullSync(t, env)
	assertSummary(t, summary, 1, 0, -1, -1)
	assertMessageCount(t, env.Store, 1)

	// Verify To recipients are deduplicated: duplicate@example.com appears twice, other once = 2 unique
	var toCount int
	err := env.Store.DB().QueryRow(env.Store.Rebind(`
		SELECT COUNT(*) FROM message_recipients mr
		JOIN messages m ON mr.message_id = m.id
		WHERE m.source_message_id = ? AND mr.recipient_type = 'to'
	`), "msg-dup-recip").Scan(&toCount)
	if err != nil {
		t.Fatalf("query To recipient count: %v", err)
	}
	if toCount != 2 {
		t.Errorf("expected 2 unique To recipients, got %d", toCount)
	}

	// Verify Cc recipients are deduplicated: cc-dup@example.com appears twice = 1 unique
	var ccCount int
	err = env.Store.DB().QueryRow(env.Store.Rebind(`
		SELECT COUNT(*) FROM message_recipients mr
		JOIN messages m ON mr.message_id = m.id
		WHERE m.source_message_id = ? AND mr.recipient_type = 'cc'
	`), "msg-dup-recip").Scan(&ccCount)
	if err != nil {
		t.Fatalf("query Cc recipient count: %v", err)
	}
	if ccCount != 1 {
		t.Errorf("expected 1 unique Cc recipient, got %d", ccCount)
	}

	// Verify Bcc recipients are deduplicated: bcc-dup@example.com appears twice = 1 unique
	var bccCount int
	err = env.Store.DB().QueryRow(env.Store.Rebind(`
		SELECT COUNT(*) FROM message_recipients mr
		JOIN messages m ON mr.message_id = m.id
		WHERE m.source_message_id = ? AND mr.recipient_type = 'bcc'
	`), "msg-dup-recip").Scan(&bccCount)
	if err != nil {
		t.Fatalf("query Bcc recipient count: %v", err)
	}
	if bccCount != 1 {
		t.Errorf("expected 1 unique Bcc recipient, got %d", bccCount)
	}

	// Verify display name preference: duplicate@example.com should prefer "Duplicate Person"
	var displayName string
	err = env.Store.DB().QueryRow(env.Store.Rebind(`
		SELECT mr.display_name FROM message_recipients mr
		JOIN messages m ON mr.message_id = m.id
		JOIN participants p ON mr.participant_id = p.id
		WHERE m.source_message_id = ? AND mr.recipient_type = 'to' AND p.email_address = ?
	`), "msg-dup-recip", "duplicate@example.com").Scan(&displayName)
	if err != nil {
		t.Fatalf("query display name: %v", err)
	}
	if displayName != "Duplicate Person" {
		t.Errorf("expected display name 'Duplicate Person' (non-empty preferred), got %q", displayName)
	}

	// Verify Cc display name preference
	err = env.Store.DB().QueryRow(env.Store.Rebind(`
		SELECT mr.display_name FROM message_recipients mr
		JOIN messages m ON mr.message_id = m.id
		JOIN participants p ON mr.participant_id = p.id
		WHERE m.source_message_id = ? AND mr.recipient_type = 'cc' AND p.email_address = ?
	`), "msg-dup-recip", "cc-dup@example.com").Scan(&displayName)
	if err != nil {
		t.Fatalf("query Cc display name: %v", err)
	}
	if displayName != "CC Duplicate" {
		t.Errorf("expected Cc display name 'CC Duplicate' (non-empty preferred), got %q", displayName)
	}
}

func TestFullSyncDateFallbackToInternalDate(t *testing.T) {
	env := newTestEnv(t)

	badDateMIME := []byte(`From: sender@example.com
To: recipient@example.com
Subject: Bad Date
Date: This is not a valid date
Content-Type: text/plain; charset="utf-8"

Message with invalid date header.
`)

	env.Mock.Profile.MessagesTotal = 1
	env.Mock.Profile.HistoryID = 12345
	env.Mock.Messages["msg-bad-date"] = &gmail.RawMessage{
		ID:           "msg-bad-date",
		ThreadID:     "thread-bad-date",
		LabelIDs:     []string{"INBOX"},
		Raw:          badDateMIME,
		InternalDate: 1705320000000, // 2024-01-15T12:00:00Z
	}
	env.Mock.MessagePages = [][]string{{"msg-bad-date"}}

	runFullSync(t, env)

	var sentAt, internalDate string
	err := env.Store.DB().QueryRow(env.Store.Rebind(`
		SELECT sent_at, internal_date FROM messages WHERE source_message_id = ?
	`), "msg-bad-date").Scan(&sentAt, &internalDate)
	if err != nil {
		t.Fatalf("query message: %v", err)
	}

	if sentAt == "" {
		t.Errorf("SentAt should not be empty (should fallback to InternalDate)")
	}
	if internalDate == "" {
		t.Errorf("InternalDate should not be empty")
	}
	if sentAt != internalDate {
		t.Errorf("SentAt (%q) should equal InternalDate (%q) when Date header is unparseable", sentAt, internalDate)
	}
	if !strings.Contains(sentAt, "2024-01-15") || !strings.Contains(sentAt, "12:00:00") {
		t.Errorf("SentAt = %q, expected to contain 2024-01-15 12:00:00", sentAt)
	}
}

func TestFullSyncEmptyRawMIME(t *testing.T) {
	env := newTestEnv(t)
	env.Mock.Profile.MessagesTotal = 2
	env.Mock.Profile.HistoryID = 12345

	env.Mock.AddMessage("msg-good", testMIME, []string{"INBOX"})
	env.Mock.Messages["msg-empty-raw"] = &gmail.RawMessage{
		ID:           "msg-empty-raw",
		ThreadID:     "thread-empty-raw",
		LabelIDs:     []string{"INBOX"},
		Raw:          []byte{},
		SizeEstimate: 0,
	}

	summary := runFullSync(t, env)
	assertSummary(t, summary, 1, 1, -1, -1)
}

func TestFullSyncEmptyThreadID(t *testing.T) {
	env := newTestEnv(t)
	env.Mock.Profile.MessagesTotal = 1
	env.Mock.Profile.HistoryID = 12345
	env.Mock.UseRawThreadID = true

	env.Mock.Messages["msg-no-thread"] = &gmail.RawMessage{
		ID:           "msg-no-thread",
		ThreadID:     "",
		LabelIDs:     []string{"INBOX"},
		Raw:          testMIME,
		SizeEstimate: int64(len(testMIME)),
	}
	env.Mock.MessagePages = [][]string{{"msg-no-thread"}}

	summary := runFullSync(t, env)
	assertSummary(t, summary, 1, 0, -1, -1)

	var threadSourceID string
	err := env.Store.DB().QueryRow(env.Store.Rebind(`
		SELECT c.source_conversation_id FROM conversations c
		JOIN messages m ON m.conversation_id = c.id
		WHERE m.source_message_id = ?
	`), "msg-no-thread").Scan(&threadSourceID)
	if err != nil {
		t.Fatalf("query thread: %v", err)
	}
	if threadSourceID != "msg-no-thread" {
		t.Errorf("expected thread source_conversation_id = 'msg-no-thread' (fallback), got %q", threadSourceID)
	}
}

func TestFullSyncListEmptyThreadIDRawPresent(t *testing.T) {
	env := newTestEnv(t)
	env.Mock.Profile.MessagesTotal = 1
	env.Mock.Profile.HistoryID = 12345

	env.Mock.ListThreadIDOverride = map[string]string{
		"msg-list-empty": "",
	}
	env.Mock.Messages["msg-list-empty"] = &gmail.RawMessage{
		ID:           "msg-list-empty",
		ThreadID:     "actual-thread-from-raw",
		LabelIDs:     []string{"INBOX"},
		Raw:          testMIME,
		SizeEstimate: int64(len(testMIME)),
	}
	env.Mock.MessagePages = [][]string{{"msg-list-empty"}}

	summary := runFullSync(t, env)
	assertSummary(t, summary, 1, 0, -1, -1)

	var threadSourceID string
	err := env.Store.DB().QueryRow(env.Store.Rebind(`
		SELECT c.source_conversation_id FROM conversations c
		JOIN messages m ON m.conversation_id = c.id
		WHERE m.source_message_id = ?
	`), "msg-list-empty").Scan(&threadSourceID)
	if err != nil {
		t.Fatalf("query thread: %v", err)
	}
	if threadSourceID != "actual-thread-from-raw" {
		t.Errorf("expected thread source_conversation_id = 'actual-thread-from-raw' (from raw), got %q", threadSourceID)
	}
}

// TestAttachmentFilePermissions verifies that attachment files are saved with
// restrictive permissions (0600) to protect email content.
func TestAttachmentFilePermissions(t *testing.T) {
	env := newTestEnv(t)
	env.Mock.Profile.MessagesTotal = 1
	env.Mock.Profile.HistoryID = 12345
	env.Mock.AddMessage("msg-with-attachment", testMIMEWithAttachment, []string{"INBOX"})

	attachDir := withAttachmentsDir(t, env)

	runFullSync(t, env)

	// Find the attachment file
	var attachmentPath string
	err := filepath.WalkDir(attachDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			attachmentPath = path
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WalkDir(%s): %v", attachDir, err)
	}
	if attachmentPath == "" {
		t.Fatal("no attachment file found")
	}

	info, err := os.Stat(attachmentPath)
	if err != nil {
		t.Fatalf("Stat(%s) error = %v", attachmentPath, err)
	}

	// File should have 0600 permissions (owner read/write only)
	got := info.Mode().Perm()
	want := os.FileMode(0600)
	if got != want {
		t.Errorf("attachment file permissions = %04o, want %04o", got, want)
	}
}
