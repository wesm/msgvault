package sync

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/wesm/msgvault/internal/gmail"
	"github.com/wesm/msgvault/internal/store"
	testemail "github.com/wesm/msgvault/internal/testutil/email"
)

// panicOnBatchAPI wraps a MockAPI and panics when GetMessagesRawBatch is called.
// Used to test that Full() recovers from panics gracefully.
type panicOnBatchAPI struct {
	*gmail.MockAPI
}

func (p *panicOnBatchAPI) GetMessagesRawBatch(_ context.Context, _ []string) ([]*gmail.RawMessage, error) {
	panic("unexpected nil pointer in batch processing")
}

func TestFullSync_PanicReturnsError(t *testing.T) {
	env := newTestEnv(t)
	seedMessages(env, 1, 12345, "msg1")

	// Replace the client with one that panics during batch fetch
	env.Syncer = New(&panicOnBatchAPI{MockAPI: env.Mock}, env.Store, nil)

	// Should return an error, NOT panic and crash the program
	_, err := env.Syncer.Full(env.Context, testEmail)
	if err == nil {
		t.Fatal("expected error from panic recovery, got nil")
	}
	if !strings.Contains(err.Error(), "panic") {
		t.Errorf("expected error to mention panic, got: %v", err)
	}
}

// panicOnHistoryAPI wraps a MockAPI and panics when ListHistory is called.
// Used to test that Incremental() recovers from panics gracefully.
type panicOnHistoryAPI struct {
	*gmail.MockAPI
}

func (p *panicOnHistoryAPI) ListHistory(_ context.Context, _ uint64, _ string) (*gmail.HistoryResponse, error) {
	panic("unexpected nil pointer in history processing")
}

func TestIncrementalSync_PanicReturnsError(t *testing.T) {
	env := newTestEnv(t)
	env.CreateSourceWithHistory(t, "12340")

	env.Mock.Profile.MessagesTotal = 10
	env.Mock.Profile.HistoryID = 12350

	// Replace the client with one that panics during history fetch
	env.Syncer = New(&panicOnHistoryAPI{MockAPI: env.Mock}, env.Store, nil)

	// Should return an error, NOT panic and crash the program
	_, err := env.Syncer.Incremental(env.Context, testEmail)
	if err == nil {
		t.Fatal("expected error from panic recovery, got nil")
	}
	if !strings.Contains(err.Error(), "panic") {
		t.Errorf("expected error to mention panic, got: %v", err)
	}
}

func TestFullSync(t *testing.T) {
	env := newTestEnv(t)
	seedMessages(env, 3, 12345, "msg1", "msg2", "msg3")
	env.Mock.Messages["msg2"].LabelIDs = []string{"INBOX", "SENT"}
	env.Mock.Messages["msg3"].LabelIDs = []string{"SENT"}

	summary := runFullSync(t, env)
	assertSummary(t, summary, WantSummary{Added: intPtr(3), Errors: intPtr(0)})
	if summary.FinalHistoryID != 12345 {
		t.Errorf("expected history ID 12345, got %d", summary.FinalHistoryID)
	}

	assertMockCalls(t, env, 1, 1, 3)
	assertMessageCount(t, env.Store, 3)
}

func TestFullSyncResume(t *testing.T) {
	env := newTestEnv(t)

	// Create mock with pagination
	env.Mock.Profile.HistoryID = 12345
	seedPagedMessages(env, 4, 2, "msg")

	summary1 := runFullSync(t, env)
	assertSummary(t, summary1, WantSummary{Added: intPtr(4)})

	// Second sync should skip already-synced messages
	env.Mock.Reset()
	env.Mock.Profile = &gmail.Profile{
		EmailAddress:  testEmail,
		MessagesTotal: 4,
		HistoryID:     12346,
	}
	env.Mock.AddMessage("msg1", testMIME(), []string{"INBOX"})
	env.Mock.AddMessage("msg2", testMIME(), []string{"INBOX"})
	env.Mock.AddMessage("msg3", testMIME(), []string{"INBOX"})
	env.Mock.AddMessage("msg4", testMIME(), []string{"INBOX"})

	summary2 := runFullSync(t, env)
	assertSummary(t, summary2, WantSummary{Added: intPtr(0)})
}

func TestFullSyncWithErrors(t *testing.T) {
	env := newTestEnv(t)
	seedMessages(env, 3, 12345, "msg1", "msg2", "msg3")

	// Make msg2 fail to fetch
	env.Mock.GetMessageError["msg2"] = &gmail.NotFoundError{Path: "/messages/msg2"}

	summary := runFullSync(t, env)
	assertSummary(t, summary, WantSummary{Added: intPtr(2), Errors: intPtr(1)})
}

func TestMIMEParsing(t *testing.T) {
	env := newTestEnv(t)

	pdfData := []byte{0x25, 0x50, 0x44, 0x46, 0x2d, 0x31, 0x2e, 0x34, 0x0a, 0x25, 0xe2, 0xe3, 0xcf, 0xd3, 0x0a, 0x31, 0x20, 0x30, 0x20, 0x6f, 0x62, 0x6a, 0x0a, 0x3c, 0x3c, 0x2f, 0x54, 0x79, 0x70, 0x65, 0x2f, 0x43, 0x61, 0x74, 0x61, 0x6c, 0x6f, 0x67, 0x2f, 0x50, 0x61, 0x67, 0x65, 0x73, 0x20, 0x32, 0x20, 0x30, 0x20, 0x52, 0x3e, 0x3e, 0x0a, 0x65, 0x6e, 0x64, 0x6f, 0x62, 0x6a}
	complexMIME := testemail.NewMessage().
		From(`"John Doe" <john@example.com>`).
		To(`"Jane Smith" <jane@example.com>, bob@example.com`).
		Cc("cc@example.com").
		Subject("Re: Meeting Notes").
		Date("Tue, 15 Jan 2024 14:30:00 -0500").
		Header("Message-ID", "<msg123@example.com>").
		Header("In-Reply-To", "<msg122@example.com>").
		Body("Hello,\n\nThis is the message body.\n\nBest regards,\nJohn\n").
		WithAttachment("document.pdf", "application/pdf", pdfData).
		Bytes()

	env.Mock.Profile.MessagesTotal = 1
	env.Mock.Profile.HistoryID = 12345
	env.Mock.AddMessage("complex1", complexMIME, []string{"INBOX"})

	env.SetOptions(t, func(o *Options) {
		o.AttachmentsDir = filepath.Join(env.TmpDir, "attachments")
	})

	summary := runFullSync(t, env)
	assertSummary(t, summary, WantSummary{Added: intPtr(1)})
	assertAttachmentCount(t, env.Store, 1)
}

func TestFullSyncEmptyInbox(t *testing.T) {
	env := newTestEnv(t)
	env.Mock.Profile.MessagesTotal = 0
	env.Mock.Profile.HistoryID = 12345

	summary := runFullSync(t, env)
	assertSummary(t, summary, WantSummary{Added: intPtr(0), Found: intPtr(0)})
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
	assertSummary(t, summary, WantSummary{Added: intPtr(0), Skipped: intPtr(3)})
}

func TestFullSyncNoResume(t *testing.T) {
	env := newTestEnv(t)
	seedMessages(env, 2, 12345, "msg1", "msg2")

	env.SetOptions(t, func(o *Options) {
		o.NoResume = true
	})

	summary := runFullSync(t, env)
	if summary.WasResumed {
		t.Error("expected WasResumed to be false with NoResume option")
	}
	assertSummary(t, summary, WantSummary{Added: intPtr(2)})
}

func TestFullSyncAllErrors(t *testing.T) {
	env := newTestEnv(t)
	seedMessages(env, 3, 12345, "msg1", "msg2", "msg3")

	env.Mock.GetMessageError["msg1"] = &gmail.NotFoundError{Path: "/messages/msg1"}
	env.Mock.GetMessageError["msg2"] = &gmail.NotFoundError{Path: "/messages/msg2"}
	env.Mock.GetMessageError["msg3"] = &gmail.NotFoundError{Path: "/messages/msg3"}

	summary := runFullSync(t, env)
	assertSummary(t, summary, WantSummary{Added: intPtr(0), Errors: intPtr(3)})
}

func TestFullSyncWithQuery(t *testing.T) {
	env := newTestEnv(t)
	seedMessages(env, 2, 12345, "msg1", "msg2")

	env.SetOptions(t, func(o *Options) {
		o.Query = "before:2024/06/01"
	})

	summary := runFullSync(t, env)

	if env.Mock.LastQuery != "before:2024/06/01" {
		t.Errorf("expected query %q, got %q", "before:2024/06/01", env.Mock.LastQuery)
	}
	assertSummary(t, summary, WantSummary{Added: intPtr(2)})
}

func TestFullSyncPagination(t *testing.T) {
	env := newTestEnv(t)
	env.Mock.Profile.HistoryID = 12345
	seedPagedMessages(env, 6, 2, "msg")

	summary := runFullSync(t, env)
	assertSummary(t, summary, WantSummary{Added: intPtr(6)})
	assertListMessagesCalls(t, env, 3)
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

	env.CreateSource(t)

	_, err := env.Syncer.Incremental(env.Context, testEmail)
	if err == nil {
		t.Error("expected error for incremental sync without history ID")
	}
}

func TestIncrementalSyncAlreadyUpToDate(t *testing.T) {
	env := newTestEnv(t)
	env.CreateSourceWithHistory(t, "12345")

	env.Mock.Profile.MessagesTotal = 10
	env.Mock.Profile.HistoryID = 12345 // Same as cursor

	summary := runIncrementalSync(t, env)
	assertSummary(t, summary, WantSummary{Added: intPtr(0)})
}

func TestIncrementalSyncWithChanges(t *testing.T) {
	env := newTestEnv(t)
	env.CreateSourceWithHistory(t, "12340")

	env.Mock.Profile.MessagesTotal = 10
	env.Mock.Profile.HistoryID = 12350
	env.Mock.AddMessage("new-msg-1", testMIME(), []string{"INBOX"})
	env.Mock.AddMessage("new-msg-2", testMIME(), []string{"INBOX"})

	env.SetHistory(12350,
		historyAdded("new-msg-1"),
		historyAdded("new-msg-2"),
	)

	summary := runIncrementalSync(t, env)
	assertSummary(t, summary, WantSummary{Added: intPtr(2)})
}

func TestIncrementalSyncWithDeletions(t *testing.T) {
	env := newTestEnv(t)
	seedMessages(env, 2, 12340, "msg1", "msg2")

	runFullSync(t, env)

	// Now simulate deletion via incremental
	env.SetHistory(12350, historyDeleted("msg1"))

	summary := runIncrementalSync(t, env)
	assertSummary(t, summary, WantSummary{Found: intPtr(1)})

	// Verify deletion was persisted
	assertDeletedFromSource(t, env.Store, "msg1", true)
	assertDeletedFromSource(t, env.Store, "msg2", false)
}

func TestIncrementalSyncHistoryExpired(t *testing.T) {
	env := newTestEnv(t)
	env.CreateSourceWithHistory(t, "1000")

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
	env.CreateSourceWithHistory(t, "12345")
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
	env.Mock.AddMessage("msg1", testMIME(), []string{"INBOX"})

	runFullSync(t, env)

	// Now simulate label addition via incremental
	env.SetHistory(12350, historyLabelAdded("msg1", "STARRED"))
	env.Mock.Messages["msg1"].LabelIDs = []string{"INBOX", "STARRED"}

	summary := runIncrementalSync(t, env)
	assertSummary(t, summary, WantSummary{Found: intPtr(1)})
}

func TestIncrementalSyncWithLabelRemoved(t *testing.T) {
	env := newTestEnv(t)
	env.Mock.Profile.MessagesTotal = 1
	env.Mock.Profile.HistoryID = 12340
	env.Mock.AddMessage("msg1", testMIME(), []string{"INBOX", "STARRED"})

	runFullSync(t, env)

	// Now simulate label removal via incremental
	env.SetHistory(12350, historyLabelRemoved("msg1", "STARRED"))
	env.Mock.Messages["msg1"].LabelIDs = []string{"INBOX"}

	summary := runIncrementalSync(t, env)
	assertSummary(t, summary, WantSummary{Found: intPtr(1)})
}

func TestIncrementalSyncLabelAddedToNewMessage(t *testing.T) {
	env := newTestEnv(t)
	source := env.CreateSourceWithHistory(t, "12340")
	if _, err := env.Store.EnsureLabel(source.ID, "INBOX", "Inbox", "system"); err != nil {
		t.Fatalf("EnsureLabel INBOX: %v", err)
	}
	if _, err := env.Store.EnsureLabel(source.ID, "STARRED", "Starred", "system"); err != nil {
		t.Fatalf("EnsureLabel STARRED: %v", err)
	}

	env.Mock.Profile.MessagesTotal = 1
	env.Mock.Profile.HistoryID = 12350
	env.Mock.AddMessage("new-msg", testMIME(), []string{"INBOX", "STARRED"})

	env.SetHistory(12350, historyLabelAdded("new-msg", "STARRED"))

	_, err := env.Syncer.Incremental(env.Context, testEmail)
	if err != nil {
		t.Fatalf("incremental sync: %v", err)
	}

	assertMessageCount(t, env.Store, 1)
}

func TestIncrementalSyncLabelRemovedFromMissingMessage(t *testing.T) {
	env := newTestEnv(t)
	env.CreateSourceWithHistory(t, "12340")

	env.Mock.Profile.MessagesTotal = 1
	env.Mock.Profile.HistoryID = 12350

	env.SetHistory(12350, historyLabelRemoved("unknown-msg", "STARRED"))

	summary := runIncrementalSync(t, env)
	assertSummary(t, summary, WantSummary{Added: intPtr(0)})
}

func TestFullSyncWithAttachment(t *testing.T) {
	env := newTestEnv(t)
	env.Mock.Profile.MessagesTotal = 1
	env.Mock.Profile.HistoryID = 12345
	env.Mock.AddMessage("msg-with-attachment", testMIMEWithAttachment(), []string{"INBOX"})

	attachDir := withAttachmentsDir(t, env)

	summary := runFullSync(t, env)
	assertSummary(t, summary, WantSummary{Added: intPtr(1)})

	if _, err := os.Stat(attachDir); os.IsNotExist(err) {
		t.Error("attachments directory should have been created")
	}

	assertAttachmentCount(t, env.Store, 1)
}

func TestFullSyncWithEmptyAttachment(t *testing.T) {
	env := newTestEnv(t)

	emptyAttachMIME := testemail.NewMessage().
		Subject("Empty Attachment").
		Body("Body text.").
		WithAttachment("empty.bin", "application/octet-stream", nil).
		Bytes()

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
	env.Mock.AddMessage("msg1-attach", testMIMEWithAttachment(), []string{"INBOX"})
	env.Mock.AddMessage("msg2-attach", testMIMEWithAttachment(), []string{"INBOX"})

	attachDir := withAttachmentsDir(t, env)

	runFullSync(t, env)
	assertAttachmentCount(t, env.Store, 2)

	if fileCount := countFiles(t, attachDir); fileCount != 1 {
		t.Errorf("expected 1 file in attachments dir (deduped), got %d", fileCount)
	}
}

// TestFullSync_MessageVariations consolidates tests for various MIME message formats.
func TestFullSync_MessageVariations(t *testing.T) {
	tests := []struct {
		name  string
		mime  func() []byte
		check func(*testing.T, *TestEnv)
	}{
		{
			name: "NoSubject",
			mime: testMIMENoSubject,
		},
		{
			name: "MultipleRecipients",
			mime: testMIMEMultipleRecipients,
		},
		{
			name: "HTMLOnly",
			mime: func() []byte {
				return testemail.NewMessage().
					Subject("HTML Only").
					ContentType(`text/html; charset="utf-8"`).
					Body("<html><body><p>This is HTML only content.</p></body></html>").
					Bytes()
			},
		},
		{
			name: "DuplicateRecipients",
			mime: testMIMEDuplicateRecipients,
			check: func(t *testing.T, env *TestEnv) {
				assertRecipientCount(t, env.Store, "msg", "to", 2)
				assertRecipientCount(t, env.Store, "msg", "cc", 1)
				assertRecipientCount(t, env.Store, "msg", "bcc", 1)
				assertDisplayName(t, env.Store, "msg", "to", "duplicate@example.com", "Duplicate Person")
				assertDisplayName(t, env.Store, "msg", "cc", "cc-dup@example.com", "CC Duplicate")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newTestEnv(t)
			seedMessages(env, 1, 12345, "msg")
			raw := tt.mime()
			env.Mock.Messages["msg"].Raw = raw
			env.Mock.Messages["msg"].SizeEstimate = int64(len(raw))

			summary := runFullSync(t, env)
			assertSummary(t, summary, WantSummary{Added: intPtr(1), Errors: intPtr(0)})
			assertMessageCount(t, env.Store, 1)

			if tt.check != nil {
				tt.check(t, env)
			}
		})
	}
}

func TestFullSync_Latin1InFromName(t *testing.T) {
	env := newTestEnv(t)
	seedMessages(env, 1, 12345, "msg")

	// Build a MIME message with an RFC 2047 encoded From name that claims UTF-8
	// but actually contains Latin-1 bytes. This is a real-world scenario where a
	// sender's MUA mis-labels the charset, producing invalid UTF-8 after decoding.
	// The =C9 byte is Latin-1 É, which is not valid UTF-8 when surrounded by ASCII.
	raw := []byte("From: =?UTF-8?Q?Jane_Doe=C9ric?= <sender@example.com>\n" +
		"To: recipient@example.com\n" +
		"Subject: Test\n" +
		"Date: Mon, 01 Jan 2024 12:00:00 +0000\n" +
		"Content-Type: text/plain; charset=\"utf-8\"\n" +
		"\n" +
		"Body text.\n")

	env.Mock.Messages["msg"].Raw = raw
	env.Mock.Messages["msg"].SizeEstimate = int64(len(raw))

	summary := runFullSync(t, env)
	assertSummary(t, summary, WantSummary{Added: intPtr(1), Errors: intPtr(0)})

	// Verify the participant display_name in the participants table is valid UTF-8.
	// Before the fix, raw Latin-1 bytes would be stored as-is, causing DuckDB errors
	// when exporting to Parquet.
	displayName, err := env.Store.InspectParticipantDisplayName("sender@example.com")
	if err != nil {
		t.Fatalf("InspectParticipantDisplayName: %v", err)
	}
	// EnsureUTF8 should convert the Latin-1 \xC9 to the UTF-8 É (U+00C9)
	want := "Jane DoeÉric"
	if displayName != want {
		t.Errorf("participant display_name = %q, want %q", displayName, want)
	}

	// Also verify the message_recipients display_name is valid
	recipDisplayName, err := env.Store.InspectDisplayName("msg", "from", "sender@example.com")
	if err != nil {
		t.Fatalf("InspectDisplayName: %v", err)
	}
	if recipDisplayName != want {
		t.Errorf("recipient display_name = %q, want %q", recipDisplayName, want)
	}
}

func TestFullSync_InvalidUTF8InAllAddressFields(t *testing.T) {
	env := newTestEnv(t)
	seedMessages(env, 1, 12345, "msg")

	// Test that UTF-8 validation applies to all address fields (To, Cc, Bcc),
	// not just From. Uses Windows-1252 smart quotes (\x93, \x94) mis-labeled as UTF-8,
	// a common real-world scenario from Outlook emails.
	raw := []byte("From: =?UTF-8?Q?=93From=94_Name?= <from@example.com>\n" +
		"To: =?UTF-8?Q?=93To=94_Name?= <to@example.com>\n" +
		"Cc: =?UTF-8?Q?=93Cc=94_Name?= <cc@example.com>\n" +
		"Bcc: =?UTF-8?Q?=93Bcc=94_Name?= <bcc@example.com>\n" +
		"Subject: Test\n" +
		"Date: Mon, 01 Jan 2024 12:00:00 +0000\n" +
		"Content-Type: text/plain; charset=\"utf-8\"\n" +
		"\n" +
		"Body text.\n")

	env.Mock.Messages["msg"].Raw = raw
	env.Mock.Messages["msg"].SizeEstimate = int64(len(raw))

	summary := runFullSync(t, env)
	assertSummary(t, summary, WantSummary{Added: intPtr(1), Errors: intPtr(0)})

	// EnsureUTF8 should detect Windows-1252 and convert smart quotes to their
	// proper Unicode equivalents: \x93 → U+201C ("), \x94 → U+201D (")
	tests := []struct {
		recipType string
		email     string
	}{
		{"from", "from@example.com"},
		{"to", "to@example.com"},
		{"cc", "cc@example.com"},
		{"bcc", "bcc@example.com"},
	}
	for _, tt := range tests {
		// Verify participants table has valid UTF-8
		displayName, err := env.Store.InspectParticipantDisplayName(tt.email)
		if err != nil {
			t.Fatalf("InspectParticipantDisplayName(%s): %v", tt.email, err)
		}
		titled := strings.ToUpper(tt.recipType[:1]) + tt.recipType[1:]
		want := "\u201c" + titled + "\u201d Name"
		if displayName != want {
			t.Errorf("participant %s display_name = %q, want %q", tt.email, displayName, want)
		}

		// Verify message_recipients table has valid UTF-8
		recipName, err := env.Store.InspectDisplayName("msg", tt.recipType, tt.email)
		if err != nil {
			t.Fatalf("InspectDisplayName(%s, %s): %v", tt.recipType, tt.email, err)
		}
		if recipName != want {
			t.Errorf("recipient %s/%s display_name = %q, want %q", tt.recipType, tt.email, recipName, want)
		}
	}
}

func TestFullSync_InvalidUTF8InAttachmentFilename(t *testing.T) {
	env := newTestEnv(t)

	// Construct a MIME message with raw Latin-1 byte \xE9 (é) in the attachment
	// filename. Enmime sanitizes invalid bytes to U+FFFD before our code sees them;
	// the sync-level EnsureUTF8 call is defense-in-depth for any future parser changes.
	raw := []byte("From: sender@example.com\n" +
		"To: recipient@example.com\n" +
		"Subject: Attachment Test\n" +
		"Date: Mon, 01 Jan 2024 12:00:00 +0000\n" +
		"MIME-Version: 1.0\n" +
		"Content-Type: multipart/mixed; boundary=\"b\"\n" +
		"\n" +
		"--b\n" +
		"Content-Type: text/plain; charset=\"utf-8\"\n\nBody text.\n" +
		"--b\n" +
		"Content-Type: application/pdf; name=\"caf\xe9.pdf\"\n" +
		"Content-Disposition: attachment; filename=\"caf\xe9.pdf\"\n" +
		"Content-Transfer-Encoding: base64\n\n" +
		"SGVsbG8gV29ybGQh\n" +
		"--b--\n")

	env.Mock.Profile.MessagesTotal = 1
	env.Mock.Profile.HistoryID = 12345
	env.Mock.AddMessage("msg-attach", raw, []string{"INBOX"})

	withAttachmentsDir(t, env)

	summary := runFullSync(t, env)
	assertSummary(t, summary, WantSummary{Added: intPtr(1)})
	assertAttachmentCount(t, env.Store, 1)

	filename, mimeType, err := env.Store.InspectAttachment("msg-attach")
	if err != nil {
		t.Fatalf("InspectAttachment: %v", err)
	}

	// Enmime replaces the invalid \xE9 byte with U+FFFD (replacement character).
	// Our EnsureUTF8 would convert it to the proper é if enmime didn't sanitize first.
	// Either way, the stored filename must be valid UTF-8.
	wantFilename := "caf\uFFFD.pdf"
	if filename != wantFilename {
		t.Errorf("attachment filename = %q, want %q", filename, wantFilename)
	}

	// Content-type should be the clean base MIME type
	if mimeType != "application/pdf" {
		t.Errorf("attachment mime_type = %q, want %q", mimeType, "application/pdf")
	}
}

func TestFullSync_MultipleEncodingIssuesSameMessage(t *testing.T) {
	env := newTestEnv(t)
	seedMessages(env, 1, 12345, "msg")

	// Real-world scenario: a single email with multiple encoding issues.
	// Latin-1 É (\xC9) in From name, and Windows-1252 smart quote (\x93) in To name.
	raw := []byte("From: =?UTF-8?Q?Doe=C9ric?= <from@example.com>\n" +
		"To: =?UTF-8?Q?=93Quoted=94?= <to@example.com>\n" +
		"Subject: =?UTF-8?Q?Caf=E9?=\n" +
		"Date: Mon, 01 Jan 2024 12:00:00 +0000\n" +
		"Content-Type: text/plain; charset=\"utf-8\"\n" +
		"\n" +
		"Body text.\n")

	env.Mock.Messages["msg"].Raw = raw
	env.Mock.Messages["msg"].SizeEstimate = int64(len(raw))

	summary := runFullSync(t, env)
	assertSummary(t, summary, WantSummary{Added: intPtr(1), Errors: intPtr(0)})

	// From name: Latin-1 \xC9 → UTF-8 É
	fromName, err := env.Store.InspectParticipantDisplayName("from@example.com")
	if err != nil {
		t.Fatalf("InspectParticipantDisplayName(from): %v", err)
	}
	if fromName != "DoeÉric" {
		t.Errorf("from display_name = %q, want %q", fromName, "DoeÉric")
	}

	// To name: Windows-1252 \x93/\x94 → Unicode left/right double quotes
	toName, err := env.Store.InspectParticipantDisplayName("to@example.com")
	if err != nil {
		t.Fatalf("InspectParticipantDisplayName(to): %v", err)
	}
	if toName != "\u201cQuoted\u201d" {
		t.Errorf("to display_name = %q, want %q", toName, "\u201cQuoted\u201d")
	}

	// Subject: Latin-1 \xE9 → UTF-8 é (already validated by existing code path)
	insp, err := env.Store.InspectMessage("msg")
	if err != nil {
		t.Fatalf("InspectMessage: %v", err)
	}
	if !strings.Contains(insp.RecipientDisplayName["from:from@example.com"], "É") {
		t.Errorf("from recipient display_name %q should contain É", insp.RecipientDisplayName["from:from@example.com"])
	}
}

func TestFullSyncWithMIMEParseError(t *testing.T) {
	env := newTestEnv(t)
	env.Mock.Profile.MessagesTotal = 2
	env.Mock.Profile.HistoryID = 12345
	env.Mock.AddMessage("msg-good", testMIME(), []string{"INBOX"})
	env.Mock.Messages["msg-bad"] = &gmail.RawMessage{
		ID:           "msg-bad",
		ThreadID:     "thread_msg-bad",
		LabelIDs:     []string{"INBOX"},
		Raw:          []byte("not valid mime at all - just garbage"),
		Snippet:      "This is the snippet preview",
		SizeEstimate: 100,
	}

	summary := runFullSync(t, env)
	assertSummary(t, summary, WantSummary{Added: intPtr(2), Errors: intPtr(0)})

	// Verify the bad message was stored with placeholder content
	assertBodyContains(t, env.Store, "msg-bad", "MIME parsing failed")
	assertRawDataExists(t, env.Store, "msg-bad")
}

func TestFullSyncMessageFetchError(t *testing.T) {
	env := newTestEnv(t)
	env.Mock.Profile.MessagesTotal = 2
	env.Mock.Profile.HistoryID = 12345
	env.Mock.AddMessage("msg-good", testMIME(), []string{"INBOX"})

	env.Mock.MessagePages = [][]string{{"msg-good", "msg-missing"}}

	summary := runFullSync(t, env)
	assertSummary(t, summary, WantSummary{Added: intPtr(1)})
}

func TestIncrementalSyncLabelsError(t *testing.T) {
	env := newTestEnv(t)
	env.CreateSourceWithHistory(t, "12340")

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
	env.Mock.Profile.HistoryID = 12345
	seedPagedMessages(env, 4, 2, "msg")

	source := env.CreateSource(t)

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
	assertSummary(t, summary, WantSummary{Added: intPtr(4)})

	assertListMessagesCalls(t, env, 1)
	assertMessageCount(t, env.Store, 4)
}

func TestFullSyncDateFallbackToInternalDate(t *testing.T) {
	env := newTestEnv(t)

	badDateMIME := testemail.NewMessage().
		Subject("Bad Date").
		Date("This is not a valid date").
		Body("Message with invalid date header.").
		Bytes()

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

	assertDateFallback(t, env.Store, "msg-bad-date", "2024-01-15", "12:00:00")
}

func TestFullSyncEmptyRawMIME(t *testing.T) {
	env := newTestEnv(t)
	env.Mock.Profile.MessagesTotal = 2
	env.Mock.Profile.HistoryID = 12345

	env.Mock.AddMessage("msg-good", testMIME(), []string{"INBOX"})
	env.Mock.Messages["msg-empty-raw"] = &gmail.RawMessage{
		ID:           "msg-empty-raw",
		ThreadID:     "thread-empty-raw",
		LabelIDs:     []string{"INBOX"},
		Raw:          []byte{},
		SizeEstimate: 0,
	}

	summary := runFullSync(t, env)
	assertSummary(t, summary, WantSummary{Added: intPtr(1), Errors: intPtr(1)})
}

func TestFullSyncEmptyThreadID(t *testing.T) {
	env := newTestEnv(t)
	env.Mock.Profile.MessagesTotal = 1
	env.Mock.Profile.HistoryID = 12345
	env.Mock.UseRawThreadID = true

	raw := testMIME()
	env.Mock.Messages["msg-no-thread"] = &gmail.RawMessage{
		ID:           "msg-no-thread",
		ThreadID:     "",
		LabelIDs:     []string{"INBOX"},
		Raw:          raw,
		SizeEstimate: int64(len(raw)),
	}
	env.Mock.MessagePages = [][]string{{"msg-no-thread"}}

	summary := runFullSync(t, env)
	assertSummary(t, summary, WantSummary{Added: intPtr(1), Errors: intPtr(0)})

	assertThreadSourceID(t, env.Store, "msg-no-thread", "msg-no-thread")
}

func TestFullSyncListEmptyThreadIDRawPresent(t *testing.T) {
	env := newTestEnv(t)
	env.Mock.Profile.MessagesTotal = 1
	env.Mock.Profile.HistoryID = 12345

	raw := testMIME()
	env.Mock.ListThreadIDOverride = map[string]string{
		"msg-list-empty": "",
	}
	env.Mock.Messages["msg-list-empty"] = &gmail.RawMessage{
		ID:           "msg-list-empty",
		ThreadID:     "actual-thread-from-raw",
		LabelIDs:     []string{"INBOX"},
		Raw:          raw,
		SizeEstimate: int64(len(raw)),
	}
	env.Mock.MessagePages = [][]string{{"msg-list-empty"}}

	summary := runFullSync(t, env)
	assertSummary(t, summary, WantSummary{Added: intPtr(1), Errors: intPtr(0)})

	assertThreadSourceID(t, env.Store, "msg-list-empty", "actual-thread-from-raw")
}

// Tests for initSyncState

func TestInitSyncState_NewSync(t *testing.T) {
	env := newTestEnv(t)
	source := env.CreateSource(t)

	state, err := env.Syncer.initSyncState(source.ID)
	if err != nil {
		t.Fatalf("initSyncState: %v", err)
	}

	if state.wasResumed {
		t.Error("expected wasResumed = false for new sync")
	}
	if state.pageToken != "" {
		t.Errorf("expected empty pageToken, got %q", state.pageToken)
	}
	if state.syncID == 0 {
		t.Error("expected non-zero syncID")
	}
	if state.checkpoint.MessagesProcessed != 0 {
		t.Errorf("expected MessagesProcessed = 0, got %d", state.checkpoint.MessagesProcessed)
	}
}

func TestInitSyncState_Resume(t *testing.T) {
	env := newTestEnv(t)
	source := env.CreateSource(t)

	// Create an active sync with checkpoint
	syncID, err := env.Store.StartSync(source.ID, "full")
	if err != nil {
		t.Fatalf("StartSync: %v", err)
	}
	checkpoint := &store.Checkpoint{
		PageToken:         "resume_token_123",
		MessagesProcessed: 50,
		MessagesAdded:     45,
		MessagesUpdated:   3,
		ErrorsCount:       2,
	}
	if err := env.Store.UpdateSyncCheckpoint(syncID, checkpoint); err != nil {
		t.Fatalf("UpdateSyncCheckpoint: %v", err)
	}

	state, err := env.Syncer.initSyncState(source.ID)
	if err != nil {
		t.Fatalf("initSyncState: %v", err)
	}

	if !state.wasResumed {
		t.Error("expected wasResumed = true")
	}
	if state.pageToken != "resume_token_123" {
		t.Errorf("expected pageToken = 'resume_token_123', got %q", state.pageToken)
	}
	if state.syncID != syncID {
		t.Errorf("expected syncID = %d, got %d", syncID, state.syncID)
	}
	if state.checkpoint.MessagesProcessed != 50 {
		t.Errorf("expected MessagesProcessed = 50, got %d", state.checkpoint.MessagesProcessed)
	}
	if state.checkpoint.MessagesAdded != 45 {
		t.Errorf("expected MessagesAdded = 45, got %d", state.checkpoint.MessagesAdded)
	}
}

func TestInitSyncState_NoResumeOption(t *testing.T) {
	env := newTestEnv(t)
	env.SetOptions(t, func(o *Options) {
		o.NoResume = true
	})
	source := env.CreateSource(t)

	// Create an active sync with checkpoint
	syncID, err := env.Store.StartSync(source.ID, "full")
	if err != nil {
		t.Fatalf("StartSync: %v", err)
	}
	checkpoint := &store.Checkpoint{
		PageToken:         "resume_token_123",
		MessagesProcessed: 50,
	}
	if err := env.Store.UpdateSyncCheckpoint(syncID, checkpoint); err != nil {
		t.Fatalf("UpdateSyncCheckpoint: %v", err)
	}

	state, err := env.Syncer.initSyncState(source.ID)
	if err != nil {
		t.Fatalf("initSyncState: %v", err)
	}

	if state.wasResumed {
		t.Error("expected wasResumed = false with NoResume option")
	}
	if state.pageToken != "" {
		t.Errorf("expected empty pageToken with NoResume, got %q", state.pageToken)
	}
	if state.syncID == syncID {
		t.Error("expected new syncID, not the existing one")
	}
}

// Tests for processBatch

func TestProcessBatch_EmptyBatch(t *testing.T) {
	env := newTestEnv(t)
	source := env.CreateSource(t)
	labelMap := make(map[string]int64)
	checkpoint := &store.Checkpoint{}
	summary := &gmail.SyncSummary{}

	listResp := &gmail.MessageListResponse{
		Messages: nil,
	}

	result, err := env.Syncer.processBatch(env.Context, source.ID, listResp, labelMap, checkpoint, summary)
	if err != nil {
		t.Fatalf("processBatch: %v", err)
	}

	if result.processed != 0 {
		t.Errorf("expected processed = 0, got %d", result.processed)
	}
	if result.added != 0 {
		t.Errorf("expected added = 0, got %d", result.added)
	}
	if result.skipped != 0 {
		t.Errorf("expected skipped = 0, got %d", result.skipped)
	}
}

func TestProcessBatch_AllNew(t *testing.T) {
	env := newTestEnv(t)
	source := env.CreateSource(t)
	labelMap, _ := env.Store.EnsureLabelsBatch(source.ID, map[string]store.LabelInfo{
		"INBOX": {Name: "Inbox", Type: "system"},
	})
	checkpoint := &store.Checkpoint{}
	summary := &gmail.SyncSummary{}

	env.Mock.AddMessage("msg1", testMIME(), []string{"INBOX"})
	env.Mock.AddMessage("msg2", testMIME(), []string{"INBOX"})

	listResp := &gmail.MessageListResponse{
		Messages: []gmail.MessageID{
			{ID: "msg1", ThreadID: "thread1"},
			{ID: "msg2", ThreadID: "thread2"},
		},
	}

	result, err := env.Syncer.processBatch(env.Context, source.ID, listResp, labelMap, checkpoint, summary)
	if err != nil {
		t.Fatalf("processBatch: %v", err)
	}

	if result.processed != 2 {
		t.Errorf("expected processed = 2, got %d", result.processed)
	}
	if result.added != 2 {
		t.Errorf("expected added = 2, got %d", result.added)
	}
	if result.skipped != 0 {
		t.Errorf("expected skipped = 0, got %d", result.skipped)
	}
}

func TestProcessBatch_AllExisting(t *testing.T) {
	env := newTestEnv(t)
	seedMessages(env, 2, 12345, "msg1", "msg2")

	// First sync to add messages
	runFullSync(t, env)

	source, _ := env.Store.GetOrCreateSource("gmail", testEmail)
	labelMap, _ := env.Store.EnsureLabelsBatch(source.ID, map[string]store.LabelInfo{
		"INBOX": {Name: "Inbox", Type: "system"},
	})
	checkpoint := &store.Checkpoint{}
	summary := &gmail.SyncSummary{}

	listResp := &gmail.MessageListResponse{
		Messages: []gmail.MessageID{
			{ID: "msg1", ThreadID: "thread1"},
			{ID: "msg2", ThreadID: "thread2"},
		},
	}

	result, err := env.Syncer.processBatch(env.Context, source.ID, listResp, labelMap, checkpoint, summary)
	if err != nil {
		t.Fatalf("processBatch: %v", err)
	}

	if result.processed != 2 {
		t.Errorf("expected processed = 2, got %d", result.processed)
	}
	if result.added != 0 {
		t.Errorf("expected added = 0 (all existing), got %d", result.added)
	}
	if result.skipped != 2 {
		t.Errorf("expected skipped = 2, got %d", result.skipped)
	}
}

func TestProcessBatch_MixedNewAndExisting(t *testing.T) {
	env := newTestEnv(t)
	seedMessages(env, 1, 12345, "msg1")

	// First sync to add msg1
	runFullSync(t, env)

	source, _ := env.Store.GetOrCreateSource("gmail", testEmail)
	labelMap, _ := env.Store.EnsureLabelsBatch(source.ID, map[string]store.LabelInfo{
		"INBOX": {Name: "Inbox", Type: "system"},
	})
	checkpoint := &store.Checkpoint{}
	summary := &gmail.SyncSummary{}

	// Add msg2 to mock
	env.Mock.AddMessage("msg2", testMIME(), []string{"INBOX"})

	listResp := &gmail.MessageListResponse{
		Messages: []gmail.MessageID{
			{ID: "msg1", ThreadID: "thread1"},
			{ID: "msg2", ThreadID: "thread2"},
		},
	}

	result, err := env.Syncer.processBatch(env.Context, source.ID, listResp, labelMap, checkpoint, summary)
	if err != nil {
		t.Fatalf("processBatch: %v", err)
	}

	if result.processed != 2 {
		t.Errorf("expected processed = 2, got %d", result.processed)
	}
	if result.added != 1 {
		t.Errorf("expected added = 1, got %d", result.added)
	}
	if result.skipped != 1 {
		t.Errorf("expected skipped = 1, got %d", result.skipped)
	}
}

func TestProcessBatch_OldestDatePropagation(t *testing.T) {
	env := newTestEnv(t)
	source := env.CreateSource(t)
	labelMap, _ := env.Store.EnsureLabelsBatch(source.ID, map[string]store.LabelInfo{
		"INBOX": {Name: "Inbox", Type: "system"},
	})
	checkpoint := &store.Checkpoint{}
	summary := &gmail.SyncSummary{}

	// Add messages with specific internal dates
	// msg1: Jan 15, 2024, msg2: Jan 10, 2024 (older)
	env.Mock.Messages["msg1"] = &gmail.RawMessage{
		ID:           "msg1",
		ThreadID:     "thread1",
		LabelIDs:     []string{"INBOX"},
		Raw:          testMIME(),
		InternalDate: 1705320000000, // 2024-01-15T12:00:00Z
	}
	env.Mock.Messages["msg2"] = &gmail.RawMessage{
		ID:           "msg2",
		ThreadID:     "thread2",
		LabelIDs:     []string{"INBOX"},
		Raw:          testMIME(),
		InternalDate: 1704888000000, // 2024-01-10T12:00:00Z
	}

	listResp := &gmail.MessageListResponse{
		Messages: []gmail.MessageID{
			{ID: "msg1", ThreadID: "thread1"},
			{ID: "msg2", ThreadID: "thread2"},
		},
	}

	result, err := env.Syncer.processBatch(env.Context, source.ID, listResp, labelMap, checkpoint, summary)
	if err != nil {
		t.Fatalf("processBatch: %v", err)
	}

	// oldestDate should be Jan 10, 2024
	if result.oldestDate.IsZero() {
		t.Error("expected oldestDate to be set")
	}
	expectedYear, expectedMonth, expectedDay := 2024, 1, 10
	gotYear, gotMonth, gotDay := result.oldestDate.Year(), int(result.oldestDate.Month()), result.oldestDate.Day()
	if gotYear != expectedYear || gotMonth != expectedMonth || gotDay != expectedDay {
		t.Errorf("expected oldestDate = 2024-01-10, got %d-%02d-%02d", gotYear, gotMonth, gotDay)
	}
}

func TestProcessBatch_ErrorsCount(t *testing.T) {
	env := newTestEnv(t)
	source := env.CreateSource(t)
	labelMap, _ := env.Store.EnsureLabelsBatch(source.ID, map[string]store.LabelInfo{
		"INBOX": {Name: "Inbox", Type: "system"},
	})
	checkpoint := &store.Checkpoint{}
	summary := &gmail.SyncSummary{}

	env.Mock.AddMessage("msg1", testMIME(), []string{"INBOX"})
	// msg2 will return nil (simulating fetch failure)
	env.Mock.GetMessageError["msg2"] = &gmail.NotFoundError{Path: "/messages/msg2"}

	listResp := &gmail.MessageListResponse{
		Messages: []gmail.MessageID{
			{ID: "msg1", ThreadID: "thread1"},
			{ID: "msg2", ThreadID: "thread2"},
		},
	}

	result, err := env.Syncer.processBatch(env.Context, source.ID, listResp, labelMap, checkpoint, summary)
	if err != nil {
		t.Fatalf("processBatch: %v", err)
	}

	if result.added != 1 {
		t.Errorf("expected added = 1, got %d", result.added)
	}
	if checkpoint.ErrorsCount != 1 {
		t.Errorf("expected ErrorsCount = 1, got %d", checkpoint.ErrorsCount)
	}
}

// TestAttachmentFilePermissions verifies that attachment files are saved with
// restrictive permissions (0600) to protect email content.
func TestAttachmentFilePermissions(t *testing.T) {
	env := newTestEnv(t)
	env.Mock.Profile.MessagesTotal = 1
	env.Mock.Profile.HistoryID = 12345
	env.Mock.AddMessage("msg-with-attachment", testMIMEWithAttachment(), []string{"INBOX"})

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
	// Windows does not support Unix permissions.
	if runtime.GOOS != "windows" {
		got := info.Mode().Perm()
		want := os.FileMode(0600)
		if got != want {
			t.Errorf("attachment file permissions = %04o, want %04o", got, want)
		}
	}
}
