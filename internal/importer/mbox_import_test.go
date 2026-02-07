package importer

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wesm/msgvault/internal/store"
	"github.com/wesm/msgvault/internal/testutil/email"
)

func TestImportMbox_IngestsMessagesThreadsAndAttachments(t *testing.T) {
	tmp := t.TempDir()

	dbPath := filepath.Join(tmp, "msgvault.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	attachmentsDir := filepath.Join(tmp, "attachments")

	raw1 := email.NewMessage().
		From("Alice <alice@example.com>").
		To("Bob <bob@example.com>").
		Subject("Hello").
		Date("Mon, 01 Jan 2024 12:00:00 +0000").
		Header("Message-ID", "<msg1@example.com>").
		Body("Hi Bob.\n").
		WithAttachment("a.txt", "text/plain", []byte("hello")).
		Bytes()

	raw2 := email.NewMessage().
		From("Bob <bob@example.com>").
		To("Alice <alice@example.com>").
		Subject("Re: Hello").
		Date("Mon, 01 Jan 2024 13:00:00 +0000").
		Header("Message-ID", "<msg2@example.com>").
		Header("In-Reply-To", "<msg1@example.com>").
		Header("References", "<msg1@example.com>").
		Body("Reply.\n").
		Bytes()

	mboxData := strings.Builder{}
	mboxData.WriteString("From alice@example.com Mon Jan 1 12:00:00 2024\n")
	mboxData.Write(raw1)
	if !strings.HasSuffix(string(raw1), "\n") {
		mboxData.WriteString("\n")
	}
	mboxData.WriteString("From bob@example.com Mon Jan 1 13:00:00 2024\n")
	mboxData.Write(raw2)
	if !strings.HasSuffix(string(raw2), "\n") {
		mboxData.WriteString("\n")
	}

	mboxPath := filepath.Join(tmp, "hey-export.mbox")
	if err := os.WriteFile(mboxPath, []byte(mboxData.String()), 0600); err != nil {
		t.Fatalf("write mbox: %v", err)
	}

	summary, err := ImportMbox(context.Background(), st, mboxPath, MboxImportOptions{
		SourceType:         "hey",
		Identifier:         "me@hey.com",
		Label:              "hey",
		NoResume:           true,
		CheckpointInterval: 1,
		AttachmentsDir:     attachmentsDir,
	})
	if err != nil {
		t.Fatalf("ImportMbox: %v", err)
	}
	if summary.MessagesAdded != 2 {
		t.Fatalf("MessagesAdded = %d, want 2", summary.MessagesAdded)
	}

	// Verify counts.
	var (
		sourceCount       int
		conversationCount int
		messageCount      int
		rawCount          int
		labelCount        int
		msgLabelCount     int
		attachmentCount   int
	)
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM sources WHERE source_type = 'hey' AND identifier = 'me@hey.com'`).Scan(&sourceCount); err != nil {
		t.Fatalf("count sources: %v", err)
	}
	if sourceCount != 1 {
		t.Fatalf("sourceCount = %d, want 1", sourceCount)
	}
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM conversations`).Scan(&conversationCount); err != nil {
		t.Fatalf("count conversations: %v", err)
	}
	if conversationCount != 1 {
		t.Fatalf("conversationCount = %d, want 1", conversationCount)
	}
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM messages`).Scan(&messageCount); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	if messageCount != 2 {
		t.Fatalf("messageCount = %d, want 2", messageCount)
	}
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM message_raw`).Scan(&rawCount); err != nil {
		t.Fatalf("count message_raw: %v", err)
	}
	if rawCount != 2 {
		t.Fatalf("rawCount = %d, want 2", rawCount)
	}
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM labels WHERE name = 'hey'`).Scan(&labelCount); err != nil {
		t.Fatalf("count labels: %v", err)
	}
	if labelCount != 1 {
		t.Fatalf("labelCount = %d, want 1", labelCount)
	}
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM message_labels`).Scan(&msgLabelCount); err != nil {
		t.Fatalf("count message_labels: %v", err)
	}
	if msgLabelCount != 2 {
		t.Fatalf("msgLabelCount = %d, want 2", msgLabelCount)
	}
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM attachments`).Scan(&attachmentCount); err != nil {
		t.Fatalf("count attachments: %v", err)
	}
	if attachmentCount != 1 {
		t.Fatalf("attachmentCount = %d, want 1", attachmentCount)
	}

	// Verify attachment file exists.
	var storagePath string
	if err := st.DB().QueryRow(`SELECT storage_path FROM attachments LIMIT 1`).Scan(&storagePath); err != nil {
		t.Fatalf("select storage_path: %v", err)
	}
	if storagePath == "" {
		t.Fatalf("storage_path empty")
	}
	if _, err := os.Stat(filepath.Join(attachmentsDir, storagePath)); err != nil {
		t.Fatalf("attachment file missing: %v", err)
	}
}
