package cmd

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wesm/msgvault/internal/config"
	"github.com/wesm/msgvault/internal/store"
	"github.com/wesm/msgvault/internal/testutil/email"
)

func TestImportMboxCmd_EndToEnd_MboxFile(t *testing.T) {
	tmp := t.TempDir()

	// Save/restore global state for cmd package.
	prevCfg := cfg
	prevLogger := logger
	prevSourceType := importMboxSourceType
	prevLabel := importMboxLabel
	prevNoResume := importMboxNoResume
	prevCheckpointInterval := importMboxCheckpointInterval
	prevNoAttachments := importMboxNoAttachments
	t.Cleanup(func() {
		cfg = prevCfg
		logger = prevLogger
		importMboxSourceType = prevSourceType
		importMboxLabel = prevLabel
		importMboxNoResume = prevNoResume
		importMboxCheckpointInterval = prevCheckpointInterval
		importMboxNoAttachments = prevNoAttachments
	})

	cfg = &config.Config{
		HomeDir: tmp,
		Data: config.DataConfig{
			DataDir: tmp,
		},
	}
	logger = slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))

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

	var mbox strings.Builder
	mbox.WriteString("From alice@example.com Mon Jan 1 12:00:00 2024\n")
	mbox.Write(raw1)
	if !strings.HasSuffix(string(raw1), "\n") {
		mbox.WriteString("\n")
	}
	mbox.WriteString("From bob@example.com Mon Jan 1 13:00:00 2024\n")
	mbox.Write(raw2)
	if !strings.HasSuffix(string(raw2), "\n") {
		mbox.WriteString("\n")
	}

	mboxPath := filepath.Join(tmp, "export.mbox")
	if err := os.WriteFile(mboxPath, []byte(mbox.String()), 0600); err != nil {
		t.Fatalf("write mbox: %v", err)
	}

	importMboxSourceType = "hey"
	importMboxLabel = "hey"
	importMboxNoResume = true
	importMboxCheckpointInterval = 1
	importMboxNoAttachments = false

	importMboxCmd.SetContext(context.Background())
	if err := importMboxCmd.RunE(importMboxCmd, []string{"me@hey.com", mboxPath}); err != nil {
		t.Fatalf("import-mbox: %v", err)
	}

	st, err := store.Open(cfg.DatabaseDSN())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	var (
		sourceCount     int
		messageCount    int
		attachmentCount int
	)
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM sources WHERE source_type = 'hey' AND identifier = 'me@hey.com'`).Scan(&sourceCount); err != nil {
		t.Fatalf("count sources: %v", err)
	}
	if sourceCount != 1 {
		t.Fatalf("sourceCount = %d, want 1", sourceCount)
	}
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM messages`).Scan(&messageCount); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	if messageCount != 2 {
		t.Fatalf("messageCount = %d, want 2", messageCount)
	}
	if err := st.DB().QueryRow(`SELECT COUNT(*) FROM attachments`).Scan(&attachmentCount); err != nil {
		t.Fatalf("count attachments: %v", err)
	}
	if attachmentCount != 1 {
		t.Fatalf("attachmentCount = %d, want 1", attachmentCount)
	}

	var storagePath string
	if err := st.DB().QueryRow(`SELECT storage_path FROM attachments LIMIT 1`).Scan(&storagePath); err != nil {
		t.Fatalf("select storage_path: %v", err)
	}
	if storagePath == "" {
		t.Fatalf("storage_path empty")
	}
	if _, err := os.Stat(filepath.Join(cfg.AttachmentsDir(), storagePath)); err != nil {
		t.Fatalf("attachment file missing: %v", err)
	}
}
