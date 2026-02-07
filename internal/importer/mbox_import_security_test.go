package importer

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/wesm/msgvault/internal/mime"
	"github.com/wesm/msgvault/internal/store"
)

func TestStoreAttachment_InvalidContentHash_ReturnsError(t *testing.T) {
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

	att := &mime.Attachment{
		Filename:    "a.txt",
		ContentType: "text/plain",
		Content:     []byte("hi"),
		ContentHash: "a", // malformed
		Size:        2,
	}

	err = storeAttachment(st, attachmentsDir, 1, att)
	if err == nil {
		t.Fatalf("expected error")
	}

	// Ensure nothing was written.
	if _, statErr := os.Stat(attachmentsDir); statErr == nil {
		t.Fatalf("attachments dir should not have been created for invalid content hash")
	}
}
