package emlx

import (
	"os"
	"path/filepath"
	"testing"
)

// mkMailbox creates a mock Apple Mail mailbox structure.
func mkMailbox(t *testing.T, base string, emlxFiles ...string) {
	t.Helper()
	msgDir := filepath.Join(base, "Messages")
	if err := os.MkdirAll(msgDir, 0700); err != nil {
		t.Fatalf("mkdir %q: %v", msgDir, err)
	}
	for _, name := range emlxFiles {
		data := "10\nFrom: x\r\n\r\n"
		path := filepath.Join(msgDir, name)
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatalf("write %q: %v", path, err)
		}
	}
}

func TestDiscoverMailboxes_SingleMailbox(t *testing.T) {
	root := t.TempDir()
	mboxDir := filepath.Join(root, "INBOX.mbox")
	mkMailbox(t, mboxDir, "1.emlx", "2.emlx")

	// Pass the .mbox directory itself.
	mailboxes, err := DiscoverMailboxes(mboxDir)
	if err != nil {
		t.Fatalf("DiscoverMailboxes: %v", err)
	}
	if len(mailboxes) != 1 {
		t.Fatalf("got %d mailboxes, want 1", len(mailboxes))
	}
	if mailboxes[0].Label != "INBOX" {
		t.Fatalf("Label = %q, want %q", mailboxes[0].Label, "INBOX")
	}
	if len(mailboxes[0].Files) != 2 {
		t.Fatalf("Files = %d, want 2", len(mailboxes[0].Files))
	}
}

func TestDiscoverMailboxes_RecursiveWalk(t *testing.T) {
	root := t.TempDir()
	mail := filepath.Join(root, "Mail")

	mkMailbox(t, filepath.Join(mail, "Mailboxes", "Classes", "Accardi.mbox"), "1.emlx")
	mkMailbox(t, filepath.Join(mail, "Mailboxes", "Sent.mbox"), "1.emlx", "2.emlx")
	mkMailbox(t, filepath.Join(mail, "IMAP-wesm@po14.mit.edu", "INBOX.imapmbox"), "1.emlx")
	mkMailbox(t, filepath.Join(mail, "POP-wesmckinn@pop.gmail.com", "Sent Messages.mbox"), "1.emlx")

	mailboxes, err := DiscoverMailboxes(mail)
	if err != nil {
		t.Fatalf("DiscoverMailboxes: %v", err)
	}
	if len(mailboxes) != 4 {
		t.Fatalf("got %d mailboxes, want 4", len(mailboxes))
	}

	labels := make(map[string]int)
	for _, mb := range mailboxes {
		labels[mb.Label] = len(mb.Files)
	}

	tests := []struct {
		label     string
		wantFiles int
	}{
		{"Classes/Accardi", 1},
		{"Sent", 2},
		{"INBOX", 1},
		{"Sent Messages", 1},
	}
	for _, tc := range tests {
		n, ok := labels[tc.label]
		if !ok {
			t.Errorf("missing label %q (have: %v)", tc.label, labels)
			continue
		}
		if n != tc.wantFiles {
			t.Errorf(
				"label %q: files = %d, want %d",
				tc.label, n, tc.wantFiles,
			)
		}
	}
}

func TestDiscoverMailboxes_EmptyMailbox(t *testing.T) {
	root := t.TempDir()
	mboxDir := filepath.Join(root, "Empty.mbox")
	// Create Messages/ but no .emlx files.
	if err := os.MkdirAll(
		filepath.Join(mboxDir, "Messages"), 0700,
	); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	mailboxes, err := DiscoverMailboxes(root)
	if err != nil {
		t.Fatalf("DiscoverMailboxes: %v", err)
	}
	if len(mailboxes) != 0 {
		t.Fatalf("got %d mailboxes, want 0", len(mailboxes))
	}
}

func TestDiscoverMailboxes_PartialEmlxSkipped(t *testing.T) {
	root := t.TempDir()
	mboxDir := filepath.Join(root, "Test.mbox")
	mkMailbox(t, mboxDir, "1.emlx", "2.partial.emlx")

	mailboxes, err := DiscoverMailboxes(mboxDir)
	if err != nil {
		t.Fatalf("DiscoverMailboxes: %v", err)
	}
	if len(mailboxes) != 1 {
		t.Fatalf("got %d mailboxes, want 1", len(mailboxes))
	}
	if len(mailboxes[0].Files) != 1 {
		t.Fatalf("Files = %d, want 1 (partial should be skipped)",
			len(mailboxes[0].Files))
	}
	if mailboxes[0].Files[0] != "1.emlx" {
		t.Fatalf("Files[0] = %q, want %q", mailboxes[0].Files[0], "1.emlx")
	}
}

func TestDiscoverMailboxes_NotADirectory(t *testing.T) {
	tmp := t.TempDir()
	file := filepath.Join(tmp, "notdir")
	if err := os.WriteFile(file, []byte("x"), 0600); err != nil {
		t.Fatalf("write: %v", err)
	}

	_, err := DiscoverMailboxes(file)
	if err == nil {
		t.Fatalf("expected error for non-directory")
	}
}

func TestLabelFromPath(t *testing.T) {
	tests := []struct {
		root string
		path string
		want string
	}{
		{
			"/Mail",
			"/Mail/Mailboxes/Classes/Accardi.mbox",
			"Classes/Accardi",
		},
		{
			"/Mail",
			"/Mail/IMAP-wesm@po14.mit.edu/INBOX.imapmbox",
			"INBOX",
		},
		{
			"/Mail",
			"/Mail/POP-wesmckinn@pop.gmail.com/Sent Messages.mbox",
			"Sent Messages",
		},
		{
			"/Mail",
			"/Mail/Mailboxes/Sent.mbox",
			"Sent",
		},
		{
			"/Mail",
			"/Mail/INBOX.mbox",
			"INBOX",
		},
	}
	for _, tc := range tests {
		got := LabelFromPath(tc.root, tc.path)
		if got != tc.want {
			t.Errorf(
				"LabelFromPath(%q, %q) = %q, want %q",
				tc.root, tc.path, got, tc.want,
			)
		}
	}
}

func TestDiscoverMailboxes_FilesSorted(t *testing.T) {
	root := t.TempDir()
	mboxDir := filepath.Join(root, "Test.mbox")
	mkMailbox(t, mboxDir, "300.emlx", "10.emlx", "2.emlx", "1.emlx")

	mailboxes, err := DiscoverMailboxes(mboxDir)
	if err != nil {
		t.Fatalf("DiscoverMailboxes: %v", err)
	}
	if len(mailboxes) != 1 {
		t.Fatalf("got %d mailboxes, want 1", len(mailboxes))
	}

	want := []string{"1.emlx", "10.emlx", "2.emlx", "300.emlx"}
	files := mailboxes[0].Files
	if len(files) != len(want) {
		t.Fatalf("files = %v, want %v", files, want)
	}
	for i := range want {
		if files[i] != want[i] {
			t.Fatalf("files[%d] = %q, want %q", i, files[i], want[i])
		}
	}
}
