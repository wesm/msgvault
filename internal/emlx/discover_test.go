package emlx

import (
	"os"
	"path/filepath"
	"testing"
)

// mkMailbox creates a mock legacy Apple Mail mailbox structure
// with a direct Messages/ subdirectory.
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

// mkV10Mailbox creates a modern V10-style mailbox structure
// with <GUID>/Data/Messages/ layout.
func mkV10Mailbox(
	t *testing.T, base, guid string, emlxFiles ...string,
) {
	t.Helper()
	msgDir := filepath.Join(base, guid, "Data", "Messages")
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
	if filepath.Base(mailboxes[0].Files[0]) != "1.emlx" {
		t.Fatalf("Files[0] basename = %q, want %q",
			filepath.Base(mailboxes[0].Files[0]), "1.emlx")
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

func TestDiscoverMailboxes_NestedMbox(t *testing.T) {
	root := t.TempDir()
	mail := filepath.Join(root, "Mail")

	// Parent .mbox contains a child .mbox inside it.
	// Apple Mail sometimes nests mailboxes this way.
	mkMailbox(t, filepath.Join(mail, "Parent.mbox"), "1.emlx")
	mkMailbox(t, filepath.Join(mail, "Parent.mbox", "Child.mbox"), "1.emlx")

	mailboxes, err := DiscoverMailboxes(mail)
	if err != nil {
		t.Fatalf("DiscoverMailboxes: %v", err)
	}
	if len(mailboxes) != 2 {
		t.Fatalf("got %d mailboxes, want 2", len(mailboxes))
	}

	labels := make(map[string]bool)
	for _, mb := range mailboxes {
		labels[mb.Label] = true
	}
	if !labels["Parent"] {
		t.Errorf("missing Parent label, have: %v", labels)
	}
	if !labels["Parent/Child"] {
		t.Errorf("missing Parent/Child label, have: %v", labels)
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
		// V10 account GUID stripped from labels.
		{
			"/Mail/V10",
			"/Mail/V10/13C9A646-EE0A-4698-B5A2-E07FFBDDEED3/INBOX.mbox",
			"INBOX",
		},
		{
			"/Mail/V10",
			"/Mail/V10/13C9A646-EE0A-4698-B5A2-E07FFBDDEED3/Sent Messages.mbox",
			"Sent Messages",
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

	wantNames := []string{"1.emlx", "10.emlx", "2.emlx", "300.emlx"}
	files := mailboxes[0].Files
	if len(files) != len(wantNames) {
		t.Fatalf("files count = %d, want %d", len(files), len(wantNames))
	}
	for i := range wantNames {
		got := filepath.Base(files[i])
		if got != wantNames[i] {
			t.Fatalf("files[%d] basename = %q, want %q", i, got, wantNames[i])
		}
	}
}

func TestDiscoverMailboxes_V10Layout(t *testing.T) {
	root := t.TempDir()
	v10 := filepath.Join(root, "V10")
	acctGUID := "13C9A646-EE0A-4698-B5A2-E07FFBDDEED3"
	mboxGUID := "9F0F15DD-4CBC-448A-9EBF-C385A47A3A67"
	acctDir := filepath.Join(v10, acctGUID)

	mkV10Mailbox(t,
		filepath.Join(acctDir, "INBOX.mbox"),
		mboxGUID, "1.emlx", "2.emlx", "3.emlx",
	)
	mkV10Mailbox(t,
		filepath.Join(acctDir, "Sent Messages.mbox"),
		mboxGUID, "10.emlx",
	)
	mkV10Mailbox(t,
		filepath.Join(acctDir, "Junk.mbox"),
		mboxGUID, "27.emlx", "28.emlx",
	)

	mailboxes, err := DiscoverMailboxes(v10)
	if err != nil {
		t.Fatalf("DiscoverMailboxes: %v", err)
	}
	if len(mailboxes) != 3 {
		for _, mb := range mailboxes {
			t.Logf("  label=%q path=%q files=%d",
				mb.Label, mb.Path, len(mb.Files))
		}
		t.Fatalf("got %d mailboxes, want 3", len(mailboxes))
	}

	labels := make(map[string]int)
	for _, mb := range mailboxes {
		labels[mb.Label] = len(mb.Files)
	}

	tests := []struct {
		label     string
		wantFiles int
	}{
		{"INBOX", 3},
		{"Sent Messages", 1},
		{"Junk", 2},
	}
	for _, tc := range tests {
		n, ok := labels[tc.label]
		if !ok {
			t.Errorf("missing label %q (have: %v)",
				tc.label, labels)
			continue
		}
		if n != tc.wantFiles {
			t.Errorf("label %q: files = %d, want %d",
				tc.label, n, tc.wantFiles)
		}
	}
}

func TestDiscoverMailboxes_V10SingleMailbox(t *testing.T) {
	root := t.TempDir()
	guid := "9F0F15DD-4CBC-448A-9EBF-C385A47A3A67"
	mboxDir := filepath.Join(root, "INBOX.mbox")
	mkV10Mailbox(t, mboxDir, guid, "1.emlx", "2.emlx")

	// Point directly at the .mbox directory.
	mailboxes, err := DiscoverMailboxes(mboxDir)
	if err != nil {
		t.Fatalf("DiscoverMailboxes: %v", err)
	}
	if len(mailboxes) != 1 {
		t.Fatalf("got %d mailboxes, want 1", len(mailboxes))
	}
	if mailboxes[0].Label != "INBOX" {
		t.Errorf("Label = %q, want %q",
			mailboxes[0].Label, "INBOX")
	}
	if len(mailboxes[0].Files) != 2 {
		t.Errorf("Files = %d, want 2",
			len(mailboxes[0].Files))
	}

	// MsgDir should point to the GUID/Data/Messages path.
	wantSuffix := filepath.Join(guid, "Data", "Messages")
	if !filepath.IsAbs(mailboxes[0].MsgDir) {
		t.Errorf("MsgDir not absolute: %q", mailboxes[0].MsgDir)
	}
	rel, _ := filepath.Rel(mboxDir, mailboxes[0].MsgDir)
	if rel != wantSuffix {
		t.Errorf("MsgDir relative = %q, want %q",
			rel, wantSuffix)
	}
}

func TestDiscoverMailboxes_V10PartialSkipped(t *testing.T) {
	root := t.TempDir()
	guid := "9F0F15DD-4CBC-448A-9EBF-C385A47A3A67"
	mboxDir := filepath.Join(root, "Test.mbox")
	mkV10Mailbox(t, mboxDir, guid,
		"1.emlx", "2.partial.emlx",
	)

	mailboxes, err := DiscoverMailboxes(mboxDir)
	if err != nil {
		t.Fatalf("DiscoverMailboxes: %v", err)
	}
	if len(mailboxes) != 1 {
		t.Fatalf("got %d mailboxes, want 1", len(mailboxes))
	}
	if len(mailboxes[0].Files) != 1 {
		t.Fatalf("Files = %d, want 1", len(mailboxes[0].Files))
	}
	if filepath.Base(mailboxes[0].Files[0]) != "1.emlx" {
		t.Errorf("Files[0] basename = %q, want %q",
			filepath.Base(mailboxes[0].Files[0]), "1.emlx")
	}
}

func TestDiscoverMailboxes_MixedLegacyAndV10(t *testing.T) {
	root := t.TempDir()
	guid := "9F0F15DD-4CBC-448A-9EBF-C385A47A3A67"
	mboxDir := filepath.Join(root, "INBOX.mbox")

	// Create empty legacy Messages/ alongside populated V10 path.
	if err := os.MkdirAll(
		filepath.Join(mboxDir, "Messages"), 0700,
	); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	mkV10Mailbox(t, mboxDir, guid, "1.emlx", "2.emlx")

	mailboxes, err := DiscoverMailboxes(mboxDir)
	if err != nil {
		t.Fatalf("DiscoverMailboxes: %v", err)
	}
	if len(mailboxes) != 1 {
		t.Fatalf("got %d mailboxes, want 1", len(mailboxes))
	}
	if len(mailboxes[0].Files) != 2 {
		t.Fatalf("Files = %d, want 2 (should use V10 path)",
			len(mailboxes[0].Files))
	}

	// MsgDir should point to the V10 path, not the empty legacy one.
	wantSuffix := filepath.Join(guid, "Data", "Messages")
	rel, _ := filepath.Rel(mboxDir, mailboxes[0].MsgDir)
	if rel != wantSuffix {
		t.Errorf("MsgDir relative = %q, want %q",
			rel, wantSuffix)
	}
}

// mkV10PartitionedMailbox creates a V10 mailbox with .emlx files in
// both the primary Messages/ directory and in numeric partition
// subdirectories at various nesting depths.
//
// Layout created:
//
//	base/<guid>/Data/Messages/1.emlx       (top-level)
//	base/<guid>/Data/0/3/Messages/123.emlx (2-level partition)
//	base/<guid>/Data/9/Messages/456.emlx   (1-level partition)
func mkV10PartitionedMailbox(t *testing.T, base, guid string) {
	t.Helper()
	dataDir := filepath.Join(base, guid, "Data")

	writeEmlxFile := func(dir, name string) {
		t.Helper()
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatalf("mkdir %q: %v", dir, err)
		}
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte("10\nFrom: x\r\n\r\n"), 0600); err != nil {
			t.Fatalf("write %q: %v", path, err)
		}
	}

	writeEmlxFile(filepath.Join(dataDir, "Messages"), "1.emlx")
	writeEmlxFile(filepath.Join(dataDir, "0", "3", "Messages"), "123.emlx")
	writeEmlxFile(filepath.Join(dataDir, "9", "Messages"), "456.emlx")
}

func TestDiscoverMailboxes_V10Partitioned(t *testing.T) {
	root := t.TempDir()
	guid := "9F0F15DD-4CBC-448A-9EBF-C385A47A3A67"
	mboxDir := filepath.Join(root, "INBOX.mbox")
	mkV10PartitionedMailbox(t, mboxDir, guid)

	mailboxes, err := DiscoverMailboxes(mboxDir)
	if err != nil {
		t.Fatalf("DiscoverMailboxes: %v", err)
	}
	if len(mailboxes) != 1 {
		t.Fatalf("got %d mailboxes, want 1", len(mailboxes))
	}

	mb := mailboxes[0]
	if mb.Label != "INBOX" {
		t.Errorf("Label = %q, want %q", mb.Label, "INBOX")
	}

	// Should find all 3 files: 1 top-level + 2 from partitions.
	if len(mb.Files) != 3 {
		t.Fatalf("Files = %v (len %d), want 3 files", mb.Files, len(mb.Files))
	}

	// Verify all paths are absolute and point to existing files.
	for _, path := range mb.Files {
		if !filepath.IsAbs(path) {
			t.Errorf("expected absolute path, got %q", path)
		}
		if _, err := os.Stat(path); err != nil {
			t.Errorf("stat %q: %v", path, err)
		}
	}

	// Verify expected basenames are present.
	baseNames := make(map[string]bool)
	for _, f := range mb.Files {
		baseNames[filepath.Base(f)] = true
	}
	for _, want := range []string{"1.emlx", "123.emlx", "456.emlx"} {
		if !baseNames[want] {
			t.Errorf("missing file %q in Files", want)
		}
	}
}

func TestDiscoverMailboxes_V10PartitionedOnly(t *testing.T) {
	root := t.TempDir()
	guid := "9F0F15DD-4CBC-448A-9EBF-C385A47A3A67"
	mboxDir := filepath.Join(root, "INBOX.mbox")

	// Create the primary Messages/ dir but leave it empty.
	// (Tests the case where Messages/ exists but is empty.)
	primaryMsg := filepath.Join(mboxDir, guid, "Data", "Messages")
	if err := os.MkdirAll(primaryMsg, 0700); err != nil {
		t.Fatalf("mkdir %q: %v", primaryMsg, err)
	}

	// Place files only in partition dirs.
	partDir := filepath.Join(mboxDir, guid, "Data", "3", "Messages")
	if err := os.MkdirAll(partDir, 0700); err != nil {
		t.Fatalf("mkdir %q: %v", partDir, err)
	}
	for _, name := range []string{"100.emlx", "200.emlx"} {
		path := filepath.Join(partDir, name)
		if err := os.WriteFile(path, []byte("10\nFrom: x\r\n\r\n"), 0600); err != nil {
			t.Fatalf("write %q: %v", path, err)
		}
	}

	mailboxes, err := DiscoverMailboxes(mboxDir)
	if err != nil {
		t.Fatalf("DiscoverMailboxes: %v", err)
	}
	if len(mailboxes) != 1 {
		t.Fatalf("got %d mailboxes, want 1 (partitioned-only mailbox should be detected)", len(mailboxes))
	}

	mb := mailboxes[0]
	if len(mb.Files) != 2 {
		t.Fatalf("Files = %v (len %d), want 2", mb.Files, len(mb.Files))
	}

	for _, path := range mb.Files {
		if _, err := os.Stat(path); err != nil {
			t.Errorf("stat %q: %v", path, err)
		}
	}
}

// TestDiscoverMailboxes_V10NoTopLevelMessages tests the case where
// Data/Messages/ does not exist at all — only numeric partition dirs.
// This matches real Apple Mail behavior for large mailboxes.
func TestDiscoverMailboxes_V10NoTopLevelMessages(t *testing.T) {
	root := t.TempDir()
	guid := "9F0F15DD-4CBC-448A-9EBF-C385A47A3A67"
	mboxDir := filepath.Join(root, "Sent Messages.mbox")

	// Do NOT create Data/Messages/ — only create partition dirs.
	for _, partPath := range []string{
		filepath.Join(mboxDir, guid, "Data", "9", "9", "Messages"),
		filepath.Join(mboxDir, guid, "Data", "0", "0", "1", "Messages"),
	} {
		if err := os.MkdirAll(partPath, 0700); err != nil {
			t.Fatalf("mkdir %q: %v", partPath, err)
		}
	}
	testFiles := map[string]string{
		"500.emlx": filepath.Join(mboxDir, guid, "Data", "9", "9", "Messages"),
		"600.emlx": filepath.Join(mboxDir, guid, "Data", "0", "0", "1", "Messages"),
		"700.emlx": filepath.Join(mboxDir, guid, "Data", "0", "0", "1", "Messages"),
	}
	for name, dir := range testFiles {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte("10\nFrom: x\r\n\r\n"), 0600); err != nil {
			t.Fatalf("write %q: %v", path, err)
		}
	}

	mailboxes, err := DiscoverMailboxes(mboxDir)
	if err != nil {
		t.Fatalf("DiscoverMailboxes: %v", err)
	}
	if len(mailboxes) != 1 {
		t.Fatalf("got %d mailboxes, want 1 (no Data/Messages/ dir)", len(mailboxes))
	}

	mb := mailboxes[0]
	if len(mb.Files) != 3 {
		t.Fatalf("Files = %v, want 3", mb.Files)
	}

	for _, path := range mb.Files {
		if _, err := os.Stat(path); err != nil {
			t.Errorf("stat %q: %v", path, err)
		}
	}
}

func TestIsUUID(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"13C9A646-EE0A-4698-B5A2-E07FFBDDEED3", true},
		{"9f0f15dd-4cbc-448a-9ebf-c385a47a3a67", true},
		{"INBOX", false},
		{"Mailboxes", false},
		{"IMAP-foo@bar.com", false},
		{"", false},
		{"not-a-uuid-at-all-nope-definitely", false},
	}
	for _, tc := range tests {
		got := IsUUID(tc.input)
		if got != tc.want {
			t.Errorf("IsUUID(%q) = %v, want %v",
				tc.input, got, tc.want)
		}
	}
}
