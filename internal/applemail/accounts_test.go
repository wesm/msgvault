package applemail

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// createTestAccountsDB creates a temporary Accounts4.sqlite with the
// minimal schema and populates it with the given accounts.
func createTestAccountsDB(t *testing.T, accounts []testAccount) string {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "Accounts4.sqlite")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("create test db: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE ZACCOUNT (
			Z_PK INTEGER PRIMARY KEY,
			ZIDENTIFIER TEXT,
			ZUSERNAME TEXT,
			ZACCOUNTDESCRIPTION TEXT,
			ZPARENTACCOUNT INTEGER
		)
	`)
	if err != nil {
		t.Fatalf("create schema: %v", err)
	}

	for _, a := range accounts {
		_, err := db.Exec(
			`INSERT INTO ZACCOUNT (Z_PK, ZIDENTIFIER, ZUSERNAME, ZACCOUNTDESCRIPTION, ZPARENTACCOUNT)
			 VALUES (?, ?, ?, ?, ?)`,
			a.pk, a.identifier, a.username, a.description, a.parentAccount,
		)
		if err != nil {
			t.Fatalf("insert account: %v", err)
		}
	}

	return dbPath
}

type testAccount struct {
	pk            int
	identifier    string
	username      *string
	description   *string
	parentAccount *int
}

func strPtr(s string) *string { return &s }
func intPtr(i int) *int       { return &i }

func mustMkdirAll(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatalf("mkdir %q: %v", path, err)
	}
}

func TestResolveAccounts(t *testing.T) {
	// Set up accounts mimicking real Accounts4.sqlite:
	// - PK 1: Google parent (has email, description "Google")
	// - PK 2: IMAP child of Google (GUID, no email, inherits from parent)
	// - PK 3: Yahoo parent (has email, description "Yahoo!")
	// - PK 4: IMAP child of Yahoo (GUID, no email, inherits from parent)
	// - PK 5: Exchange account (GUID, has own email)
	// - PK 6: "On My Mac" (GUID, no email, description only)
	// - PK 7: iCloud parent (has email, description "iCloud")
	// - PK 8: IMAP child of iCloud with empty-string fields (not NULL)
	accounts := []testAccount{
		{pk: 1, identifier: "google-parent-id", username: strPtr("user@gmail.com"), description: strPtr("Google"), parentAccount: nil},
		{pk: 2, identifier: "13C9A646-1234-5678-9ABC-E07FFBDDEED3", username: nil, description: nil, parentAccount: intPtr(1)},
		{pk: 3, identifier: "yahoo-parent-id", username: strPtr("user@yahoo.com"), description: strPtr("Yahoo!"), parentAccount: nil},
		{pk: 4, identifier: "AABBCCDD-1111-2222-3333-445566778899", username: nil, description: nil, parentAccount: intPtr(3)},
		{pk: 5, identifier: "EXCHANGE1-AAAA-BBBB-CCCC-DDDDEEEEEEEE", username: strPtr("user@exchange.com"), description: strPtr("Exchange"), parentAccount: nil},
		{pk: 6, identifier: "LOCALONLY-0000-0000-0000-000000000000", username: nil, description: strPtr("On My Mac"), parentAccount: nil},
		{pk: 7, identifier: "icloud-parent-id", username: strPtr("user@icloud.com"), description: strPtr("iCloud"), parentAccount: nil},
		{pk: 8, identifier: "ICLOUDCH-1111-2222-3333-444455556666", username: strPtr(""), description: strPtr(""), parentAccount: intPtr(7)},
	}

	dbPath := createTestAccountsDB(t, accounts)

	tests := []struct {
		name        string
		guids       []string
		wantLen     int
		wantEmail   map[string]string // guid → expected email
		wantDesc    map[string]string // guid → expected description
		wantMissing []string          // guids not in result
	}{
		{
			name:    "IMAP child resolves parent email (Google)",
			guids:   []string{"13C9A646-1234-5678-9ABC-E07FFBDDEED3"},
			wantLen: 1,
			wantEmail: map[string]string{
				"13C9A646-1234-5678-9ABC-E07FFBDDEED3": "user@gmail.com",
			},
			wantDesc: map[string]string{
				"13C9A646-1234-5678-9ABC-E07FFBDDEED3": "Google",
			},
		},
		{
			name:    "IMAP child resolves parent email (Yahoo)",
			guids:   []string{"AABBCCDD-1111-2222-3333-445566778899"},
			wantLen: 1,
			wantEmail: map[string]string{
				"AABBCCDD-1111-2222-3333-445566778899": "user@yahoo.com",
			},
			wantDesc: map[string]string{
				"AABBCCDD-1111-2222-3333-445566778899": "Yahoo!",
			},
		},
		{
			name:    "Exchange account with own email",
			guids:   []string{"EXCHANGE1-AAAA-BBBB-CCCC-DDDDEEEEEEEE"},
			wantLen: 1,
			wantEmail: map[string]string{
				"EXCHANGE1-AAAA-BBBB-CCCC-DDDDEEEEEEEE": "user@exchange.com",
			},
			wantDesc: map[string]string{
				"EXCHANGE1-AAAA-BBBB-CCCC-DDDDEEEEEEEE": "Exchange",
			},
		},
		{
			name:    "On My Mac has no email",
			guids:   []string{"LOCALONLY-0000-0000-0000-000000000000"},
			wantLen: 1,
			wantEmail: map[string]string{
				"LOCALONLY-0000-0000-0000-000000000000": "",
			},
			wantDesc: map[string]string{
				"LOCALONLY-0000-0000-0000-000000000000": "On My Mac",
			},
		},
		{
			name:        "Missing GUID returns no entry",
			guids:       []string{"NOTEXIST-0000-0000-0000-000000000000"},
			wantLen:     0,
			wantMissing: []string{"NOTEXIST-0000-0000-0000-000000000000"},
		},
		{
			name:    "Multiple GUIDs resolved at once",
			guids:   []string{"13C9A646-1234-5678-9ABC-E07FFBDDEED3", "AABBCCDD-1111-2222-3333-445566778899"},
			wantLen: 2,
			wantEmail: map[string]string{
				"13C9A646-1234-5678-9ABC-E07FFBDDEED3": "user@gmail.com",
				"AABBCCDD-1111-2222-3333-445566778899": "user@yahoo.com",
			},
		},
		{
			name:    "Empty-string child fields fall through to parent",
			guids:   []string{"ICLOUDCH-1111-2222-3333-444455556666"},
			wantLen: 1,
			wantEmail: map[string]string{
				"ICLOUDCH-1111-2222-3333-444455556666": "user@icloud.com",
			},
			wantDesc: map[string]string{
				"ICLOUDCH-1111-2222-3333-444455556666": "iCloud",
			},
		},
		{
			name:    "Empty GUID list",
			guids:   nil,
			wantLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ResolveAccounts(dbPath, tt.guids)
			if err != nil {
				t.Fatalf("ResolveAccounts: %v", err)
			}

			if len(result) != tt.wantLen {
				t.Errorf("got %d results, want %d", len(result), tt.wantLen)
			}

			for guid, wantEmail := range tt.wantEmail {
				info, ok := result[guid]
				if !ok {
					t.Errorf("GUID %s not found in result", guid)
					continue
				}
				if info.Email != wantEmail {
					t.Errorf("GUID %s: email = %q, want %q", guid, info.Email, wantEmail)
				}
			}

			for guid, wantDesc := range tt.wantDesc {
				info, ok := result[guid]
				if !ok {
					continue // already reported above
				}
				if info.Description != wantDesc {
					t.Errorf("GUID %s: description = %q, want %q", guid, info.Description, wantDesc)
				}
			}

			for _, guid := range tt.wantMissing {
				if _, ok := result[guid]; ok {
					t.Errorf("GUID %s should not be in result", guid)
				}
			}
		})
	}
}

func TestResolveAccounts_BadPath(t *testing.T) {
	_, err := ResolveAccounts("/nonexistent/path/Accounts4.sqlite", []string{"some-guid"})
	if err == nil {
		t.Fatal("expected error for bad DB path")
	}
}

func TestAccountInfo_Identifier(t *testing.T) {
	tests := []struct {
		name string
		info AccountInfo
		want string
	}{
		{
			name: "has email",
			info: AccountInfo{Email: "user@gmail.com", Description: "Google"},
			want: "user@gmail.com",
		},
		{
			name: "no email uses description",
			info: AccountInfo{Email: "", Description: "On My Mac"},
			want: "On My Mac",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.info.Identifier(); got != tt.want {
				t.Errorf("Identifier() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDiscoverV10Accounts(t *testing.T) {
	// Create a fake Mail directory with V10 layout.
	mailDir := t.TempDir()
	v10Dir := filepath.Join(mailDir, "V10")
	guid1 := "13C9A646-1234-5678-9ABC-E07FFBDDEED3"
	guid2 := "AABBCCDD-1111-2222-3333-445566778899"

	// Create UUID dirs under V10.
	if err := os.MkdirAll(filepath.Join(v10Dir, guid1), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(v10Dir, guid2), 0o755); err != nil {
		t.Fatal(err)
	}
	// Also create a non-UUID dir that should be ignored.
	if err := os.MkdirAll(filepath.Join(v10Dir, "MailData"), 0o755); err != nil {
		t.Fatal(err)
	}

	// Create accounts DB with these GUIDs.
	accounts := []testAccount{
		{pk: 1, identifier: "google-parent", username: strPtr("user@gmail.com"), description: strPtr("Google"), parentAccount: nil},
		{pk: 2, identifier: guid1, username: nil, description: nil, parentAccount: intPtr(1)},
		{pk: 3, identifier: "yahoo-parent", username: strPtr("user@yahoo.com"), description: strPtr("Yahoo!"), parentAccount: nil},
		{pk: 4, identifier: guid2, username: nil, description: nil, parentAccount: intPtr(3)},
	}
	dbPath := createTestAccountsDB(t, accounts)

	result, err := DiscoverV10Accounts(mailDir, dbPath, nil)
	if err != nil {
		t.Fatalf("DiscoverV10Accounts: %v", err)
	}

	if len(result) != 2 {
		t.Fatalf("got %d accounts, want 2", len(result))
	}

	// Check both accounts resolved.
	byGUID := make(map[string]AccountInfo)
	for _, a := range result {
		byGUID[a.GUID] = a
	}

	if info, ok := byGUID[guid1]; !ok {
		t.Errorf("GUID %s not found", guid1)
	} else if info.Email != "user@gmail.com" {
		t.Errorf("GUID %s: email = %q, want %q", guid1, info.Email, "user@gmail.com")
	}

	if info, ok := byGUID[guid2]; !ok {
		t.Errorf("GUID %s not found", guid2)
	} else if info.Email != "user@yahoo.com" {
		t.Errorf("GUID %s: email = %q, want %q", guid2, info.Email, "user@yahoo.com")
	}
}

func TestFindV10GUIDs(t *testing.T) {
	guidA := "AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE"
	guidB := "11111111-2222-3333-4444-555555555555"
	guidC := "99999999-8888-7777-6666-555544443333"

	tests := []struct {
		name      string
		setup     func(t *testing.T, mailDir string)
		wantGUIDs []string
	}{
		{
			name: "single V10 dir",
			setup: func(t *testing.T, mailDir string) {
				t.Helper()
				mustMkdirAll(t, filepath.Join(mailDir, "V10", guidA))
				mustMkdirAll(t, filepath.Join(mailDir, "V10", "MailData"))
			},
			wantGUIDs: []string{guidA},
		},
		{
			name: "same GUID in V2 and V10 deduplicates",
			setup: func(t *testing.T, mailDir string) {
				t.Helper()
				mustMkdirAll(t, filepath.Join(mailDir, "V2", guidA))
				mustMkdirAll(t, filepath.Join(mailDir, "V10", guidA))
			},
			wantGUIDs: []string{guidA},
		},
		{
			name: "partially populated V10 discovers older-only accounts",
			setup: func(t *testing.T, mailDir string) {
				t.Helper()
				mustMkdirAll(t, filepath.Join(mailDir, "V10", guidA))
				mustMkdirAll(t, filepath.Join(mailDir, "V9", guidA))
				mustMkdirAll(t, filepath.Join(mailDir, "V9", guidB))
			},
			wantGUIDs: []string{guidA, guidB},
		},
		{
			name: "empty V10 discovers from V9",
			setup: func(t *testing.T, mailDir string) {
				t.Helper()
				mustMkdirAll(t, filepath.Join(mailDir, "V10"))
				mustMkdirAll(t, filepath.Join(mailDir, "V9", guidC))
			},
			wantGUIDs: []string{guidC},
		},
		{
			name: "non-V directory ignored",
			setup: func(t *testing.T, mailDir string) {
				t.Helper()
				mustMkdirAll(t, filepath.Join(mailDir, "Other", guidA))
			},
			wantGUIDs: nil,
		},
		{
			name: "non-UUID subdirs ignored",
			setup: func(t *testing.T, mailDir string) {
				t.Helper()
				mustMkdirAll(t, filepath.Join(mailDir, "V10", "MailData"))
			},
			wantGUIDs: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mailDir := t.TempDir()
			tt.setup(t, mailDir)

			guids, err := findV10GUIDs(mailDir)
			if err != nil {
				t.Fatalf("findV10GUIDs: %v", err)
			}

			if len(guids) != len(tt.wantGUIDs) {
				t.Fatalf("got %d GUIDs %v, want %d %v",
					len(guids), guids, len(tt.wantGUIDs), tt.wantGUIDs)
			}

			got := make(map[string]bool)
			for _, g := range guids {
				got[g] = true
			}
			for _, want := range tt.wantGUIDs {
				if !got[want] {
					t.Errorf("missing GUID %s in result %v", want, guids)
				}
			}
		})
	}
}

// writeTestEmlx creates a minimal .emlx file at the given path.
func writeTestEmlx(t *testing.T, dir, name string) {
	t.Helper()
	mustMkdirAll(t, dir)
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("10\nFrom: x\r\n\r\n"), 0o600); err != nil {
		t.Fatalf("write %q: %v", path, err)
	}
}

func TestV10AccountDir_PrefersPopulated(t *testing.T) {
	guid := "AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE"
	mailDir := t.TempDir()

	// V10 has the GUID dir with an empty .mbox stub (no .emlx files).
	mustMkdirAll(t, filepath.Join(mailDir, "V10", guid, "INBOX.mbox", "Messages"))

	// V9 has actual messages.
	writeTestEmlx(t,
		filepath.Join(mailDir, "V9", guid, "INBOX.mbox", "Messages"),
		"1.emlx",
	)

	got, err := V10AccountDir(mailDir, guid)
	if err != nil {
		t.Fatalf("V10AccountDir: %v", err)
	}

	want := filepath.Join(mailDir, "V9", guid)
	if got != want {
		t.Errorf("got %q, want %q (should prefer populated V9 over empty V10)", got, want)
	}
}

func TestV10AccountDir_NewestPopulatedWins(t *testing.T) {
	guid := "AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE"
	mailDir := t.TempDir()

	// Both have actual messages; newest wins.
	writeTestEmlx(t,
		filepath.Join(mailDir, "V10", guid, "INBOX.mbox", "Messages"),
		"1.emlx",
	)
	writeTestEmlx(t,
		filepath.Join(mailDir, "V9", guid, "INBOX.mbox", "Messages"),
		"1.emlx",
	)

	got, err := V10AccountDir(mailDir, guid)
	if err != nil {
		t.Fatalf("V10AccountDir: %v", err)
	}

	want := filepath.Join(mailDir, "V10", guid)
	if got != want {
		t.Errorf("got %q, want %q (newest populated should win)", got, want)
	}
}
