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

func TestResolveAccounts(t *testing.T) {
	// Set up accounts mimicking real Accounts4.sqlite:
	// - PK 1: Google parent (has email, description "Google")
	// - PK 2: IMAP child of Google (GUID, no email, inherits from parent)
	// - PK 3: Yahoo parent (has email, description "Yahoo!")
	// - PK 4: IMAP child of Yahoo (GUID, no email, inherits from parent)
	// - PK 5: Exchange account (GUID, has own email)
	// - PK 6: "On My Mac" (GUID, no email, description only)
	accounts := []testAccount{
		{pk: 1, identifier: "google-parent-id", username: strPtr("user@gmail.com"), description: strPtr("Google"), parentAccount: nil},
		{pk: 2, identifier: "13C9A646-1234-5678-9ABC-E07FFBDDEED3", username: nil, description: nil, parentAccount: intPtr(1)},
		{pk: 3, identifier: "yahoo-parent-id", username: strPtr("user@yahoo.com"), description: strPtr("Yahoo!"), parentAccount: nil},
		{pk: 4, identifier: "AABBCCDD-1111-2222-3333-445566778899", username: nil, description: nil, parentAccount: intPtr(3)},
		{pk: 5, identifier: "EXCHANGE1-AAAA-BBBB-CCCC-DDDDEEEEEEEE", username: strPtr("user@exchange.com"), description: strPtr("Exchange"), parentAccount: nil},
		{pk: 6, identifier: "LOCALONLY-0000-0000-0000-000000000000", username: nil, description: strPtr("On My Mac"), parentAccount: nil},
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
	mailDir := t.TempDir()

	// Create V10 with UUID dirs plus non-UUID.
	v10 := filepath.Join(mailDir, "V10")
	guid := "AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE"
	if err := os.MkdirAll(filepath.Join(v10, guid), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(v10, "MailData"), 0o755); err != nil {
		t.Fatal(err)
	}

	// Create V2 with another UUID.
	v2 := filepath.Join(mailDir, "V2")
	guid2 := "11111111-2222-3333-4444-555555555555"
	if err := os.MkdirAll(filepath.Join(v2, guid2), 0o755); err != nil {
		t.Fatal(err)
	}

	// Non-V directory should be ignored.
	if err := os.MkdirAll(filepath.Join(mailDir, "Other", "FFFFFFFF-0000-0000-0000-000000000000"), 0o755); err != nil {
		t.Fatal(err)
	}

	guids, err := findV10GUIDs(mailDir)
	if err != nil {
		t.Fatalf("findV10GUIDs: %v", err)
	}

	if len(guids) != 2 {
		t.Fatalf("got %d GUIDs, want 2: %v", len(guids), guids)
	}

	seen := make(map[string]bool)
	for _, g := range guids {
		seen[g] = true
	}

	if !seen[guid] {
		t.Errorf("expected GUID %s", guid)
	}
	if !seen[guid2] {
		t.Errorf("expected GUID %s", guid2)
	}
}
