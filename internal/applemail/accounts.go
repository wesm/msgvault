package applemail

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"github.com/wesm/msgvault/internal/emlx"
)

// AccountInfo describes an Apple Mail account resolved from Accounts4.sqlite.
type AccountInfo struct {
	// GUID is the V10 directory UUID.
	GUID string

	// Email is the resolved email address. Empty for local accounts
	// like "On My Mac".
	Email string

	// Description is the account description (e.g. "Google", "Yahoo!",
	// "On My Mac").
	Description string
}

// Identifier returns the best identifier for this account: the email
// address if available, otherwise the description.
func (a AccountInfo) Identifier() string {
	if a.Email != "" {
		return a.Email
	}
	return a.Description
}

// DefaultAccountsDBPath returns the default path to Apple's
// Accounts4.sqlite database.
func DefaultAccountsDBPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, "Library", "Accounts", "Accounts4.sqlite")
}

// ResolveAccounts opens the Accounts4.sqlite database at dbPath and
// resolves the given GUIDs to account information. Returns a map of
// GUID → AccountInfo for each GUID that was found.
func ResolveAccounts(dbPath string, guids []string) (map[string]AccountInfo, error) {
	if len(guids) == 0 {
		return nil, nil
	}

	db, err := sql.Open("sqlite3", dbPath+"?mode=ro")
	if err != nil {
		return nil, fmt.Errorf("open accounts db: %w", err)
	}
	defer db.Close()

	// Build placeholders for IN clause.
	placeholders := make([]string, len(guids))
	args := make([]interface{}, len(guids))
	for i, g := range guids {
		placeholders[i] = "?"
		args[i] = g
	}

	query := `
		SELECT
			child.ZIDENTIFIER,
			COALESCE(child.ZUSERNAME, parent.ZUSERNAME, '') AS email,
			COALESCE(parent.ZACCOUNTDESCRIPTION, child.ZACCOUNTDESCRIPTION, '') AS description
		FROM ZACCOUNT child
		LEFT JOIN ZACCOUNT parent ON parent.Z_PK = child.ZPARENTACCOUNT
		WHERE child.ZIDENTIFIER IN (` + strings.Join(placeholders, ",") + `)
	`

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("query accounts: %w", err)
	}
	defer rows.Close()

	result := make(map[string]AccountInfo)
	for rows.Next() {
		var guid, email, description string
		if err := rows.Scan(&guid, &email, &description); err != nil {
			return nil, fmt.Errorf("scan account row: %w", err)
		}
		result[guid] = AccountInfo{
			GUID:        guid,
			Email:       email,
			Description: description,
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate account rows: %w", err)
	}

	return result, nil
}

// DiscoverV10Accounts scans mailDir for V10-style directories containing
// UUID subdirectories and resolves them to account information using the
// Accounts4.sqlite database at accountsDBPath.
func DiscoverV10Accounts(mailDir, accountsDBPath string, logger *slog.Logger) ([]AccountInfo, error) {
	if logger == nil {
		logger = slog.Default()
	}

	// Scan for V* directories containing UUID subdirectories.
	guids, err := findV10GUIDs(mailDir)
	if err != nil {
		return nil, fmt.Errorf("scan V10 directories: %w", err)
	}

	if len(guids) == 0 {
		return nil, nil
	}

	resolved, err := ResolveAccounts(accountsDBPath, guids)
	if err != nil {
		return nil, fmt.Errorf("resolve accounts: %w", err)
	}

	var accounts []AccountInfo
	for _, guid := range guids {
		info, ok := resolved[guid]
		if !ok {
			logger.Warn("GUID not found in Accounts4.sqlite, skipping",
				"guid", guid)
			continue
		}
		accounts = append(accounts, info)
	}

	return accounts, nil
}

// findV10GUIDs scans mailDir for V*/ directories containing UUID
// subdirectories and returns the unique GUIDs found.
func findV10GUIDs(mailDir string) ([]string, error) {
	entries, err := os.ReadDir(mailDir)
	if err != nil {
		return nil, err
	}

	var guids []string
	seen := make(map[string]bool)

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		// Look for V* directories (V2, V10, etc.).
		if !strings.HasPrefix(name, "V") {
			continue
		}

		vDir := filepath.Join(mailDir, name)
		subEntries, err := os.ReadDir(vDir)
		if err != nil {
			continue
		}

		for _, sub := range subEntries {
			if !sub.IsDir() {
				continue
			}
			if emlx.IsUUID(sub.Name()) && !seen[sub.Name()] {
				seen[sub.Name()] = true
				guids = append(guids, sub.Name())
			}
		}
	}

	return guids, nil
}

// V10AccountDir returns the path to a V10 account directory for the
// given GUID within mailDir. It searches all V* directories.
func V10AccountDir(mailDir, guid string) (string, error) {
	entries, err := os.ReadDir(mailDir)
	if err != nil {
		return "", err
	}

	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), "V") {
			continue
		}
		candidate := filepath.Join(mailDir, e.Name(), guid)
		if info, err := os.Stat(candidate); err == nil && info.IsDir() {
			return candidate, nil
		}
	}

	return "", fmt.Errorf("no V10 directory found for GUID %s in %s", guid, mailDir)
}
