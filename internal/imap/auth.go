package imap

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type credentialsFile struct {
	Password string `json:"password"`
}

// credentialsPath returns the path to the credentials file for the given identifier.
func credentialsPath(tokensDir, identifier string) string {
	hash := sha256.Sum256([]byte(identifier))
	prefix := fmt.Sprintf("%x", hash[:8])
	return filepath.Join(tokensDir, "imap_"+prefix+".json")
}

// SaveCredentials saves an IMAP password for the given identifier.
func SaveCredentials(tokensDir, identifier, password string) error {
	if err := os.MkdirAll(tokensDir, 0700); err != nil {
		return fmt.Errorf("create tokens dir: %w", err)
	}
	creds := credentialsFile{Password: password}
	data, err := json.Marshal(creds)
	if err != nil {
		return err
	}
	path := credentialsPath(tokensDir, identifier)
	if err := os.WriteFile(path, data, 0600); err != nil {
		return fmt.Errorf("write credentials: %w", err)
	}
	return nil
}

// LoadCredentials loads an IMAP password for the given identifier.
func LoadCredentials(tokensDir, identifier string) (string, error) {
	path := credentialsPath(tokensDir, identifier)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("no credentials found for %s (run 'add-imap' first)", identifier)
		}
		return "", fmt.Errorf("read credentials: %w", err)
	}
	var creds credentialsFile
	if err := json.Unmarshal(data, &creds); err != nil {
		return "", fmt.Errorf("parse credentials: %w", err)
	}
	return creds.Password, nil
}

// HasCredentials returns true if credentials exist for the given identifier.
func HasCredentials(tokensDir, identifier string) bool {
	path := credentialsPath(tokensDir, identifier)
	_, err := os.Stat(path)
	return err == nil
}
