package oauth

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func writeServiceAccountKey(t *testing.T, path string, perm os.FileMode) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("MarshalPKCS8PrivateKey: %v", err)
	}
	pemKey := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: der,
	})

	data, err := json.Marshal(map[string]string{
		"type":           "service_account",
		"project_id":     "test-project",
		"private_key_id": "test-key-id",
		"private_key":    string(pemKey),
		"client_email":   "svc@test-project.iam.gserviceaccount.com",
		"client_id":      "123456789",
		"token_uri":      "https://oauth2.googleapis.com/token",
	})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if err := os.WriteFile(path, data, perm); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if runtime.GOOS != "windows" {
		if err := os.Chmod(path, perm); err != nil {
			t.Fatalf("Chmod: %v", err)
		}
	}
}

func TestNewServiceAccountManagerRejectsInsecureKeyPermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission bits are not enforced on Windows")
	}

	path := filepath.Join(t.TempDir(), "service-account.json")
	writeServiceAccountKey(t, path, 0644)

	_, err := NewServiceAccountManager(path, Scopes)
	if err == nil {
		t.Fatal("expected insecure permission error")
	}
	if !strings.Contains(err.Error(), "service account key permissions") {
		t.Fatalf("error = %v, want service account key permissions", err)
	}
}

func TestNewServiceAccountManagerAcceptsOwnerOnlyKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "service-account.json")
	writeServiceAccountKey(t, path, 0600)

	if _, err := NewServiceAccountManager(path, Scopes); err != nil {
		t.Fatalf("NewServiceAccountManager: %v", err)
	}
}

func TestNewServiceAccountManagerRejectsEmptyScopes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "service-account.json")
	writeServiceAccountKey(t, path, 0600)

	_, err := NewServiceAccountManager(path, nil)
	if err == nil {
		t.Fatal("expected empty scopes error")
	}
	if !strings.Contains(err.Error(), "at least one scope") {
		t.Fatalf("error = %v, want at least one scope", err)
	}
}

func TestNewServiceAccountManagerRejectsMalformedJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "service-account.json")
	if err := os.WriteFile(path, []byte("not json"), 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	_, err := NewServiceAccountManager(path, Scopes)
	if err == nil {
		t.Fatal("expected parse error")
	}
	if !strings.Contains(err.Error(), "parse service account key") {
		t.Fatalf("error = %v, want parse service account key", err)
	}
}
