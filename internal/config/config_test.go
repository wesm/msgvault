package config

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestExpandPath(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("failed to get user home dir: %v", err)
	}

	tests := []struct {
		name     string
		input    string
		expected string
		unixOnly bool // skip on Windows (uses Unix-style absolute paths)
	}{
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "just tilde",
			input:    "~",
			expected: home,
		},
		{
			name:     "tilde with slash and path",
			input:    "~/foo",
			expected: filepath.Join(home, "foo"),
		},
		{
			name:     "tilde with trailing slash only",
			input:    "~/",
			expected: home,
		},
		{
			name:     "tilde user notation not expanded",
			input:    "~user",
			expected: "~user",
		},
		{
			name:     "tilde with double slash",
			input:    "~//foo",
			expected: filepath.Join(home, "foo"),
		},
		{
			name:     "absolute path unchanged",
			input:    "/var/log/test",
			expected: "/var/log/test",
			unixOnly: true,
		},
		{
			name:     "relative path unchanged",
			input:    "relative/path",
			expected: "relative/path",
		},
		{
			name:     "tilde in middle not expanded",
			input:    "/home/~user/foo",
			expected: "/home/~user/foo",
			unixOnly: true,
		},
		{
			name:     "nested path after tilde",
			input:    "~/foo/bar/baz",
			expected: filepath.Join(home, "foo/bar/baz"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.unixOnly && runtime.GOOS == "windows" {
				t.Skip("skipping Unix-specific path test on Windows")
			}
			got := expandPath(tt.input)
			if got != tt.expected {
				t.Errorf("expandPath(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestLoadEmptyPath(t *testing.T) {
	// Use a temp directory as MSGVAULT_HOME
	tmpDir := t.TempDir()
	t.Setenv("MSGVAULT_HOME", tmpDir)

	// Load with empty path should use defaults
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load(\"\") failed: %v", err)
	}

	// Verify default values
	if cfg.HomeDir != tmpDir {
		t.Errorf("HomeDir = %q, want %q", cfg.HomeDir, tmpDir)
	}
	if cfg.Data.DataDir != tmpDir {
		t.Errorf("Data.DataDir = %q, want %q", cfg.Data.DataDir, tmpDir)
	}
	if cfg.Sync.RateLimitQPS != 5 {
		t.Errorf("Sync.RateLimitQPS = %d, want 5", cfg.Sync.RateLimitQPS)
	}

	// DatabaseDSN should return default path
	expectedDB := filepath.Join(tmpDir, "msgvault.db")
	if cfg.DatabaseDSN() != expectedDB {
		t.Errorf("DatabaseDSN() = %q, want %q", cfg.DatabaseDSN(), expectedDB)
	}
}

func TestLoadWithConfigFile(t *testing.T) {
	// Use a temp directory as MSGVAULT_HOME
	tmpDir := t.TempDir()
	t.Setenv("MSGVAULT_HOME", tmpDir)

	// Create a config file with custom values
	configPath := filepath.Join(tmpDir, "config.toml")
	configContent := `
[data]
data_dir = "~/custom/data"

[oauth]
client_secrets = "~/secrets/client.json"

[sync]
rate_limit_qps = 10
`
	if err := os.WriteFile(configPath, []byte(configContent), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load(\"\") failed: %v", err)
	}

	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("failed to get user home dir: %v", err)
	}

	// Verify paths were expanded
	expectedDataDir := filepath.Join(home, "custom/data")
	if cfg.Data.DataDir != expectedDataDir {
		t.Errorf("Data.DataDir = %q, want %q", cfg.Data.DataDir, expectedDataDir)
	}

	expectedSecrets := filepath.Join(home, "secrets/client.json")
	if cfg.OAuth.ClientSecrets != expectedSecrets {
		t.Errorf("OAuth.ClientSecrets = %q, want %q", cfg.OAuth.ClientSecrets, expectedSecrets)
	}

	if cfg.Sync.RateLimitQPS != 10 {
		t.Errorf("Sync.RateLimitQPS = %d, want 10", cfg.Sync.RateLimitQPS)
	}
}

func TestNewDefaultConfig(t *testing.T) {
	// Use a temp directory as MSGVAULT_HOME
	tmpDir := t.TempDir()
	t.Setenv("MSGVAULT_HOME", tmpDir)

	cfg := NewDefaultConfig()

	if cfg.HomeDir != tmpDir {
		t.Errorf("HomeDir = %q, want %q", cfg.HomeDir, tmpDir)
	}
	if cfg.Data.DataDir != tmpDir {
		t.Errorf("Data.DataDir = %q, want %q", cfg.Data.DataDir, tmpDir)
	}
	if cfg.Sync.RateLimitQPS != 5 {
		t.Errorf("Sync.RateLimitQPS = %d, want 5", cfg.Sync.RateLimitQPS)
	}
}
