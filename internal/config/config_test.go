package config

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestServerConfigDefaults(t *testing.T) {
	// Create a temp dir without a config file
	tmpDir := t.TempDir()
	t.Setenv("MSGVAULT_HOME", tmpDir)

	cfg, err := Load("", "")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Check server defaults
	if cfg.Server.APIPort != 8080 {
		t.Errorf("Server.APIPort = %d, want 8080", cfg.Server.APIPort)
	}
	if cfg.Server.APIKey != "" {
		t.Errorf("Server.APIKey = %q, want empty", cfg.Server.APIKey)
	}
}

func TestAccountScheduleEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("MSGVAULT_HOME", tmpDir)

	cfg, err := Load("", "")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if len(cfg.Accounts) != 0 {
		t.Errorf("Accounts = %v, want empty slice", cfg.Accounts)
	}

	scheduled := cfg.ScheduledAccounts()
	if len(scheduled) != 0 {
		t.Errorf("ScheduledAccounts() = %v, want empty slice", scheduled)
	}
}

func TestLoadWithServerConfig(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("MSGVAULT_HOME", tmpDir)

	configContent := `
[server]
api_port = 9090
api_key = "test-secret-key"

[[accounts]]
email = "test@gmail.com"
schedule = "0 2 * * *"
enabled = true

[[accounts]]
email = "other@gmail.com"
schedule = "0 3 * * *"
enabled = false
`
	configPath := filepath.Join(tmpDir, "config.toml")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	cfg, err := Load(configPath, "")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Check server config
	if cfg.Server.APIPort != 9090 {
		t.Errorf("Server.APIPort = %d, want 9090", cfg.Server.APIPort)
	}
	if cfg.Server.APIKey != "test-secret-key" {
		t.Errorf("Server.APIKey = %q, want test-secret-key", cfg.Server.APIKey)
	}

	// Check accounts
	if len(cfg.Accounts) != 2 {
		t.Fatalf("len(Accounts) = %d, want 2", len(cfg.Accounts))
	}

	if cfg.Accounts[0].Email != "test@gmail.com" {
		t.Errorf("Accounts[0].Email = %q, want test@gmail.com", cfg.Accounts[0].Email)
	}
	if cfg.Accounts[0].Schedule != "0 2 * * *" {
		t.Errorf("Accounts[0].Schedule = %q, want '0 2 * * *'", cfg.Accounts[0].Schedule)
	}
	if cfg.Accounts[0].Enabled != true {
		t.Errorf("Accounts[0].Enabled = %v, want true", cfg.Accounts[0].Enabled)
	}
}

func TestScheduledAccounts(t *testing.T) {
	cfg := &Config{
		Accounts: []AccountSchedule{
			{Email: "enabled@gmail.com", Schedule: "0 2 * * *", Enabled: true},
			{Email: "disabled@gmail.com", Schedule: "0 3 * * *", Enabled: false},
			{Email: "noschedule@gmail.com", Schedule: "", Enabled: true},
			{Email: "both@gmail.com", Schedule: "0 4 * * *", Enabled: true},
		},
	}

	scheduled := cfg.ScheduledAccounts()

	if len(scheduled) != 2 {
		t.Fatalf("len(ScheduledAccounts()) = %d, want 2", len(scheduled))
	}

	// Should contain only enabled accounts with schedules
	emails := make(map[string]bool)
	for _, acc := range scheduled {
		emails[acc.Email] = true
	}

	if !emails["enabled@gmail.com"] {
		t.Error("ScheduledAccounts() missing enabled@gmail.com")
	}
	if !emails["both@gmail.com"] {
		t.Error("ScheduledAccounts() missing both@gmail.com")
	}
	if emails["disabled@gmail.com"] {
		t.Error("ScheduledAccounts() should not include disabled account")
	}
	if emails["noschedule@gmail.com"] {
		t.Error("ScheduledAccounts() should not include account without schedule")
	}
}

func TestGetAccountSchedule(t *testing.T) {
	cfg := &Config{
		Accounts: []AccountSchedule{
			{Email: "test@gmail.com", Schedule: "0 2 * * *", Enabled: true},
			{Email: "other@gmail.com", Schedule: "0 3 * * *", Enabled: false},
		},
	}

	tests := []struct {
		email     string
		wantNil   bool
		wantSched string
	}{
		{"test@gmail.com", false, "0 2 * * *"},
		{"other@gmail.com", false, "0 3 * * *"},
		{"notfound@gmail.com", true, ""},
	}

	for _, tt := range tests {
		t.Run(tt.email, func(t *testing.T) {
			acc := cfg.GetAccountSchedule(tt.email)
			if tt.wantNil {
				if acc != nil {
					t.Errorf("GetAccountSchedule(%q) = %v, want nil", tt.email, acc)
				}
				return
			}
			if acc == nil {
				t.Fatalf("GetAccountSchedule(%q) = nil, want non-nil", tt.email)
			}
			if acc.Schedule != tt.wantSched {
				t.Errorf("GetAccountSchedule(%q).Schedule = %q, want %q", tt.email, acc.Schedule, tt.wantSched)
			}
		})
	}
}

func TestGetAccountScheduleReturnsCopy(t *testing.T) {
	cfg := &Config{
		Accounts: []AccountSchedule{
			{Email: "test@gmail.com", Schedule: "0 2 * * *", Enabled: true},
		},
	}

	// Get a reference and mutate it
	acc := cfg.GetAccountSchedule("test@gmail.com")
	if acc == nil {
		t.Fatal("GetAccountSchedule returned nil")
	}

	// Mutate the returned copy
	acc.Schedule = "modified"
	acc.Enabled = false
	acc.Email = "hacked@gmail.com"

	// Original config must be unchanged
	if cfg.Accounts[0].Schedule != "0 2 * * *" {
		t.Errorf("original Schedule = %q, want '0 2 * * *' (mutation leaked)", cfg.Accounts[0].Schedule)
	}
	if cfg.Accounts[0].Enabled != true {
		t.Error("original Enabled = false, want true (mutation leaked)")
	}
	if cfg.Accounts[0].Email != "test@gmail.com" {
		t.Errorf("original Email = %q, want test@gmail.com (mutation leaked)", cfg.Accounts[0].Email)
	}
}

func TestExpandPath(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("failed to get user home dir: %v", err)
	}

	tests := []struct {
		name        string
		input       string
		expected    string
		unixOnly    bool // skip on Windows (uses Unix-style absolute paths)
		windowsOnly bool // skip on non-Windows (quote stripping is Windows-only)
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
			name:        "single-quoted path (Windows CMD)",
			input:       `'C:\Users\wesmc\testing'`,
			expected:    `C:\Users\wesmc\testing`,
			windowsOnly: true,
		},
		{
			name:        "double-quoted path (Windows CMD)",
			input:       `"C:\Users\wesmc\testing"`,
			expected:    `C:\Users\wesmc\testing`,
			windowsOnly: true,
		},
		{
			name:        "single-quoted tilde path",
			input:       "'~/custom-data'",
			expected:    filepath.Join(home, "custom-data"),
			windowsOnly: true,
		},
		{
			name:     "mismatched quotes not stripped",
			input:    `'C:\Users\wesmc"`,
			expected: `'C:\Users\wesmc"`,
		},
		{
			name:     "single char not stripped",
			input:    "'",
			expected: "'",
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
			if tt.windowsOnly && runtime.GOOS != "windows" {
				t.Skip("skipping Windows-specific path test on non-Windows")
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
	cfg, err := Load("", "")
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

	cfg, err := Load("", "")
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

func TestLoadExplicitPathNotFound(t *testing.T) {
	// When --config explicitly specifies a file that doesn't exist, Load should error
	_, err := Load("/nonexistent/path/config.toml", "")
	if err == nil {
		t.Fatal("Load with explicit nonexistent path should return error")
	}
	if got := err.Error(); !strings.Contains(got, "config file not found") {
		t.Errorf("error = %q, want it to contain %q", got, "config file not found")
	}
}

func TestLoadExplicitPathDerivedHomeDir(t *testing.T) {
	// When --config points to a custom location, HomeDir and DataDir
	// should derive from the config file's parent directory
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.toml")

	// Write a minimal config (no data_dir override)
	configContent := `
[oauth]
client_secrets = "/tmp/secret.json"

[sync]
rate_limit_qps = 3
`
	if err := os.WriteFile(configPath, []byte(configContent), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := Load(configPath, "")
	if err != nil {
		t.Fatalf("Load(%q) failed: %v", configPath, err)
	}

	if cfg.HomeDir != tmpDir {
		t.Errorf("HomeDir = %q, want %q", cfg.HomeDir, tmpDir)
	}
	if cfg.Data.DataDir != tmpDir {
		t.Errorf("Data.DataDir = %q, want %q", cfg.Data.DataDir, tmpDir)
	}
	if cfg.Sync.RateLimitQPS != 3 {
		t.Errorf("Sync.RateLimitQPS = %d, want 3", cfg.Sync.RateLimitQPS)
	}

	// Derived paths should use the custom directory
	expectedDB := filepath.Join(tmpDir, "msgvault.db")
	if cfg.DatabaseDSN() != expectedDB {
		t.Errorf("DatabaseDSN() = %q, want %q", cfg.DatabaseDSN(), expectedDB)
	}
	expectedTokens := filepath.Join(tmpDir, "tokens")
	if cfg.TokensDir() != expectedTokens {
		t.Errorf("TokensDir() = %q, want %q", cfg.TokensDir(), expectedTokens)
	}
}

func TestLoadExplicitPathWithDataDirOverride(t *testing.T) {
	// When config file explicitly sets data_dir, that should take precedence
	tmpDir := t.TempDir()
	customDataDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.toml")

	// Use forward slashes in TOML (works cross-platform)
	configContent := `
[data]
data_dir = "` + filepath.ToSlash(customDataDir) + `"
`
	if err := os.WriteFile(configPath, []byte(configContent), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := Load(configPath, "")
	if err != nil {
		t.Fatalf("Load(%q) failed: %v", configPath, err)
	}

	// HomeDir should be config file's directory
	if cfg.HomeDir != tmpDir {
		t.Errorf("HomeDir = %q, want %q", cfg.HomeDir, tmpDir)
	}
	// DataDir should be the explicit override from config.
	// Normalize both sides since TOML preserves forward slashes on Windows.
	if filepath.Clean(cfg.Data.DataDir) != filepath.Clean(customDataDir) {
		t.Errorf("Data.DataDir = %q, want %q", cfg.Data.DataDir, customDataDir)
	}
}

func TestLoadExplicitPathRelativePaths(t *testing.T) {
	// When --config is used, relative data_dir and client_secrets should
	// resolve against the config file's directory, not the working directory.
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.toml")

	configContent := `
[data]
data_dir = "data"

[oauth]
client_secrets = "secrets/client.json"
`
	if err := os.WriteFile(configPath, []byte(configContent), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := Load(configPath, "")
	if err != nil {
		t.Fatalf("Load(%q) failed: %v", configPath, err)
	}

	expectedDataDir := filepath.Join(tmpDir, "data")
	if cfg.Data.DataDir != expectedDataDir {
		t.Errorf("Data.DataDir = %q, want %q", cfg.Data.DataDir, expectedDataDir)
	}

	expectedSecrets := filepath.Join(tmpDir, "secrets/client.json")
	if cfg.OAuth.ClientSecrets != expectedSecrets {
		t.Errorf("OAuth.ClientSecrets = %q, want %q", cfg.OAuth.ClientSecrets, expectedSecrets)
	}
}

func TestLoadExplicitPathWithTilde(t *testing.T) {
	// Explicit --config with ~ should be expanded before stat
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("failed to get user home dir: %v", err)
	}

	// Create a config file in a temp subdir of home to test ~ expansion
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.toml")
	if err := os.WriteFile(configPath, []byte("[sync]\nrate_limit_qps = 7\n"), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	// Construct a ~ path: replace the home prefix with ~
	if !strings.HasPrefix(tmpDir, home) {
		t.Skip("temp dir is not under home directory, cannot test ~ expansion")
	}
	tildePath := "~" + tmpDir[len(home):] + "/config.toml"

	cfg, err := Load(tildePath, "")
	if err != nil {
		t.Fatalf("Load(%q) failed: %v", tildePath, err)
	}

	if cfg.Sync.RateLimitQPS != 7 {
		t.Errorf("Sync.RateLimitQPS = %d, want 7", cfg.Sync.RateLimitQPS)
	}
}

func TestLoadConfigFilePath(t *testing.T) {
	// ConfigFilePath should return the actual loaded path, not the default
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.toml")
	if err := os.WriteFile(configPath, []byte(""), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := Load(configPath, "")
	if err != nil {
		t.Fatalf("Load(%q) failed: %v", configPath, err)
	}

	if cfg.ConfigFilePath() != configPath {
		t.Errorf("ConfigFilePath() = %q, want %q", cfg.ConfigFilePath(), configPath)
	}
}

func TestDefaultHomeExpandsTilde(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("failed to get user home dir: %v", err)
	}

	t.Setenv("MSGVAULT_HOME", "~/.msgvault")
	got := DefaultHome()
	expected := filepath.Join(home, ".msgvault")
	if got != expected {
		t.Errorf("DefaultHome() = %q, want %q", got, expected)
	}
}

// assertTempDirSecured checks that a temp dir has permissions no more
// permissive than 0700. This is umask-tolerant (stricter is fine).
func assertTempDirSecured(t *testing.T, dir string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		return // Windows uses DACLs, not Unix permission bits
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("Stat temp dir: %v", err)
	}
	got := info.Mode().Perm()
	if got&^os.FileMode(0700) != 0 {
		t.Errorf("temp dir perm = %04o, has bits beyond 0700 (extra: %04o)", got, got&^0700)
	}
}

func TestMkTempDir(t *testing.T) {
	t.Run("uses system temp when no preferred dirs", func(t *testing.T) {
		dir, err := MkTempDir("test-*")
		if err != nil {
			t.Fatalf("MkTempDir failed: %v", err)
		}
		defer os.RemoveAll(dir)

		if _, err := os.Stat(dir); err != nil {
			t.Errorf("temp dir does not exist: %v", err)
		}
		assertTempDirSecured(t, dir)
	})

	t.Run("uses preferred dir when available", func(t *testing.T) {
		preferred := t.TempDir()
		dir, err := MkTempDir("test-*", preferred)
		if err != nil {
			t.Fatalf("MkTempDir failed: %v", err)
		}
		defer os.RemoveAll(dir)

		if !strings.HasPrefix(dir, preferred) {
			t.Errorf("temp dir %q not under preferred %q", dir, preferred)
		}
		assertTempDirSecured(t, dir)
	})

	t.Run("skips empty preferred dir strings", func(t *testing.T) {
		dir, err := MkTempDir("test-*", "")
		if err != nil {
			t.Fatalf("MkTempDir failed: %v", err)
		}
		defer os.RemoveAll(dir)

		// Should have used system temp, not errored
		if _, err := os.Stat(dir); err != nil {
			t.Errorf("temp dir does not exist: %v", err)
		}
	})

	t.Run("falls back to system temp when preferred dir is inaccessible", func(t *testing.T) {
		dir, err := MkTempDir("test-*", "/nonexistent-dir-that-does-not-exist")
		if err != nil {
			t.Fatalf("MkTempDir failed: %v", err)
		}
		defer os.RemoveAll(dir)

		// Should have fallen back to system temp
		if strings.Contains(dir, "nonexistent") {
			t.Errorf("should not have used nonexistent dir, got %q", dir)
		}
	})

	t.Run("falls back to msgvault home when system temp is unavailable", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("cannot make system temp dir unwritable on Windows")
		}

		// Create a restricted temp dir so os.MkdirTemp("", ...) fails
		restrictedTmp := t.TempDir()
		if err := os.Chmod(restrictedTmp, 0o500); err != nil {
			t.Fatalf("chmod failed: %v", err)
		}
		t.Cleanup(func() { _ = os.Chmod(restrictedTmp, 0o700) })

		// Probe whether the restriction actually works (root and some ACL
		// configurations can still write to 0500 directories).
		probe, probeErr := os.MkdirTemp(restrictedTmp, "probe-*")
		if probeErr == nil {
			os.Remove(probe)
			t.Skip("chmod 0500 did not restrict writes (running as root or permissive ACLs)")
		}

		// Point TMPDIR to the restricted dir and MSGVAULT_HOME to a writable dir
		msgvaultHome := t.TempDir()
		t.Setenv("TMPDIR", restrictedTmp)
		t.Setenv("MSGVAULT_HOME", msgvaultHome)

		dir, err := MkTempDir("test-*")
		if err != nil {
			t.Fatalf("MkTempDir failed: %v", err)
		}
		defer os.RemoveAll(dir)

		expectedBase := filepath.Join(msgvaultHome, "tmp")
		if !strings.HasPrefix(dir, expectedBase) {
			t.Errorf("temp dir %q not under fallback %q", dir, expectedBase)
		}

		// Verify the tmp dir was created with restrictive permissions
		assertTempDirSecured(t, expectedBase)
		assertTempDirSecured(t, dir)
	})
}

func TestLoadBackslashErrorHint(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{
			name: "invalid escape (backslash G)",
			// \G is not a valid TOML escape → "invalid escape" error
			content: "[data]\ndata_dir = \"C:\\Games\\msgvault\"\n",
		},
		{
			name: "unicode escape (backslash U)",
			// \U is a TOML Unicode escape expecting 8 hex digits → "hexadecimal digits" error
			content: "[data]\ndata_dir = \"C:\\Users\\wesmc\\msgvault\"\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			t.Setenv("MSGVAULT_HOME", tmpDir)

			configPath := filepath.Join(tmpDir, "config.toml")
			if err := os.WriteFile(configPath, []byte(tt.content), 0o644); err != nil {
				t.Fatalf("failed to write config file: %v", err)
			}

			_, err := Load("", "")
			if err == nil {
				t.Fatal("Load should fail on TOML backslash error")
			}

			errMsg := err.Error()
			if !strings.Contains(errMsg, "hint:") {
				t.Errorf("error should contain hint, got: %s", errMsg)
			}
			if !strings.Contains(errMsg, "forward slashes") {
				t.Errorf("error should mention forward slashes, got: %s", errMsg)
			}
			if !strings.Contains(errMsg, "single quotes") {
				t.Errorf("error should mention single quotes, got: %s", errMsg)
			}
		})
	}
}

func TestLoadWithHomeDir(t *testing.T) {
	homeDir := t.TempDir()

	cfg, err := Load("", homeDir)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.HomeDir != homeDir {
		t.Errorf("HomeDir = %q, want %q", cfg.HomeDir, homeDir)
	}
	if cfg.Data.DataDir != homeDir {
		t.Errorf("Data.DataDir = %q, want %q", cfg.Data.DataDir, homeDir)
	}

	// Derived paths should use the home directory
	expectedDB := filepath.Join(homeDir, "msgvault.db")
	if cfg.DatabaseDSN() != expectedDB {
		t.Errorf("DatabaseDSN() = %q, want %q", cfg.DatabaseDSN(), expectedDB)
	}
	expectedTokens := filepath.Join(homeDir, "tokens")
	if cfg.TokensDir() != expectedTokens {
		t.Errorf("TokensDir() = %q, want %q", cfg.TokensDir(), expectedTokens)
	}
}

func TestLoadWithHomeDirReadsConfig(t *testing.T) {
	// --home should load config.toml from that directory
	homeDir := t.TempDir()
	configPath := filepath.Join(homeDir, "config.toml")
	configContent := `[sync]
rate_limit_qps = 42
`
	if err := os.WriteFile(configPath, []byte(configContent), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := Load("", homeDir)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.Sync.RateLimitQPS != 42 {
		t.Errorf("Sync.RateLimitQPS = %d, want 42", cfg.Sync.RateLimitQPS)
	}
	if cfg.HomeDir != homeDir {
		t.Errorf("HomeDir = %q, want %q", cfg.HomeDir, homeDir)
	}
}

func TestLoadWithHomeDirExpandsTilde(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("failed to get user home dir: %v", err)
	}

	cfg, err := Load("", "~/custom-data")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	expected := filepath.Join(home, "custom-data")
	if cfg.HomeDir != expected {
		t.Errorf("HomeDir = %q, want %q", cfg.HomeDir, expected)
	}
	if cfg.Data.DataDir != expected {
		t.Errorf("Data.DataDir = %q, want %q", cfg.Data.DataDir, expected)
	}
}

// TestLoadDeprecatedMCPEnabled verifies that old config files containing the
// removed mcp_enabled field still load successfully. BurntSushi/toml silently
// ignores unknown keys, so existing configs should not break after the field
// was removed from ServerConfig.
func TestLoadDeprecatedMCPEnabled(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("MSGVAULT_HOME", tmpDir)

	configContent := `
[server]
api_port = 9090
mcp_enabled = true
`
	configPath := filepath.Join(tmpDir, "config.toml")
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	cfg, err := Load("", "")
	if err != nil {
		t.Fatalf("Load() should succeed with deprecated mcp_enabled, got error: %v", err)
	}

	if cfg.Server.APIPort != 9090 {
		t.Errorf("Server.APIPort = %d, want 9090", cfg.Server.APIPort)
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
