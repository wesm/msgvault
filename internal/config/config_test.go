package config

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
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

func TestLoadExplicitPathNotFound(t *testing.T) {
	// When --config explicitly specifies a file that doesn't exist, Load should error
	_, err := Load("/nonexistent/path/config.toml")
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

	cfg, err := Load(configPath)
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

	cfg, err := Load(configPath)
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

	cfg, err := Load(configPath)
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

	cfg, err := Load(tildePath)
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

	cfg, err := Load(configPath)
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
	// A TOML file with Windows-style backslash paths should produce a helpful hint
	tmpDir := t.TempDir()
	t.Setenv("MSGVAULT_HOME", tmpDir)

	configPath := filepath.Join(tmpDir, "config.toml")
	// \G is not a valid TOML escape, so this triggers an "invalid escape" error
	configContent := `[data]
data_dir = "C:\Games\msgvault"
`
	if err := os.WriteFile(configPath, []byte(configContent), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	_, err := Load("")
	if err == nil {
		t.Fatal("Load should fail on invalid TOML escape")
	}

	errMsg := err.Error()
	if !strings.Contains(errMsg, "invalid escape") {
		t.Errorf("error should mention invalid escape, got: %s", errMsg)
	}
	if !strings.Contains(errMsg, "hint:") {
		t.Errorf("error should contain hint, got: %s", errMsg)
	}
	if !strings.Contains(errMsg, "forward slashes") {
		t.Errorf("error should mention forward slashes, got: %s", errMsg)
	}
	if !strings.Contains(errMsg, "--home") {
		t.Errorf("error should mention --home flag, got: %s", errMsg)
	}
}

func TestOverrideHome(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("MSGVAULT_HOME", tmpDir)

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	overrideDir := t.TempDir()
	cfg.OverrideHome(overrideDir)

	if cfg.Data.DataDir != overrideDir {
		t.Errorf("Data.DataDir = %q, want %q", cfg.Data.DataDir, overrideDir)
	}
	if cfg.HomeDir != overrideDir {
		t.Errorf("HomeDir = %q, want %q", cfg.HomeDir, overrideDir)
	}

	// Derived paths should use the override directory
	expectedDB := filepath.Join(overrideDir, "msgvault.db")
	if cfg.DatabaseDSN() != expectedDB {
		t.Errorf("DatabaseDSN() = %q, want %q", cfg.DatabaseDSN(), expectedDB)
	}
	expectedTokens := filepath.Join(overrideDir, "tokens")
	if cfg.TokensDir() != expectedTokens {
		t.Errorf("TokensDir() = %q, want %q", cfg.TokensDir(), expectedTokens)
	}
}

func TestOverrideHomeExpandsTilde(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("failed to get user home dir: %v", err)
	}

	cfg := NewDefaultConfig()
	cfg.OverrideHome("~/custom-data")

	expected := filepath.Join(home, "custom-data")
	if cfg.Data.DataDir != expected {
		t.Errorf("Data.DataDir = %q, want %q", cfg.Data.DataDir, expected)
	}
	if cfg.HomeDir != expected {
		t.Errorf("HomeDir = %q, want %q", cfg.HomeDir, expected)
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
