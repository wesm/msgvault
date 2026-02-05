// Package config handles loading and managing msgvault configuration.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
)

// Config represents the msgvault configuration.
type Config struct {
	Data  DataConfig  `toml:"data"`
	OAuth OAuthConfig `toml:"oauth"`
	Sync  SyncConfig  `toml:"sync"`

	// Computed paths (not from config file)
	HomeDir    string `toml:"-"`
	configPath string // resolved path to the loaded config file
}

// DataConfig holds data storage configuration.
type DataConfig struct {
	DataDir     string `toml:"data_dir"`
	DatabaseURL string `toml:"database_url"`
}

// OAuthConfig holds OAuth configuration.
type OAuthConfig struct {
	ClientSecrets string `toml:"client_secrets"`
}

// SyncConfig holds sync-related configuration.
type SyncConfig struct {
	RateLimitQPS int `toml:"rate_limit_qps"`
}

// DefaultHome returns the default msgvault home directory.
// Respects MSGVAULT_HOME environment variable and expands ~ in its value.
func DefaultHome() string {
	if h := os.Getenv("MSGVAULT_HOME"); h != "" {
		return expandPath(h)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ".msgvault"
	}
	return filepath.Join(home, ".msgvault")
}

// NewDefaultConfig returns a configuration with default values.
func NewDefaultConfig() *Config {
	homeDir := DefaultHome()
	return &Config{
		HomeDir: homeDir,
		Data: DataConfig{
			DataDir: homeDir,
		},
		Sync: SyncConfig{
			RateLimitQPS: 5,
		},
	}
}

// Load reads the configuration from the specified file.
// If path is empty, uses the default location (~/.msgvault/config.toml),
// which is optional (missing file returns defaults).
// If path is explicitly provided, the file must exist.
func Load(path string) (*Config, error) {
	explicit := path != ""

	cfg := NewDefaultConfig()

	if !explicit {
		path = filepath.Join(cfg.HomeDir, "config.toml")
	} else {
		// Expand ~ for explicit paths (e.g. --config "~/.msgvault/config.toml"
		// where the shell didn't expand it, or on Windows where ~ is never expanded).
		path = expandPath(path)
	}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		if explicit {
			return nil, fmt.Errorf("config file not found: %s", path)
		}
		// Default config file is optional
		return cfg, nil
	}

	cfg.configPath = path

	// When --config points to a custom location, derive HomeDir and
	// default DataDir from the config file's parent directory so that
	// tokens, database, attachments, etc. live alongside the config.
	if explicit {
		cfg.HomeDir = filepath.Dir(path)
		cfg.Data.DataDir = cfg.HomeDir
	}

	if _, err := toml.DecodeFile(path, cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}

	// Expand ~ in paths
	cfg.Data.DataDir = expandPath(cfg.Data.DataDir)
	cfg.OAuth.ClientSecrets = expandPath(cfg.OAuth.ClientSecrets)

	// When --config is used, resolve relative paths against the config file's
	// directory so behavior doesn't depend on the working directory.
	if explicit {
		cfg.Data.DataDir = resolveRelative(cfg.Data.DataDir, cfg.HomeDir)
		cfg.OAuth.ClientSecrets = resolveRelative(cfg.OAuth.ClientSecrets, cfg.HomeDir)
	}

	return cfg, nil
}

// DatabaseDSN returns the database connection string or file path.
func (c *Config) DatabaseDSN() string {
	if c.Data.DatabaseURL != "" {
		return c.Data.DatabaseURL
	}
	return filepath.Join(c.Data.DataDir, "msgvault.db")
}

// AttachmentsDir returns the path to the attachments directory.
func (c *Config) AttachmentsDir() string {
	return filepath.Join(c.Data.DataDir, "attachments")
}

// TokensDir returns the path to the OAuth tokens directory.
func (c *Config) TokensDir() string {
	return filepath.Join(c.Data.DataDir, "tokens")
}

// AnalyticsDir returns the path to the Parquet analytics directory.
func (c *Config) AnalyticsDir() string {
	return filepath.Join(c.Data.DataDir, "analytics")
}

// EnsureHomeDir creates the msgvault home directory if it doesn't exist.
func (c *Config) EnsureHomeDir() error {
	return os.MkdirAll(c.HomeDir, 0700)
}

// ConfigFilePath returns the path to the config file.
// If a config was loaded (including via --config), returns the actual path used.
// Otherwise returns the default location based on HomeDir.
func (c *Config) ConfigFilePath() string {
	if c.configPath != "" {
		return c.configPath
	}
	return filepath.Join(c.HomeDir, "config.toml")
}

// MkTempDir creates a temporary directory with fallback logic for restricted
// environments (e.g. Windows where %TEMP% may be inaccessible due to
// permissions, antivirus, or group policy).
//
// It tries the following locations in order:
//  1. Each directory in preferredDirs (if any)
//  2. The system default temp directory (os.TempDir())
//  3. A "tmp" subdirectory under the msgvault home directory (~/.msgvault/tmp/)
//
// The first successful location is used. If all locations fail, the error
// from the system temp dir attempt is returned along with the final fallback error.
func MkTempDir(pattern string, preferredDirs ...string) (string, error) {
	// Try preferred directories first
	for _, base := range preferredDirs {
		if base == "" {
			continue
		}
		dir, err := os.MkdirTemp(base, pattern)
		if err == nil {
			return dir, nil
		}
	}

	// Try system temp dir
	dir, sysErr := os.MkdirTemp("", pattern)
	if sysErr == nil {
		return dir, nil
	}

	// Fallback: use ~/.msgvault/tmp/
	fallbackBase := filepath.Join(DefaultHome(), "tmp")
	if err := os.MkdirAll(fallbackBase, 0700); err != nil {
		return "", fmt.Errorf("create temp dir: %w (fallback also failed: %v)", sysErr, err)
	}
	dir, err := os.MkdirTemp(fallbackBase, pattern)
	if err != nil {
		return "", fmt.Errorf("create temp dir: %w (fallback also failed: %v)", sysErr, err)
	}
	return dir, nil
}

// resolveRelative makes a relative path absolute by joining it with base.
// Absolute paths and empty strings are returned unchanged.
func resolveRelative(path, base string) string {
	if path == "" || filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(base, path)
}

// expandPath expands ~ to the user's home directory.
// Only expands paths that are exactly "~" or start with "~/".
func expandPath(path string) string {
	if path == "" {
		return path
	}
	if path == "~" || strings.HasPrefix(path, "~"+string(os.PathSeparator)) || strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return path
		}
		if path == "~" {
			return home
		}
		// Trim leading slashes from the suffix to handle cases like "~//foo"
		suffix := path[2:]
		for len(suffix) > 0 && (suffix[0] == '/' || suffix[0] == os.PathSeparator) {
			suffix = suffix[1:]
		}
		return filepath.Join(home, suffix)
	}
	return path
}
