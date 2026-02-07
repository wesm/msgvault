// Package config handles loading and managing msgvault configuration.
package config

import (
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/wesm/msgvault/internal/fileutil"
)

// ChatConfig holds chat/LLM configuration.
type ChatConfig struct {
	Server     string `toml:"server"`      // Ollama server URL
	Model      string `toml:"model"`       // Model name
	MaxResults int    `toml:"max_results"` // Top-K messages to retrieve
}

// ServerConfig holds HTTP API server configuration.
type ServerConfig struct {
	APIPort         int      `toml:"api_port"`         // HTTP server port (default: 8080)
	BindAddr        string   `toml:"bind_addr"`        // Bind address (default: 127.0.0.1)
	APIKey          string   `toml:"api_key"`          // API authentication key
	MCPEnabled      bool     `toml:"mcp_enabled"`      // Enable MCP server endpoint
	AllowInsecure   bool     `toml:"allow_insecure"`   // Allow unauthenticated non-loopback access
	CORSOrigins     []string `toml:"cors_origins"`     // Allowed CORS origins (empty = disabled)
	CORSCredentials bool     `toml:"cors_credentials"` // Allow credentials in CORS
	CORSMaxAge      int      `toml:"cors_max_age"`     // Preflight cache duration in seconds (default: 86400)
}

// IsLoopback returns true if the bind address is a loopback address.
// Handles the full 127.0.0.0/8 range, IPv6 ::1, and "localhost".
func (s ServerConfig) IsLoopback() bool {
	addr := s.BindAddr
	if addr == "" || addr == "localhost" {
		return true
	}
	ip := net.ParseIP(addr)
	return ip != nil && ip.IsLoopback()
}

// ValidateSecure returns an error if the server is configured insecurely
// without an explicit opt-in via allow_insecure.
func (s ServerConfig) ValidateSecure() error {
	if !s.IsLoopback() && s.APIKey == "" && !s.AllowInsecure {
		return fmt.Errorf("refusing to start: bind address %q is not loopback and no api_key is set\n\n"+
			"Set [server] api_key in config.toml, or set allow_insecure = true to override", s.BindAddr)
	}
	return nil
}

// AccountSchedule defines sync schedule for a single account.
type AccountSchedule struct {
	Email    string `toml:"email"`    // Gmail account email
	Schedule string `toml:"schedule"` // Cron expression (e.g., "0 2 * * *" for 2am daily)
	Enabled  bool   `toml:"enabled"`  // Whether scheduled sync is active
}

// Config represents the msgvault configuration.
type Config struct {
	Data     DataConfig        `toml:"data"`
	OAuth    OAuthConfig       `toml:"oauth"`
	Sync     SyncConfig        `toml:"sync"`
	Chat     ChatConfig        `toml:"chat"`
	Server   ServerConfig      `toml:"server"`
	Accounts []AccountSchedule `toml:"accounts"`

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
		Chat: ChatConfig{
			Server:     "http://localhost:11434",
			Model:      "gpt-oss-128k",
			MaxResults: 20,
		},
		Server: ServerConfig{
			APIPort:    8080,
			BindAddr:   "127.0.0.1",
			MCPEnabled: false,
		},
		Accounts: []AccountSchedule{},
	}
}

// Load reads the configuration from the specified file.
// If path is empty, uses the default location (~/.msgvault/config.toml),
// which is optional (missing file returns defaults).
// If path is explicitly provided, the file must exist.
//
// homeDir overrides the home directory (equivalent to MSGVAULT_HOME).
// When set, config.toml is loaded from homeDir unless path is also set.
func Load(path, homeDir string) (*Config, error) {
	explicit := path != ""

	cfg := NewDefaultConfig()

	// --home overrides the default home directory, just like MSGVAULT_HOME.
	if homeDir != "" {
		homeDir = expandPath(homeDir)
		cfg.HomeDir = homeDir
		cfg.Data.DataDir = homeDir
	}

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

	// When --config points to a custom location without --home,
	// derive HomeDir and default DataDir from the config file's parent
	// directory so that tokens, database, attachments, etc. live alongside
	// the config.
	if explicit && homeDir == "" {
		cfg.HomeDir = filepath.Dir(path)
		cfg.Data.DataDir = cfg.HomeDir
	}

	if _, err := toml.DecodeFile(path, cfg); err != nil {
		if strings.Contains(err.Error(), "invalid escape") ||
			strings.Contains(err.Error(), "hexadecimal digits after") {
			return nil, fmt.Errorf("decode config: %w\n\nhint: Windows paths in TOML must use "+
				"forward slashes (C:/Games/msgvault) or single quotes ('C:\\Games\\msgvault').", err)
		}
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
	return fileutil.SecureMkdirAll(c.HomeDir, 0700)
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

// ScheduledAccounts returns accounts with scheduling enabled.
func (c *Config) ScheduledAccounts() []AccountSchedule {
	var scheduled []AccountSchedule
	for _, acc := range c.Accounts {
		if acc.Enabled && acc.Schedule != "" {
			scheduled = append(scheduled, acc)
		}
	}
	return scheduled
}

// GetAccountSchedule returns the schedule for a specific account email.
// Returns nil if the account is not configured for scheduling.
// The returned value is a copy, so mutations won't affect the config.
func (c *Config) GetAccountSchedule(email string) *AccountSchedule {
	for i := range c.Accounts {
		if c.Accounts[i].Email == email {
			acc := c.Accounts[i]
			return &acc
		}
	}
	return nil
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
			secureTempDir(dir)
			return dir, nil
		}
	}

	// Try system temp dir
	dir, sysErr := os.MkdirTemp("", pattern)
	if sysErr == nil {
		secureTempDir(dir)
		return dir, nil
	}

	// Fallback: use ~/.msgvault/tmp/
	fallbackBase := filepath.Join(DefaultHome(), "tmp")
	if err := fileutil.SecureMkdirAll(fallbackBase, 0700); err != nil {
		return "", fmt.Errorf("create temp dir: %w (fallback also failed: %v)", sysErr, err)
	}
	dir, err := os.MkdirTemp(fallbackBase, pattern)
	if err != nil {
		return "", fmt.Errorf("create temp dir: %w (fallback also failed: %v)", sysErr, err)
	}
	secureTempDir(dir)
	return dir, nil
}

// secureTempDir applies owner-only permissions to a temp directory created by
// os.MkdirTemp, which uses default permissions. On Windows, this also sets an
// owner-only DACL. Failures are logged but non-fatal.
func secureTempDir(dir string) {
	if err := fileutil.SecureChmod(dir, 0700); err != nil {
		slog.Warn("failed to secure temp directory permissions", "path", dir, "err", err)
	}
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
// It also strips surrounding single or double quotes, which Windows CMD
// passes through literally (unlike Unix shells which strip them).
func expandPath(path string) string {
	if path == "" {
		return path
	}
	// Strip surrounding quotes left by Windows CMD (e.g. --home 'C:\Users\foo').
	// Only on Windows — Unix shells strip quotes before the process sees them,
	// and literal quote characters in Unix paths are valid (if unusual).
	if runtime.GOOS == "windows" && len(path) >= 2 &&
		((path[0] == '\'' && path[len(path)-1] == '\'') ||
			(path[0] == '"' && path[len(path)-1] == '"')) {
		path = path[1 : len(path)-1]
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
