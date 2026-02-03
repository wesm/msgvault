// Package config handles loading and managing msgvault configuration.
package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
)

// Config represents the msgvault configuration.
// ChatConfig holds chat/LLM configuration.
type ChatConfig struct {
	Server     string `toml:"server"`      // Ollama server URL
	Model      string `toml:"model"`       // Model name
	MaxResults int    `toml:"max_results"` // Top-K messages to retrieve
}

// ServerConfig holds HTTP API server configuration.
type ServerConfig struct {
	APIPort    int    `toml:"api_port"`    // HTTP server port (default: 8080)
	APIKey     string `toml:"api_key"`     // API authentication key
	MCPEnabled bool   `toml:"mcp_enabled"` // Enable MCP server endpoint
}

// AccountSchedule defines sync schedule for a single account.
type AccountSchedule struct {
	Email    string `toml:"email"`    // Gmail account email
	Schedule string `toml:"schedule"` // Cron expression (e.g., "0 2 * * *" for 2am daily)
	Enabled  bool   `toml:"enabled"`  // Whether scheduled sync is active
}

type Config struct {
	Data     DataConfig        `toml:"data"`
	OAuth    OAuthConfig       `toml:"oauth"`
	Sync     SyncConfig        `toml:"sync"`
	Chat     ChatConfig        `toml:"chat"`
	Server   ServerConfig      `toml:"server"`
	Accounts []AccountSchedule `toml:"accounts"`

	// Computed paths (not from config file)
	HomeDir string `toml:"-"`
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
// Respects MSGVAULT_HOME environment variable.
func DefaultHome() string {
	if h := os.Getenv("MSGVAULT_HOME"); h != "" {
		return h
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ".msgvault"
	}
	return filepath.Join(home, ".msgvault")
}

// Load reads the configuration from the specified file.
// If path is empty, uses the default location (~/.msgvault/config.toml).
func Load(path string) (*Config, error) {
	homeDir := DefaultHome()

	if path == "" {
		path = filepath.Join(homeDir, "config.toml")
	}

	cfg := &Config{
		HomeDir: homeDir,
		// Defaults
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
			MCPEnabled: false,
		},
		Accounts: []AccountSchedule{},
	}

	// Config file is optional - use defaults if not present
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return cfg, nil
	}

	if _, err := toml.DecodeFile(path, cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}

	// Expand ~ in paths
	cfg.Data.DataDir = expandPath(cfg.Data.DataDir)
	cfg.OAuth.ClientSecrets = expandPath(cfg.OAuth.ClientSecrets)

	return cfg, nil
}

// DatabasePath returns the path to the SQLite database.
func (c *Config) DatabasePath() string {
	if c.Data.DatabaseURL != "" {
		// If a full URL is specified, it might be PostgreSQL
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
func (c *Config) GetAccountSchedule(email string) *AccountSchedule {
	for i := range c.Accounts {
		if c.Accounts[i].Email == email {
			return &c.Accounts[i]
		}
	}
	return nil
}

// expandPath expands ~ to the user's home directory.
func expandPath(path string) string {
	if path == "" {
		return path
	}
	if path[0] == '~' {
		home, err := os.UserHomeDir()
		if err != nil {
			return path
		}
		return filepath.Join(home, path[1:])
	}
	return path
}
