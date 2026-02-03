package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestServerConfigDefaults(t *testing.T) {
	// Create a temp dir without a config file
	tmpDir := t.TempDir()
	os.Setenv("MSGVAULT_HOME", tmpDir)
	defer os.Unsetenv("MSGVAULT_HOME")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Check server defaults
	if cfg.Server.APIPort != 8080 {
		t.Errorf("Server.APIPort = %d, want 8080", cfg.Server.APIPort)
	}
	if cfg.Server.MCPEnabled != false {
		t.Errorf("Server.MCPEnabled = %v, want false", cfg.Server.MCPEnabled)
	}
	if cfg.Server.APIKey != "" {
		t.Errorf("Server.APIKey = %q, want empty", cfg.Server.APIKey)
	}
}

func TestAccountScheduleEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	os.Setenv("MSGVAULT_HOME", tmpDir)
	defer os.Unsetenv("MSGVAULT_HOME")

	cfg, err := Load("")
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
	os.Setenv("MSGVAULT_HOME", tmpDir)
	defer os.Unsetenv("MSGVAULT_HOME")

	configContent := `
[server]
api_port = 9090
api_key = "test-secret-key"
mcp_enabled = true

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

	cfg, err := Load(configPath)
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
	if cfg.Server.MCPEnabled != true {
		t.Errorf("Server.MCPEnabled = %v, want true", cfg.Server.MCPEnabled)
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
		email    string
		wantNil  bool
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
