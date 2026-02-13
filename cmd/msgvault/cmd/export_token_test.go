package cmd

import (
	"path/filepath"
	"testing"
)

func TestSanitizeExportTokenPath(t *testing.T) {
	tokensDir := "/data/tokens"

	tests := []struct {
		name  string
		email string
		want  string
	}{
		{
			"normal email",
			"user@gmail.com",
			filepath.Join(tokensDir, "user@gmail.com.json"),
		},
		{
			"email with dots",
			"first.last@example.co.uk",
			filepath.Join(tokensDir, "first.last@example.co.uk.json"),
		},
		{
			"email with plus",
			"user+tag@gmail.com",
			filepath.Join(tokensDir, "user+tag@gmail.com.json"),
		},
		{
			"strips slashes",
			"user/evil@gmail.com",
			filepath.Join(tokensDir, "userevil@gmail.com.json"),
		},
		{
			"strips backslashes",
			"user\\evil@gmail.com",
			filepath.Join(tokensDir, "userevil@gmail.com.json"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeExportTokenPath(tokensDir, tt.email)
			if got != tt.want {
				t.Errorf("sanitizeExportTokenPath(%q) = %q, want %q", tt.email, got, tt.want)
			}
		})
	}
}

func TestEmailValidation(t *testing.T) {
	// These are the inline validation checks from runExportToken.
	// Test them directly to verify the ContainsAny fix.
	tests := []struct {
		name    string
		email   string
		wantErr bool
	}{
		{"normal email", "user@gmail.com", false},
		{"dotted local", "first.last@example.com", false},
		{"dotted domain", "user@mail.example.co.uk", false},
		{"plus tag", "user+tag@gmail.com", false},
		{"missing @", "usergmail.com", true},
		{"missing dot", "user@localhost", true},
		{"path traversal slash", "user/@gmail.com", true},
		{"path traversal backslash", "user\\@gmail.com", true},
		{"double dot traversal", "user@../evil.com", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateExportEmail(tt.email)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateExportEmail(%q) error = %v, wantErr %v",
					tt.email, err, tt.wantErr)
			}
		})
	}
}
