package cmd

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestErrOAuthNotConfigured(t *testing.T) {
	err := errOAuthNotConfigured()

	if err == nil {
		t.Fatal("errOAuthNotConfigured() = nil, want error")
	}

	msg := err.Error()

	// Should contain the main message
	if !strings.Contains(msg, "OAuth client secrets not configured") {
		t.Errorf("error message missing 'not configured': %q", msg)
	}

	// Should contain setup URL
	if !strings.Contains(msg, "https://msgvault.io/guides/oauth-setup/") {
		t.Errorf("error message missing setup URL: %q", msg)
	}

	// Should contain config.toml instructions
	if !strings.Contains(msg, "config.toml") {
		t.Errorf("error message missing config.toml reference: %q", msg)
	}
}

func TestWrapOAuthError_NotExist(t *testing.T) {
	originalErr := fmt.Errorf("open /path/to/secrets.json: %w", os.ErrNotExist)

	wrapped := wrapOAuthError(originalErr)

	msg := wrapped.Error()

	// Should contain accessible message (not "not found" anymore)
	if !strings.Contains(msg, "not accessible") {
		t.Errorf("error message missing 'not accessible': %q", msg)
	}

	// Should contain setup hint
	if !strings.Contains(msg, "https://msgvault.io/guides/oauth-setup/") {
		t.Errorf("error message missing setup URL: %q", msg)
	}
}

func TestWrapOAuthError_Permission(t *testing.T) {
	originalErr := fmt.Errorf("open /path/to/secrets.json: %w", os.ErrPermission)

	wrapped := wrapOAuthError(originalErr)

	msg := wrapped.Error()

	// Should contain accessible message
	if !strings.Contains(msg, "not accessible") {
		t.Errorf("error message missing 'not accessible': %q", msg)
	}

	// Should contain setup hint
	if !strings.Contains(msg, "https://msgvault.io/guides/oauth-setup/") {
		t.Errorf("error message missing setup URL: %q", msg)
	}
}

func TestWrapOAuthError_OtherError(t *testing.T) {
	originalErr := errors.New("some other error")

	wrapped := wrapOAuthError(originalErr)

	// Should return the original error unchanged
	if wrapped != originalErr {
		t.Errorf("wrapOAuthError() changed unrelated error: got %v, want %v", wrapped, originalErr)
	}
}

func TestWrapOAuthError_NestedNotExist(t *testing.T) {
	// Test that errors.Is can find nested os.ErrNotExist
	innerErr := fmt.Errorf("file error: %w", os.ErrNotExist)
	outerErr := fmt.Errorf("oauth manager: %w", innerErr)

	wrapped := wrapOAuthError(outerErr)

	msg := wrapped.Error()

	// Should detect the nested os.ErrNotExist and wrap appropriately
	if !strings.Contains(msg, "not accessible") {
		t.Errorf("failed to detect nested os.ErrNotExist: %q", msg)
	}
}
