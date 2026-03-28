package imap

import (
	"context"
	"testing"
)

func TestNewClient_WithTokenSource(t *testing.T) {
	cfg := &Config{
		Host:       "outlook.office365.com",
		Port:       993,
		TLS:        true,
		Username:   "user@company.com",
		AuthMethod: AuthXOAuth2,
	}
	called := false
	ts := func(ctx context.Context) (string, error) {
		called = true
		return "test-token", nil
	}
	c := NewClient(cfg, "", WithTokenSource(ts))
	if c.tokenSource == nil {
		t.Fatal("tokenSource should be set")
	}
	// Verify the token source is callable
	token, err := c.tokenSource(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if token != "test-token" {
		t.Errorf("token = %q, want %q", token, "test-token")
	}
	if !called {
		t.Error("token source was not called")
	}
}
