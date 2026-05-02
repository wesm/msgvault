package cmd

import (
	"strings"
	"testing"
)

func TestNormalizeMCPHTTPAddr(t *testing.T) {
	t.Run("bare_port_defaults_to_loopback", func(t *testing.T) {
		got, err := normalizeMCPHTTPAddr("8080", false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "127.0.0.1:8080" {
			t.Fatalf("got %q, want 127.0.0.1:8080", got)
		}
	})

	t.Run("colon_port_defaults_to_loopback", func(t *testing.T) {
		got, err := normalizeMCPHTTPAddr(":8080", false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "127.0.0.1:8080" {
			t.Fatalf("got %q, want 127.0.0.1:8080", got)
		}
	})

	t.Run("explicit_loopback_passes", func(t *testing.T) {
		cases := []string{"127.0.0.1:8080", "localhost:8080", "[::1]:8080"}
		for _, c := range cases {
			got, err := normalizeMCPHTTPAddr(c, false)
			if err != nil {
				t.Errorf("%s: unexpected error: %v", c, err)
				continue
			}
			if got != c {
				t.Errorf("%s: got %q, want unchanged", c, got)
			}
		}
	})

	t.Run("non_loopback_rejected_without_optin", func(t *testing.T) {
		cases := []string{
			"0.0.0.0:8080",
			"192.168.1.5:8080",
			"vault.local:8080",
			// Regression: empty-bracket host parses cleanly via
			// net.SplitHostPort but binds to all interfaces. Must
			// be rejected, not silently treated as loopback.
			"[]:8080",
		}
		for _, c := range cases {
			_, err := normalizeMCPHTTPAddr(c, false)
			if err == nil {
				t.Errorf("%s: expected refusal, got nil", c)
				continue
			}
			if !strings.Contains(err.Error(), "--http-allow-insecure") {
				t.Errorf("%s: expected hint about --http-allow-insecure, got %v", c, err)
			}
		}
	})

	t.Run("non_loopback_allowed_with_optin", func(t *testing.T) {
		got, err := normalizeMCPHTTPAddr("0.0.0.0:8080", true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "0.0.0.0:8080" {
			t.Fatalf("got %q, want unchanged", got)
		}
	})

	t.Run("empty_rejected", func(t *testing.T) {
		_, err := normalizeMCPHTTPAddr("", false)
		if err == nil {
			t.Fatal("expected error for empty addr")
		}
	})

	t.Run("garbage_rejected", func(t *testing.T) {
		_, err := normalizeMCPHTTPAddr("not-a-port", false)
		if err == nil {
			t.Fatal("expected error for non-port, non-host:port")
		}
	})
}
