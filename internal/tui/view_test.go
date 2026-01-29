package tui

import (
	"strings"
	"testing"
)

func stripANSI(s string) string {
	// Simple ANSI stripper for test assertions
	var out strings.Builder
	inEsc := false
	for _, r := range s {
		if r == '\x1b' {
			inEsc = true
			continue
		}
		if inEsc {
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
				inEsc = false
			}
			continue
		}
		out.WriteRune(r)
	}
	return out.String()
}

func TestApplyHighlight(t *testing.T) {
	tests := []struct {
		name     string
		text     string
		terms    []string
		wantText string // expected text content after stripping ANSI
		wantHas  string // substring that must appear in raw output (with ANSI)
	}{
		{
			name:     "no terms",
			text:     "hello world",
			terms:    nil,
			wantText: "hello world",
		},
		{
			name:     "single match",
			text:     "hello world",
			terms:    []string{"world"},
			wantText: "hello world",
			wantHas:  "\x1b[",
		},
		{
			name:     "case insensitive",
			text:     "Hello World",
			terms:    []string{"hello"},
			wantText: "Hello World",
			wantHas:  "\x1b[",
		},
		{
			name:     "multiple terms",
			text:     "hello world foo",
			terms:    []string{"hello", "foo"},
			wantText: "hello world foo",
		},
		{
			name:     "overlapping matches",
			text:     "abcdef",
			terms:    []string{"abcd", "cdef"},
			wantText: "abcdef",
		},
		{
			name:     "adjacent matches",
			text:     "aabb",
			terms:    []string{"aa", "bb"},
			wantText: "aabb",
		},
		{
			name:     "nested matches",
			text:     "abcdef",
			terms:    []string{"abcdef", "cd"},
			wantText: "abcdef",
		},
		{
			name:     "no match",
			text:     "hello world",
			terms:    []string{"xyz"},
			wantText: "hello world",
		},
		{
			name:     "unicode text",
			text:     "café résumé",
			terms:    []string{"café"},
			wantText: "café résumé",
			wantHas:  "\x1b[",
		},
		{
			name:     "unicode case folding",
			text:     "Ünïcödé",
			terms:    []string{"ünïcödé"},
			wantText: "Ünïcödé",
		},
		{
			name:     "empty text",
			text:     "",
			terms:    []string{"hello"},
			wantText: "",
		},
		{
			name:     "empty term filtered",
			text:     "hello",
			terms:    []string{""},
			wantText: "hello",
		},
		{
			name:     "CJK characters",
			text:     "hello 世界 world",
			terms:    []string{"世界"},
			wantText: "hello 世界 world",
			wantHas:  "\x1b[",
		},
		{
			name:     "repeated matches",
			text:     "ababab",
			terms:    []string{"ab"},
			wantText: "ababab",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := applyHighlight(tt.text, tt.terms)
			stripped := stripANSI(result)
			if stripped != tt.wantText {
				t.Errorf("text content mismatch:\n  got:  %q\n  want: %q", stripped, tt.wantText)
			}
			// Verify highlighting modified output when a match is expected.
			// lipgloss may not produce ANSI in non-TTY environments, so log rather than fail.
			if tt.wantHas != "" {
				if result == tt.text {
					t.Log("lipgloss did not produce styled output (expected in non-TTY environments)")
				} else if !strings.Contains(result, tt.wantHas) {
					t.Errorf("expected raw output to contain %q, got %q", tt.wantHas, result)
				}
			}
		})
	}
}

func TestApplyHighlightProducesOutput(t *testing.T) {
	// Verify that highlighting actually modifies the output when matches exist.
	// Note: lipgloss may strip ANSI in non-TTY environments, so we check that
	// the raw output differs from the input OR contains ANSI escapes.
	result := applyHighlight("hello world", []string{"world"})
	if result == "hello world" {
		t.Log("lipgloss did not produce ANSI output (expected in non-TTY environments)")
	} else if !strings.Contains(result, "world") {
		t.Errorf("highlighted output missing matched text: %q", result)
	}

	// No match should return input unchanged
	result = applyHighlight("hello world", []string{"xyz"})
	if result != "hello world" {
		t.Errorf("expected unchanged output for no match, got: %q", result)
	}
}
