package tui

import (
	"strings"
	"testing"
)

// assertHighlight checks that applyHighlight produces the expected plain text
// (after stripping ANSI) and validates ANSI behavior based on wantANSI:
// - wantANSI=true: output must contain ANSI escapes and differ from input
// - wantANSI=false: output must be unchanged from input
func assertHighlight(t *testing.T, text string, terms []string, wantANSI bool) {
	t.Helper()
	result := applyHighlight(text, terms)
	stripped := stripANSI(result)

	// Content integrity check
	if stripped != text {
		t.Errorf("text content mismatch:\n  got:  %q\n  want: %q", stripped, text)
	}

	// ANSI/change check
	if wantANSI {
		if result == text {
			t.Errorf("expected highlighting (ANSI) but output was unchanged")
		}
		if !strings.Contains(result, ansiStart) {
			t.Errorf("expected output to contain ANSI start sequence, got %q", result)
		}
	} else {
		if result != text {
			t.Errorf("expected unchanged output, got: %q", result)
		}
	}
}

func TestApplyHighlight(t *testing.T) {
	tests := []struct {
		name     string
		text     string
		terms    []string
		wantANSI bool
	}{
		{"no terms", "hello world", nil, false},
		{"single match", "hello world", []string{"world"}, true},
		{"case insensitive", "Hello World", []string{"hello"}, true},
		{"multiple terms", "hello world foo", []string{"hello", "foo"}, true},
		{"overlapping matches", "abcdef", []string{"abcd", "cdef"}, true},
		{"adjacent matches", "aabb", []string{"aa", "bb"}, true},
		{"nested matches", "abcdef", []string{"abcdef", "cd"}, true},
		{"no match", "hello world", []string{"xyz"}, false},
		{"unicode text", "café résumé", []string{"café"}, true},
		{"unicode case folding", "Ünïcödé", []string{"ünïcödé"}, true},
		{"empty text", "", []string{"hello"}, false},
		{"empty term filtered", "hello", []string{""}, false},
		{"CJK characters", "hello 世界 world", []string{"世界"}, true},
		{"repeated matches", "ababab", []string{"ab"}, true},
	}

	forceColorProfile(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertHighlight(t, tt.text, tt.terms, tt.wantANSI)
		})
	}
}
