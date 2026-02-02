package tui

import (
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"
)

// ansiStart is the escape sequence prefix found in styled terminal output.
const ansiStart = "\x1b["

// colorProfileMu serializes tests that mutate the global lipgloss color profile.
var colorProfileMu sync.Mutex

// forceColorProfile sets lipgloss to ANSI color output for tests that assert
// on styled output. It acquires colorProfileMu to prevent data races with
// parallel tests and restores the original profile via t.Cleanup.
func forceColorProfile(t *testing.T) {
	t.Helper()
	colorProfileMu.Lock()
	orig := lipgloss.ColorProfile()
	lipgloss.SetColorProfile(termenv.ANSI)
	t.Cleanup(func() {
		lipgloss.SetColorProfile(orig)
		colorProfileMu.Unlock()
	})
}

var ansiPattern = regexp.MustCompile(`\x1b\[[0-9;?]*[ -/]*[@-~]`)

func stripANSI(s string) string {
	return ansiPattern.ReplaceAllString(s, "")
}

// assertHighlight checks that applyHighlight produces the expected plain text
// (after stripping ANSI) and, when wantANSI is true, that the raw output
// contains ANSI escape sequences.
func assertHighlight(t *testing.T, text string, terms []string, wantANSI bool) {
	t.Helper()
	result := applyHighlight(text, terms)
	stripped := stripANSI(result)
	if stripped != text {
		t.Errorf("text content mismatch:\n  got:  %q\n  want: %q", stripped, text)
	}
	if wantANSI {
		if !strings.Contains(result, ansiStart) {
			t.Errorf("expected raw output to contain ANSI escapes, got %q", result)
		}
	}
}

// assertHighlightUnchanged checks that applyHighlight returns the input
// unchanged when no terms match.
func assertHighlightUnchanged(t *testing.T, text string, terms []string) {
	t.Helper()
	result := applyHighlight(text, terms)
	if result != text {
		t.Errorf("expected unchanged output for no match, got: %q", result)
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

func TestApplyHighlightProducesOutput(t *testing.T) {
	forceColorProfile(t)

	// Verify that highlighting actually modifies the output when matches exist.
	result := applyHighlight("hello world", []string{"world"})
	if result == "hello world" {
		t.Errorf("expected styled output to differ from input, got unchanged: %q", result)
	}
	if !strings.Contains(result, "world") {
		t.Errorf("highlighted output missing matched text: %q", result)
	}

	// No match should return input unchanged
	assertHighlightUnchanged(t, "hello world", []string{"xyz"})
}
