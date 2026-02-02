package testutil

import (
	"strings"
	"testing"
	"unicode/utf8"
)

// StringSet builds a map[string]bool from the given keys.
// Useful for constructing selection sets in tests.
func StringSet(keys ...string) map[string]bool {
	m := make(map[string]bool, len(keys))
	for _, k := range keys {
		m[k] = true
	}
	return m
}

// IDSet builds a map[int64]bool from the given IDs.
// Useful for constructing ID selection sets in tests.
func IDSet(ids ...int64) map[int64]bool {
	m := make(map[int64]bool, len(ids))
	for _, id := range ids {
		m[id] = true
	}
	return m
}

// AssertStrings compares two string slices element-by-element.
func AssertStrings(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("got len %d, want %d: %v", len(got), len(want), got)
		return
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("at index %d: got %q, want %q", i, got[i], want[i])
		}
	}
}

// AssertValidUTF8 asserts that the given string is valid UTF-8.
func AssertValidUTF8(t *testing.T, s string) {
	t.Helper()
	if !utf8.ValidString(s) {
		t.Errorf("result is not valid UTF-8: %q", s)
	}
}

// AssertContainsAll asserts that got contains every substring in subs.
func AssertContainsAll(t *testing.T, got string, subs []string) {
	t.Helper()
	for _, substr := range subs {
		if !strings.Contains(got, substr) {
			t.Errorf("result %q should contain %q", got, substr)
		}
	}
}
