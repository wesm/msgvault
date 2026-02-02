package testutil

import (
	"strings"
	"testing"
	"unicode/utf8"
)

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
