package testutil

import (
	"strings"
	"testing"
	"unicode/utf8"
)

// MakeSet builds a map[T]bool from the given items.
// Useful for constructing selection sets in tests.
func MakeSet[T comparable](items ...T) map[T]bool {
	m := make(map[T]bool, len(items))
	for _, item := range items {
		m[item] = true
	}
	return m
}

// AssertEqualSlices compares two slices element-by-element.
func AssertEqualSlices[T comparable](t *testing.T, got []T, want ...T) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("got len %d, want %d: %v", len(got), len(want), got)
		return
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("at index %d: got %v, want %v", i, got[i], want[i])
		}
	}
}

// AssertStrings compares two string slices element-by-element.
// It provides nicer %q formatting for string values.
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

// AssertStringSet asserts that got contains exactly the expected strings,
// ignoring order. Useful when the slice order is non-deterministic.
// Duplicates are counted: ["a", "a"] does not match ["a", "b"].
func AssertStringSet(t testing.TB, got []string, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("got %d items %v, want %d items %v", len(got), got, len(want), want)
		return
	}

	// Count occurrences in both slices
	gotCounts := make(map[string]int, len(got))
	for _, s := range got {
		gotCounts[s]++
	}
	wantCounts := make(map[string]int, len(want))
	for _, s := range want {
		wantCounts[s]++
	}

	// Check for missing or extra items
	for s, wantN := range wantCounts {
		if gotN := gotCounts[s]; gotN != wantN {
			t.Errorf("item %q: got %d occurrences, want %d (got %v, want %v)", s, gotN, wantN, got, want)
		}
	}
	for s, gotN := range gotCounts {
		if _, ok := wantCounts[s]; !ok {
			t.Errorf("unexpected item %q (%d occurrences) in %v (want %v)", s, gotN, got, want)
		}
	}
}

// MustNoErr fails the test immediately if err is non-nil.
// Use this for setup operations where failure means the test cannot proceed.
func MustNoErr(t *testing.T, err error, msg string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", msg, err)
	}
}

// AssertEqual compares two comparable values and fails the test if they differ.
func AssertEqual[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
