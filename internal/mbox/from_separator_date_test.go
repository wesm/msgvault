package mbox

import (
	"testing"
	"time"
)

func TestParseFromSeparatorDateStrict_ParsesKnownTZAbbrev(t *testing.T) {
	line := "From a@b Mon Jan 1 00:00:00 PST 2024"
	ts, ok := ParseFromSeparatorDateStrict(line)
	if !ok {
		t.Fatalf("expected ok")
	}
	if got, want := ts.UTC().Format(time.RFC3339), "2024-01-01T08:00:00Z"; got != want {
		t.Fatalf("ts=%q, want %q", got, want)
	}
}

func TestParseFromSeparatorDateStrict_ParsesKnownTZAbbrevAfterYear(t *testing.T) {
	line := "From a@b Mon Jan 1 00:00:00 2024 PST"
	ts, ok := ParseFromSeparatorDateStrict(line)
	if !ok {
		t.Fatalf("expected ok")
	}
	if got, want := ts.UTC().Format(time.RFC3339), "2024-01-01T08:00:00Z"; got != want {
		t.Fatalf("ts=%q, want %q", got, want)
	}
}

func TestParseFromSeparatorDateStrict_ParsesNumericOffset(t *testing.T) {
	line := "From a@b Mon Jan 1 00:00:00 -0700 2024"
	ts, ok := ParseFromSeparatorDateStrict(line)
	if !ok {
		t.Fatalf("expected ok")
	}
	if got, want := ts.UTC().Format(time.RFC3339), "2024-01-01T07:00:00Z"; got != want {
		t.Fatalf("ts=%q, want %q", got, want)
	}
}

func TestParseFromSeparatorDateStrict_RejectsUnknownTZAbbrev(t *testing.T) {
	line := "From a@b Mon Jan 1 00:00:00 FOO 2024"
	if _, ok := ParseFromSeparatorDateStrict(line); ok {
		t.Fatalf("expected not ok")
	}
	lineAfterYear := "From a@b Mon Jan 1 00:00:00 2024 FOO"
	if _, ok := ParseFromSeparatorDateStrict(lineAfterYear); ok {
		t.Fatalf("expected not ok")
	}
	if _, ok := ParseFromSeparatorDate(line); !ok {
		t.Fatalf("expected permissive ParseFromSeparatorDate to accept line for separator detection")
	}
}
