package query

import (
	"errors"
	"fmt"
	"testing"
)

func TestIsEncodingError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{"unrelated error", errors.New("connection refused"), false},
		{"encoding error", errors.New("Invalid string encoding found in Parquet file"), true},
		{"wrapped encoding error", fmt.Errorf("aggregate query: %w", errors.New("Invalid string encoding found in Parquet file")), true},
		{"encoding error substring", errors.New("scan: Invalid string encoding found in Parquet file foo.parquet"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsEncodingError(tt.err); got != tt.want {
				t.Errorf("IsEncodingError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHintRepairEncoding(t *testing.T) {
	t.Run("nil error", func(t *testing.T) {
		if got := HintRepairEncoding(nil); got != nil {
			t.Errorf("HintRepairEncoding(nil) = %v, want nil", got)
		}
	})

	t.Run("unrelated error passes through", func(t *testing.T) {
		orig := errors.New("something else")
		got := HintRepairEncoding(orig)
		if got != orig {
			t.Errorf("HintRepairEncoding should return original error unchanged, got %v", got)
		}
	})

	t.Run("encoding error gets hint", func(t *testing.T) {
		orig := errors.New("Invalid string encoding found in Parquet file")
		got := HintRepairEncoding(orig)
		if got == nil {
			t.Fatal("HintRepairEncoding returned nil")
		}
		msg := got.Error()
		if want := "repair-encoding"; !containsSubstring(msg, want) {
			t.Errorf("expected hint containing %q, got: %s", want, msg)
		}
		// Original error should be preserved in the chain
		if !errors.Is(got, orig) {
			t.Error("wrapped error should preserve original via errors.Is")
		}
	})

	t.Run("wrapped encoding error gets hint", func(t *testing.T) {
		inner := errors.New("Invalid string encoding found in Parquet file")
		wrapped := fmt.Errorf("aggregate query: %w", inner)
		got := HintRepairEncoding(wrapped)
		msg := got.Error()
		if want := "repair-encoding"; !containsSubstring(msg, want) {
			t.Errorf("expected hint containing %q, got: %s", want, msg)
		}
	})
}

func containsSubstring(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && contains(s, sub))
}

func contains(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
