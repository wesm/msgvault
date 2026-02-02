package testutil

import (
	"bytes"
	"testing"
)

func TestEncodedSamplesDefensiveCopy(t *testing.T) {
	first := EncodedSamples()
	original := make([]byte, len(first.ShiftJIS_Konnichiwa))
	copy(original, first.ShiftJIS_Konnichiwa)

	// Mutate the returned slice.
	first.ShiftJIS_Konnichiwa[0] = 0xFF

	// A second call must return the original, unmodified bytes.
	second := EncodedSamples()
	if !bytes.Equal(second.ShiftJIS_Konnichiwa, original) {
		t.Fatalf("EncodedSamples() returned mutated data: got %x, want %x",
			second.ShiftJIS_Konnichiwa, original)
	}
}
