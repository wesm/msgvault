package testutil

import (
	"bytes"
	"testing"
)

func TestEncodedSamplesDefensiveCopy(t *testing.T) {
	first := EncodedSamples()
	target := first.ShiftJIS_Konnichiwa

	if len(target) == 0 {
		t.Fatal("ShiftJIS_Konnichiwa sample is empty, cannot test mutation")
	}

	original := bytes.Clone(target)

	// Mutate the returned slice.
	first.ShiftJIS_Konnichiwa[0] ^= 0xFF

	// A second call must return the original, unmodified bytes.
	second := EncodedSamples()
	if !bytes.Equal(second.ShiftJIS_Konnichiwa, original) {
		t.Fatalf("EncodedSamples() returned mutated data: got %x, want %x",
			second.ShiftJIS_Konnichiwa, original)
	}
}
