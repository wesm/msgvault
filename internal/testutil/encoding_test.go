package testutil

import (
	"bytes"
	"reflect"
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

func TestEncodedSamplesAllSliceFieldsDeepCopied(t *testing.T) {
	// Get a reference copy and a copy to mutate
	reference := EncodedSamples()
	mutated := EncodedSamples()

	refVal := reflect.ValueOf(reference)
	mutVal := reflect.ValueOf(&mutated).Elem()

	// Mutate all byte slice fields in the mutated copy
	for i := 0; i < mutVal.NumField(); i++ {
		field := mutVal.Field(i)
		if field.Kind() == reflect.Slice && field.Len() > 0 {
			// Mutate the first byte
			field.Index(0).Set(reflect.ValueOf(field.Index(0).Interface().(byte) ^ 0xFF))
		}
	}

	// Get a fresh copy and verify it matches the original reference
	fresh := EncodedSamples()
	freshVal := reflect.ValueOf(fresh)

	for i := 0; i < freshVal.NumField(); i++ {
		fieldName := refVal.Type().Field(i).Name
		refField := refVal.Field(i)
		freshField := freshVal.Field(i)

		if refField.Kind() == reflect.Slice {
			refBytes := refField.Bytes()
			freshBytes := freshField.Bytes()
			if !bytes.Equal(refBytes, freshBytes) {
				t.Errorf("Field %s was affected by mutation: original %x, got %x",
					fieldName, refBytes, freshBytes)
			}
		} else if refField.Kind() == reflect.String {
			if refField.String() != freshField.String() {
				t.Errorf("String field %s changed: original %q, got %q",
					fieldName, refField.String(), freshField.String())
			}
		}
	}
}

func TestEncodedSamplesAllFieldsCopied(t *testing.T) {
	// Verify that all fields in the returned struct have values
	// (not left at zero values due to unhandled types)
	samples := EncodedSamples()
	original := reflect.ValueOf(encodedSamples)
	copied := reflect.ValueOf(samples)

	for i := 0; i < original.NumField(); i++ {
		fieldName := original.Type().Field(i).Name
		origField := original.Field(i)
		copyField := copied.Field(i)

		switch origField.Kind() {
		case reflect.Slice:
			if origField.Len() > 0 && copyField.Len() == 0 {
				t.Errorf("Field %s: original has %d elements, copy has 0",
					fieldName, origField.Len())
			}
			if origField.Len() != copyField.Len() {
				t.Errorf("Field %s: length mismatch, original %d, copy %d",
					fieldName, origField.Len(), copyField.Len())
			}
		case reflect.String:
			if origField.String() != copyField.String() {
				t.Errorf("Field %s: string mismatch, original %q, copy %q",
					fieldName, origField.String(), copyField.String())
			}
		default:
			if !reflect.DeepEqual(origField.Interface(), copyField.Interface()) {
				t.Errorf("Field %s: value mismatch", fieldName)
			}
		}
	}
}
