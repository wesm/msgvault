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

	// Mutate all slice fields in the mutated copy
	for i := 0; i < mutVal.NumField(); i++ {
		field := mutVal.Field(i)
		if field.Kind() == reflect.Slice && field.Len() > 0 {
			// Handle different slice element types
			if field.Type().Elem().Kind() == reflect.Uint8 {
				// For []byte, mutate the first byte
				field.Index(0).Set(reflect.ValueOf(field.Index(0).Interface().(byte) ^ 0xFF))
			} else {
				// For other slice types, use mutateSliceElement to guarantee a change
				mutateSliceElement(t, field, 0)
			}
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
			// Use DeepEqual for generic slice comparison (works for []byte and other types)
			if !reflect.DeepEqual(refField.Interface(), freshField.Interface()) {
				t.Errorf("Field %s was affected by mutation", fieldName)
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

		// Skip unexported fields (reflect cannot access their values).
		// Note: This means unexported fields added to EncodedSamplesT won't be
		// validated by this test. To maintain coverage, keep EncodedSamplesT fields
		// exported, or add explicit tests for any unexported fields.
		if !origField.CanInterface() {
			continue
		}

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

// mutateSliceElement mutates the element at index idx of a slice to guarantee
// a different value. This handles the case where the original value might
// already be zero, making a simple "set to zero" mutation a no-op.
func mutateSliceElement(t *testing.T, slice reflect.Value, idx int) {
	t.Helper()
	elem := slice.Index(idx)
	elemKind := elem.Kind()

	switch elemKind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		// Increment to guarantee change (works even if original is 0)
		elem.SetInt(elem.Int() + 1)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		// Increment to guarantee change (works even if original is 0)
		elem.SetUint(elem.Uint() + 1)
	case reflect.Float32, reflect.Float64:
		// Add 1.0 to guarantee change
		elem.SetFloat(elem.Float() + 1.0)
	case reflect.Bool:
		// Toggle the boolean
		elem.SetBool(!elem.Bool())
	case reflect.String:
		// Append to guarantee change (works even if original is empty)
		elem.SetString(elem.String() + "_mutated")
	case reflect.Struct:
		// For structs, try to mutate the first settable field
		for i := 0; i < elem.NumField(); i++ {
			field := elem.Field(i)
			if field.CanSet() {
				mutateValue(t, field)
				return
			}
		}
		t.Logf("Warning: could not mutate struct element at index %d (no settable fields)", idx)
	case reflect.Ptr:
		if !elem.IsNil() && elem.Elem().CanSet() {
			mutateValue(t, elem.Elem())
		} else {
			t.Logf("Warning: could not mutate pointer element at index %d", idx)
		}
	default:
		t.Logf("Warning: unhandled slice element kind %v at index %d", elemKind, idx)
	}
}

// mutateValue mutates a single reflect.Value to guarantee a different value.
func mutateValue(t *testing.T, v reflect.Value) {
	t.Helper()
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(v.Int() + 1)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(v.Uint() + 1)
	case reflect.Float32, reflect.Float64:
		v.SetFloat(v.Float() + 1.0)
	case reflect.Bool:
		v.SetBool(!v.Bool())
	case reflect.String:
		v.SetString(v.String() + "_mutated")
	default:
		t.Logf("Warning: unhandled value kind %v for mutation", v.Kind())
	}
}
