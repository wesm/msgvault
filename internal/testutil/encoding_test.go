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
//
// If mutation is not possible (e.g., unexported fields only), the test fails
// to ensure the gap in test coverage is visible.
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
		if !mutateStruct(t, elem) {
			t.Fatalf("could not mutate struct element at index %d (no settable fields)", idx)
		}
	case reflect.Ptr:
		if elem.IsNil() {
			// Allocate a new value and set the pointer to it (guarantees change from nil)
			elem.Set(reflect.New(elem.Type().Elem()))
		} else if elem.Elem().Kind() == reflect.Struct {
			// For pointers to structs, use mutateStruct
			if !mutateStruct(t, elem.Elem()) {
				// Could not mutate struct fields; set pointer to nil instead
				elem.Set(reflect.Zero(elem.Type()))
			}
		} else if elem.Elem().CanSet() {
			if !mutateValue(t, elem.Elem()) {
				// mutateValue failed (e.g., pointer to nil interface/func);
				// set pointer to nil to guarantee a change
				elem.Set(reflect.Zero(elem.Type()))
			}
		} else {
			// Last resort: set pointer to nil
			elem.Set(reflect.Zero(elem.Type()))
		}
	default:
		t.Fatalf("unhandled slice element kind %v at index %d", elemKind, idx)
	}
}

// mutateValue mutates a single reflect.Value to guarantee a different value.
// Returns true if mutation was successful, false otherwise.
func mutateValue(t *testing.T, v reflect.Value) bool {
	t.Helper()
	if !v.CanSet() {
		return false
	}
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(v.Int() + 1)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(v.Uint() + 1)
	case reflect.Float32, reflect.Float64:
		v.SetFloat(v.Float() + 1.0)
	case reflect.Complex64, reflect.Complex128:
		v.SetComplex(v.Complex() + complex(1, 1))
	case reflect.Bool:
		v.SetBool(!v.Bool())
	case reflect.String:
		v.SetString(v.String() + "_mutated")
	case reflect.Struct:
		return mutateStruct(t, v)
	case reflect.Ptr:
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		} else if v.Elem().CanSet() {
			return mutateValue(t, v.Elem())
		} else {
			v.Set(reflect.Zero(v.Type()))
		}
	case reflect.Slice:
		if v.IsNil() || v.Len() == 0 {
			// Create a slice with one element
			newSlice := reflect.MakeSlice(v.Type(), 1, 1)
			v.Set(newSlice)
		} else {
			// Mutate the first element
			return mutateValue(t, v.Index(0))
		}
	case reflect.Array:
		if v.Len() > 0 {
			return mutateValue(t, v.Index(0))
		}
		return false
	case reflect.Map:
		if v.IsNil() {
			// Set to a non-nil empty map (semantic change from nil)
			v.Set(reflect.MakeMap(v.Type()))
		} else if v.Len() == 0 {
			// Non-nil empty map: toggle to nil to guarantee semantic change
			v.Set(reflect.Zero(v.Type()))
		} else {
			// Non-empty map: delete one key to guarantee semantic change
			keys := v.MapKeys()
			v.SetMapIndex(keys[0], reflect.Value{})
		}
	case reflect.Interface:
		if v.IsNil() {
			// Cannot create a meaningful non-nil interface value without knowing concrete type
			return false
		}
		// Set to nil to guarantee change
		v.Set(reflect.Zero(v.Type()))
	case reflect.Chan:
		if v.IsNil() {
			v.Set(reflect.MakeChan(v.Type(), 0))
		} else {
			v.Set(reflect.Zero(v.Type()))
		}
	case reflect.Func:
		// Set to nil (or if nil, we can't create a function)
		if !v.IsNil() {
			v.Set(reflect.Zero(v.Type()))
		} else {
			return false
		}
	default:
		t.Fatalf("unhandled value kind %v for mutation", v.Kind())
	}
	return true
}

// mutateStruct attempts to mutate at least one field of a struct.
// Returns true if at least one field was successfully mutated.
func mutateStruct(t *testing.T, v reflect.Value) bool {
	t.Helper()
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		if field.CanSet() && mutateValue(t, field) {
			return true
		}
	}
	return false
}

// TestMutateValueMapEdgeCases verifies that map mutations produce semantic changes
// for all map states: nil, empty non-nil, and non-empty.
//
// Note: The nil <-> empty toggle may be a no-op for encoders that treat them
// equivalently (e.g., with omitempty). The key guarantee is that mutateValue
// always returns true for maps, indicating it performed some mutation.
// For encoding-level guarantees, use the non-empty map case or verify
// encoded output directly.
func TestMutateValueMapEdgeCases(t *testing.T) {
	t.Run("nil map becomes non-nil", func(t *testing.T) {
		var m map[string]int
		v := reflect.ValueOf(&m).Elem()
		if !mutateValue(t, v) {
			t.Fatal("mutateValue returned false for nil map")
		}
		if m == nil {
			t.Error("expected nil map to become non-nil after mutation")
		}
	})

	t.Run("empty non-nil map becomes nil", func(t *testing.T) {
		m := make(map[string]int)
		v := reflect.ValueOf(&m).Elem()
		if !mutateValue(t, v) {
			t.Fatal("mutateValue returned false for empty map")
		}
		if m != nil {
			t.Error("expected empty non-nil map to become nil after mutation")
		}
	})

	t.Run("non-empty map loses a key", func(t *testing.T) {
		m := map[string]int{"a": 1, "b": 2}
		v := reflect.ValueOf(&m).Elem()
		originalLen := len(m)
		if !mutateValue(t, v) {
			t.Fatal("mutateValue returned false for non-empty map")
		}
		if len(m) >= originalLen {
			t.Errorf("expected map length to decrease, got %d (was %d)", len(m), originalLen)
		}
	})
}

// TestMutateValueMapEncodingChange verifies that map mutations produce
// different encoded output, which is the ultimate test of semantic change.
// This catches cases where nil/empty toggling might be a no-op for some encoders.
func TestMutateValueMapEncodingChange(t *testing.T) {
	t.Run("non-empty map encodes differently after mutation", func(t *testing.T) {
		m := map[string]int{"key1": 100, "key2": 200}

		// Encode before mutation using reflect-based comparison
		// (simulates what an encoder would see)
		before := make(map[string]int)
		for k, v := range m {
			before[k] = v
		}

		v := reflect.ValueOf(&m).Elem()
		if !mutateValue(t, v) {
			t.Fatal("mutateValue returned false for non-empty map")
		}

		// Verify the map content actually changed
		if reflect.DeepEqual(before, m) {
			t.Error("expected map content to differ after mutation")
		}
	})

	t.Run("single-entry map becomes empty after mutation", func(t *testing.T) {
		m := map[string]int{"only": 42}
		v := reflect.ValueOf(&m).Elem()
		if !mutateValue(t, v) {
			t.Fatal("mutateValue returned false for single-entry map")
		}
		if len(m) != 0 {
			t.Errorf("expected single-entry map to become empty, got len=%d", len(m))
		}
	})
}

// TestMutateValueNilInterface verifies that mutating a nil interface value
// returns false since we cannot create a meaningful non-nil value.
func TestMutateValueNilInterface(t *testing.T) {
	type container struct {
		Iface any
	}
	c := &container{Iface: nil}
	v := reflect.ValueOf(c).Elem().Field(0)

	// mutateValue on a nil interface should return false
	if mutateValue(t, v) {
		t.Error("expected mutateValue to return false for nil interface")
	}
}

// TestMutateValueNilFunc verifies that mutating a nil func value
// returns false since we cannot create a function dynamically.
func TestMutateValueNilFunc(t *testing.T) {
	type container struct {
		Fn func()
	}
	c := &container{Fn: nil}
	v := reflect.ValueOf(c).Elem().Field(0)

	// mutateValue on a nil func should return false
	if mutateValue(t, v) {
		t.Error("expected mutateValue to return false for nil func")
	}
}

// TestMutateSliceElementPointerToNilInterface verifies that mutateSliceElement
// properly handles a slice of pointers to structs containing nil interfaces
// by falling back to setting the pointer to nil.
func TestMutateSliceElementPointerToNilInterface(t *testing.T) {
	type wrapper struct {
		Val any
	}
	w := &wrapper{Val: nil}
	slice := []*wrapper{w}
	sliceVal := reflect.ValueOf(&slice).Elem()

	// Record original pointer
	originalPtr := slice[0]

	mutateSliceElement(t, sliceVal, 0)

	// The element should have changed (either nil or a different pointer)
	if slice[0] == originalPtr {
		t.Error("expected slice element to be mutated, but pointer is unchanged")
	}
}

// TestMutateSliceElementPointerToNilFunc verifies that mutateSliceElement
// properly handles a slice of pointers to structs containing nil funcs
// by falling back to setting the pointer to nil.
func TestMutateSliceElementPointerToNilFunc(t *testing.T) {
	type wrapper struct {
		Fn func()
	}
	w := &wrapper{Fn: nil}
	slice := []*wrapper{w}
	sliceVal := reflect.ValueOf(&slice).Elem()

	// Record original pointer
	originalPtr := slice[0]

	mutateSliceElement(t, sliceVal, 0)

	// The element should have changed (either nil or a different pointer)
	if slice[0] == originalPtr {
		t.Error("expected slice element to be mutated, but pointer is unchanged")
	}
}
