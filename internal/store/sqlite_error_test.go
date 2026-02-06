package store

import (
	"errors"
	"fmt"
	"testing"

	"github.com/mattn/go-sqlite3"
)

func TestIsSQLiteError_ValueForm(t *testing.T) {
	// Create a sqlite3.Error value
	sqliteErr := sqlite3.Error{
		Code:         sqlite3.ErrConstraint,
		ExtendedCode: sqlite3.ErrConstraintUnique,
	}

	// Wrap the error
	wrappedErr := fmt.Errorf("insert failed: %w", sqliteErr)

	// sqlite3.Error.Error() returns the code description, e.g. "constraint failed"
	if !isSQLiteError(wrappedErr, "constraint failed") {
		t.Errorf("isSQLiteError should match constraint error, got: %v", sqliteErr.Error())
	}

	if isSQLiteError(wrappedErr, "no such table") {
		t.Error("isSQLiteError should not match unrelated substring")
	}
}

func TestIsSQLiteError_PointerForm(t *testing.T) {
	// Create a *sqlite3.Error pointer
	sqliteErr := &sqlite3.Error{
		Code:         sqlite3.ErrConstraint,
		ExtendedCode: sqlite3.ErrConstraintForeignKey,
	}

	// Wrap the error
	wrappedErr := fmt.Errorf("insert failed: %w", sqliteErr)

	// sqlite3.Error.Error() returns the code description, e.g. "constraint failed"
	if !isSQLiteError(wrappedErr, "constraint failed") {
		t.Errorf("isSQLiteError should match constraint error via pointer, got: %v", sqliteErr.Error())
	}

	if isSQLiteError(wrappedErr, "no such table") {
		t.Error("isSQLiteError should not match unrelated substring via pointer")
	}
}

func TestIsSQLiteError_TypedNilPointer(t *testing.T) {
	// Create a typed nil *sqlite3.Error (interface value non-nil, underlying pointer nil)
	var sqliteErr *sqlite3.Error = nil

	// Wrap in an interface to create a typed nil scenario
	// errors.As can succeed with typed nil in certain edge cases
	wrappedErr := typedNilError{sqliteErr}

	// This should not panic - the nil guard should protect us
	result := isSQLiteError(wrappedErr, "any")
	if result {
		t.Error("isSQLiteError should return false for typed nil pointer")
	}
}

func TestIsSQLiteError_NonSQLiteError(t *testing.T) {
	plainErr := errors.New("some other error")

	if isSQLiteError(plainErr, "error") {
		t.Error("isSQLiteError should return false for non-sqlite errors")
	}
}

func TestIsSQLiteError_NilError(t *testing.T) {
	if isSQLiteError(nil, "anything") {
		t.Error("isSQLiteError should return false for nil error")
	}
}

// typedNilError is a helper type that implements error and allows
// errors.As to extract a typed nil *sqlite3.Error
type typedNilError struct {
	err *sqlite3.Error
}

func (e typedNilError) Error() string {
	return "typed nil error wrapper"
}

func (e typedNilError) As(target any) bool {
	if ptr, ok := target.(**sqlite3.Error); ok {
		*ptr = e.err
		return true
	}
	return false
}
