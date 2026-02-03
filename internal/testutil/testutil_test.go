package testutil

import (
	"path/filepath"
	"testing"
)

// errRecorder is a minimal testing.TB stub that records Errorf calls
// without calling runtime.Goexit. It wraps a real testing.TB so that any
// unoverridden methods delegate safely instead of panicking on nil.
type errRecorder struct {
	testing.TB // wrapped real TB for safe delegation
	failed     bool
}

// newErrRecorder creates an errRecorder wrapping the given testing.TB.
func newErrRecorder(t testing.TB) *errRecorder {
	return &errRecorder{TB: t}
}

func (e *errRecorder) Helper() {}

func (e *errRecorder) Errorf(format string, args ...interface{}) {
	e.failed = true
}

func (e *errRecorder) Failed() bool {
	return e.failed
}

func TestNewTestStore(t *testing.T) {
	st := NewTestStore(t)

	// Verify store is usable
	stats, err := st.GetStats()
	if err != nil {
		t.Fatalf("get stats: %v", err)
	}

	// Fresh database should have no messages
	if stats.MessageCount != 0 {
		t.Errorf("expected 0 messages, got %d", stats.MessageCount)
	}
}

// validRelativePaths returns a fresh slice of relative paths that should pass
// validation and be writable. Used by TestValidateRelativePath and
// TestWriteFileWithValidPaths.
func validRelativePaths() []string {
	return []string{
		"simple.txt",
		"subdir/file.txt",
		"a/b/c/deep.txt",
		"file-with-dots.test.txt",
		"./current.txt",
		// Paths that look like ".." but are actually valid filenames
		"....",            // four dots - valid filename, not parent escape
		"..foo",           // starts with dots but is a valid filename
		"subdir/..hidden", // hidden-style name in subdir
	}
}

func TestWriteFileAndReadBack(t *testing.T) {
	dir := t.TempDir()
	WriteAndVerifyFile(t, dir, "test.txt", []byte("hello world"))
}

func TestWriteFileSubdir(t *testing.T) {
	dir := t.TempDir()

	WriteAndVerifyFile(t, dir, "subdir/nested/test.txt", []byte("nested content"))
	MustExist(t, filepath.Join(dir, "subdir", "nested"))
}

func TestMustExist(t *testing.T) {
	dir := t.TempDir()
	WriteAndVerifyFile(t, dir, "exists.txt", []byte("data"))
	MustExist(t, dir)
}

func TestMustNotExist(t *testing.T) {
	dir := t.TempDir()

	// Should not panic for non-existent path
	MustNotExist(t, filepath.Join(dir, "does-not-exist.txt"))
}

func TestValidateRelativePath(t *testing.T) {
	dir := t.TempDir()

	// Invalid paths from shared fixture
	for _, tt := range PathTraversalCases() {
		t.Run(tt.Name, func(t *testing.T) {
			if err := validateRelativePath(dir, tt.Path); err == nil {
				t.Errorf("validateRelativePath(%q) expected error, got nil", tt.Path)
			}
		})
	}

	// Valid paths from shared fixture
	for _, path := range validRelativePaths() {
		t.Run("valid "+path, func(t *testing.T) {
			if err := validateRelativePath(dir, path); err != nil {
				t.Errorf("validateRelativePath(%q) unexpected error: %v", path, err)
			}
		})
	}
}

func TestPathTraversalCasesReturnsFreshSlice(t *testing.T) {
	a := PathTraversalCases()
	b := PathTraversalCases()

	// Mutate the first slice and verify the second is unaffected.
	if len(a) == 0 {
		t.Fatal("PathTraversalCases() returned empty slice")
	}
	if len(b) == 0 {
		t.Fatal("PathTraversalCases() returned empty slice on second call")
	}
	original := b[0].Name
	a[0].Name = "MUTATED"
	if b[0].Name != original {
		t.Errorf("PathTraversalCases() returned shared slice: mutating one affected the other")
	}
}

func TestWriteFileWithValidPaths(t *testing.T) {
	dir := t.TempDir()

	for _, name := range validRelativePaths() {
		t.Run(name, func(t *testing.T) {
			WriteAndVerifyFile(t, dir, name, []byte("data"))
		})
	}
}

func TestAssertStringSet(t *testing.T) {
	tests := []struct {
		name       string
		got        []string
		want       []string
		shouldFail bool
	}{
		{
			name:       "exact match",
			got:        []string{"a", "b", "c"},
			want:       []string{"a", "b", "c"},
			shouldFail: false,
		},
		{
			name:       "different order",
			got:        []string{"c", "a", "b"},
			want:       []string{"a", "b", "c"},
			shouldFail: false,
		},
		{
			name:       "duplicates match",
			got:        []string{"a", "a"},
			want:       []string{"a", "a"},
			shouldFail: false,
		},
		{
			name:       "duplicates mismatch different elements",
			got:        []string{"a", "a"},
			want:       []string{"a", "b"},
			shouldFail: true,
		},
		{
			name:       "duplicates mismatch different counts",
			got:        []string{"a", "a", "b"},
			want:       []string{"a", "b", "b"},
			shouldFail: true,
		},
		{
			name:       "empty slices match",
			got:        []string{},
			want:       []string{},
			shouldFail: false,
		},
		{
			name:       "nil vs empty slice",
			got:        nil,
			want:       []string{},
			shouldFail: false,
		},
		{
			name:       "length mismatch",
			got:        []string{"a"},
			want:       []string{"a", "b"},
			shouldFail: true,
		},
		{
			name:       "multiple duplicates match",
			got:        []string{"a", "b", "a", "b", "c"},
			want:       []string{"b", "a", "c", "a", "b"},
			shouldFail: false,
		},
		{
			name:       "unexpected element",
			got:        []string{"a", "b", "c"},
			want:       []string{"a", "b", "d"},
			shouldFail: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := newErrRecorder(t)
			AssertStringSet(rec, tt.got, tt.want...)
			if rec.Failed() != tt.shouldFail {
				if tt.shouldFail {
					t.Errorf("expected AssertStringSet to fail for got=%v, want=%v", tt.got, tt.want)
				} else {
					t.Errorf("expected AssertStringSet to pass for got=%v, want=%v", tt.got, tt.want)
				}
			}
		})
	}
}
