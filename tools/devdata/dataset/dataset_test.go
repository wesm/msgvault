package dataset

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestValidateDatasetName(t *testing.T) {
	valid := []string{"gold", "dev", "test-data", "my_dataset", "v2", "A", "foo123", "a-b_c"}
	for _, name := range valid {
		if err := ValidateDatasetName(name); err != nil {
			t.Errorf("ValidateDatasetName(%q) = %v, want nil", name, err)
		}
	}

	invalid := []string{
		"",                           // empty
		"../etc",                     // path traversal
		"test db",                    // space
		"'; DROP TABLE messages; --", // SQL injection
		"foo/bar",                    // slash
		"test\x00name",               // null byte
		"hello world",                // space
		"a.b",                        // dot
		"data!",                      // exclamation
		"name\nline",                 // newline
	}
	for _, name := range invalid {
		if err := ValidateDatasetName(name); err == nil {
			t.Errorf("ValidateDatasetName(%q) = nil, want error", name)
		}
	}
}

func TestIsSymlink(t *testing.T) {
	dir := t.TempDir()

	// Real directory
	realDir := filepath.Join(dir, "real")
	if err := os.Mkdir(realDir, 0755); err != nil {
		t.Fatal(err)
	}
	isSym, err := IsSymlink(realDir)
	if err != nil {
		t.Fatal(err)
	}
	if isSym {
		t.Error("expected real directory to not be a symlink")
	}

	// Symlink
	linkPath := filepath.Join(dir, "link")
	if err := os.Symlink(realDir, linkPath); err != nil {
		t.Fatal(err)
	}
	isSym, err = IsSymlink(linkPath)
	if err != nil {
		t.Fatal(err)
	}
	if !isSym {
		t.Error("expected symlink to be detected")
	}

	// Non-existent path
	_, err = IsSymlink(filepath.Join(dir, "nonexistent"))
	if err == nil {
		t.Error("expected error for non-existent path")
	}
}

func TestReadTarget(t *testing.T) {
	dir := t.TempDir()

	target := filepath.Join(dir, "target")
	if err := os.Mkdir(target, 0755); err != nil {
		t.Fatal(err)
	}

	linkPath := filepath.Join(dir, "link")
	if err := os.Symlink(target, linkPath); err != nil {
		t.Fatal(err)
	}

	got, err := ReadTarget(linkPath)
	if err != nil {
		t.Fatal(err)
	}
	if got != target {
		t.Errorf("ReadTarget = %q, want %q", got, target)
	}
}

func TestExists(t *testing.T) {
	dir := t.TempDir()

	if !Exists(dir) {
		t.Error("expected existing directory to return true")
	}

	if Exists(filepath.Join(dir, "nonexistent")) {
		t.Error("expected non-existent path to return false")
	}
}

func TestHasDatabase(t *testing.T) {
	dir := t.TempDir()

	// Without database
	if HasDatabase(dir) {
		t.Error("expected false when no msgvault.db")
	}

	// With database
	dbPath := filepath.Join(dir, "msgvault.db")
	if err := os.WriteFile(dbPath, []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}
	if !HasDatabase(dir) {
		t.Error("expected true when msgvault.db exists")
	}
}

func TestDatabaseSize(t *testing.T) {
	dir := t.TempDir()

	// Without database
	if size := DatabaseSize(dir); size != 0 {
		t.Errorf("expected 0 for missing db, got %d", size)
	}

	// With database
	content := []byte("test data here")
	dbPath := filepath.Join(dir, "msgvault.db")
	if err := os.WriteFile(dbPath, content, 0644); err != nil {
		t.Fatal(err)
	}
	if size := DatabaseSize(dir); size != int64(len(content)) {
		t.Errorf("expected %d, got %d", len(content), size)
	}
}

func TestReplaceSymlink(t *testing.T) {
	// Discussion of options for this test in Windows
	// https://github.com/wesm/msgvault/pull/101#issuecomment-3867402422
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation requires elevated privileges on Windows")
	}

	dir := t.TempDir()

	// Create two target directories
	targetA := filepath.Join(dir, "target-a")
	targetB := filepath.Join(dir, "target-b")
	if err := os.Mkdir(targetA, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(targetB, 0755); err != nil {
		t.Fatal(err)
	}

	// Create initial symlink to A
	linkPath := filepath.Join(dir, "link")
	if err := os.Symlink(targetA, linkPath); err != nil {
		t.Fatal(err)
	}

	// Replace with B
	if err := ReplaceSymlink(linkPath, targetB); err != nil {
		t.Fatal(err)
	}

	// Verify now points to B
	got, err := ReadTarget(linkPath)
	if err != nil {
		t.Fatal(err)
	}
	if got != targetB {
		t.Errorf("after replace, target = %q, want %q", got, targetB)
	}
}

func TestReplaceSymlink_RefusesRealDirectory(t *testing.T) {
	dir := t.TempDir()

	// Create a real directory (not a symlink)
	realDir := filepath.Join(dir, "real")
	if err := os.Mkdir(realDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Put a file in it to verify it's not deleted
	sentinel := filepath.Join(realDir, "important.txt")
	if err := os.WriteFile(sentinel, []byte("keep"), 0644); err != nil {
		t.Fatal(err)
	}

	// ReplaceSymlink should refuse
	target := filepath.Join(dir, "target")
	if err := os.Mkdir(target, 0755); err != nil {
		t.Fatal(err)
	}

	err := ReplaceSymlink(realDir, target)
	if err == nil {
		t.Fatal("expected error when replacing a real directory")
	}

	// Verify directory and contents still exist
	if !Exists(sentinel) {
		t.Error("real directory was deleted — safety check failed")
	}
}

func TestListDatasets(t *testing.T) {
	dir := t.TempDir()

	// Create dataset directories
	fooDir := filepath.Join(dir, ".msgvault-foo")
	barDir := filepath.Join(dir, ".msgvault-bar")
	if err := os.Mkdir(fooDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(barDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Add db to foo only
	if err := os.WriteFile(filepath.Join(fooDir, "msgvault.db"), []byte("db"), 0644); err != nil {
		t.Fatal(err)
	}

	// Create symlink for .msgvault -> .msgvault-foo
	mvPath := filepath.Join(dir, ".msgvault")
	if err := os.Symlink(fooDir, mvPath); err != nil {
		t.Fatal(err)
	}

	datasets, err := ListDatasets(dir)
	if err != nil {
		t.Fatal(err)
	}

	if len(datasets) != 2 {
		t.Fatalf("expected 2 datasets, got %d", len(datasets))
	}

	// Should be sorted: bar, foo
	if datasets[0].Name != "bar" {
		t.Errorf("datasets[0].Name = %q, want %q", datasets[0].Name, "bar")
	}
	if datasets[1].Name != "foo" {
		t.Errorf("datasets[1].Name = %q, want %q", datasets[1].Name, "foo")
	}

	// foo should have DB and be active
	if !datasets[1].HasDB {
		t.Error("foo should have HasDB = true")
	}
	if !datasets[1].Active {
		t.Error("foo should be active (symlink target)")
	}

	// bar should not have DB and not be active
	if datasets[0].HasDB {
		t.Error("bar should have HasDB = false")
	}
	if datasets[0].Active {
		t.Error("bar should not be active")
	}
}

func TestListDatasets_NoSymlink(t *testing.T) {
	dir := t.TempDir()

	// Create .msgvault as real directory
	mvPath := filepath.Join(dir, ".msgvault")
	if err := os.Mkdir(mvPath, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mvPath, "msgvault.db"), []byte("db"), 0644); err != nil {
		t.Fatal(err)
	}

	datasets, err := ListDatasets(dir)
	if err != nil {
		t.Fatal(err)
	}

	if len(datasets) != 1 {
		t.Fatalf("expected 1 dataset, got %d", len(datasets))
	}

	if datasets[0].Name != "(default)" {
		t.Errorf("datasets[0].Name = %q, want %q", datasets[0].Name, "(default)")
	}
	if !datasets[0].IsDefault {
		t.Error("expected IsDefault = true")
	}
	if !datasets[0].Active {
		t.Error("expected Active = true for default")
	}
	if !datasets[0].HasDB {
		t.Error("expected HasDB = true")
	}
}
