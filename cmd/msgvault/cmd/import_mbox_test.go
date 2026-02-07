package cmd

import (
	"archive/zip"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeZipFile(t *testing.T, path string, entries map[string]string) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create zip: %v", err)
	}
	defer func() { _ = f.Close() }()

	zw := zip.NewWriter(f)
	for name, content := range entries {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatalf("create zip entry %q: %v", name, err)
		}
		if _, err := w.Write([]byte(content)); err != nil {
			t.Fatalf("write zip entry %q: %v", name, err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("close zip: %v", err)
	}
}

func TestResolveMboxExport_ZipExtractsAndCaches(t *testing.T) {
	tmp := t.TempDir()

	zipPath := filepath.Join(tmp, "export.zip")
	writeZipFile(t, zipPath, map[string]string{
		"inbox.mbox": "From a@b Sat Jan 1 00:00:00 2024\nSubject: x\n\nBody\n",
		"sent.mbx":   "From a@b Sat Jan 1 00:00:01 2024\nSubject: y\n\nBody2\n",
	})

	files1, err := resolveMboxExport(zipPath, tmp)
	if err != nil {
		t.Fatalf("resolveMboxExport: %v", err)
	}
	if len(files1) != 2 {
		t.Fatalf("len(files) = %d, want 2", len(files1))
	}
	if files1[0] == files1[1] {
		t.Fatalf("expected distinct extracted files, got %q", files1[0])
	}

	// Verify files exist and are in the expected extracted directory.
	for _, p := range files1 {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("stat extracted file %q: %v", p, err)
		}
		if !strings.Contains(filepath.Dir(p), filepath.Join(tmp, "imports", "mbox")) {
			t.Fatalf("unexpected extracted dir for %q", p)
		}
	}

	// Second run should reuse the extracted files (sentinel-based caching).
	files2, err := resolveMboxExport(zipPath, tmp)
	if err != nil {
		t.Fatalf("resolveMboxExport (2nd): %v", err)
	}
	if strings.Join(files1, "|") != strings.Join(files2, "|") {
		t.Fatalf("cached files mismatch:\n1=%v\n2=%v", files1, files2)
	}
}

func TestExtractMboxFromZip_DisambiguatesCollidingBaseNames(t *testing.T) {
	tmp := t.TempDir()

	zipPath := filepath.Join(tmp, "export.zip")
	writeZipFile(t, zipPath, map[string]string{
		"a/inbox.mbox": "From a@b Sat Jan 1 00:00:00 2024\nSubject: x\n\nBody\n",
		"b/inbox.mbox": "From a@b Sat Jan 1 00:00:01 2024\nSubject: y\n\nBody2\n",
	})

	destDir := filepath.Join(tmp, "extract")
	files, err := extractMboxFromZip(zipPath, destDir)
	if err != nil {
		t.Fatalf("extractMboxFromZip: %v", err)
	}
	if len(files) != 2 {
		t.Fatalf("len(files) = %d, want 2", len(files))
	}

	b0 := filepath.Base(files[0])
	b1 := filepath.Base(files[1])
	if b0 == b1 {
		t.Fatalf("expected disambiguated output names, got %q", b0)
	}
}

func TestExtractMboxFromZip_FlattensTraversalNamesSafely(t *testing.T) {
	tmp := t.TempDir()

	zipPath := filepath.Join(tmp, "export.zip")
	writeZipFile(t, zipPath, map[string]string{
		"../evil.mbox": "From a@b Sat Jan 1 00:00:00 2024\nSubject: x\n\nBody\n",
	})

	destDir := filepath.Join(tmp, "extract")
	files, err := extractMboxFromZip(zipPath, destDir)
	if err != nil {
		t.Fatalf("extractMboxFromZip: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("len(files) = %d, want 1", len(files))
	}
	if filepath.Dir(files[0]) != destDir {
		t.Fatalf("expected extracted file under destDir; got %q", files[0])
	}
	if filepath.Base(files[0]) != "evil.mbox" {
		t.Fatalf("expected flattened base name evil.mbox, got %q", filepath.Base(files[0]))
	}
}

func TestExtractMboxFromZip_EnforcesEntrySizeLimit(t *testing.T) {
	tmp := t.TempDir()

	zipPath := filepath.Join(tmp, "export.zip")
	writeZipFile(t, zipPath, map[string]string{
		"big.mbox": strings.Repeat("a", 11),
	})

	destDir := filepath.Join(tmp, "extract")
	_, err := extractMboxFromZipWithLimits(zipPath, destDir, zipExtractLimits{
		MaxEntryBytes: 10,
		MaxTotalBytes: 0,
	})
	if err == nil {
		t.Fatalf("expected error")
	}
	if !strings.Contains(err.Error(), "limit") {
		t.Fatalf("expected limit error, got %v", err)
	}
}

func TestExtractMboxFromZip_EnforcesTotalSizeLimit(t *testing.T) {
	tmp := t.TempDir()

	zipPath := filepath.Join(tmp, "export.zip")
	writeZipFile(t, zipPath, map[string]string{
		"a.mbox": strings.Repeat("a", 6),
		"b.mbox": strings.Repeat("b", 6),
	})

	destDir := filepath.Join(tmp, "extract")
	_, err := extractMboxFromZipWithLimits(zipPath, destDir, zipExtractLimits{
		MaxEntryBytes: 100,
		MaxTotalBytes: 10,
	})
	if err == nil {
		t.Fatalf("expected error")
	}
	if !strings.Contains(err.Error(), "limit") {
		t.Fatalf("expected limit error, got %v", err)
	}
}
