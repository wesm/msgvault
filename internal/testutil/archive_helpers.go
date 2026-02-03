package testutil

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// ArchiveEntry describes a single entry in a tar.gz archive for testing.
type ArchiveEntry struct {
	Name     string
	Content  string
	TypeFlag byte
	LinkName string
	Mode     int64
}

// CreateTarGz creates a tar.gz archive at path containing the given entries.
func CreateTarGz(t *testing.T, path string, entries []ArchiveEntry) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	gzw := gzip.NewWriter(f)
	defer gzw.Close()
	tw := tar.NewWriter(gzw)
	defer tw.Close()

	for _, e := range entries {
		mode := e.Mode
		if mode == 0 {
			mode = 0644
		}
		h := &tar.Header{
			Name:     e.Name,
			Mode:     mode,
			Size:     int64(len(e.Content)),
			Typeflag: e.TypeFlag,
			Linkname: e.LinkName,
		}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if len(e.Content) > 0 {
			if _, err := tw.Write([]byte(e.Content)); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// CreateTempZip creates a zip file in a temporary directory containing the
// provided entries (filename -> content). Returns the path to the zip file.
func CreateTempZip(t *testing.T, entries map[string]string) string {
	t.Helper()

	zipPath := filepath.Join(t.TempDir(), "test.zip")
	f, err := os.Create(zipPath)
	if err != nil {
		t.Fatalf("create zip file: %v", err)
	}
	defer f.Close()

	w := zip.NewWriter(f)
	keys := make([]string, 0, len(entries))
	for name := range entries {
		keys = append(keys, name)
	}
	sort.Strings(keys)
	for _, name := range keys {
		content := entries[name]
		fw, err := w.Create(name)
		if err != nil {
			t.Fatalf("create zip entry %s: %v", name, err)
		}
		if _, err := fw.Write([]byte(content)); err != nil {
			t.Fatalf("write zip entry %s: %v", name, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close zip writer: %v", err)
	}

	return zipPath
}
