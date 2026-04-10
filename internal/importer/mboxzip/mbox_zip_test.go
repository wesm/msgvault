package mboxzip

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

const validMbox = "From sender@example.com Mon Jan 1 12:00:00 2024\n" +
	"From: sender@example.com\n" +
	"Subject: Test\n" +
	"\n" +
	"Body.\n"

func TestResolveMboxExport_NoExtension(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "maildata")
	if err := os.WriteFile(p, []byte(validMbox), 0600); err != nil {
		t.Fatal(err)
	}

	files, err := ResolveMboxExport(
		p, dir, slog.Default(),
	)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}
	abs, _ := filepath.Abs(p)
	if files[0] != abs {
		t.Errorf("expected %q, got %q", abs, files[0])
	}
}

func TestResolveMboxExport_NonStandardExtension(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "archive.mail")
	if err := os.WriteFile(p, []byte(validMbox), 0600); err != nil {
		t.Fatal(err)
	}

	files, err := ResolveMboxExport(
		p, dir, slog.Default(),
	)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}
	abs, _ := filepath.Abs(p)
	if files[0] != abs {
		t.Errorf("expected %q, got %q", abs, files[0])
	}
}

func TestResolveMboxExport_StandardExtensionsStillWork(t *testing.T) {
	for _, ext := range []string{".mbox", ".mbx"} {
		t.Run(ext, func(t *testing.T) {
			dir := t.TempDir()
			p := filepath.Join(dir, "archive"+ext)
			if err := os.WriteFile(
				p, []byte(validMbox), 0600,
			); err != nil {
				t.Fatal(err)
			}

			files, err := ResolveMboxExport(
				p, dir, slog.Default(),
			)
			if err != nil {
				t.Fatalf("expected no error, got: %v", err)
			}
			if len(files) != 1 {
				t.Fatalf("expected 1 file, got %d", len(files))
			}
			abs, _ := filepath.Abs(p)
			if files[0] != abs {
				t.Errorf("expected %q, got %q", abs, files[0])
			}
		})
	}
}
