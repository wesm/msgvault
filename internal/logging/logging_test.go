package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBuildHandler_WritesToFileAndStderr(t *testing.T) {
	dir := t.TempDir()
	var stderr bytes.Buffer
	fixed := time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC)

	res, err := BuildHandler(Options{
		LogsDir:     dir,
		LevelString: "info",
		Stderr:      &stderr,
		Now:         func() time.Time { return fixed },
	})
	if err != nil {
		t.Fatalf("BuildHandler: %v", err)
	}
	defer res.Close()

	logger := slog.New(res.Handler)
	logger.Info("hello", "key", "value")

	// Stderr got a text record.
	if !strings.Contains(stderr.String(), "hello") {
		t.Errorf("stderr missing msg: %q", stderr.String())
	}
	if !strings.Contains(stderr.String(), "run_id="+res.RunID) {
		t.Errorf("stderr missing run_id")
	}

	// Log file path uses today's UTC date.
	want := filepath.Join(dir, "msgvault-2026-04-11.log")
	if res.FilePath != want {
		t.Errorf("FilePath = %q, want %q", res.FilePath, want)
	}

	// File got a JSON record.
	data, err := os.ReadFile(res.FilePath)
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	var rec map[string]any
	if err := json.Unmarshal(
		bytes.TrimSpace(data), &rec,
	); err != nil {
		t.Fatalf("log file is not JSON: %v\n%s", err, data)
	}
	if rec["msg"] != "hello" {
		t.Errorf("msg = %v, want hello", rec["msg"])
	}
	if rec["run_id"] != res.RunID {
		t.Errorf("run_id = %v, want %s", rec["run_id"], res.RunID)
	}
	if rec["level"] != "INFO" {
		t.Errorf("level = %v, want INFO", rec["level"])
	}
}

func TestBuildHandler_FileDisabledKeepsStderr(t *testing.T) {
	var stderr bytes.Buffer
	res, err := BuildHandler(Options{
		FileDisabled: true,
		LevelString:  "info",
		Stderr:       &stderr,
	})
	if err != nil {
		t.Fatalf("BuildHandler: %v", err)
	}
	defer res.Close()

	if res.FilePath != "" {
		t.Errorf("FilePath = %q, want empty", res.FilePath)
	}
	slog.New(res.Handler).Info("no-file")
	if !strings.Contains(stderr.String(), "no-file") {
		t.Errorf("stderr missing msg: %q", stderr.String())
	}
}

func TestBuildHandler_LevelOverrideBeatsLevelString(t *testing.T) {
	var stderr bytes.Buffer
	debug := slog.LevelDebug
	res, err := BuildHandler(Options{
		FileDisabled:  true,
		LevelString:   "error",
		LevelOverride: &debug,
		Stderr:        &stderr,
	})
	if err != nil {
		t.Fatalf("BuildHandler: %v", err)
	}
	defer res.Close()

	if res.Level != slog.LevelDebug {
		t.Errorf("Level = %v, want Debug", res.Level)
	}
	logger := slog.New(res.Handler)
	logger.Debug("dbg-line")
	if !strings.Contains(stderr.String(), "dbg-line") {
		t.Errorf("debug line missing: %q", stderr.String())
	}
}

func TestRotate_RotatesDailyFileOverLimit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "msgvault-2026-04-11.log")
	// Seed a "big" file so BuildHandler will rotate it.
	if err := os.WriteFile(
		path, bytes.Repeat([]byte("x"), 200), 0o600,
	); err != nil {
		t.Fatalf("seed: %v", err)
	}

	res, err := BuildHandler(Options{
		LogsDir:      dir,
		LevelString:  "info",
		MaxFileBytes: 100, // force rotation
		KeepRotated:  3,
		Stderr:       &bytes.Buffer{},
		Now: func() time.Time {
			return time.Date(2026, 4, 11, 0, 0, 0, 0, time.UTC)
		},
	})
	if err != nil {
		t.Fatalf("BuildHandler: %v", err)
	}
	defer res.Close()

	// Old file must now live at .1; new file is path itself.
	if _, err := os.Stat(path + ".1"); err != nil {
		t.Errorf("rotated sibling missing: %v", err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("current log missing: %v", err)
	}
	if fi.Size() >= 200 {
		t.Errorf("new log should start empty or small, size=%d", fi.Size())
	}
}

func TestParseLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"":        slog.LevelInfo,
		"info":    slog.LevelInfo,
		"INFO":    slog.LevelInfo,
		"debug":   slog.LevelDebug,
		"warn":    slog.LevelWarn,
		"warning": slog.LevelWarn,
		"error":   slog.LevelError,
		"garbage": slog.LevelInfo,
	}
	for in, want := range cases {
		if got := parseLevel(in); got != want {
			t.Errorf("parseLevel(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestMultiHandler_FansOutAndFiltersByLevel(t *testing.T) {
	var textBuf, jsonBuf bytes.Buffer
	textH := slog.NewTextHandler(&textBuf, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	})
	jsonH := slog.NewJSONHandler(&jsonBuf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	m := newMultiHandler(textH, jsonH)

	logger := slog.New(m.WithAttrs(
		[]slog.Attr{slog.String("run_id", "abc123")},
	))
	logger.DebugContext(context.Background(), "dbg")
	logger.Warn("warned")

	// Text handler ignores debug, JSON handler keeps it.
	if strings.Contains(textBuf.String(), "dbg") {
		t.Errorf("text handler should not have debug, got %q",
			textBuf.String())
	}
	if !strings.Contains(jsonBuf.String(), "dbg") {
		t.Errorf("json handler missing debug, got %q", jsonBuf.String())
	}
	// Both handlers must see the Warn.
	if !strings.Contains(textBuf.String(), "warned") {
		t.Errorf("text handler missing warn: %q", textBuf.String())
	}
	if !strings.Contains(jsonBuf.String(), "warned") {
		t.Errorf("json handler missing warn: %q", jsonBuf.String())
	}
	// Attr fan-out should include run_id in both.
	if !strings.Contains(jsonBuf.String(), "abc123") {
		t.Errorf("json handler lost run_id: %q", jsonBuf.String())
	}
}
