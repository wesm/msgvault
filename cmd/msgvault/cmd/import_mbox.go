package cmd

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/internal/export"
	"github.com/wesm/msgvault/internal/fileutil"
	"github.com/wesm/msgvault/internal/importer"
	"github.com/wesm/msgvault/internal/store"
)

var (
	importMboxSourceType         string
	importMboxLabel              string
	importMboxNoResume           bool
	importMboxCheckpointInterval int
	importMboxNoAttachments      bool
)

type mboxCheckpoint struct {
	File   string `json:"file"`
	Offset int64  `json:"offset"`
	Seq    int64  `json:"seq,omitempty"`
}

var errZipExtractLimitExceeded = errors.New("zip extraction limit exceeded")

// These are intentionally very high defaults; they exist to prevent zip-bomb
// style resource exhaustion while still supporting large real-world exports.
const (
	defaultMaxZipEntryBytes int64 = 50 << 30  // 50 GiB per extracted mbox file
	defaultMaxZipTotalBytes int64 = 200 << 30 // 200 GiB total extracted bytes
)

type zipExtractLimits struct {
	MaxEntryBytes int64
	MaxTotalBytes int64
}

var importMboxCmd = &cobra.Command{
	Use:   "import-mbox <identifier> <export-file>",
	Short: "Import an MBOX export into msgvault",
	Long: `Import an MBOX export into msgvault.

The export file may be a plain .mbox/.mbx file or a .zip containing one or
more .mbox files.

This is useful for email providers that offer an export but no IMAP/POP access.
The importer stores raw MIME, bodies, recipients, and optional attachments.

Examples:
  msgvault init-db
  msgvault import-mbox you@example.com /path/to/export.mbox
  msgvault import-mbox you@example.com /path/to/export.zip

  # HEY.com export (still MBOX)
  msgvault import-mbox you@hey.com hey-export.zip --source-type hey --label hey
`,
	Args:         cobra.ExactArgs(2),
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		identifier := args[0]
		exportPath := args[1]

		// Handle Ctrl+C gracefully (save checkpoint and exit cleanly).
		ctx, cancel := context.WithCancel(cmd.Context())
		defer cancel()
		sigChan := make(chan os.Signal, 2)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		done := make(chan struct{})
		defer func() {
			close(done)
			signal.Stop(sigChan)
			for {
				select {
				case <-sigChan:
					// Drain queued signals to avoid late os.Exit(130) during teardown.
				default:
					return
				}
			}
		}()
		go func() {
			signals := 0
			for {
				select {
				case <-done:
					return
				case <-sigChan:
					select {
					case <-done:
						return
					default:
					}
					signals++
					if signals == 1 {
						fmt.Fprintln(cmd.ErrOrStderr(), "\nInterrupted. Saving checkpoint...")
						cancel()
						continue
					}
					fmt.Fprintln(cmd.ErrOrStderr(), "Interrupted again. Exiting immediately.")
					os.Exit(130)
				}
			}
		}()

		dbPath := cfg.DatabaseDSN()
		st, err := store.Open(dbPath)
		if err != nil {
			return fmt.Errorf("open database: %w", err)
		}
		defer st.Close()

		if err := st.InitSchema(); err != nil {
			return fmt.Errorf("init schema: %w", err)
		}

		attachmentsDir := cfg.AttachmentsDir()
		if importMboxNoAttachments {
			attachmentsDir = ""
		}

		mboxFiles, err := resolveMboxExport(exportPath, cfg.Data.DataDir)
		if err != nil {
			return err
		}

		// If we're resuming, start from the active file in a multi-file zip export.
		if !importMboxNoResume {
			src, err := st.GetOrCreateSource(importMboxSourceType, identifier)
			if err != nil {
				return fmt.Errorf("get/create source: %w", err)
			}
			active, err := st.GetActiveSync(src.ID)
			if err != nil {
				return fmt.Errorf("check active sync: %w", err)
			}
			if active != nil && active.CursorBefore.Valid && active.CursorBefore.String != "" {
				var cp mboxCheckpoint
				if err := json.Unmarshal([]byte(active.CursorBefore.String), &cp); err == nil && cp.File != "" {
					cpFile := cp.File
					if abs, err := filepath.Abs(cpFile); err == nil {
						cpFile = abs
					}
					cpFile = filepath.Clean(cpFile)
					if resolved, err := filepath.EvalSymlinks(cpFile); err == nil {
						cpFile = resolved
					}
					cpInfo, cpInfoErr := os.Stat(cpFile)

					startIdx := -1
					for i, p := range mboxFiles {
						pp := p
						if abs, err := filepath.Abs(pp); err == nil {
							pp = abs
						}
						pp = filepath.Clean(pp)

						if pp == cpFile {
							startIdx = i
							break
						}
						if cpInfoErr == nil {
							if info, err := os.Stat(pp); err == nil && os.SameFile(cpInfo, info) {
								startIdx = i
								break
							}
						}
					}
					if startIdx == -1 {
						return fmt.Errorf("active mbox import is for a different file (%q); rerun with --no-resume to start fresh", cp.File)
					}
					mboxFiles = mboxFiles[startIdx:]
				}
			} else if len(mboxFiles) > 1 {
				// If we don't have an active sync, fall back to the last successful
				// sync run. This avoids rescanning already-finished files when a
				// multi-file import is interrupted between files.
				last, err := st.GetLastSuccessfulSync(src.ID)
				if err != nil {
					return fmt.Errorf("check last successful sync: %w", err)
				}
				if last != nil && last.CursorBefore.Valid && last.CursorBefore.String != "" {
					var cp mboxCheckpoint
					if err := json.Unmarshal([]byte(last.CursorBefore.String), &cp); err == nil && cp.File != "" {
						cpFile := cp.File
						if abs, err := filepath.Abs(cpFile); err == nil {
							cpFile = abs
						}
						cpFile = filepath.Clean(cpFile)
						if resolved, err := filepath.EvalSymlinks(cpFile); err == nil {
							cpFile = resolved
						}
						cpInfo, cpInfoErr := os.Stat(cpFile)

						startIdx := -1
						var matchedPath string
						for i, p := range mboxFiles {
							pp := p
							if abs, err := filepath.Abs(pp); err == nil {
								pp = abs
							}
							pp = filepath.Clean(pp)

							if pp == cpFile {
								startIdx = i
								matchedPath = p
								break
							}
							if cpInfoErr == nil {
								if info, err := os.Stat(pp); err == nil && os.SameFile(cpInfo, info) {
									startIdx = i
									matchedPath = p
									break
								}
							}
						}
						if startIdx != -1 {
							cursorOK := true
							if st, err := os.Stat(matchedPath); err == nil {
								if cp.Offset == st.Size() {
									startIdx++
								} else if cp.Offset > st.Size() {
									// Ignore an invalid cursor (beyond EOF). Falling back to scanning
									// all files is safer than skipping the matched file.
									cursorOK = false
								}
							}
							if cursorOK {
								if startIdx >= len(mboxFiles) {
									mboxFiles = nil
								} else {
									mboxFiles = mboxFiles[startIdx:]
								}
							}
						}
					}
				}
			}
		}

		var (
			totalProcessed int64
			totalAdded     int64
			totalUpdated   int64
			totalSkipped   int64
			totalErrors    int64
			totalBytes     int64
			hadHardErrors  bool
		)
		type processedFile struct {
			Path    string
			Partial bool
		}
		processedFiles := make([]processedFile, 0, len(mboxFiles))

		for _, mboxPath := range mboxFiles {
			summary, err := importer.ImportMbox(ctx, st, mboxPath, importer.MboxImportOptions{
				SourceType:         importMboxSourceType,
				Identifier:         identifier,
				Label:              importMboxLabel,
				NoResume:           importMboxNoResume,
				CheckpointInterval: importMboxCheckpointInterval,
				AttachmentsDir:     attachmentsDir,
				Logger:             logger,
			})
			if err != nil {
				return err
			}

			totalProcessed += summary.MessagesProcessed
			totalAdded += summary.MessagesAdded
			totalUpdated += summary.MessagesUpdated
			totalSkipped += summary.MessagesSkipped
			totalErrors += summary.Errors
			totalBytes += summary.BytesProcessed
			if summary.HardErrors {
				hadHardErrors = true
			}

			var partial bool
			if fi, err := os.Stat(mboxPath); err == nil && summary.FinalOffset < fi.Size() {
				partial = true
			}
			processedFiles = append(processedFiles, processedFile{Path: mboxPath, Partial: partial})

			if ctx.Err() != nil {
				break
			}
		}

		out := cmd.OutOrStdout()
		if ctx.Err() != nil {
			fmt.Fprintln(out, "Import interrupted. Run again to resume.")
		} else if totalErrors > 0 {
			fmt.Fprintln(out, "Import complete (with errors).")
		} else {
			fmt.Fprintln(out, "Import complete.")
		}
		for _, p := range processedFiles {
			if p.Partial {
				fmt.Fprintf(out, "  Imported (partial): %s\n", p.Path)
			} else {
				fmt.Fprintf(out, "  Imported:           %s\n", p.Path)
			}
		}
		fmt.Fprintf(out, "  Processed:      %d messages\n", totalProcessed)
		fmt.Fprintf(out, "  Added:          %d messages\n", totalAdded)
		fmt.Fprintf(out, "  Updated:        %d messages\n", totalUpdated)
		fmt.Fprintf(out, "  Skipped:        %d messages\n", totalSkipped)
		fmt.Fprintf(out, "  Errors:         %d\n", totalErrors)
		fmt.Fprintf(out, "  Bytes:          %.2f MB\n", float64(totalBytes)/(1024*1024))

		if ctx.Err() == nil && hadHardErrors {
			return fmt.Errorf("import completed with %d errors", totalErrors)
		}
		if ctx.Err() != nil {
			return context.Canceled
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(importMboxCmd)

	importMboxCmd.Flags().StringVar(&importMboxSourceType, "source-type", "mbox", "Source type to record in the database (e.g. mbox, hey)")
	importMboxCmd.Flags().StringVar(&importMboxLabel, "label", "", "Label to apply to newly imported messages")
	importMboxCmd.Flags().BoolVar(&importMboxNoResume, "no-resume", false, "Do not resume from an interrupted import")
	importMboxCmd.Flags().IntVar(&importMboxCheckpointInterval, "checkpoint-interval", 200, "Save progress every N messages")
	importMboxCmd.Flags().BoolVar(&importMboxNoAttachments, "no-attachments", false, "Do not store attachments (disk or database). Messages will still be marked as having attachments. Note: rerunning later without --no-attachments will not backfill attachments for already-imported messages.")
}

func resolveMboxExport(exportPath string, importsDir string) ([]string, error) {
	st, err := os.Stat(exportPath)
	if err != nil {
		return nil, fmt.Errorf("export file: %w", err)
	}
	if !st.Mode().IsRegular() {
		return nil, fmt.Errorf("export file %q is not a regular file", exportPath)
	}
	abs, err := filepath.Abs(exportPath)
	if err != nil {
		return nil, fmt.Errorf("abs path: %w", err)
	}
	importsAbs, err := filepath.Abs(importsDir)
	if err != nil {
		return nil, fmt.Errorf("abs imports dir: %w", err)
	}
	importsAbsOrig := importsAbs
	if resolved, err := filepath.EvalSymlinks(importsAbs); err == nil {
		importsAbs = resolved
	}

	ext := strings.ToLower(filepath.Ext(abs))
	switch ext {
	case ".mbox", ".mbx":
		return []string{abs}, nil
	case ".zip":
		// Use a cheap cache key derived from zip entry metadata (central directory)
		// plus basic zip file metadata. This avoids hashing multi-GB zip files, but
		// is not a cryptographic integrity check.
		zipKey, err := zipMboxCacheKey(abs)
		if err != nil {
			return nil, err
		}

		if st, err := os.Lstat(importsAbsOrig); err == nil {
			if st.Mode()&os.ModeSymlink != 0 {
				return nil, fmt.Errorf("imports dir %q is a symlink", importsAbsOrig)
			}
		} else if !os.IsNotExist(err) {
			return nil, fmt.Errorf("lstat imports dir: %w", err)
		}

		for _, p := range []string{
			filepath.Join(importsAbs, "imports"),
			filepath.Join(importsAbs, "imports", "mbox"),
		} {
			if st, err := os.Lstat(p); err == nil {
				if st.Mode()&os.ModeSymlink != 0 {
					return nil, fmt.Errorf("extract base %q is a symlink", p)
				}
				if !st.IsDir() {
					return nil, fmt.Errorf("extract base %q is not a directory", p)
				}
			} else if !os.IsNotExist(err) {
				return nil, fmt.Errorf("lstat extract base: %w", err)
			}
		}

		destDir := filepath.Join(importsAbs, "imports", "mbox", zipKey)
		rel, err := filepath.Rel(importsAbs, destDir)
		if err != nil {
			return nil, fmt.Errorf("rel extract dir: %w", err)
		}
		if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
			return nil, fmt.Errorf("extract dir %q escapes imports dir %q", destDir, importsAbs)
		}
		return extractMboxFromZip(abs, destDir)
	default:
		return nil, fmt.Errorf("unsupported export format %q (expected .mbox/.mbx or .zip)", ext)
	}
}

func zipMboxCacheKey(zipPath string) (string, error) {
	st, err := os.Stat(zipPath)
	if err != nil {
		return "", fmt.Errorf("stat zip: %w", err)
	}

	zr, err := zip.OpenReader(zipPath)
	if err != nil {
		return "", fmt.Errorf("open zip: %w", err)
	}
	defer zr.Close()

	type zipEntry struct {
		Name string
		Size uint64
		CRC  uint32
	}
	var entries []zipEntry
	for _, zf := range zr.File {
		if zf.FileInfo().IsDir() {
			continue
		}

		name, err := zipEntryBaseName(zf.Name)
		if err != nil {
			return "", fmt.Errorf("invalid zip entry name %q: %w", zf.Name, err)
		}
		ext := strings.ToLower(filepath.Ext(name))
		if ext != ".mbox" && ext != ".mbx" {
			continue
		}

		cleanName := path.Clean(strings.ReplaceAll(zf.Name, "\\", "/"))
		entries = append(entries, zipEntry{
			Name: cleanName,
			Size: zf.UncompressedSize64,
			CRC:  zf.CRC32,
		})
	}
	if len(entries) == 0 {
		return "", fmt.Errorf("zip contains no .mbox or .mbx files")
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })

	h := sha256.New()
	fmt.Fprintf(h, "zip:%x\n", st.Size())
	for _, e := range entries {
		fmt.Fprintf(h, "%s\x00%x\x00%x\n", e.Name, e.Size, e.CRC)
	}
	sum := hex.EncodeToString(h.Sum(nil))
	return "z" + sum[:16], nil
}

func extractMboxFromZip(zipPath, destDir string) ([]string, error) {
	return extractMboxFromZipWithLimits(zipPath, destDir, zipExtractLimits{
		MaxEntryBytes: defaultMaxZipEntryBytes,
		MaxTotalBytes: defaultMaxZipTotalBytes,
	})
}

func extractMboxFromZipWithLimits(zipPath, destDir string, limits zipExtractLimits) ([]string, error) {
	if st, err := os.Lstat(filepath.Dir(destDir)); err == nil {
		if st.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("extract parent dir %q is a symlink", filepath.Dir(destDir))
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("lstat extract parent dir: %w", err)
	}

	if st, err := os.Lstat(destDir); err == nil {
		if st.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("extract dir %q is a symlink", destDir)
		}
		if !st.IsDir() {
			return nil, fmt.Errorf("extract dir %q is not a directory", destDir)
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("lstat extract dir: %w", err)
	}

	// Use a sentinel file so we can reuse the extracted path across retries/resumes.
	sentinel := filepath.Join(destDir, ".done")
	if _, err := os.Stat(sentinel); err == nil {
		files, err := validateExtractedMboxCache(zipPath, destDir, limits)
		if err == nil && len(files) > 0 {
			return files, nil
		}
		// Sentinel exists but no files found; fall through to re-extract.
	}

	parentDir := filepath.Dir(destDir)
	if err := fileutil.SecureMkdirAll(parentDir, 0700); err != nil {
		return nil, fmt.Errorf("create extract parent dir: %w", err)
	}
	tmpDir, err := os.MkdirTemp(parentDir, filepath.Base(destDir)+".tmp.")
	if err != nil {
		return nil, fmt.Errorf("create temp extract dir: %w", err)
	}
	cleanupTmp := true
	defer func() {
		if cleanupTmp {
			_ = os.RemoveAll(tmpDir)
		}
	}()

	zr, err := zip.OpenReader(zipPath)
	if err != nil {
		return nil, fmt.Errorf("open zip: %w", err)
	}
	defer zr.Close()

	var outFiles []string
	seenNames := make(map[string]struct{})
	var totalWritten int64

	for _, zf := range zr.File {
		if zf.FileInfo().IsDir() {
			continue
		}

		name, err := zipEntryBaseName(zf.Name)
		if err != nil {
			return nil, fmt.Errorf("invalid zip entry name %q: %w", zf.Name, err)
		}
		ext := strings.ToLower(filepath.Ext(name))
		if ext != ".mbox" && ext != ".mbx" {
			continue
		}

		if limits.MaxTotalBytes > 0 && totalWritten >= limits.MaxTotalBytes {
			return nil, fmt.Errorf("%w: total extracted bytes exceeds limit (%d bytes)", errZipExtractLimitExceeded, limits.MaxTotalBytes)
		}
		if limits.MaxEntryBytes > 0 && zf.UncompressedSize64 > uint64(limits.MaxEntryBytes) {
			return nil, fmt.Errorf("%w: zip entry %q too large (%d bytes > %d bytes)", errZipExtractLimitExceeded, zf.Name, zf.UncompressedSize64, limits.MaxEntryBytes)
		}

		outName := export.SanitizeFilename(name)
		outName, err = zipMboxDisambiguateName(zf.Name, outName, seenNames)
		if err != nil {
			return nil, err
		}

		outPath, err := safeJoinUnderDir(tmpDir, outName)
		if err != nil {
			return nil, fmt.Errorf("invalid extracted path for %q: %w", zf.Name, err)
		}
		rc, err := zf.Open()
		if err != nil {
			return nil, fmt.Errorf("open zip entry %q: %w", zf.Name, err)
		}

		w, err := fileutil.SecureOpenFile(outPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
		if err != nil {
			rc.Close()
			return nil, fmt.Errorf("create extracted file: %w", err)
		}

		limit := limits.MaxEntryBytes
		if limits.MaxTotalBytes > 0 {
			remaining := limits.MaxTotalBytes - totalWritten
			if remaining <= 0 {
				_ = w.Close()
				_ = rc.Close()
				_ = os.Remove(outPath)
				return nil, fmt.Errorf("%w: total extracted bytes exceeds limit (%d bytes)", errZipExtractLimitExceeded, limits.MaxTotalBytes)
			}
			if limit <= 0 || remaining < limit {
				limit = remaining
			}
		}

		n, copyErr := copyWithLimit(w, rc, limit)
		closeErr := w.Close()
		rcCloseErr := rc.Close()

		if copyErr != nil {
			_ = os.Remove(outPath)
			if errors.Is(copyErr, errZipExtractLimitExceeded) {
				return nil, fmt.Errorf("%w: extract %q: %v", errZipExtractLimitExceeded, zf.Name, copyErr)
			}
			return nil, fmt.Errorf("extract %q: %w", zf.Name, copyErr)
		}
		if closeErr != nil {
			_ = os.Remove(outPath)
			return nil, fmt.Errorf("close extracted file: %w", closeErr)
		}
		if rcCloseErr != nil {
			_ = os.Remove(outPath)
			return nil, fmt.Errorf("extract %q: %w", zf.Name, rcCloseErr)
		}

		totalWritten += n
		outFiles = append(outFiles, outPath)
	}

	if len(outFiles) == 0 {
		return nil, fmt.Errorf("zip contains no .mbox or .mbx files")
	}

	sort.Strings(outFiles)

	var oldDir string
	if st, err := os.Lstat(destDir); err == nil {
		if st.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("extract dir %q is a symlink", destDir)
		}
		if !st.IsDir() {
			return nil, fmt.Errorf("extract dir %q is not a directory", destDir)
		}

		f, err := os.CreateTemp(parentDir, filepath.Base(destDir)+".old.")
		if err != nil {
			return nil, fmt.Errorf("reserve old extract dir name: %w", err)
		}
		oldDir = f.Name()
		if err := f.Close(); err != nil {
			_ = os.Remove(oldDir)
			return nil, fmt.Errorf("close temp file: %w", err)
		}
		if err := os.Remove(oldDir); err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("remove temp file: %w", err)
		}

		if err := os.Rename(destDir, oldDir); err != nil {
			return nil, fmt.Errorf("move old extract dir out of the way: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("lstat extract dir: %w", err)
	}

	if err := os.Rename(tmpDir, destDir); err != nil {
		if oldDir != "" {
			_ = os.Rename(oldDir, destDir)
		}
		return nil, fmt.Errorf("rename extracted dir into place: %w", err)
	}
	cleanupTmp = false
	if oldDir != "" {
		if err := os.RemoveAll(oldDir); err != nil && logger != nil {
			logger.Warn("failed to remove old extract dir", "path", oldDir, "error", err)
		}
	}

	finalFiles := make([]string, 0, len(outFiles))
	for _, p := range outFiles {
		rel, err := filepath.Rel(tmpDir, p)
		if err != nil {
			return nil, fmt.Errorf("rel extracted path: %w", err)
		}
		if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
			return nil, fmt.Errorf("extracted path %q escapes temp dir %q", p, tmpDir)
		}
		finalFiles = append(finalFiles, filepath.Join(destDir, rel))
	}
	sort.Strings(finalFiles)

	if err := fileutil.SecureWriteFile(sentinel, []byte("ok\n"), 0600); err != nil {
		if logger != nil {
			logger.Warn("failed to write zip extraction sentinel", "path", sentinel, "error", err)
		}
	}
	return finalFiles, nil
}

type expectedMboxFile struct {
	Path      string
	Size      int64
	SizeKnown bool
	CRC32     uint32
}

func zipCacheValidateCRC32() bool {
	v := strings.TrimSpace(os.Getenv("MSGVAULT_ZIP_CACHE_VALIDATE_CRC32"))
	if v == "" || v == "0" || strings.EqualFold(v, "false") {
		return false
	}
	return true
}

func validateExtractedMboxCache(zipPath, destDir string, limits zipExtractLimits) ([]string, error) {
	expected, err := expectedMboxFilesFromZip(zipPath, destDir, limits)
	if err != nil {
		return nil, err
	}
	if len(expected) == 0 {
		return nil, fmt.Errorf("zip contains no .mbox or .mbx files")
	}

	expectedSet := make(map[string]expectedMboxFile, len(expected))
	var expectedFiles []string
	for _, f := range expected {
		expectedSet[f.Path] = f
		expectedFiles = append(expectedFiles, f.Path)
	}
	sort.Strings(expectedFiles)

	entries, err := os.ReadDir(destDir)
	if err != nil {
		return nil, err
	}
	for _, ent := range entries {
		name := ent.Name()
		p := filepath.Join(destDir, name)
		if name == ".done" {
			st, err := os.Lstat(p)
			if err != nil {
				return nil, err
			}
			if st.Mode()&os.ModeSymlink != 0 {
				return nil, fmt.Errorf("cached extraction sentinel %q is a symlink", p)
			}
			if !st.Mode().IsRegular() {
				return nil, fmt.Errorf("cached extraction sentinel %q is not a regular file", p)
			}
			continue
		}
		if ent.IsDir() {
			return nil, fmt.Errorf("cached extraction contains unexpected directory %q", p)
		}
		ext := strings.ToLower(filepath.Ext(name))
		if ext != ".mbox" && ext != ".mbx" {
			return nil, fmt.Errorf("cached extraction contains unexpected file %q", p)
		}
		if _, ok := expectedSet[p]; !ok {
			return nil, fmt.Errorf("cached extraction contains unexpected file %q", p)
		}
	}

	actualFiles, err := findExtractedMboxFiles(destDir)
	if err != nil {
		return nil, err
	}
	if len(actualFiles) != len(expectedFiles) {
		return nil, fmt.Errorf("cached extraction has %d mbox files, want %d", len(actualFiles), len(expectedFiles))
	}

	validateCRC32 := zipCacheValidateCRC32()
	var total int64
	for _, p := range actualFiles {
		exp, ok := expectedSet[p]
		if !ok {
			return nil, fmt.Errorf("cached extraction contains unexpected file %q", p)
		}
		st, err := os.Lstat(p)
		if err != nil {
			return nil, err
		}
		if exp.SizeKnown {
			if st.Size() != exp.Size {
				return nil, fmt.Errorf("cached extraction file %q has size %d, want %d", p, st.Size(), exp.Size)
			}
		} else {
			// Some zip producers leave uncompressed size unset in the central
			// directory. Treat these entries as "best-effort cacheable" and validate
			// via basic filesystem checks, extraction limits, and CRC32.
			if limits.MaxEntryBytes > 0 && st.Size() > limits.MaxEntryBytes {
				return nil, fmt.Errorf("cached extraction file %q exceeds size limit (%d bytes > %d bytes)", p, st.Size(), limits.MaxEntryBytes)
			}
		}

		total += st.Size()
		if limits.MaxTotalBytes > 0 && total > limits.MaxTotalBytes {
			return nil, fmt.Errorf("cached extraction exceeds total size limit (%d bytes > %d bytes)", total, limits.MaxTotalBytes)
		}

		if validateCRC32 || !exp.SizeKnown {
			gotCRC, err := crc32File(p)
			if err != nil {
				return nil, fmt.Errorf("crc32 cached extraction file %q: %w", p, err)
			}
			if gotCRC != exp.CRC32 {
				return nil, fmt.Errorf("cached extraction file %q has crc32 %08x, want %08x", p, gotCRC, exp.CRC32)
			}
		}
	}

	return expectedFiles, nil
}

func crc32File(path string) (uint32, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	h := crc32.NewIEEE()
	if _, err := io.Copy(h, f); err != nil {
		return 0, err
	}
	return h.Sum32(), nil
}

func expectedMboxFilesFromZip(zipPath, destDir string, limits zipExtractLimits) ([]expectedMboxFile, error) {
	zr, err := zip.OpenReader(zipPath)
	if err != nil {
		return nil, fmt.Errorf("open zip: %w", err)
	}
	defer zr.Close()

	var expected []expectedMboxFile
	seenNames := make(map[string]struct{})
	var total int64

	for _, zf := range zr.File {
		if zf.FileInfo().IsDir() {
			continue
		}

		name, err := zipEntryBaseName(zf.Name)
		if err != nil {
			return nil, fmt.Errorf("invalid zip entry name %q: %w", zf.Name, err)
		}
		ext := strings.ToLower(filepath.Ext(name))
		if ext != ".mbox" && ext != ".mbx" {
			continue
		}

		if limits.MaxTotalBytes > 0 && total >= limits.MaxTotalBytes {
			return nil, fmt.Errorf("%w: total extracted bytes exceeds limit (%d bytes)", errZipExtractLimitExceeded, limits.MaxTotalBytes)
		}
		if limits.MaxEntryBytes > 0 && zf.UncompressedSize64 > uint64(limits.MaxEntryBytes) {
			return nil, fmt.Errorf("%w: zip entry %q too large (%d bytes > %d bytes)", errZipExtractLimitExceeded, zf.Name, zf.UncompressedSize64, limits.MaxEntryBytes)
		}
		if limits.MaxTotalBytes > 0 {
			remaining := limits.MaxTotalBytes - total
			if remaining <= 0 {
				return nil, fmt.Errorf("%w: total extracted bytes exceeds limit (%d bytes)", errZipExtractLimitExceeded, limits.MaxTotalBytes)
			}
			if zf.UncompressedSize64 > uint64(remaining) {
				return nil, fmt.Errorf("%w: total extracted bytes exceeds limit (%d bytes)", errZipExtractLimitExceeded, limits.MaxTotalBytes)
			}
		}

		outName := export.SanitizeFilename(name)
		outName, err = zipMboxDisambiguateName(zf.Name, outName, seenNames)
		if err != nil {
			return nil, err
		}
		outPath, err := safeJoinUnderDir(destDir, outName)
		if err != nil {
			return nil, fmt.Errorf("invalid extracted path for %q: %w", zf.Name, err)
		}
		expected = append(expected, expectedMboxFile{
			Path:      outPath,
			Size:      int64(zf.UncompressedSize64),
			SizeKnown: zf.UncompressedSize64 != 0 || (zf.UncompressedSize64 == 0 && zf.CompressedSize64 == 0),
			CRC32:     zf.CRC32,
		})
		total += int64(zf.UncompressedSize64)
	}

	sort.Slice(expected, func(i, j int) bool { return expected[i].Path < expected[j].Path })
	return expected, nil
}

func zipMboxDisambiguateName(zipEntryName string, outName string, seen map[string]struct{}) (string, error) {
	key := strings.ToLower(outName)
	if _, ok := seen[key]; !ok {
		seen[key] = struct{}{}
		return outName, nil
	}

	// Flatten to base name to avoid zip-slip directory traversal.
	// Disambiguate collisions by suffixing a short hash of the original zip entry name.
	cleanName := path.Clean(strings.ReplaceAll(zipEntryName, "\\", "/"))
	sum := sha256.Sum256([]byte(cleanName))
	ext := filepath.Ext(outName)
	if ext == "" {
		ext = filepath.Ext(path.Base(cleanName))
	}
	base := strings.TrimSuffix(outName, filepath.Ext(outName))
	hashSuffix := hex.EncodeToString(sum[:4])
	for i := 0; ; i++ {
		candidate := fmt.Sprintf("%s_%s%s", base, hashSuffix, ext)
		if i > 0 {
			candidate = fmt.Sprintf("%s_%s_%d%s", base, hashSuffix, i, ext)
		}
		candidateKey := strings.ToLower(candidate)
		if _, exists := seen[candidateKey]; !exists {
			seen[candidateKey] = struct{}{}
			return candidate, nil
		}
		if i > 1000 {
			return "", fmt.Errorf("too many colliding zip entries for base name %q", outName)
		}
	}
}

func zipEntryBaseName(name string) (string, error) {
	// ZIP uses forward slashes, but some producers include backslashes.
	cleaned := path.Clean(strings.ReplaceAll(name, "\\", "/"))
	base := path.Base(cleaned)
	if base == "." || base == ".." || base == "/" || base == "" {
		return "", fmt.Errorf("invalid base name %q", base)
	}
	// Be strict: the extracted name should be a single path segment.
	if strings.Contains(base, "/") || strings.Contains(base, "\\") {
		return "", fmt.Errorf("base name contains path separator: %q", base)
	}
	return base, nil
}

func safeJoinUnderDir(dir, name string) (string, error) {
	outPath := filepath.Join(dir, name)
	rel, err := filepath.Rel(dir, outPath)
	if err != nil {
		return "", err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("path escapes destination dir")
	}
	return outPath, nil
}

func copyWithLimit(dst io.Writer, src io.Reader, max int64) (int64, error) {
	if max <= 0 {
		n, err := io.Copy(dst, src)
		return n, err
	}

	// On overflow, this function may read (and discard) one extra byte from src to
	// detect that more data exists, so src should not be reused after a limit error.
	var n int64
	buf := make([]byte, 32*1024)
	for {
		if n == max {
			// Peek one byte to determine whether there's more data.
			var one [1]byte
			nr, er := src.Read(one[:])
			if nr > 0 {
				return n, fmt.Errorf("%w: limit %d bytes", errZipExtractLimitExceeded, max)
			}
			if er == io.EOF {
				return n, nil
			}
			if er != nil {
				return n, er
			}
			return n, io.ErrNoProgress
		}

		toRead := len(buf)
		rem := max - n
		if rem < int64(toRead) {
			toRead = int(rem)
		}

		nr, er := src.Read(buf[:toRead])
		if nr == 0 && er == nil {
			return n, io.ErrNoProgress
		}
		if nr > 0 {
			nw, ew := dst.Write(buf[:nr])
			n += int64(nw)
			if ew != nil {
				return n, ew
			}
			if nw != nr {
				return n, io.ErrShortWrite
			}
		}
		if er != nil {
			if er == io.EOF {
				return n, nil
			}
			return n, er
		}
	}
}

func findExtractedMboxFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		ext := strings.ToLower(filepath.Ext(ent.Name()))
		if ext != ".mbox" && ext != ".mbx" {
			continue
		}
		p := filepath.Join(dir, ent.Name())
		st, err := os.Lstat(p)
		if err != nil {
			return nil, err
		}
		if st.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("extracted file %q is a symlink", p)
		}
		if !st.Mode().IsRegular() {
			return nil, fmt.Errorf("extracted file %q is not a regular file", p)
		}
		files = append(files, p)
	}
	sort.Strings(files)
	return files, nil
}
