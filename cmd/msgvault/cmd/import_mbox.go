package cmd

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
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
}

var errZipExtractLimitExceeded = errors.New("zip extraction limit exceeded")

var (
	// These are intentionally very high defaults; they exist to prevent zip-bomb
	// style resource exhaustion while still supporting large real-world exports.
	//
	// Tests may temporarily override these values.
	maxZipEntryBytes int64 = 50 << 30  // 50 GiB per extracted mbox file
	maxZipTotalBytes int64 = 200 << 30 // 200 GiB total extracted bytes
)

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
	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		identifier := args[0]
		exportPath := args[1]

		// Handle Ctrl+C gracefully (save checkpoint and exit cleanly).
		ctx, cancel := context.WithCancel(cmd.Context())
		defer cancel()
		sigChan := make(chan os.Signal, 2)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		defer signal.Stop(sigChan)
		done := make(chan struct{})
		defer close(done)
		go func() {
			signals := 0
			for {
				select {
				case <-done:
					return
				case <-sigChan:
					signals++
					if signals == 1 {
						fmt.Fprintln(os.Stderr, "\nInterrupted. Saving checkpoint...")
						cancel()
						continue
					}
					fmt.Fprintln(os.Stderr, "Interrupted again. Exiting immediately.")
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
					startIdx := -1
					for i, p := range mboxFiles {
						if p == cp.File {
							startIdx = i
							break
						}
					}
					if startIdx == -1 {
						return fmt.Errorf("active mbox import is for a different file (%q); rerun with --no-resume to start fresh", cp.File)
					}
					mboxFiles = mboxFiles[startIdx:]
				}
			}
		}

		var (
			totalProcessed int64
			totalAdded     int64
			totalSkipped   int64
			totalErrors    int64
			totalBytes     int64
		)
		processedFiles := make([]string, 0, len(mboxFiles))

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
			totalSkipped += summary.MessagesSkipped
			totalErrors += summary.Errors
			totalBytes += summary.BytesProcessed
			processedFiles = append(processedFiles, mboxPath)

			if ctx.Err() != nil {
				break
			}
		}

		if ctx.Err() != nil {
			fmt.Printf("Import interrupted. Run again to resume.\n")
		} else {
			fmt.Printf("Import complete.\n")
		}
		for _, p := range processedFiles {
			fmt.Printf("  Imported:       %s\n", p)
		}
		fmt.Printf("  Processed:      %d messages\n", totalProcessed)
		fmt.Printf("  Added:          %d messages\n", totalAdded)
		fmt.Printf("  Skipped:        %d messages\n", totalSkipped)
		fmt.Printf("  Errors:         %d\n", totalErrors)
		fmt.Printf("  Bytes:          %.2f MB\n", float64(totalBytes)/(1024*1024))

		return nil
	},
}

func init() {
	rootCmd.AddCommand(importMboxCmd)

	importMboxCmd.Flags().StringVar(&importMboxSourceType, "source-type", "mbox", "Source type to record in the database (e.g. mbox, hey)")
	importMboxCmd.Flags().StringVar(&importMboxLabel, "label", "", "Label to apply to newly imported messages")
	importMboxCmd.Flags().BoolVar(&importMboxNoResume, "no-resume", false, "Do not resume from an interrupted import")
	importMboxCmd.Flags().IntVar(&importMboxCheckpointInterval, "checkpoint-interval", 200, "Save progress every N messages")
	importMboxCmd.Flags().BoolVar(&importMboxNoAttachments, "no-attachments", false, "Do not store attachments (disk or database)")
}

func resolveMboxExport(path string, importsDir string) ([]string, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("export file: %w", err)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("abs path: %w", err)
	}

	ext := strings.ToLower(filepath.Ext(abs))
	switch ext {
	case ".mbox", ".mbx":
		return []string{abs}, nil
	case ".zip":
		hash, err := sha256File(abs)
		if err != nil {
			return nil, err
		}
		destDir := filepath.Join(importsDir, "imports", "mbox", hash)
		return extractMboxFromZip(abs, destDir)
	default:
		return nil, fmt.Errorf("unsupported export format %q (expected .mbox or .zip)", ext)
	}
}

func sha256File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open file for hash: %w", err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("hash file: %w", err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func extractMboxFromZip(zipPath, destDir string) ([]string, error) {
	// Use a sentinel file so we can reuse the extracted path across retries/resumes.
	sentinel := filepath.Join(destDir, ".done")
	if _, err := os.Stat(sentinel); err == nil {
		files, err := findExtractedMboxFiles(destDir)
		if err == nil && len(files) > 0 {
			return files, nil
		}
		// Sentinel exists but no files found; fall through to re-extract.
	}

	if err := fileutil.SecureMkdirAll(destDir, 0700); err != nil {
		return nil, fmt.Errorf("create extract dir: %w", err)
	}

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

		if maxZipTotalBytes > 0 && totalWritten >= maxZipTotalBytes {
			return nil, fmt.Errorf("%w: total extracted bytes exceeds limit (%d bytes)", errZipExtractLimitExceeded, maxZipTotalBytes)
		}
		if maxZipEntryBytes > 0 && zf.UncompressedSize64 > uint64(maxZipEntryBytes) {
			return nil, fmt.Errorf("%w: zip entry %q too large (%d bytes > %d bytes)", errZipExtractLimitExceeded, zf.Name, zf.UncompressedSize64, maxZipEntryBytes)
		}

		outName := name
		if _, ok := seenNames[outName]; ok {
			// Flatten to base name to avoid zip-slip directory traversal.
			// Disambiguate collisions by suffixing a short hash of the original zip entry name.
			cleanName := path.Clean(strings.ReplaceAll(zf.Name, "\\", "/"))
			sum := sha256.Sum256([]byte(cleanName))
			outName = fmt.Sprintf("%s_%s%s", strings.TrimSuffix(name, ext), hex.EncodeToString(sum[:4]), ext)
		}
		seenNames[outName] = struct{}{}

		outPath, err := safeJoinUnderDir(destDir, outName)
		if err != nil {
			return nil, fmt.Errorf("invalid extracted path for %q: %w", zf.Name, err)
		}
		rc, err := zf.Open()
		if err != nil {
			return nil, fmt.Errorf("open zip entry %q: %w", zf.Name, err)
		}

		w, err := fileutil.SecureOpenFile(outPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		if err != nil {
			rc.Close()
			return nil, fmt.Errorf("create extracted file: %w", err)
		}

		limit := maxZipEntryBytes
		if maxZipTotalBytes > 0 {
			remaining := maxZipTotalBytes - totalWritten
			if remaining <= 0 {
				_ = w.Close()
				_ = rc.Close()
				_ = os.Remove(outPath)
				return nil, fmt.Errorf("%w: total extracted bytes exceeds limit (%d bytes)", errZipExtractLimitExceeded, maxZipTotalBytes)
			}
			if limit <= 0 || remaining < limit {
				limit = remaining
			}
		}

		n, copyErr := copyWithLimit(w, rc, limit)
		closeErr := w.Close()
		_ = rc.Close()

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

		totalWritten += n
		outFiles = append(outFiles, outPath)
	}

	if len(outFiles) == 0 {
		return nil, fmt.Errorf("zip contains no .mbox or .mbx files")
	}

	sort.Strings(outFiles)
	_ = fileutil.SecureWriteFile(sentinel, []byte("ok\n"), 0600)
	return outFiles, nil
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
			continue
		}

		toRead := len(buf)
		rem := max - n
		if rem < int64(toRead) {
			toRead = int(rem)
		}

		nr, er := src.Read(buf[:toRead])
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
		files = append(files, filepath.Join(dir, ent.Name()))
	}
	sort.Strings(files)
	return files, nil
}
