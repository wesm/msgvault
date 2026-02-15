package export

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/wesm/msgvault/internal/fileutil"
	"github.com/wesm/msgvault/internal/mime"
)

var validatedAttachmentFiles sync.Map // key: fullPath + size + mtime + expectedHash

// StoreAttachmentFile stores att.Content on disk under attachmentsDir using
// content-addressed storage (hash[:2]/hash). It validates existing files when
// de-duping. If attachmentsDir is a symlink, it is resolved before writing.
//
// Returns the storage path relative to attachmentsDir (e.g. "ab/<hash>"), or
// empty string if nothing was stored.
func StoreAttachmentFile(attachmentsDir string, att *mime.Attachment) (string, error) {
	if attachmentsDir == "" || len(att.Content) == 0 {
		return "", nil
	}

	sum := sha256.Sum256(att.Content)
	computedHash := hex.EncodeToString(sum[:])

	if att.ContentHash == "" {
		att.ContentHash = computedHash
	}
	att.ContentHash = strings.ToLower(att.ContentHash)
	if err := ValidateContentHash(att.ContentHash); err != nil {
		return "", fmt.Errorf("invalid attachment content hash %q: %w", att.ContentHash, err)
	}
	if att.ContentHash != computedHash {
		return "", fmt.Errorf("attachment content hash mismatch: provided %q, computed %q", att.ContentHash, computedHash)
	}

	// Content-addressed storage: first 2 chars / full hash
	subdir := att.ContentHash[:2]
	storagePath := path.Join(subdir, att.ContentHash)
	baseDir, err := filepath.Abs(attachmentsDir)
	if err != nil {
		return "", fmt.Errorf("abs attachments dir %q: %w", attachmentsDir, err)
	}
	if err := fileutil.SecureMkdirAll(baseDir, 0700); err != nil {
		return "", fmt.Errorf("create attachments dir: %w", err)
	}
	if err := fileutil.SecureChmod(baseDir, 0700); err != nil {
		return "", fmt.Errorf("chmod attachments dir: %w", err)
	}
	resolved, err := filepath.EvalSymlinks(baseDir)
	if err != nil {
		return "", fmt.Errorf("resolve attachments dir %q: %w", attachmentsDir, err)
	}
	baseDir = resolved
	if st, err := os.Lstat(baseDir); err == nil {
		if !st.IsDir() {
			return "", fmt.Errorf("attachments dir %q is not a directory", baseDir)
		}
	} else {
		return "", fmt.Errorf("lstat attachments dir: %w", err)
	}

	fullPath := filepath.Join(baseDir, subdir, att.ContentHash)
	subdirPath := filepath.Join(baseDir, subdir)
	if st, err := os.Lstat(subdirPath); err == nil {
		if st.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("attachment dir %q is a symlink", subdirPath)
		}
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("lstat attachment dir: %w", err)
	}

	if err := fileutil.SecureMkdirAll(filepath.Dir(fullPath), 0700); err != nil {
		return "", fmt.Errorf("create attachment dir: %w", err)
	}

	expectedSize := int64(len(att.Content))
	if _, err := os.Lstat(fullPath); err == nil {
		if err := validateExistingAttachmentFile(fullPath, expectedSize, att.ContentHash); err != nil {
			return "", err
		}
		return storagePath, nil
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("lstat attachment file: %w", err)
	}

	// Write to a temp file and rename into place to avoid other concurrent
	// writers observing a partially written final path.
	tmp, err := os.CreateTemp(filepath.Dir(fullPath), att.ContentHash+".tmp.")
	if err != nil {
		return "", fmt.Errorf("create temp attachment file: %w", err)
	}
	tmpPath := tmp.Name()
	cleanupTmp := true
	defer func() {
		if cleanupTmp {
			_ = tmp.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	if err := fileutil.SecureChmod(tmpPath, 0600); err != nil {
		return "", fmt.Errorf("chmod temp attachment file: %w", err)
	}

	b := att.Content
	for len(b) > 0 {
		n, err := tmp.Write(b)
		if err != nil {
			return "", fmt.Errorf("write attachment file: %w", err)
		}
		b = b[n:]
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("close attachment file: %w", err)
	}

	if err := os.Rename(tmpPath, fullPath); err != nil {
		// Another writer may have installed the final file first (notably on
		// Windows; Unix rename typically overwrites). Validate the existing file.
		if _, statErr := os.Lstat(fullPath); statErr == nil {
			cleanupTmp = false
			_ = os.Remove(tmpPath)
			if err := validateExistingAttachmentFile(fullPath, expectedSize, att.ContentHash); err != nil {
				return "", err
			}
			return storagePath, nil
		}
		return "", fmt.Errorf("rename attachment file into place: %w", err)
	}
	cleanupTmp = false

	return storagePath, nil
}

func validateExistingAttachmentFile(fullPath string, expectedSize int64, expectedHash string) error {
	f, err := openNoFollow(fullPath)
	if err != nil {
		return fmt.Errorf("open attachment file for validation: %w", err)
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat attachment file: %w", err)
	}
	if !st.Mode().IsRegular() {
		return fmt.Errorf("attachment file %q is not a regular file", fullPath)
	}
	if st.Size() != expectedSize {
		return fmt.Errorf("attachment file %q has size %d, want %d", fullPath, st.Size(), expectedSize)
	}

	key := fmt.Sprintf("%s\x00%d\x00%d\x00%s", fullPath, expectedSize, st.ModTime().UnixNano(), expectedHash)
	if _, ok := validatedAttachmentFiles.Load(key); ok {
		return nil
	}

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return fmt.Errorf("hash attachment file: %w", err)
	}
	gotHash := hex.EncodeToString(h.Sum(nil))
	if gotHash != expectedHash {
		return fmt.Errorf("attachment file %q has hash %q, want %q", fullPath, gotHash, expectedHash)
	}
	validatedAttachmentFiles.Store(key, struct{}{})
	return nil
}
