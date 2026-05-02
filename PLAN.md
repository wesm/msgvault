# Fix Plan: PST Import Code Review Issues

11 issues found during code review of the `import-pst` branch. Grouped by file to minimize context switching.

---

## Group 1: `internal/pst/mime.go` + `internal/pst/mime_test.go` (Issues 1-4)

### Issue 1 — Header injection via unsanitized MAPI properties (HIGH)

**Problem:** `writeSynthesizedHeaders` (line 178-228) writes PST fields directly into RFC 5322 headers. Email addresses are not sanitized — a `SenderEmail` containing `\r\n` could inject arbitrary headers (e.g. `evil@bad.com\r\nBcc: victim@example.com`).

**Fix:**

1. Add a `sanitizeHeaderValue` helper near the other utility functions (after `formatDisplayList`):

```go
// sanitizeHeaderValue strips CR and LF characters to prevent header injection.
func sanitizeHeaderValue(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '\r' || r == '\n' {
			return -1
		}
		return r
	}, s)
}
```

2. In `formatAddr`, sanitize the `email` parameter (the only unsanitized value — `name` already goes through `mime.QEncoding.Encode`):

```go
func formatAddr(name, email string) string {
	email = sanitizeHeaderValue(email)
	// ... rest unchanged
}
```

3. In `writeSynthesizedHeaders`, sanitize `msg.MessageID`, `msg.InReplyTo`, and `msg.References` before use:

- Lines 207-209: change to `mid := sanitizeHeaderValue(msg.MessageID)`
- Lines 215-218: change to `irt := sanitizeHeaderValue(msg.InReplyTo)`
- Line 222: change to `writeHeader(buf, "References", sanitizeHeaderValue(msg.References))`

Note: `msg.Subject`, `msg.DisplayTo/Cc/Bcc`, and `msg.SenderName` all go through `mime.QEncoding.Encode` which is safe.

4. Add tests in `mime_test.go`:

```go
func TestSanitizeHeaderValue(t *testing.T) {
	tests := []struct{ in, want string }{
		{"normal@example.com", "normal@example.com"},
		{"evil@example.com\r\nBcc: victim@evil.com", "evil@example.comBcc: victim@evil.com"},
		{"has\nnewline", "hasnewline"},
		{"has\rreturn", "hasreturn"},
	}
	for _, tt := range tests {
		got := sanitizeHeaderValue(tt.in)
		if got != tt.want {
			t.Errorf("sanitizeHeaderValue(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestBuildRFC5322_HeaderInjection(t *testing.T) {
	msg := &MessageEntry{
		EntryID:     "1",
		SenderEmail: "evil@example.com\r\nBcc: victim@evil.com",
		Subject:     "Test",
		BodyText:    "body",
	}
	raw, err := BuildRFC5322(msg, nil)
	if err != nil {
		t.Fatalf("BuildRFC5322: %v", err)
	}
	if strings.Contains(string(raw), "Bcc:") {
		t.Error("header injection: Bcc header was injected via SenderEmail")
	}
}
```

---

### Issue 2 — No filename sanitization on attachments (MEDIUM)

**Problem:** Attachment filenames from PST (line 82-90) are used directly in Content-Type and Content-Disposition headers without sanitization. A malicious PST could embed `../../etc/passwd` or null bytes.

**Fix:**

1. Add `"path/filepath"` to the import block.

2. Add a `sanitizeFilename` helper:

```go
// sanitizeFilename strips path components and dangerous characters from
// attachment filenames sourced from PST data.
func sanitizeFilename(name string) string {
	name = filepath.Base(name)
	if name == "." {
		return ""
	}
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, name)
}
```

3. In `BuildRFC5322`, in the attachment loop (line 74), after `att := &attachments[i]`, add:

```go
fname := sanitizeFilename(att.Filename)
```

Then use `fname` instead of `att.Filename` on lines 82, 88, and 90.

4. Add test in `mime_test.go`:

```go
func TestSanitizeFilename(t *testing.T) {
	tests := []struct{ in, want string }{
		{"report.pdf", "report.pdf"},
		{"../../etc/passwd", "passwd"},
		{`C:\Users\evil\payload.exe`, "payload.exe"},
		{"file\x00name.txt", "filename.txt"},
		{"normal.doc", "normal.doc"},
		{"", ""},
	}
	for _, tt := range tests {
		got := sanitizeFilename(tt.in)
		if got != tt.want {
			t.Errorf("sanitizeFilename(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
```

---

### Issue 3 — ContentID not sanitized (MEDIUM)

**Problem:** `att.ContentID` (line 87) is wrapped in `<` and `>` without sanitizing. If the ContentID contains `>` or newlines, it could break MIME structure.

**Fix:**

1. Add a `sanitizeContentID` helper:

```go
// sanitizeContentID strips characters that could break the Content-Id
// angle-bracket wrapper.
func sanitizeContentID(s string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case '<', '>', '\r', '\n':
			return -1
		default:
			return r
		}
	}, s)
}
```

2. At line 86-87, change to:

```go
if att.ContentID != "" {
	cid := sanitizeContentID(att.ContentID)
	ah.Set("Content-Id", "<"+cid+">")
```

Use `cid` also in the condition for Content-Disposition (inline vs attachment) — or keep using `att.ContentID != ""` for the condition, which is fine since we only sanitize the *value*.

3. Add test in `mime_test.go`:

```go
func TestSanitizeContentID(t *testing.T) {
	tests := []struct{ in, want string }{
		{"abc123@example.com", "abc123@example.com"},
		{"<injected>header\r\n", "injectedheader"},
	}
	for _, tt := range tests {
		got := sanitizeContentID(tt.in)
		if got != tt.want {
			t.Errorf("sanitizeContentID(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
```

---

### Issue 4 — QP encoder trailing whitespace not encoded (MEDIUM)

**Problem:** `writeQP` (line 268-305) doesn't encode trailing spaces at end of lines before `\n`. RFC 2045 section 6.7 requires trailing whitespace on a line to be encoded.

**Fix:**

Replace the `writeQP` function with this corrected version:

```go
func writeQP(dst interface{ Write([]byte) (int, error) }, s string) {
	const maxLine = 76
	var line strings.Builder

	encodeTrailingWS := func() {
		str := line.String()
		if len(str) == 0 {
			return
		}
		last := str[len(str)-1]
		if last == ' ' || last == '\t' {
			line.Reset()
			line.WriteString(str[:len(str)-1])
			line.WriteString(fmt.Sprintf("=%02X", last))
		}
	}

	flush := func(soft bool) {
		if !soft {
			encodeTrailingWS()
		}
		if soft {
			_, _ = dst.Write([]byte(line.String() + "=\r\n"))
		} else {
			_, _ = dst.Write([]byte(line.String() + "\r\n"))
		}
		line.Reset()
	}

	for _, b := range []byte(s) {
		var encoded string
		switch {
		case b == '\r':
			continue
		case b == '\n':
			flush(false)
			continue
		case b == '=':
			encoded = "=3D"
		case b < 32 || b > 126:
			encoded = fmt.Sprintf("=%02X", b)
		default:
			encoded = string(rune(b))
		}

		if line.Len()+len(encoded) > maxLine {
			flush(true)
		}
		line.WriteString(encoded)
	}
	if line.Len() > 0 {
		flush(false)
	}
}
```

Add test in `mime_test.go` (add `"bytes"` to the test file imports):

```go
func TestWriteQP_TrailingSpace(t *testing.T) {
	var buf bytes.Buffer
	writeQP(&buf, "hello \nworld")
	got := buf.String()
	if !strings.Contains(got, "hello=20\r\n") {
		t.Errorf("trailing space not encoded: got %q", got)
	}
}
```

---

## Group 2: `internal/pst/reader.go` (Issues 7, 8, 10)

### Issue 8 — strings.Builder for binary attachment data (LOW)

**Problem:** `ReadAttachments` (line 247-265) streams binary attachment content into `strings.Builder` then converts via `[]byte(buf.String())`, causing unnecessary copies. Also requires the `stringBuilderWriter` adapter type.

**Fix:**

1. Add `"bytes"` to the import block.

2. In `ReadAttachments`, change lines 247-248 from:

```go
var buf strings.Builder
written, err := att.WriteTo((*stringBuilderWriter)(&buf))
```

To:

```go
var buf bytes.Buffer
written, err := att.WriteTo(&buf)
```

3. Change line 264 from `[]byte(buf.String())` to `buf.Bytes()`.

4. Delete the `stringBuilderWriter` type and its `Write` method (lines 293-297):

```go
// stringBuilderWriter adapts strings.Builder to io.Writer.
type stringBuilderWriter strings.Builder

func (w *stringBuilderWriter) Write(p []byte) (int, error) {
	return (*strings.Builder)(w).Write(p)
}
```

Note: `"strings"` import is still needed by `isExchangeDN`, `extractCN`, etc.

---

### Issue 7 — Single large attachment can exceed memory limit (MEDIUM)

**Problem:** `ReadAttachments` reads each full attachment into memory *before* checking `maxBytes`. A single 128 MiB attachment is fully buffered before the limit triggers.

**Fix:**

In `ReadAttachments`, after the `mimeType` assignment (line 241) and *before* the `var buf bytes.Buffer` line (which will be the new line after Issue 8 fix), add a pre-check:

```go
		// Pre-check: skip remaining attachments if this one would exceed the limit.
		if maxBytes > 0 {
			estimatedSize := int64(att.GetAttachSize())
			if estimatedSize > 0 && totalBytes+estimatedSize > maxBytes {
				break
			}
		}
```

Keep the existing post-read check (lines 253-256) as a safety net since `GetAttachSize()` may be inaccurate or zero.

---

### Issue 10 — windowsFiletimeToTime no validation on negative values (LOW)

**Problem:** Negative FILETIME values (corrupted PST data) produce nonsensical dates via the arithmetic.

**Fix:**

1. In `windowsFiletimeToTime` (line 27), change `if ft == 0` to `if ft <= 0`.

2. Add a test case to the existing `TestWindowsFiletimeToTime` in `mime_test.go`:

```go
{
	name: "negative",
	ft:   -1,
	want: time.Time{},
},
```

---

## Group 3: `internal/importer/pst_import.go` (Issues 5, 6, 9)

### Issue 9 — flushPending takes unused labelIDs parameter (LOW)

**Problem:** `flushPending` (line 281) accepts `labelIDs map[string]int64` but never reads it.

**Fix:**

1. Change line 281 from:

```go
flushPending := func(labelIDs map[string]int64) (stop bool) {
```

To:

```go
flushPending := func() (stop bool) {
```

2. Update call site at line ~503 from `flushPending(labelCache)` to `flushPending()`.

3. Update call site at line ~518 from `flushPending(labelCache)` to `flushPending()`.

---

### Issue 5 — checkpointBlocked never resets (MEDIUM)

**Problem:** Once `checkpointBlocked` is set `true` after an ingest error (line 359), it never resets. No further checkpoints are saved for the rest of the import. If interrupted after that, all progress since the last checkpoint is lost.

**Fix:**

At lines 377-380 (the cleanup block at end of `flushPending`), add a reset before `return false`:

```go
	clear(pending)
	pending = pending[:0]
	pendingBytes = 0
	// Reset checkpoint blocking so future successful batches can checkpoint.
	// checkpointBlocked is set when an ingest error occurs within a batch;
	// once the batch completes, we allow checkpointing again.
	checkpointBlocked = false
	return false
```

---

### Issue 6 — Resume by folder index is fragile (MEDIUM)

**Problem:** Resume saves `FolderIndex` (position in the folders slice) but doesn't validate that the folder at that index is still the same folder. If folder ordering changes between runs, the resume skips the wrong folder.

**Fix:**

After line 244 (`summary.FoldersTotal = len(folders)`) and before line 247 (the `const` block), add:

```go
	// Validate resume folder still matches.
	if summary.WasResumed && resume.FolderIndex > 0 {
		if resume.FolderIndex >= len(folders) {
			log.Warn("resume folder index out of range; restarting from beginning",
				"saved_index", resume.FolderIndex,
				"folder_count", len(folders),
			)
			resume.FolderIndex = 0
			resume.MsgIndex = 0
		} else if folders[resume.FolderIndex].Entry.Path != resume.FolderPath {
			log.Warn("resume folder path mismatch; restarting from beginning",
				"saved_path", resume.FolderPath,
				"actual_path", folders[resume.FolderIndex].Entry.Path,
			)
			resume.FolderIndex = 0
			resume.MsgIndex = 0
		}
	}
```

---

## Group 4: `cmd/msgvault/cmd/import_pst.go` (Issue 11)

### Issue 11 — os.Exit(130) bypasses deferred cleanup (LOW — document only)

**Problem:** The second SIGINT calls `os.Exit(130)` which skips all deferred functions (`st.Close()`, `pstFile.Close()`). This is a deliberate UX trade-off but not documented.

**Fix:**

Add a comment above `os.Exit(130)` at line 85:

```go
					// NOTE: os.Exit bypasses all deferred cleanup (db.Close,
					// pstFile.Close, etc.). This is deliberate: the first
					// Ctrl+C already triggered graceful shutdown with checkpoint
					// saving via context cancellation. SQLite WAL journaling
					// ensures database consistency even on hard exit.
					fmt.Fprintln(cmd.ErrOrStderr(), "Interrupted again. Exiting immediately.")
					os.Exit(130)
```

---

## Verification

After all changes, run:

```bash
go fmt ./...
go vet ./...
go test ./internal/pst/ ./internal/importer/
```

All existing tests must still pass, plus the new tests added above.
