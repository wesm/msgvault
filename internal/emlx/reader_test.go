package emlx

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestParse_ValidEmlxWithPlist(t *testing.T) {
	mime := "From: alice@example.com\r\nSubject: Hello\r\n\r\nBody\r\n"
	plist := `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>date-sent</key>
	<real>252460800</real>
	<key>flags</key>
	<integer>8590195713</integer>
	<key>original-mailbox</key>
	<string>imap://user@example.com/INBOX</string>
</dict>
</plist>`

	data := fmt.Sprintf("%d\n%s%s", len(mime), mime, plist)

	msg, err := Parse([]byte(data))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if string(msg.Raw) != mime {
		t.Fatalf("Raw = %q, want %q", msg.Raw, mime)
	}

	// date-sent 252460800 seconds from Apple epoch (2001-01-01)
	// = 2009-01-01 00:00:00 UTC
	wantDate := time.Date(2009, 1, 1, 0, 0, 0, 0, time.UTC)
	if !msg.PlistDate.Equal(wantDate) {
		t.Fatalf("PlistDate = %v, want %v", msg.PlistDate, wantDate)
	}
	if msg.Flags != 8590195713 {
		t.Fatalf("Flags = %d, want 8590195713", msg.Flags)
	}
	if msg.OrigMailbox != "imap://user@example.com/INBOX" {
		t.Fatalf(
			"OrigMailbox = %q, want %q",
			msg.OrigMailbox,
			"imap://user@example.com/INBOX",
		)
	}
}

func TestParse_ValidEmlxNoPlist(t *testing.T) {
	mime := "From: alice@example.com\r\nSubject: Hello\r\n\r\nBody\r\n"
	data := fmt.Sprintf("%d\n%s", len(mime), mime)

	msg, err := Parse([]byte(data))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if string(msg.Raw) != mime {
		t.Fatalf("Raw = %q, want %q", msg.Raw, mime)
	}
	if !msg.PlistDate.IsZero() {
		t.Fatalf("PlistDate = %v, want zero", msg.PlistDate)
	}
}

func TestParse_ByteCountMismatch(t *testing.T) {
	// Byte count is larger than available data.
	data := "9999\nshort"
	_, err := Parse([]byte(data))
	if err == nil {
		t.Fatalf("expected error for byte count mismatch")
	}
}

func TestParse_NonNumericByteCount(t *testing.T) {
	data := "abc\nFrom: test\r\n\r\n"
	_, err := Parse([]byte(data))
	if err == nil {
		t.Fatalf("expected error for non-numeric byte count")
	}
}

func TestParse_ZeroByteCount(t *testing.T) {
	plist := `<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0"><dict></dict></plist>`
	data := fmt.Sprintf("0\n%s", plist)

	msg, err := Parse([]byte(data))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(msg.Raw) != 0 {
		t.Fatalf("Raw length = %d, want 0", len(msg.Raw))
	}
}

func TestParse_EmptyFile(t *testing.T) {
	_, err := Parse([]byte{})
	if err == nil {
		t.Fatalf("expected error for empty file")
	}
}

func TestParse_NoNewline(t *testing.T) {
	_, err := Parse([]byte("42"))
	if err == nil {
		t.Fatalf("expected error for missing newline")
	}
}

func TestParse_NegativeByteCount(t *testing.T) {
	data := "-1\nstuff"
	_, err := Parse([]byte(data))
	if err == nil {
		t.Fatalf("expected error for negative byte count")
	}
}

func TestParse_PlistWithIntegerDateSent(t *testing.T) {
	mime := "From: test@example.com\r\n\r\n"
	plist := `<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0">
<dict>
	<key>date-sent</key>
	<integer>252460800</integer>
</dict>
</plist>`
	data := fmt.Sprintf("%d\n%s%s", len(mime), mime, plist)

	msg, err := Parse([]byte(data))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	wantDate := time.Date(2009, 1, 1, 0, 0, 0, 0, time.UTC)
	if !msg.PlistDate.Equal(wantDate) {
		t.Fatalf("PlistDate = %v, want %v", msg.PlistDate, wantDate)
	}
}

func TestParse_MalformedPlist(t *testing.T) {
	mime := "From: test@example.com\r\n\r\n"
	// Garbage after MIME — not valid XML.
	data := fmt.Sprintf("%d\n%sNOT XML AT ALL", len(mime), mime)

	msg, err := Parse([]byte(data))
	if err != nil {
		t.Fatalf("Parse: %v (should succeed with best-effort plist)", err)
	}
	if string(msg.Raw) != mime {
		t.Fatalf("Raw mismatch")
	}
	if !msg.PlistDate.IsZero() {
		t.Fatalf("PlistDate should be zero for malformed plist")
	}
}

func TestParseFile(t *testing.T) {
	mime := "From: alice@example.com\r\nSubject: Test\r\n\r\nHi\r\n"
	data := fmt.Sprintf("%d\n%s", len(mime), mime)

	dir := t.TempDir()
	path := filepath.Join(dir, "1234.emlx")
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatalf("write: %v", err)
	}

	msg, err := ParseFile(path)
	if err != nil {
		t.Fatalf("ParseFile: %v", err)
	}
	if string(msg.Raw) != mime {
		t.Fatalf("Raw = %q, want %q", msg.Raw, mime)
	}
}

func TestParseFile_NotFound(t *testing.T) {
	_, err := ParseFile("/nonexistent/12345.emlx")
	if err == nil {
		t.Fatalf("expected error for missing file")
	}
}

func TestParse_WhitespaceAroundByteCount(t *testing.T) {
	mime := "From: test@example.com\r\n\r\n"
	// Byte count with leading/trailing spaces.
	data := fmt.Sprintf("  %d  \n%s", len(mime), mime)

	msg, err := Parse([]byte(data))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if string(msg.Raw) != mime {
		t.Fatalf("Raw = %q, want %q", msg.Raw, mime)
	}
}
