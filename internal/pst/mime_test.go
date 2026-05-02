package pst

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestWindowsFiletimeToTime(t *testing.T) {
	tests := []struct {
		name string
		ft   int64
		want time.Time
	}{
		{
			name: "zero",
			ft:   0,
			want: time.Time{},
		},
		{
			name: "unix epoch",
			// 1970-01-01 00:00:00 UTC in Windows FILETIME
			ft:   116444736000000000,
			want: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "2024-01-15 10:30:00 UTC",
			// (2024-01-15T10:30:00 UTC - 1601-01-01) in 100ns intervals
			ft:   133497882000000000,
			want: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		},
		{
			name: "negative",
			ft:   -1,
			want: time.Time{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := windowsFiletimeToTime(tt.ft)
			if !got.Equal(tt.want) {
				t.Errorf("windowsFiletimeToTime(%d) = %v, want %v", tt.ft, got, tt.want)
			}
		})
	}
}

func TestExtractCN(t *testing.T) {
	tests := []struct {
		dn   string
		want string
	}{
		{"/O=CORP/OU=EXCHANGE/CN=RECIPIENTS/CN=JSMITH", "JSMITH"},
		{"/o=Contoso/ou=Exchange/cn=Recipients/cn=jdoe", "jdoe"},
		{"user@example.com", "user@example.com"}, // not a DN
		{"", ""},
	}
	for _, tt := range tests {
		got := extractCN(tt.dn)
		if got != tt.want {
			t.Errorf("extractCN(%q) = %q, want %q", tt.dn, got, tt.want)
		}
	}
}

func TestIsExchangeDN(t *testing.T) {
	if !isExchangeDN("/O=CORP/OU=EXCH/CN=user") {
		t.Error("expected true for /O= DN")
	}
	if !isExchangeDN("/o=corp/cn=user") {
		t.Error("expected true for /o= DN")
	}
	if isExchangeDN("user@example.com") {
		t.Error("expected false for SMTP address")
	}
}

func TestBuildRFC5322_SynthesizedHeaders(t *testing.T) {
	msg := &MessageEntry{
		EntryID:     "12345",
		FolderPath:  "Inbox",
		Subject:     "Hello World",
		BodyText:    "This is a test message.",
		SenderName:  "Alice",
		SenderEmail: "alice@example.com",
		DisplayTo:   "Bob",
		MessageID:   "<abc123@example.com>",
		SentAt:      time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
	}

	raw, err := BuildRFC5322(msg, nil)
	if err != nil {
		t.Fatalf("BuildRFC5322: %v", err)
	}

	s := string(raw)
	if !strings.Contains(s, "From:") {
		t.Error("missing From header")
	}
	if !strings.Contains(s, "alice@example.com") {
		t.Error("missing sender email")
	}
	if !strings.Contains(s, "Subject:") {
		t.Error("missing Subject header")
	}
	if !strings.Contains(s, "Message-Id:") {
		t.Error("missing Message-Id header")
	}
	if !strings.Contains(s, "X-Msgvault-Synthesized: true") {
		t.Error("missing X-Msgvault-Synthesized header")
	}
	if !strings.Contains(s, "text/plain") {
		t.Error("missing text/plain content type")
	}
	if !strings.Contains(s, "This is a test message") {
		t.Error("body text not found in output")
	}
}

func TestBuildRFC5322_TransportHeaders(t *testing.T) {
	transportHeaders := "From: alice@example.com\r\nTo: bob@example.com\r\nSubject: Test\r\nMessage-ID: <orig@example.com>\r\nDate: Mon, 15 Jan 2024 10:30:00 +0000\r\n"

	msg := &MessageEntry{
		EntryID:          "99",
		TransportHeaders: transportHeaders,
		BodyText:         "Body text here.",
		BodyHTML:         "<p>Body HTML here.</p>",
	}

	raw, err := BuildRFC5322(msg, nil)
	if err != nil {
		t.Fatalf("BuildRFC5322: %v", err)
	}

	s := string(raw)
	// Original headers should be present.
	if !strings.Contains(s, "From: alice@example.com") {
		t.Error("missing original From header")
	}
	if !strings.Contains(s, "Message-ID: <orig@example.com>") {
		t.Error("missing original Message-ID header")
	}
	// Should NOT have synthesized header.
	if strings.Contains(s, "X-Msgvault-Synthesized") {
		t.Error("should not have X-Msgvault-Synthesized when transport headers present")
	}
	// Both text and HTML → multipart/alternative.
	if !strings.Contains(s, "multipart/alternative") {
		t.Error("expected multipart/alternative for text+html body")
	}
}

func TestBuildRFC5322_WithAttachments(t *testing.T) {
	msg := &MessageEntry{
		EntryID:     "42",
		Subject:     "With attachment",
		BodyText:    "See attached.",
		SenderEmail: "sender@example.com",
	}
	attachments := []AttachmentEntry{
		{
			Filename: "report.pdf",
			MIMEType: "application/pdf",
			Content:  []byte("%PDF-1.4 test"),
		},
	}

	raw, err := BuildRFC5322(msg, attachments)
	if err != nil {
		t.Fatalf("BuildRFC5322: %v", err)
	}

	s := string(raw)
	if !strings.Contains(s, "multipart/mixed") {
		t.Error("expected multipart/mixed for message with attachments")
	}
	if !strings.Contains(s, "report.pdf") {
		t.Error("attachment filename not found")
	}
	if !strings.Contains(s, "application/pdf") {
		t.Error("attachment content type not found")
	}
}

func TestBuildRFC5322_EmptyBody(t *testing.T) {
	msg := &MessageEntry{
		EntryID:     "1",
		SenderEmail: "a@b.com",
		Subject:     "No body",
	}

	raw, err := BuildRFC5322(msg, nil)
	if err != nil {
		t.Fatalf("BuildRFC5322: %v", err)
	}

	s := string(raw)
	if !strings.Contains(s, "text/plain") {
		t.Error("expected text/plain even for empty body")
	}
}

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
	// Check that "Bcc:" does not appear as a separate header line (the actual
	// injection vector). A sanitized value may still contain "Bcc:" as a
	// substring within the From address, but not as a new header line.
	if strings.Contains(string(raw), "\r\nBcc:") {
		t.Error("header injection: Bcc header was injected via SenderEmail")
	}
}

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

func TestWriteQP_TrailingSpace(t *testing.T) {
	var buf bytes.Buffer
	writeQP(&buf, "hello \nworld")
	got := buf.String()
	if !strings.Contains(got, "hello=20\r\n") {
		t.Errorf("trailing space not encoded: got %q", got)
	}
}

func TestBuildRFC5322_TransportHeadersStripMIME(t *testing.T) {
	// Transport headers that include MIME headers — these should be stripped.
	transportHeaders := "From: alice@example.com\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=us-ascii\r\nContent-Transfer-Encoding: 7bit\r\nSubject: Old MIME\r\n"

	msg := &MessageEntry{
		TransportHeaders: transportHeaders,
		BodyText:         "Hello.",
	}

	raw, err := BuildRFC5322(msg, nil)
	if err != nil {
		t.Fatalf("BuildRFC5322: %v", err)
	}

	s := string(raw)
	// From and Subject should be present.
	if !strings.Contains(s, "From: alice@example.com") {
		t.Error("From header missing")
	}
	if !strings.Contains(s, "Subject: Old MIME") {
		t.Error("Subject header missing")
	}
	// The old Content-Type from transport headers should not appear verbatim.
	// (Our rebuilt MIME-Version and Content-Type replaces it.)
	// We expect exactly one Content-Type occurrence (ours, for text/plain).
	count := strings.Count(s, "Content-Type:")
	if count != 1 {
		t.Errorf("expected 1 Content-Type header, got %d", count)
	}
}
