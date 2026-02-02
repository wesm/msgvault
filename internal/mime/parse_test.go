package mime

import (
	"testing"
	"time"

	"github.com/jhillyerd/enmime"
	testemail "github.com/wesm/msgvault/internal/testutil/email"
)

// emailOptions is an alias for testemail.Options for local convenience.
type emailOptions = testemail.Options

// makeRawEmail delegates to testemail.MakeRaw.
func makeRawEmail(opts emailOptions) []byte {
	return testemail.MakeRaw(opts)
}

// mustParse calls Parse and fails the test on error.
func mustParse(t *testing.T, raw []byte) *Message {
	t.Helper()
	msg, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse() failed: %v", err)
	}
	return msg
}

// parseEmail combines makeRawEmail and mustParse into a single test helper.
func parseEmail(t *testing.T, opts emailOptions) *Message {
	t.Helper()
	return mustParse(t, makeRawEmail(opts))
}

// assertSubject checks that msg.Subject equals want.
func assertSubject(t *testing.T, msg *Message, want string) {
	t.Helper()
	if msg.Subject != want {
		t.Errorf("Subject = %q, want %q", msg.Subject, want)
	}
}

// assertStringSliceEqual delegates to testemail.AssertStringSliceEqual.
func assertStringSliceEqual(t *testing.T, got, want []string, label string) {
	t.Helper()
	testemail.AssertStringSliceEqual(t, got, want, label)
}

// assertParseDateOK checks that parseDate succeeds and returns a non-zero time.
func assertParseDateOK(t *testing.T, input string) {
	t.Helper()
	got, err := parseDate(input)
	if err != nil {
		t.Errorf("parseDate(%q) unexpected error: %v", input, err)
	}
	if got.IsZero() {
		t.Errorf("parseDate(%q) returned zero time, expected parsed date", input)
	}
}

// assertParseDateZero checks that parseDate returns zero time without error.
func assertParseDateZero(t *testing.T, input string) {
	t.Helper()
	got, err := parseDate(input)
	if err != nil {
		t.Errorf("parseDate(%q) unexpected error: %v (should return zero time, not error)", input, err)
	}
	if !got.IsZero() {
		t.Errorf("parseDate(%q) = %v, expected zero time for invalid input", input, got)
	}
}

// assertParseDateUTC checks that parseDate returns the expected UTC time.
func assertParseDateUTC(t *testing.T, input string, want time.Time) {
	t.Helper()
	got, err := parseDate(input)
	if err != nil {
		t.Fatalf("parseDate(%q) unexpected error: %v", input, err)
	}
	if got.Location() != time.UTC {
		t.Errorf("parseDate(%q) returned location %v, want UTC", input, got.Location())
	}
	if !got.Equal(want) {
		t.Errorf("parseDate(%q) = %v, want %v", input, got, want)
	}
}

// logParseDiagnostics logs To addresses and parsing errors for debugging.
func logParseDiagnostics(t *testing.T, msg *Message) {
	t.Helper()
	t.Logf("To addresses: %v", msg.To)
	t.Logf("Parsing errors: %v", msg.Errors)
}

func TestExtractDomain(t *testing.T) {
	tests := []struct {
		email  string
		domain string
	}{
		{"user@example.com", "example.com"},
		{"USER@EXAMPLE.COM", "example.com"},
		{"user@sub.domain.org", "sub.domain.org"},
		{"nodomain", ""},
		{"", ""},
		{"@domain.com", "domain.com"},
	}

	for _, tc := range tests {
		t.Run(tc.email, func(t *testing.T) {
			got := extractDomain(tc.email)
			if got != tc.domain {
				t.Errorf("extractDomain(%q) = %q, want %q", tc.email, got, tc.domain)
			}
		})
	}
}

func TestParseReferences(t *testing.T) {
	tests := []struct {
		input string
		want  []string
	}{
		{"<abc@example.com>", []string{"abc@example.com"}},
		{"<a@x.com> <b@y.com>", []string{"a@x.com", "b@y.com"}},
		{"<a@x.com>\n\t<b@y.com>", []string{"a@x.com", "b@y.com"}},
		{"", nil},
		{"   ", nil},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got := parseReferences(tc.input)
			assertStringSliceEqual(t, got, tc.want, "parseReferences("+tc.input+")")
		})
	}
}

func TestParseDate(t *testing.T) {
	// parseDate returns zero time (not error) for unparseable dates.
	// This is intentional - malformed dates are common in email and
	// shouldn't fail the entire parse.

	// Valid RFC date formats should parse successfully
	validDates := []struct {
		input string
	}{
		{"Mon, 02 Jan 2006 15:04:05 -0700"},
		{"Mon, 2 Jan 2006 15:04:05 MST"},
		{"02 Jan 2006 15:04:05 -0700"},
		{"Mon, 02 Jan 2006 15:04:05 -0700 (PST)"},
		{"Mon,  2 Dec 2024 11:42:03 +0000 (UTC)"}, // Double space after comma (real-world case)
		{"2006-01-02T15:04:05Z"},                  // ISO 8601 UTC
		{"2006-01-02T15:04:05-07:00"},             // ISO 8601 with offset
		{"2006-01-02 15:04:05 -0700"},             // SQL-like with timezone
		{"2006-01-02 15:04:05"},                   // SQL-like without timezone (assumes UTC)
	}

	for _, tc := range validDates {
		t.Run("valid/"+tc.input, func(t *testing.T) {
			assertParseDateOK(t, tc.input)
		})
	}

	// Invalid/unparseable dates should return zero time without error
	invalidDates := []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"garbage", "not a date"},
		{"date_only", "2006-01-02"},
		{"spelled_month", "January 2, 2006"},
	}

	for _, tc := range invalidDates {
		t.Run("invalid/"+tc.name, func(t *testing.T) {
			assertParseDateZero(t, tc.input)
		})
	}

	// Verify parsed values are converted to UTC
	// 15:04:05 -0700 = 22:04:05 UTC
	assertParseDateUTC(t, "Mon, 02 Jan 2006 15:04:05 -0700",
		time.Date(2006, 1, 2, 22, 4, 5, 0, time.UTC))

	// Verify double-space handling with parenthesized timezone
	assertParseDateUTC(t, "Mon,  2 Dec 2024 11:42:03 +0000 (UTC)",
		time.Date(2024, 12, 2, 11, 42, 3, 0, time.UTC))
}

func TestStripHTML(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		// Basic tag stripping
		{"paragraph", "<p>Hello</p>", "Hello"},
		{"nested_span", "<div><span>Nested</span></div>", "Nested"},
		{"no_tags", "No tags", "No tags"},
		{"inline_tags", "<b>Bold</b> and <i>italic</i>", "Bold and italic"},
		{"empty", "", ""},

		// Script/style removal (including content)
		{"script_removed", "<script>alert('xss')</script>Text", "Text"},
		{"style_removed", "<style>.class{color:red}</style>Content", "Content"},
		{"head_removed", "<head><title>Title</title></head>Body", "Body"},

		// Newline normalization
		{"crlf_to_lf", "Line1\r\nLine2\r\nLine3", "Line1\nLine2\nLine3"},
		{"collapse_newlines", "Multiple\n\n\n\nNewlines", "Multiple\n\nNewlines"},

		// HTML entities
		{"nbsp_entity", "Hello&nbsp;World", "Hello World"},
		{"amp_entity", "Tom &amp; Jerry", "Tom & Jerry"},
		{"lt_gt_entities", "5 &lt; 10 &gt; 3", "5 < 10 > 3"},
		{"quote_entity", "&quot;quoted&quot;", "\"quoted\""},
		{"numeric_entity", "&#169; 2024", "© 2024"},
		{"hex_entity", "&#x2022; bullet", "• bullet"},

		// Block elements create line breaks
		{"br_tag", "Line1<br>Line2", "Line1\nLine2"},
		{"br_self_close", "Line1<br/>Line2", "Line1\nLine2"},
		{"paragraph_breaks", "<p>Para1</p><p>Para2</p>", "Para1\n\nPara2"},
		{"div_breaks", "<div>Block1</div><div>Block2</div>", "Block1\n\nBlock2"},
		{"heading_breaks", "<h1>Title</h1><p>Content</p>", "Title\n\nContent"},

		// Complex HTML email
		{
			"complex_html",
			`<html><head><style>.x{}</style></head><body>
			<p>Hello,</p>
			<p>This is a <b>test</b> email with &amp; special chars.</p>
			<br>
			<p>Thanks!</p>
			</body></html>`,
			"Hello,\n\nThis is a test email with & special chars.\n\nThanks!",
		},

		// Whitespace collapse
		{"multiple_spaces", "Hello    World", "Hello World"},
		{"nbsp_spaces", "Hello&nbsp;&nbsp;&nbsp;World", "Hello World"},

		// Preformatted content - whitespace is NOT preserved (documented behavior)
		// This is acceptable for email preview where code formatting is secondary
		{"pre_whitespace_collapsed", "<pre>  code  here  </pre>", "code here"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := StripHTML(tc.input)
			if got != tc.want {
				t.Errorf("StripHTML() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestMessage_GetBodyText(t *testing.T) {
	// Prefers plain text
	msg := &Message{BodyText: "plain", BodyHTML: "<p>html</p>"}
	if got := msg.GetBodyText(); got != "plain" {
		t.Errorf("GetBodyText() = %q, want %q", got, "plain")
	}

	// Falls back to HTML
	msg = &Message{BodyHTML: "<p>html only</p>"}
	if got := msg.GetBodyText(); got != "html only" {
		t.Errorf("GetBodyText() = %q, want %q", got, "html only")
	}

	// Empty
	msg = &Message{}
	if got := msg.GetBodyText(); got != "" {
		t.Errorf("GetBodyText() = %q, want empty", got)
	}
}

func TestMessage_GetFirstFrom(t *testing.T) {
	msg := &Message{
		From: []Address{
			{Name: "Alice", Email: "alice@example.com", Domain: "example.com"},
			{Name: "Bob", Email: "bob@example.com", Domain: "example.com"},
		},
	}

	got := msg.GetFirstFrom()
	if got.Email != "alice@example.com" {
		t.Errorf("GetFirstFrom() = %v, want alice@example.com", got)
	}

	// Empty
	msg = &Message{}
	got = msg.GetFirstFrom()
	if got.Email != "" {
		t.Errorf("GetFirstFrom() on empty = %v, want empty", got)
	}
}

// TestParse_MinimalMessage tests our Parse wrapper with a minimal valid message.
// This verifies our wrapper works, not enmime's parsing logic.
func TestParse_MinimalMessage(t *testing.T) {
	msg := parseEmail(t, emailOptions{
		Body: "Body text",
		Headers: map[string]string{
			"Date": "Mon, 02 Jan 2006 15:04:05 -0700",
		},
	})

	if len(msg.From) != 1 || msg.From[0].Email != "sender@example.com" {
		t.Errorf("From = %v, want sender@example.com", msg.From)
	}

	if len(msg.To) != 1 || msg.To[0].Email != "recipient@example.com" {
		t.Errorf("To = %v, want recipient@example.com", msg.To)
	}

	assertSubject(t, msg, "Test")

	if msg.BodyText != "Body text" {
		t.Errorf("BodyText = %q, want %q", msg.BodyText, "Body text")
	}

	// Verify domain extraction worked
	if msg.From[0].Domain != "example.com" {
		t.Errorf("From domain = %q, want %q", msg.From[0].Domain, "example.com")
	}
}

// TestParse_InvalidCharset verifies enmime handles malformed charsets gracefully.
// Enmime should not fail on invalid charset - it attempts conversion and collects errors.
func TestParse_InvalidCharset(t *testing.T) {
	// Message with non-existent charset - enmime should handle this gracefully
	msg := parseEmail(t, emailOptions{
		ContentType: "text/plain; charset=invalid-charset-xyz",
		Body:        "Body text",
	})

	// Should still be able to access subject and addresses
	assertSubject(t, msg, "Test")

	// Body might be garbled or empty, but should not crash
	t.Logf("Body text with invalid charset: %q", msg.BodyText)
	t.Logf("Parsing errors: %v", msg.Errors)
}

// TestParse_Latin1Charset verifies Latin-1 (ISO-8859-1) charset is handled.
func TestParse_Latin1Charset(t *testing.T) {
	// Latin-1 encoded content with special characters.
	// This test uses raw bytes because the subject/body contain non-UTF-8 Latin-1 bytes.
	raw := []byte("From: sender@example.com\r\nTo: recipient@example.com\r\nSubject: Caf\xe9\r\nContent-Type: text/plain; charset=iso-8859-1\r\n\r\nCaf\xe9 au lait")

	msg := mustParse(t, raw)

	// enmime should convert Latin-1 to UTF-8
	// é in Latin-1 is 0xe9, in UTF-8 it's 0xc3 0xa9
	if msg.BodyText != "Café au lait" {
		t.Errorf("BodyText = %q, want %q", msg.BodyText, "Café au lait")
	}
}

// TestParse_RFC2822GroupAddress verifies RFC 2822 group address syntax is handled.
// Group syntax: "group-name: addr1, addr2, ...;"
func TestParse_RFC2822GroupAddress(t *testing.T) {
	// Message with undisclosed-recipients group (common in BCC scenarios)
	msg := parseEmail(t, emailOptions{
		To:   "undisclosed-recipients:;",
		Body: "Body",
	})

	// Group with no addresses should result in empty To list
	logParseDiagnostics(t, msg)

	// Should not crash - that's the main requirement
	assertSubject(t, msg, "Test")
}

// TestParse_RFC2822GroupAddressWithMembers verifies group with actual addresses.
func TestParse_RFC2822GroupAddressWithMembers(t *testing.T) {
	// Group with member addresses
	msg := parseEmail(t, emailOptions{
		To:   "team: alice@example.com, bob@example.com;",
		Body: "Body",
	})

	logParseDiagnostics(t, msg)

	// Ideally we'd extract alice and bob from the group
	// Let's see how enmime handles this
	assertSubject(t, msg, "Test")
}

// TestIsBodyPart_ContentTypeWithParams tests that Content-Type with charset
// parameters is correctly identified as body content.
func TestIsBodyPart_ContentTypeWithParams(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
		filename    string
		disposition string
		wantIsBody  bool
	}{
		// Content-Type with charset parameter should still be body
		{"text/plain with charset", "text/plain; charset=utf-8", "", "", true},
		{"text/html with charset", "text/html; charset=utf-8", "", "", true},
		{"text/plain with format", "text/plain; format=flowed", "", "", true},
		{"TEXT/PLAIN uppercase with charset", "TEXT/PLAIN; CHARSET=UTF-8", "", "", true},

		// Non-text types are not body parts
		{"application/pdf", "application/pdf", "", "", false},
		{"image/png", "image/png", "", "", false},

		// With filename → attachment, not body
		{"text/plain with filename", "text/plain; charset=utf-8", "file.txt", "", false},
		{"text/html with filename", "text/html; charset=utf-8", "page.html", "", false},

		// Explicit disposition: attachment (with or without params)
		{"attachment disposition", "text/plain", "", "attachment", false},
		{"attachment with params", "text/plain", "", "attachment; filename=\"x.txt\"", false},
		{"ATTACHMENT uppercase", "text/plain", "", "ATTACHMENT; filename=\"x.txt\"", false},

		// Inline disposition is still body
		{"inline disposition", "text/plain; charset=utf-8", "", "inline", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock enmime.Part (we only need the fields isBodyPart checks)
			part := &enmime.Part{
				ContentType: tt.contentType,
				FileName:    tt.filename,
				Disposition: tt.disposition,
			}
			got := isBodyPart(part)
			if got != tt.wantIsBody {
				t.Errorf("isBodyPart() = %v, want %v", got, tt.wantIsBody)
			}
		})
	}
}
