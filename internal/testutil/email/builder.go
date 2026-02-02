package email

import (
	"encoding/base64"
	"fmt"
	"strings"
)

// Attachment represents a MIME attachment for the builder.
type Attachment struct {
	Filename    string
	ContentType string
	Data        []byte // raw bytes; will be base64-encoded
}

// MessageBuilder constructs MIME messages with a fluent API.
// By default, messages use \n line endings matching Go raw string literals.
type MessageBuilder struct {
	from        string
	to          string
	cc          string
	bcc         string
	subject     string
	date        string
	contentType string
	body        string
	headerKeys  []string
	headerVals  []string
	attachments []Attachment
	boundary    string
	crlf        bool // if true, use \r\n line endings
	noSubject   bool
}

// NewMessage creates a MessageBuilder with sensible defaults.
func NewMessage() *MessageBuilder {
	return &MessageBuilder{
		from:     "sender@example.com",
		to:       "recipient@example.com",
		date:     "Mon, 01 Jan 2024 12:00:00 +0000",
		subject:  "Test Message",
		body:     "This is a test message body.",
		boundary: "boundary123",
		headerKeys: nil,
		headerVals: nil,
	}
}

// From sets the From header.
func (b *MessageBuilder) From(v string) *MessageBuilder { b.from = v; return b }

// To sets the To header.
func (b *MessageBuilder) To(v string) *MessageBuilder { b.to = v; return b }

// Cc sets the Cc header.
func (b *MessageBuilder) Cc(v string) *MessageBuilder { b.cc = v; return b }

// Bcc sets the Bcc header.
func (b *MessageBuilder) Bcc(v string) *MessageBuilder { b.bcc = v; return b }

// Subject sets the Subject header. Use NoSubject() to omit it entirely.
func (b *MessageBuilder) Subject(v string) *MessageBuilder { b.subject = v; b.noSubject = false; return b }

// NoSubject omits the Subject header from the output.
func (b *MessageBuilder) NoSubject() *MessageBuilder { b.noSubject = true; return b }

// Date sets the Date header.
func (b *MessageBuilder) Date(v string) *MessageBuilder { b.date = v; return b }

// ContentType overrides the Content-Type header (for non-multipart messages).
func (b *MessageBuilder) ContentType(v string) *MessageBuilder { b.contentType = v; return b }

// Body sets the message body text.
func (b *MessageBuilder) Body(v string) *MessageBuilder { b.body = v; return b }

// Header adds an arbitrary header.
func (b *MessageBuilder) Header(key, value string) *MessageBuilder {
	b.headerKeys = append(b.headerKeys, key)
	b.headerVals = append(b.headerVals, value)
	return b
}

// Boundary sets the multipart boundary string.
func (b *MessageBuilder) Boundary(v string) *MessageBuilder { b.boundary = v; return b }

// WithAttachment adds an attachment to the message.
func (b *MessageBuilder) WithAttachment(filename, contentType string, data []byte) *MessageBuilder {
	b.attachments = append(b.attachments, Attachment{
		Filename:    filename,
		ContentType: contentType,
		Data:        data,
	})
	return b
}

// CRLF switches to \r\n line endings (RFC 2822 compliant).
func (b *MessageBuilder) CRLF() *MessageBuilder { b.crlf = true; return b }

// Bytes builds the complete MIME message.
func (b *MessageBuilder) Bytes() []byte {
	nl := "\n"
	if b.crlf {
		nl = "\r\n"
	}

	var s strings.Builder

	s.WriteString("From: " + b.from + nl)
	s.WriteString("To: " + b.to + nl)
	if b.cc != "" {
		s.WriteString("Cc: " + b.cc + nl)
	}
	if b.bcc != "" {
		s.WriteString("Bcc: " + b.bcc + nl)
	}
	if !b.noSubject {
		s.WriteString("Subject: " + b.subject + nl)
	}
	if b.date != "" {
		s.WriteString("Date: " + b.date + nl)
	}

	for i, k := range b.headerKeys {
		s.WriteString(k + ": " + b.headerVals[i] + nl)
	}

	if len(b.attachments) > 0 {
		s.WriteString("MIME-Version: 1.0" + nl)
		s.WriteString(fmt.Sprintf("Content-Type: multipart/mixed; boundary=%q", b.boundary) + nl)
		s.WriteString(nl)

		// Body part
		s.WriteString("--" + b.boundary + nl)
		s.WriteString(`Content-Type: text/plain; charset="utf-8"` + nl)
		s.WriteString(nl)
		s.WriteString(b.body + nl)

		// Attachment parts
		for _, att := range b.attachments {
			s.WriteString("--" + b.boundary + nl)
			ct := att.ContentType
			if ct == "" {
				ct = "application/octet-stream"
			}
			s.WriteString(fmt.Sprintf("Content-Type: %s; name=%q", ct, att.Filename) + nl)
			s.WriteString(fmt.Sprintf("Content-Disposition: attachment; filename=%q", att.Filename) + nl)
			s.WriteString("Content-Transfer-Encoding: base64" + nl)
			s.WriteString(nl)
			s.WriteString(base64.StdEncoding.EncodeToString(att.Data) + nl)
		}

		s.WriteString("--" + b.boundary + "--" + nl)
	} else {
		ct := b.contentType
		if ct == "" {
			ct = `text/plain; charset="utf-8"`
		}
		s.WriteString("Content-Type: " + ct + nl)
		s.WriteString(nl)
		s.WriteString(b.body + nl)
	}

	return []byte(s.String())
}
