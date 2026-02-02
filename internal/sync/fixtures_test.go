package sync

import testemail "github.com/wesm/msgvault/internal/testutil/email"

// testMIME is a simple plain-text MIME message for testing.
var testMIME = testemail.NewMessage().Bytes()

// testMIMEWithAttachment is a MIME message with a binary attachment.
var testMIMEWithAttachment = testemail.NewMessage().
	Subject("Test with Attachment").
	Body("This is the message body.").
	WithAttachment("test.bin", "application/octet-stream", []byte("Hello World!")).
	Bytes()

// testMIMENoSubject is a MIME message with no Subject header.
var testMIMENoSubject = testemail.NewMessage().
	NoSubject().
	Body("Message with no subject line.").
	Bytes()

// testMIMEMultipleRecipients is a MIME message with To, Cc, and Bcc recipients.
var testMIMEMultipleRecipients = testemail.NewMessage().
	To("to1@example.com, to2@example.com").
	Cc("cc1@example.com").
	Bcc("bcc1@example.com").
	Subject("Multiple Recipients").
	Body("Message with multiple recipients.").
	Bytes()

// testMIMEDuplicateRecipients is a MIME message with duplicate addresses across To/Cc/Bcc.
var testMIMEDuplicateRecipients = testemail.NewMessage().
	To(`duplicate@example.com, other@example.com, "Duplicate Person" <duplicate@example.com>`).
	Cc(`cc-dup@example.com, "CC Duplicate" <cc-dup@example.com>`).
	Bcc("bcc-dup@example.com, bcc-dup@example.com").
	Subject("Duplicate Recipients").
	Body("Message with duplicate recipients in To, Cc, and Bcc fields.").
	Bytes()
