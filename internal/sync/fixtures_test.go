package sync

// testMIME is a simple plain-text MIME message for testing.
var testMIME = []byte(`From: sender@example.com
To: recipient@example.com
Subject: Test Message
Date: Mon, 01 Jan 2024 12:00:00 +0000
Content-Type: text/plain; charset="utf-8"

This is a test message body.
`)

// testMIMEWithAttachment is a MIME message with a binary attachment.
var testMIMEWithAttachment = []byte(`From: sender@example.com
To: recipient@example.com
Subject: Test with Attachment
Date: Mon, 01 Jan 2024 12:00:00 +0000
MIME-Version: 1.0
Content-Type: multipart/mixed; boundary="boundary123"

--boundary123
Content-Type: text/plain; charset="utf-8"

This is the message body.
--boundary123
Content-Type: application/octet-stream; name="test.bin"
Content-Disposition: attachment; filename="test.bin"
Content-Transfer-Encoding: base64

SGVsbG8gV29ybGQh
--boundary123--
`)

// testMIMENoSubject is a MIME message with no Subject header.
var testMIMENoSubject = []byte(`From: sender@example.com
To: recipient@example.com
Date: Mon, 01 Jan 2024 12:00:00 +0000
Content-Type: text/plain; charset="utf-8"

Message with no subject line.
`)

// testMIMEMultipleRecipients is a MIME message with To, Cc, and Bcc recipients.
var testMIMEMultipleRecipients = []byte(`From: sender@example.com
To: to1@example.com, to2@example.com
Cc: cc1@example.com
Bcc: bcc1@example.com
Subject: Multiple Recipients
Date: Mon, 01 Jan 2024 12:00:00 +0000
Content-Type: text/plain; charset="utf-8"

Message with multiple recipients.
`)

// testMIMEDuplicateRecipients is a MIME message with duplicate addresses across To/Cc/Bcc.
var testMIMEDuplicateRecipients = []byte(`From: sender@example.com
To: duplicate@example.com, other@example.com, "Duplicate Person" <duplicate@example.com>
Cc: cc-dup@example.com, "CC Duplicate" <cc-dup@example.com>
Bcc: bcc-dup@example.com, bcc-dup@example.com
Subject: Duplicate Recipients
Date: Mon, 01 Jan 2024 12:00:00 +0000
Content-Type: text/plain; charset="utf-8"

Message with duplicate recipients in To, Cc, and Bcc fields.
`)
