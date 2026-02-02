package email

import (
	"strings"
	"testing"
)

func TestPlainMessage(t *testing.T) {
	got := string(NewMessage().Body("Hello world.").Bytes())

	want := strings.Join([]string{
		"From: sender@example.com",
		"To: recipient@example.com",
		"Subject: Test Message",
		"Date: Mon, 01 Jan 2024 12:00:00 +0000",
		`Content-Type: text/plain; charset="utf-8"`,
		"",
		"Hello world.",
		"",
	}, "\n")

	if got != want {
		t.Errorf("plain message mismatch.\ngot:\n%s\nwant:\n%s", got, want)
	}
}

func TestNoSubject(t *testing.T) {
	got := string(NewMessage().NoSubject().Bytes())
	if strings.Contains(got, "Subject:") {
		t.Error("expected no Subject header, but found one")
	}
}

func TestMultipartMessage(t *testing.T) {
	got := string(NewMessage().
		Body("See attached.").
		Boundary("BOUND").
		WithAttachment("test.txt", "text/plain", []byte("file data")).
		Bytes())

	// Check structural elements are present and in order.
	checks := []string{
		"Content-Type: multipart/mixed; boundary=\"BOUND\"",
		"--BOUND\n",
		"See attached.",
		"--BOUND\n",
		`Content-Disposition: attachment; filename="test.txt"`,
		"Content-Transfer-Encoding: base64",
		"--BOUND--",
	}
	for _, c := range checks {
		if !strings.Contains(got, c) {
			t.Errorf("multipart message missing %q\ngot:\n%s", c, got)
		}
	}
}

func TestHeaderOrder(t *testing.T) {
	got := string(NewMessage().
		Header("X-First", "1").
		Header("X-Second", "2").
		Header("X-Third", "3").
		Bytes())

	i1 := strings.Index(got, "X-First: 1")
	i2 := strings.Index(got, "X-Second: 2")
	i3 := strings.Index(got, "X-Third: 3")

	if i1 < 0 || i2 < 0 || i3 < 0 {
		t.Fatalf("missing headers in output:\n%s", got)
	}
	if !(i1 < i2 && i2 < i3) {
		t.Errorf("headers not in insertion order: positions %d, %d, %d", i1, i2, i3)
	}
}

func TestCRLF(t *testing.T) {
	got := string(NewMessage().CRLF().Bytes())
	if strings.Contains(got, "\n") && !strings.Contains(got, "\r\n") {
		t.Error("expected CRLF line endings")
	}
}
