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
	got := NewMessage().CRLF().Bytes()
	for i, b := range got {
		if b == '\n' && (i == 0 || got[i-1] != '\r') {
			t.Fatalf("bare \\n at byte %d; expected all line endings to be \\r\\n", i)
		}
	}
	if !strings.Contains(string(got), "\r\n") {
		t.Error("expected at least one CRLF line ending")
	}
}

func TestHeaderOverwrite(t *testing.T) {
	got := string(NewMessage().
		Header("X-Custom", "first").
		Header("X-Custom", "second").
		Bytes())

	if strings.Count(got, "X-Custom:") != 1 {
		t.Errorf("expected exactly one X-Custom header, got:\n%s", got)
	}
	if !strings.Contains(got, "X-Custom: second") {
		t.Errorf("expected overwritten value 'second', got:\n%s", got)
	}
}

func TestHeaderCaseInsensitiveOverwrite(t *testing.T) {
	got := string(NewMessage().
		Header("X-Custom", "first").
		Header("x-custom", "second").
		Bytes())

	// Case-insensitive dedup should produce exactly one header line.
	count := 0
	for _, line := range strings.Split(got, "\n") {
		lower := strings.ToLower(line)
		if strings.HasPrefix(lower, "x-custom:") {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly one x-custom header (case-insensitive), got %d:\n%s", count, got)
	}
	if !strings.Contains(got, "x-custom: second") {
		t.Errorf("expected latest value with latest casing, got:\n%s", got)
	}
}

func TestHeaderAppendAllowsDuplicates(t *testing.T) {
	got := string(NewMessage().
		HeaderAppend("Received", "from server1").
		HeaderAppend("Received", "from server2").
		Bytes())

	if strings.Count(got, "Received:") != 2 {
		t.Errorf("expected two Received headers, got:\n%s", got)
	}
}
