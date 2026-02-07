package mbox

import (
	"io"
	"strings"
	"testing"
)

func TestReader_Next_SplitsAndUnescapes(t *testing.T) {
	mboxData := strings.Join([]string{
		"From sender@example.com Mon Jan 1 00:00:00 2024",
		"Subject: One",
		"",
		">From should-unescape",
		">>From keep-one",
		"Normal",
		"",
		"From sender@example.com Mon Jan 1 00:00:01 2024",
		"Subject: Two",
		"",
		"Body2",
		"",
	}, "\n")

	r := NewReader(strings.NewReader(mboxData))

	msg1, err := r.Next()
	if err != nil {
		t.Fatalf("Next(): %v", err)
	}
	if got := msg1.FromLine; !strings.HasPrefix(got, "From sender@example.com") {
		t.Fatalf("FromLine mismatch: %q", got)
	}
	raw1 := string(msg1.Raw)
	if !strings.Contains(raw1, "From should-unescape\n") {
		t.Fatalf("expected unescaped From line, got raw:\n%s", raw1)
	}
	if !strings.Contains(raw1, ">From keep-one\n") {
		t.Fatalf("expected unescaped >>From -> >From, got raw:\n%s", raw1)
	}
	if strings.Contains(raw1, ">>From keep-one\n") {
		t.Fatalf("expected mboxrd unescape to remove one '>', got raw:\n%s", raw1)
	}

	msg2, err := r.Next()
	if err != nil {
		t.Fatalf("Next() (msg2): %v", err)
	}
	raw2 := string(msg2.Raw)
	if !strings.Contains(raw2, "Subject: Two\n") || !strings.Contains(raw2, "\n\nBody2\n") {
		t.Fatalf("unexpected msg2 raw:\n%s", raw2)
	}

	_, err = r.Next()
	if err != io.EOF {
		t.Fatalf("expected EOF, got: %v", err)
	}
}

func TestReader_Offset_RespectsSeekPosition(t *testing.T) {
	mboxData := strings.Join([]string{
		"From a@example.com Mon Jan 1 00:00:00 2024",
		"Subject: One",
		"",
		"Body1",
		"",
		"From b@example.com Mon Jan 1 00:00:01 2024",
		"Subject: Two",
		"",
		"Body2",
		"",
	}, "\n")

	start := strings.Index(mboxData, "From b@example.com")
	if start < 0 {
		t.Fatalf("missing second From line")
	}

	sr := strings.NewReader(mboxData)
	if _, err := sr.Seek(int64(start), io.SeekStart); err != nil {
		t.Fatalf("Seek(): %v", err)
	}

	r := NewReader(sr)
	if got := r.Offset(); got != int64(start) {
		t.Fatalf("Offset() = %d, want %d", got, start)
	}

	msg, err := r.Next()
	if err != nil {
		t.Fatalf("Next(): %v", err)
	}
	if !strings.HasPrefix(msg.FromLine, "From b@example.com") {
		t.Fatalf("unexpected FromLine: %q", msg.FromLine)
	}
}

func TestValidate_FindsSeparator(t *testing.T) {
	data := "not mbox\nFrom a@b Sat Jan 1 00:00:00 2024\nSubject: x\n\nBody\n"
	if err := Validate(strings.NewReader(data), 1024); err != nil {
		t.Fatalf("Validate(): %v", err)
	}
}
