package mbox

import (
	"errors"
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

func TestReader_Next_CanDisableUnescape(t *testing.T) {
	mboxData := strings.Join([]string{
		"From sender@example.com Mon Jan 1 00:00:00 2024",
		"Subject: One",
		"",
		">From should-stay-escaped",
		"",
	}, "\n")

	r := NewReader(strings.NewReader(mboxData))
	r.SetUnescapeFrom(false)

	msg, err := r.Next()
	if err != nil {
		t.Fatalf("Next(): %v", err)
	}
	raw := string(msg.Raw)
	if !strings.Contains(raw, ">From should-stay-escaped\n") {
		t.Fatalf("expected no unescaping, got raw:\n%s", raw)
	}
	if strings.Contains(raw, "\n\nFrom should-stay-escaped\n") {
		t.Fatalf("expected >From line to remain escaped, got raw:\n%s", raw)
	}
}

func TestReader_Next_AllowsLongLines(t *testing.T) {
	longValue := strings.Repeat("a", 10_000)
	mboxData := strings.Join([]string{
		"From sender@example.com Mon Jan 1 00:00:00 2024",
		"Subject: One",
		"X-Long: " + longValue,
		"",
		"Body1",
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
		t.Fatalf("Next() (msg1): %v", err)
	}
	if !strings.Contains(string(msg1.Raw), "X-Long: "+longValue+"\n") {
		t.Fatalf("expected full long header line, got raw:\n%s", string(msg1.Raw))
	}

	msg2, err := r.Next()
	if err != nil {
		t.Fatalf("Next() (msg2): %v", err)
	}
	if !strings.Contains(string(msg2.Raw), "Subject: Two\n") {
		t.Fatalf("unexpected msg2 raw:\n%s", string(msg2.Raw))
	}

	_, err = r.Next()
	if err != io.EOF {
		t.Fatalf("expected EOF, got: %v", err)
	}
}

func TestReader_Next_EnforcesMaxMessageBytesAndContinues(t *testing.T) {
	mboxData := strings.Join([]string{
		"From sender@example.com Mon Jan 1 00:00:00 2024",
		"Subject: " + strings.Repeat("a", 200),
		"",
		"Body1",
		"",
		"From sender@example.com Mon Jan 1 00:00:01 2024",
		"Subject: Two",
		"",
		"Body2",
		"",
	}, "\n")

	r := NewReaderWithMaxMessageBytes(strings.NewReader(mboxData), 64)

	_, err := r.Next()
	if err == nil || !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("expected ErrMessageTooLarge, got: %v", err)
	}

	msg2, err := r.Next()
	if err != nil {
		t.Fatalf("Next() (msg2): %v", err)
	}
	if !strings.Contains(string(msg2.Raw), "Subject: Two\n") {
		t.Fatalf("unexpected msg2 raw:\n%s", string(msg2.Raw))
	}
}

func TestReader_Next_DoesNotSplitOnUnescapedFromInBody(t *testing.T) {
	mboxData := strings.Join([]string{
		"From sender@example.com Mon Jan 1 00:00:00 2024",
		"Subject: One",
		"",
		"Body1",
		"From this is not a separator",
		"Body3",
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
		t.Fatalf("Next() (msg1): %v", err)
	}
	if !strings.Contains(string(msg1.Raw), "From this is not a separator\n") {
		t.Fatalf("expected body to contain unescaped From line, got raw:\n%s", string(msg1.Raw))
	}

	msg2, err := r.Next()
	if err != nil {
		t.Fatalf("Next() (msg2): %v", err)
	}
	if !strings.Contains(string(msg2.Raw), "Subject: Two\n") {
		t.Fatalf("unexpected msg2 raw:\n%s", string(msg2.Raw))
	}

	_, err = r.Next()
	if err != io.EOF {
		t.Fatalf("expected EOF, got: %v", err)
	}
}

func TestReader_Next_AcceptsNamedTimezoneSeparators(t *testing.T) {
	mboxData := strings.Join([]string{
		"From sender@example.com Mon Jan 1 00:00:00 MST 2024",
		"Subject: One",
		"",
		"Body1",
		"",
		"From sender@example.com Mon Jan 1 00:00:01 MST 2024",
		"Subject: Two",
		"",
		"Body2",
		"",
	}, "\n")

	r := NewReader(strings.NewReader(mboxData))

	msg1, err := r.Next()
	if err != nil {
		t.Fatalf("Next() (msg1): %v", err)
	}
	if !strings.Contains(string(msg1.Raw), "Subject: One\n") {
		t.Fatalf("unexpected msg1 raw:\n%s", string(msg1.Raw))
	}

	msg2, err := r.Next()
	if err != nil {
		t.Fatalf("Next() (msg2): %v", err)
	}
	if !strings.Contains(string(msg2.Raw), "Subject: Two\n") {
		t.Fatalf("unexpected msg2 raw:\n%s", string(msg2.Raw))
	}

	_, err = r.Next()
	if err != io.EOF {
		t.Fatalf("expected EOF, got: %v", err)
	}
}

func TestReader_Next_AcceptsRemoteFromSuffixSeparators(t *testing.T) {
	mboxData := strings.Join([]string{
		"From sender@example.com Mon Jan 1 00:00:00 2024 remote from mail.example.com",
		"Subject: One",
		"",
		"Body1",
		"",
		"From sender@example.com Mon Jan 1 00:00:01 2024 remote from mail.example.com",
		"Subject: Two",
		"",
		"Body2",
		"",
	}, "\n")

	r := NewReader(strings.NewReader(mboxData))

	msg1, err := r.Next()
	if err != nil {
		t.Fatalf("Next() (msg1): %v", err)
	}
	if !strings.Contains(string(msg1.Raw), "Subject: One\n") {
		t.Fatalf("unexpected msg1 raw:\n%s", string(msg1.Raw))
	}

	msg2, err := r.Next()
	if err != nil {
		t.Fatalf("Next() (msg2): %v", err)
	}
	if !strings.Contains(string(msg2.Raw), "Subject: Two\n") {
		t.Fatalf("unexpected msg2 raw:\n%s", string(msg2.Raw))
	}

	_, err = r.Next()
	if err != io.EOF {
		t.Fatalf("expected EOF, got: %v", err)
	}
}

func TestReader_Next_AcceptsNoSecondsSeparators(t *testing.T) {
	mboxData := strings.Join([]string{
		"From sender@example.com Mon Jan 1 00:00 2024",
		"Subject: One",
		"",
		"Body1",
		"",
		"From sender@example.com Mon Jan 1 00:01 2024",
		"Subject: Two",
		"",
		"Body2",
		"",
	}, "\n")

	r := NewReader(strings.NewReader(mboxData))

	msg1, err := r.Next()
	if err != nil {
		t.Fatalf("Next() (msg1): %v", err)
	}
	if !strings.Contains(string(msg1.Raw), "Subject: One\n") {
		t.Fatalf("unexpected msg1 raw:\n%s", string(msg1.Raw))
	}

	msg2, err := r.Next()
	if err != nil {
		t.Fatalf("Next() (msg2): %v", err)
	}
	if !strings.Contains(string(msg2.Raw), "Subject: Two\n") {
		t.Fatalf("unexpected msg2 raw:\n%s", string(msg2.Raw))
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

func TestValidate_FindsSeparator_WithRemoteFromSuffix(t *testing.T) {
	data := "not mbox\nFrom a@b Sat Jan 1 00:00:00 2024 remote from mail.example.com\nSubject: x\n\nBody\n"
	if err := Validate(strings.NewReader(data), 1024); err != nil {
		t.Fatalf("Validate(): %v", err)
	}
}
