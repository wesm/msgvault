package imessage

import (
	"strings"
	"testing"
	"time"
)

func TestAppleTimestampToTime(t *testing.T) {
	tests := []struct {
		name     string
		ts       int64
		wantYear int
		wantZero bool
	}{
		{
			name:     "zero returns zero time",
			ts:       0,
			wantZero: true,
		},
		{
			name:     "nanoseconds - 2024-01-01",
			ts:       725760000000000000, // 2024-01-01 00:00:00 UTC in Apple nanoseconds
			wantYear: 2024,
		},
		{
			name:     "seconds - 2024-01-01",
			ts:       725760000, // 2024-01-01 00:00:00 UTC in Apple seconds
			wantYear: 2024,
		},
		{
			name:     "nanoseconds - 2020-06-15",
			ts:       613872000000000000, // 2020-06-15 in Apple nanoseconds
			wantYear: 2020,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := appleTimestampToTime(tt.ts)
			if tt.wantZero {
				if !got.IsZero() {
					t.Errorf("expected zero time, got %v", got)
				}
				return
			}
			if got.Year() != tt.wantYear {
				t.Errorf("expected year %d, got %d (time: %v)", tt.wantYear, got.Year(), got)
			}
		})
	}
}

func TestTimeToAppleTimestamp(t *testing.T) {
	// 2024-01-01 00:00:00 UTC
	tm := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// In seconds
	gotSec := timeToAppleTimestamp(tm, false)
	wantSec := int64(725760000) // 1704067200 - 978307200
	if gotSec != wantSec {
		t.Errorf("seconds: got %d, want %d", gotSec, wantSec)
	}

	// In nanoseconds
	gotNano := timeToAppleTimestamp(tm, true)
	wantNano := int64(725760000000000000)
	if gotNano != wantNano {
		t.Errorf("nanoseconds: got %d, want %d", gotNano, wantNano)
	}
}

func TestRoundTripTimestamp(t *testing.T) {
	original := time.Date(2024, 6, 15, 12, 30, 45, 0, time.UTC)

	// Round trip through nanoseconds
	appleNano := timeToAppleTimestamp(original, true)
	recovered := appleTimestampToTime(appleNano)
	if !recovered.Equal(original) {
		t.Errorf("nanosecond round trip: got %v, want %v", recovered, original)
	}

	// Round trip through seconds (loses sub-second precision)
	appleSec := timeToAppleTimestamp(original, false)
	recoveredSec := appleTimestampToTime(appleSec)
	expected := time.Date(2024, 6, 15, 12, 30, 45, 0, time.UTC)
	if !recoveredSec.Equal(expected) {
		t.Errorf("second round trip: got %v, want %v", recoveredSec, expected)
	}
}

func TestNormalizeIdentifier(t *testing.T) {
	tests := []struct {
		name       string
		handleID   string
		wantEmail  string
		wantDomain string
	}{
		{
			name:       "email address",
			handleID:   "John@Example.com",
			wantEmail:  "john@example.com",
			wantDomain: "example.com",
		},
		{
			name:       "US phone with +1",
			handleID:   "+15551234567",
			wantEmail:  "+15551234567@phone.imessage",
			wantDomain: "phone.imessage",
		},
		{
			name:       "US phone 10 digits",
			handleID:   "5551234567",
			wantEmail:  "+15551234567@phone.imessage",
			wantDomain: "phone.imessage",
		},
		{
			name:       "US phone with formatting",
			handleID:   "(555) 123-4567",
			wantEmail:  "+15551234567@phone.imessage",
			wantDomain: "phone.imessage",
		},
		{
			name:       "US phone 11 digits with 1",
			handleID:   "15551234567",
			wantEmail:  "+15551234567@phone.imessage",
			wantDomain: "phone.imessage",
		},
		{
			name:       "international phone",
			handleID:   "+447911123456",
			wantEmail:  "+447911123456@phone.imessage",
			wantDomain: "phone.imessage",
		},
		{
			name:       "empty string",
			handleID:   "",
			wantEmail:  "",
			wantDomain: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotEmail, gotDomain, _ := normalizeIdentifier(tt.handleID)
			if gotEmail != tt.wantEmail {
				t.Errorf("email: got %q, want %q", gotEmail, tt.wantEmail)
			}
			if gotDomain != tt.wantDomain {
				t.Errorf("domain: got %q, want %q", gotDomain, tt.wantDomain)
			}
		})
	}
}

func TestBuildMIME(t *testing.T) {
	date := time.Date(2024, 6, 15, 14, 30, 0, 0, time.UTC)
	mime := buildMIME(
		[]string{"sender@example.com"},
		[]string{"recipient@example.com", "other@example.com"},
		date,
		"p:0/ABC123",
		"Hello, world!",
	)

	mimeStr := string(mime)

	// Check required headers
	if !strings.Contains(mimeStr, "From: <sender@example.com>") {
		t.Error("missing or incorrect From header")
	}
	if !strings.Contains(mimeStr, "To: <recipient@example.com>, <other@example.com>") {
		t.Error("missing or incorrect To header")
	}
	if !strings.Contains(mimeStr, "Date: ") {
		t.Error("missing Date header")
	}
	if !strings.Contains(mimeStr, "Message-ID: <p:0/ABC123@imessage.local>") {
		t.Error("missing Message-ID header")
	}
	if !strings.Contains(mimeStr, "Content-Type: text/plain; charset=utf-8") {
		t.Error("missing Content-Type header")
	}
	if !strings.Contains(mimeStr, "MIME-Version: 1.0") {
		t.Error("missing MIME-Version header")
	}
	// Check body is after blank line
	if !strings.Contains(mimeStr, "\r\n\r\nHello, world!") {
		t.Error("body not found after header separator")
	}
}

func TestBuildMIME_EmptyBody(t *testing.T) {
	date := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	mime := buildMIME(
		[]string{"sender@example.com"},
		[]string{"recipient@example.com"},
		date,
		"test-guid",
		"",
	)

	mimeStr := string(mime)

	// Should still have headers and separator
	if !strings.Contains(mimeStr, "\r\n\r\n") {
		t.Error("missing header/body separator")
	}
	// Body should be empty
	parts := strings.SplitN(mimeStr, "\r\n\r\n", 2)
	if len(parts) != 2 || parts[1] != "" {
		t.Errorf("expected empty body, got %q", parts[1])
	}
}

func TestSnippet(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		maxLen int
		want   string
	}{
		{"short text", "hello", 100, "hello"},
		{"long text", "hello world this is a long message", 10, "hello worl"},
		{"empty", "", 100, ""},
		{"multiline", "line1\nline2\nline3", 100, "line1 line2 line3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := snippet(tt.input, tt.maxLen)
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}
