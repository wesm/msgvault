package imessage

import (
	"fmt"
	"net/mail"
	"strings"
	"time"
)

// appleEpochOffset is the number of seconds between Unix epoch (1970-01-01)
// and Apple/Core Data epoch (2001-01-01).
const appleEpochOffset int64 = 978307200

// appleTimestampToTime converts an Apple epoch timestamp to time.Time.
// macOS High Sierra+ stores dates as nanoseconds since Apple epoch;
// older versions use seconds. We detect the format by checking magnitude.
func appleTimestampToTime(ts int64) time.Time {
	if ts == 0 {
		return time.Time{}
	}
	// Values > 1e12 are nanoseconds (1e12 ns = ~16 minutes from epoch,
	// while 1e12 seconds from epoch = year ~33700).
	if ts > 1_000_000_000_000 {
		sec := ts / 1_000_000_000
		nsec := ts % 1_000_000_000
		return time.Unix(sec+appleEpochOffset, nsec).UTC()
	}
	return time.Unix(ts+appleEpochOffset, 0).UTC()
}

// timeToAppleTimestamp converts a time.Time to an Apple epoch timestamp.
// If useNano is true, returns nanoseconds; otherwise returns seconds.
func timeToAppleTimestamp(t time.Time, useNano bool) int64 {
	appleSec := t.Unix() - appleEpochOffset
	if useNano {
		return appleSec*1_000_000_000 + int64(t.Nanosecond())
	}
	return appleSec
}

// normalizeIdentifier converts a phone number or email address from iMessage's
// handle table into a normalized email-like identifier for the participants table.
// Returns the normalized email, domain, and display name.
func normalizeIdentifier(handleID string) (email, domain, displayName string) {
	handleID = strings.TrimSpace(handleID)
	if handleID == "" {
		return "", "", ""
	}

	// Email addresses: use as-is (lowercased)
	if strings.Contains(handleID, "@") {
		email = strings.ToLower(handleID)
		if idx := strings.LastIndex(email, "@"); idx >= 0 {
			domain = email[idx+1:]
		}
		return email, domain, ""
	}

	// Phone numbers: normalize and use a synthetic domain
	phone := normalizePhone(handleID)
	return phone + "@phone.imessage", "phone.imessage", phone
}

// normalizePhone strips non-digit characters from a phone number and attempts
// to produce a consistent E.164-like format.
func normalizePhone(phone string) string {
	// Preserve leading +
	hasPlus := strings.HasPrefix(phone, "+")

	// Extract digits only
	var digits strings.Builder
	for _, r := range phone {
		if r >= '0' && r <= '9' {
			digits.WriteRune(r)
		}
	}
	d := digits.String()
	if d == "" {
		return phone // Return original if no digits found
	}

	// Try to normalize to E.164
	if hasPlus {
		return "+" + d
	}
	// 10-digit US number
	if len(d) == 10 {
		return "+1" + d
	}
	// 11-digit number starting with 1 (US with country code)
	if len(d) == 11 && d[0] == '1' {
		return "+" + d
	}
	// Other: prefix with +
	return "+" + d
}

// buildMIME constructs a minimal RFC 2822 message from iMessage data.
// The resulting bytes can be parsed by enmime for the sync pipeline.
func buildMIME(fromAddr, toAddrs []string, date time.Time, messageID, body string) []byte {
	var b strings.Builder

	// From header
	if len(fromAddr) > 0 {
		b.WriteString("From: ")
		b.WriteString(formatMIMEAddress(fromAddr[0]))
		b.WriteString("\r\n")
	}

	// To header
	if len(toAddrs) > 0 {
		b.WriteString("To: ")
		for i, addr := range toAddrs {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(formatMIMEAddress(addr))
		}
		b.WriteString("\r\n")
	}

	// Date header
	if !date.IsZero() {
		b.WriteString("Date: ")
		b.WriteString(date.Format(time.RFC1123Z))
		b.WriteString("\r\n")
	}

	// Subject (empty for iMessage - messages don't have subjects)
	b.WriteString("Subject: \r\n")

	// Message-ID
	if messageID != "" {
		fmt.Fprintf(&b, "Message-ID: <%s@imessage.local>\r\n", messageID)
	}

	// MIME version and content type
	b.WriteString("MIME-Version: 1.0\r\n")
	b.WriteString("Content-Type: text/plain; charset=utf-8\r\n")

	// Header/body separator
	b.WriteString("\r\n")

	// Body
	if body != "" {
		b.WriteString(body)
	}

	return []byte(b.String())
}

// formatMIMEAddress formats an email address for MIME headers.
func formatMIMEAddress(addr string) string {
	return (&mail.Address{Address: addr}).String()
}

// snippet returns the first n characters of s, suitable for message preview.
func snippet(s string, maxLen int) string {
	// Normalize whitespace
	s = strings.Join(strings.Fields(s), " ")
	runes := []rune(s)
	if len(runes) > maxLen {
		return string(runes[:maxLen])
	}
	return s
}
