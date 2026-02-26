package whatsapp

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/wesm/msgvault/internal/store"
)

// vcardContact represents a parsed contact from a vCard file.
type vcardContact struct {
	FullName string
	Phones   []string // normalized to E.164
}

// ImportContacts reads a .vcf file and updates participant display names
// for any phone numbers that match existing participants in the store.
// Only updates existing participants — does not create new ones.
// Returns the number of existing participants whose names were updated.
func ImportContacts(s *store.Store, vcfPath string) (matched, total int, err error) {
	contacts, err := parseVCardFile(vcfPath)
	if err != nil {
		return 0, 0, fmt.Errorf("parse vcard: %w", err)
	}

	total = len(contacts)
	var errCount int
	for _, c := range contacts {
		if c.FullName == "" {
			continue
		}
		for _, phone := range c.Phones {
			if phone == "" {
				continue
			}
			// Only update display_name for participants that already exist.
			// Does not create new participants — those are created during message import.
			updated, updateErr := s.UpdateParticipantDisplayNameByPhone(phone, c.FullName)
			if updateErr != nil {
				errCount++
				continue
			}
			if updated {
				matched++
			}
		}
	}

	if errCount > 0 {
		return matched, total, fmt.Errorf("contact import completed with %d database errors", errCount)
	}

	return matched, total, nil
}

// parseVCardFile reads a .vcf file and returns parsed contacts.
// Handles vCard 2.1 and 3.0 formats, including RFC 2425 line folding
// and QUOTED-PRINTABLE encoded values.
func parseVCardFile(path string) ([]vcardContact, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Read all lines and unfold continuation lines (RFC 2425: lines starting
	// with a space or tab are continuations of the previous line).
	var rawLines []string
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 && (line[0] == ' ' || line[0] == '\t') {
			// Continuation line — append to previous.
			if len(rawLines) > 0 {
				rawLines[len(rawLines)-1] += strings.TrimLeft(line, " \t")
				continue
			}
		}
		rawLines = append(rawLines, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan vcard: %w", err)
	}

	var contacts []vcardContact
	var current *vcardContact

	for _, line := range rawLines {
		line = strings.TrimSpace(line)
		// vCard field names are case-insensitive (RFC 2426).
		// Uppercase the key portion for matching, but preserve original value bytes.
		upper := strings.ToUpper(line)

		switch {
		case upper == "BEGIN:VCARD":
			current = &vcardContact{}

		case upper == "END:VCARD":
			if current != nil && (current.FullName != "" || len(current.Phones) > 0) {
				contacts = append(contacts, *current)
			}
			current = nil

		case current == nil:
			continue

		case strings.HasPrefix(upper, "FN:") || strings.HasPrefix(upper, "FN;"):
			// FN (formatted name) — preferred over N because it's the display name.
			name := extractVCardValue(line)
			if isQuotedPrintable(line) {
				name = decodeQuotedPrintable(name)
			}
			if name != "" {
				current.FullName = name
			}

		case strings.HasPrefix(upper, "TEL"):
			// TEL;CELL:+447... or TEL;TYPE=CELL:+447... or TEL:+447...
			raw := extractVCardValue(line)
			phone := normalizeVCardPhone(raw)
			if phone != "" {
				current.Phones = append(current.Phones, phone)
			}
		}
	}

	return contacts, nil
}

// extractVCardValue extracts the value part from a vCard line.
// Handles both "KEY:value" and "KEY;params:value" formats.
func extractVCardValue(line string) string {
	// Find the first colon that separates key from value.
	idx := strings.Index(line, ":")
	if idx < 0 {
		return ""
	}
	return strings.TrimSpace(line[idx+1:])
}

// isQuotedPrintable returns true if a vCard line indicates QUOTED-PRINTABLE encoding.
func isQuotedPrintable(line string) bool {
	upper := strings.ToUpper(line)
	return strings.Contains(upper, "ENCODING=QUOTED-PRINTABLE") ||
		strings.Contains(upper, ";QUOTED-PRINTABLE")
}

// decodeQuotedPrintable decodes a QUOTED-PRINTABLE encoded string.
// Handles =XX hex sequences (e.g., =C3=A9 → é).
func decodeQuotedPrintable(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '=' && i+2 < len(s) {
			hi := unhex(s[i+1])
			lo := unhex(s[i+2])
			if hi >= 0 && lo >= 0 {
				b.WriteByte(byte(hi<<4 | lo))
				i += 2
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// unhex returns the numeric value of a hex digit, or -1 if invalid.
func unhex(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'A' && c <= 'F':
		return int(c - 'A' + 10)
	case c >= 'a' && c <= 'f':
		return int(c - 'a' + 10)
	default:
		return -1
	}
}

// nonDigitRe matches any non-digit character.
var nonDigitRe = regexp.MustCompile(`[^\d]`)

// normalizeVCardPhone normalizes a phone number from a vCard to E.164 format.
// Handles various formats: +447..., 003-362-..., 077-380-06043, etc.
func normalizeVCardPhone(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}

	// Check if it starts with + (already has country code).
	hasPlus := strings.HasPrefix(raw, "+")

	// Strip everything except digits.
	digits := nonDigitRe.ReplaceAllString(raw, "")
	if digits == "" {
		return ""
	}

	// If originally had +, it's already E.164-ish.
	if hasPlus {
		return "+" + digits
	}

	// Handle 00-prefixed international format (e.g., 003-362-4921221 → +33624921221).
	if strings.HasPrefix(digits, "00") && len(digits) > 4 {
		return "+" + digits[2:]
	}

	// Local numbers starting with 0 (e.g., 07738006043) are country-specific
	// and cannot be reliably normalized without knowing the country code.
	// Skip these rather than hardcoding a country assumption.

	// If long enough to be an international number without prefix, assume it has a country code.
	if len(digits) >= 10 && !strings.HasPrefix(digits, "0") {
		return "+" + digits
	}

	// Too short, local format, or ambiguous — skip.
	return ""
}
