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
			updated, err := s.UpdateParticipantDisplayNameByPhone(phone, c.FullName)
			if err == nil && updated {
				matched++
			}
		}
	}

	return matched, total, nil
}

// parseVCardFile reads a .vcf file and returns parsed contacts.
// Handles vCard 2.1 and 3.0 formats.
func parseVCardFile(path string) ([]vcardContact, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var contacts []vcardContact
	var current *vcardContact

	scanner := bufio.NewScanner(f)
	// Increase buffer for long lines (e.g., base64-encoded photos).
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		switch {
		case line == "BEGIN:VCARD":
			current = &vcardContact{}

		case line == "END:VCARD":
			if current != nil && (current.FullName != "" || len(current.Phones) > 0) {
				contacts = append(contacts, *current)
			}
			current = nil

		case current == nil:
			continue

		case strings.HasPrefix(line, "FN:") || strings.HasPrefix(line, "FN;"):
			// FN (formatted name) — preferred over N because it's the display name.
			name := extractVCardValue(line)
			if name != "" {
				current.FullName = name
			}

		case strings.HasPrefix(line, "TEL"):
			// TEL;CELL:+447... or TEL;TYPE=CELL:+447... or TEL:+447...
			raw := extractVCardValue(line)
			phone := normalizeVCardPhone(raw)
			if phone != "" {
				current.Phones = append(current.Phones, phone)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan vcard: %w", err)
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

	// Handle UK-style local numbers starting with 0 (e.g., 07738006043 → +447738006043).
	if strings.HasPrefix(digits, "0") && len(digits) >= 10 {
		return "+44" + digits[1:]
	}

	// If long enough to be an international number, assume it has a country code.
	if len(digits) >= 10 {
		return "+" + digits
	}

	// Too short or ambiguous — skip.
	return ""
}
