package whatsapp

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNormalizeVCardPhone(t *testing.T) {
	tests := []struct {
		raw  string
		want string
	}{
		// Already E.164
		{"+447700900000", "+447700900000"},
		{"+12025551234", "+12025551234"},
		{"+33624921221", "+33624921221"},

		// With dashes/spaces
		{"+44 7700 900000", "+447700900000"},
		{"+1-202-555-1234", "+12025551234"},

		// 00 prefix (international)
		{"003-362-4921221", "+33624921221"},
		{"0033624921221", "+33624921221"},
		{"004-479-35975580", "+447935975580"},

		// 0 prefix (local) — skipped, country-ambiguous
		{"011-585-73843", ""},
		{"07738006043", ""},
		{"077-380-06043", ""},

		// Already international without +
		{"447700900000", "+447700900000"},

		// Empty/invalid
		{"", ""},
		{"   ", ""},
		{"abc", ""},
		{"12", ""},  // too short
	}

	for _, tt := range tests {
		got := normalizeVCardPhone(tt.raw)
		if got != tt.want {
			t.Errorf("normalizeVCardPhone(%q) = %q, want %q", tt.raw, got, tt.want)
		}
	}
}

func TestParseVCardFile(t *testing.T) {
	// Write a test vCard file.
	vcf := `BEGIN:VCARD
VERSION:2.1
N:McGregor;Alastair;;;
FN:Alastair McGregor
TEL;CELL:+447984959428
END:VCARD
BEGIN:VCARD
VERSION:2.1
N:France;Geoff;;;
FN:Geoff France
TEL;X-Mobile:+33562645735
END:VCARD
BEGIN:VCARD
VERSION:2.1
N:Studios;Claire Mohacek -;Amazon;;
FN:Claire Mohacek - Amazon Studios
TEL;CELL:077-380-06043
END:VCARD
BEGIN:VCARD
VERSION:2.1
TEL;CELL:
END:VCARD
BEGIN:VCARD
VERSION:3.0
FN:Multi Phone Person
TEL;TYPE=CELL:+447700900001
TEL;TYPE=WORK:+442071234567
END:VCARD
`
	dir := t.TempDir()
	path := filepath.Join(dir, "test.vcf")
	if err := os.WriteFile(path, []byte(vcf), 0644); err != nil {
		t.Fatal(err)
	}

	contacts, err := parseVCardFile(path)
	if err != nil {
		t.Fatalf("parseVCardFile() error: %v", err)
	}

	if len(contacts) != 4 { // 4 with names/phones, 1 empty entry skipped
		t.Fatalf("got %d contacts, want 4", len(contacts))
	}

	// First contact
	if contacts[0].FullName != "Alastair McGregor" {
		t.Errorf("contact 0 name = %q, want %q", contacts[0].FullName, "Alastair McGregor")
	}
	if len(contacts[0].Phones) != 1 || contacts[0].Phones[0] != "+447984959428" {
		t.Errorf("contact 0 phones = %v, want [+447984959428]", contacts[0].Phones)
	}

	// Third contact — local number (0-prefix) is now skipped (country-ambiguous)
	if contacts[2].FullName != "Claire Mohacek - Amazon Studios" {
		t.Errorf("contact 2 name = %q", contacts[2].FullName)
	}
	if len(contacts[2].Phones) != 0 {
		t.Errorf("contact 2 phones = %v, want [] (local numbers skipped)", contacts[2].Phones)
	}

	// Multi phone contact
	if contacts[3].FullName != "Multi Phone Person" {
		t.Errorf("contact 3 name = %q", contacts[3].FullName)
	}
	if len(contacts[3].Phones) != 2 {
		t.Errorf("contact 3 phone count = %d, want 2", len(contacts[3].Phones))
	}
}

func TestExtractVCardValue(t *testing.T) {
	tests := []struct {
		line string
		want string
	}{
		{"FN:John Doe", "John Doe"},
		{"FN;CHARSET=UTF-8:John Doe", "John Doe"},
		{"TEL;CELL:+447700900000", "+447700900000"},
		{"TEL;TYPE=CELL:+447700900000", "+447700900000"},
		{"TEL:+447700900000", "+447700900000"},
		{"NO_COLON", ""},
	}

	for _, tt := range tests {
		got := extractVCardValue(tt.line)
		if got != tt.want {
			t.Errorf("extractVCardValue(%q) = %q, want %q", tt.line, got, tt.want)
		}
	}
}
