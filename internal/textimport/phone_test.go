package textimport

import "testing"

func TestNormalizePhone(t *testing.T) {
	tests := []struct {
		input   string
		want    string
		wantErr bool
	}{
		// Valid E.164
		{"+15551234567", "+15551234567", false},
		// Strip formatting
		{"+1 (555) 123-4567", "+15551234567", false},
		{"+1-555-123-4567", "+15551234567", false},
		{"1-555-123-4567", "+15551234567", false},
		// International
		{"+447700900000", "+447700900000", false},
		{"+44 7700 900000", "+447700900000", false},
		// No country code — assume US
		{"5551234567", "+15551234567", false},
		{"(555) 123-4567", "+15551234567", false},
		// Email — not a phone
		{"alice@icloud.com", "", true},
		// Short code
		{"12345", "", true},
		// Empty
		{"", "", true},
		// System identifier
		{"status@broadcast", "", true},
		// International 00-prefix
		{"0033624921221", "+33624921221", false},
		// Leading whitespace
		{" +15551234567", "+15551234567", false},
		{"\t+44 7700 900000", "+447700900000", false},
		// 00-prefix too short after conversion
		{"0012345", "", true},
		// Trunk prefix (0)
		{"+44 (0)7700 900000", "+447700900000", false},
		// Embedded + (invalid)
		{"1+5551234567", "", true},
		// Too long (>15 digits)
		{"+1234567890123456", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := NormalizePhone(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("NormalizePhone(%q) = %q, want error", tt.input, got)
				}
				return
			}
			if err != nil {
				t.Errorf("NormalizePhone(%q) error: %v", tt.input, err)
				return
			}
			if got != tt.want {
				t.Errorf("NormalizePhone(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
