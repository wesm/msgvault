package cmd

import (
	"testing"

	"github.com/wesm/msgvault/internal/testutil"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/encoding/korean"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/traditionalchinese"
)

func TestDetectAndDecode_Windows1252(t *testing.T) {
	enc := testutil.EncodedSamples()
	// Windows-1252 specific characters: smart quotes (0x91-0x94), en/em dash (0x96, 0x97)
	tests := []struct {
		name     string
		input    []byte
		expected string
	}{
		{
			name:     "smart single quote (apostrophe)",
			input:    enc.Win1252_SmartQuoteRight,
			expected: "Rand\u2019s Opponent",
		},
		{
			name:     "en dash",
			input:    []byte("Limited Time Only \x96 50 Percent"), // different text than fixture
			expected: "Limited Time Only \u2013 50 Percent",
		},
		{
			name:     "em dash",
			input:    []byte("Costco Travel\x97Exclusive"), // different text than fixture
			expected: "Costco Travel\u2014Exclusive",
		},
		{
			name:     "trademark symbol",
			input:    []byte("Craftsman\xae Tools"),
			expected: "Craftsman® Tools",
		},
		{
			name:     "registered trademark in Windows-1252",
			input:    []byte("Windows\xae 7"),
			expected: "Windows® 7",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := detectAndDecode(tt.input)
			if err != nil {
				t.Fatalf("detectAndDecode() error = %v", err)
			}
			if result != tt.expected {
				t.Errorf("detectAndDecode() = %q, want %q", result, tt.expected)
			}
			testutil.AssertValidUTF8(t, result)
		})
	}
}

func TestDetectAndDecode_Latin1(t *testing.T) {
	enc := testutil.EncodedSamples()
	tests := []struct {
		name     string
		input    []byte
		expected string
	}{
		{
			name:     "o with acute accent",
			input:    enc.Latin1_OAcute,
			expected: "Miró - Picasso",
		},
		{
			name:     "c with cedilla",
			input:    enc.Latin1_CCedilla,
			expected: "Garçon",
		},
		{
			name:     "u with umlaut",
			input:    enc.Latin1_UUmlaut,
			expected: "München",
		},
		{
			name:     "n with tilde",
			input:    enc.Latin1_NTilde,
			expected: "España",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := detectAndDecode(tt.input)
			if err != nil {
				t.Fatalf("detectAndDecode() error = %v", err)
			}
			if result != tt.expected {
				t.Errorf("detectAndDecode() = %q, want %q", result, tt.expected)
			}
			testutil.AssertValidUTF8(t, result)
		})
	}
}

func TestDetectAndDecode_AsianEncodings(t *testing.T) {
	enc := testutil.EncodedSamples()
	tests := []struct {
		name  string
		input []byte
	}{
		{"Shift-JIS Japanese", enc.ShiftJIS_Konnichiwa},
		{"GBK Simplified Chinese", enc.GBK_Nihao},
		{"Big5 Traditional Chinese", enc.Big5_Nihao},
		{"EUC-KR Korean", enc.EUCKR_Annyeong},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := detectAndDecode(tt.input)
			if err != nil {
				t.Fatalf("detectAndDecode() error = %v", err)
			}
			testutil.AssertValidUTF8(t, result)
			if len(result) == 0 {
				t.Errorf("detectAndDecode() returned empty string")
			}
		})
	}
}

func TestDetectAndDecode_AlreadyUTF8(t *testing.T) {
	// Already valid UTF-8 should pass through
	input := []byte("Hello, 世界! Привет!")
	expected := "Hello, 世界! Привет!"

	result, err := detectAndDecode(input)
	if err != nil {
		t.Fatalf("detectAndDecode() error = %v", err)
	}
	if result != expected {
		t.Errorf("detectAndDecode() = %q, want %q", result, expected)
	}
}

func TestGetEncodingByName(t *testing.T) {
	tests := []struct {
		name     string
		charset  string
		expected interface{}
	}{
		{"Windows-1252 standard", "windows-1252", charmap.Windows1252},
		{"Windows-1252 CP1252", "CP1252", charmap.Windows1252},
		{"ISO-8859-1 standard", "ISO-8859-1", charmap.ISO8859_1},
		{"ISO-8859-1 lowercase", "iso-8859-1", charmap.ISO8859_1},
		{"ISO-8859-1 latin1", "latin1", charmap.ISO8859_1},
		{"Shift_JIS standard", "Shift_JIS", japanese.ShiftJIS},
		{"Shift_JIS lowercase", "shift_jis", japanese.ShiftJIS},
		{"EUC-JP standard", "EUC-JP", japanese.EUCJP},
		{"EUC-KR standard", "EUC-KR", korean.EUCKR},
		{"GBK standard", "GBK", simplifiedchinese.GBK},
		{"GB2312 maps to GBK", "GB2312", simplifiedchinese.GBK},
		{"Big5 standard", "Big5", traditionalchinese.Big5},
		{"KOI8-R standard", "KOI8-R", charmap.KOI8R},
		{"Unknown returns nil", "unknown-charset", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getEncodingByName(tt.charset)
			if result != tt.expected {
				t.Errorf("getEncodingByName(%q) = %v, want %v", tt.charset, result, tt.expected)
			}
		})
	}
}

func TestSanitizeUTF8(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "valid UTF-8 unchanged",
			input:    "Hello, 世界!",
			expected: "Hello, 世界!",
		},
		{
			name:     "invalid byte replaced",
			input:    "Hello\x80World",
			expected: "Hello\ufffdWorld",
		},
		{
			name:     "multiple invalid bytes",
			input:    "Test\x80\x81\x82String",
			expected: "Test\ufffd\ufffd\ufffdString",
		},
		{
			name:     "truncated UTF-8 sequence",
			input:    "Hello\xc3", // Incomplete UTF-8 sequence
			expected: "Hello\ufffd",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeUTF8(tt.input)
			if result != tt.expected {
				t.Errorf("sanitizeUTF8(%q) = %q, want %q", tt.input, result, tt.expected)
			}
			testutil.AssertValidUTF8(t, result)
		})
	}
}
