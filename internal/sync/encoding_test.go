package sync

import (
	"strings"
	"testing"
	"unicode/utf8"
)

// encodingCase defines a test case for encoding conversion functions.
type encodingCase struct {
	name     string
	input    []byte
	expected string
}

// assertValidUTF8 asserts that the given string is valid UTF-8.
func assertValidUTF8(t *testing.T, s string) {
	t.Helper()
	if !utf8.ValidString(s) {
		t.Errorf("result is not valid UTF-8: %q", s)
	}
}

// assertContainsAll asserts that got contains every substring in subs.
func assertContainsAll(t *testing.T, got string, subs []string) {
	t.Helper()
	for _, substr := range subs {
		if !strings.Contains(got, substr) {
			t.Errorf("result %q should contain %q", got, substr)
		}
	}
}

// runEncodingTests runs table-driven tests that call ensureUTF8 on byte input
// and check both the expected output and UTF-8 validity.
func runEncodingTests(t *testing.T, tests []encodingCase) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ensureUTF8(string(tt.input))
			if result != tt.expected {
				t.Errorf("got %q, want %q", result, tt.expected)
			}
			assertValidUTF8(t, result)
		})
	}
}

func TestEnsureUTF8_AlreadyValid(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"ASCII", "Hello, World!"},
		{"UTF-8 Chinese", "你好世界"},
		{"UTF-8 Japanese", "こんにちは"},
		{"UTF-8 Korean", "안녕하세요"},
		{"UTF-8 Cyrillic", "Привет мир"},
		{"UTF-8 mixed", "Hello 世界! Привет!"},
		{"UTF-8 emoji", "Hello 👋 World 🌍"},
		{"empty string", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ensureUTF8(tt.input)
			if result != tt.input {
				t.Errorf("ensureUTF8(%q) = %q, want unchanged", tt.input, result)
			}
		})
	}
}

func TestEnsureUTF8_Windows1252(t *testing.T) {
	runEncodingTests(t, []encodingCase{
		{"smart single quote (right)", []byte("Rand\x92s Opponent"), "Rand\u2019s Opponent"},
		{"en dash", []byte("2020 \x96 2024"), "2020 \u2013 2024"},
		{"em dash", []byte("Hello\x97World"), "Hello\u2014World"},
		{"left double quote", []byte("\x93Hello\x94"), "\u201cHello\u201d"},
		{"trademark", []byte("Brand\x99"), "Brand\u2122"},
		{"bullet", []byte("\x95 Item"), "\u2022 Item"},
		{"euro sign", []byte("Price: \x80100"), "Price: \u20ac100"},
	})
}

func TestEnsureUTF8_Latin1(t *testing.T) {
	runEncodingTests(t, []encodingCase{
		{"o with acute", []byte("Mir\xf3 - Picasso"), "Miró - Picasso"},
		{"c with cedilla", []byte("Gar\xe7on"), "Garçon"},
		{"u with umlaut", []byte("M\xfcnchen"), "München"},
		{"n with tilde", []byte("Espa\xf1a"), "España"},
		{"registered trademark", []byte("Laguiole.com \xae"), "Laguiole.com ®"},
		{"degree symbol", []byte("25\xb0C"), "25°C"},
	})
}

func TestEnsureUTF8_AsianEncodings(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
	}{
		{"Shift-JIS Japanese", []byte{0x82, 0xb1, 0x82, 0xf1, 0x82, 0xc9, 0x82, 0xbf, 0x82, 0xcd}},
		{"GBK Simplified Chinese", []byte{0xc4, 0xe3, 0xba, 0xc3}},
		{"Big5 Traditional Chinese", []byte{0xa9, 0x6f, 0xa6, 0x6e}},
		{"EUC-KR Korean", []byte{0xbe, 0xc8, 0xb3, 0xe7}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ensureUTF8(string(tt.input))
			assertValidUTF8(t, result)
			if len(result) == 0 {
				t.Errorf("result is empty")
			}
		})
	}
}

func TestEnsureUTF8_MixedContent(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		contains []string
	}{
		{
			"email subject with smart quotes",
			[]byte("Re: Can\x92t access the \x93dashboard\x94"),
			[]string{"Re:", "Can", "access the", "dashboard"},
		},
		{
			"price with currency",
			[]byte("Only \x80199.99 \x96 Limited Time"),
			[]string{"Only", "199.99", "Limited Time"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ensureUTF8(string(tt.input))
			assertValidUTF8(t, result)
			assertContainsAll(t, result, tt.contains)
		})
	}
}

func TestSanitizeUTF8(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"valid UTF-8 unchanged", "Hello, 世界!", "Hello, 世界!"},
		{"single invalid byte", "Hello\x80World", "Hello\ufffdWorld"},
		{"multiple invalid bytes", "Test\x80\x81\x82String", "Test\ufffd\ufffd\ufffdString"},
		{"truncated UTF-8 sequence", "Hello\xc3", "Hello\ufffd"},
		{"invalid continuation byte", "Test\xc3\x00End", "Test\ufffd\x00End"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeUTF8(tt.input)
			if result != tt.expected {
				t.Errorf("sanitizeUTF8(%q) = %q, want %q", tt.input, result, tt.expected)
			}
			assertValidUTF8(t, result)
		})
	}
}

func TestGetEncodingByName(t *testing.T) {
	tests := []struct {
		charset string
		wantNil bool
	}{
		{"windows-1252", false},
		{"CP1252", false},
		{"ISO-8859-1", false},
		{"iso-8859-1", false},
		{"latin1", false},
		{"Shift_JIS", false},
		{"shift_jis", false},
		{"EUC-JP", false},
		{"EUC-KR", false},
		{"GBK", false},
		{"GB2312", false},
		{"Big5", false},
		{"KOI8-R", false},
		{"unknown-charset", true},
		{"", true},
	}

	for _, tt := range tests {
		t.Run(tt.charset, func(t *testing.T) {
			result := getEncodingByName(tt.charset)
			if tt.wantNil && result != nil {
				t.Errorf("getEncodingByName(%q) = %v, want nil", tt.charset, result)
			}
			if !tt.wantNil && result == nil {
				t.Errorf("getEncodingByName(%q) = nil, want encoding", tt.charset)
			}
		})
	}
}
