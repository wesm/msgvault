package sync

import (
	"testing"

	"github.com/wesm/msgvault/internal/testutil"
)

// encodingCase defines a test case for encoding conversion functions.
type encodingCase struct {
	name     string
	input    []byte
	expected string
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
			testutil.AssertValidUTF8(t, result)
		})
	}
}

func TestEnsureUTF8_AlreadyValid(t *testing.T) {
	runEncodingTests(t, []encodingCase{
		{"ASCII", []byte("Hello, World!"), "Hello, World!"},
		{"UTF-8 Chinese", []byte("你好世界"), "你好世界"},
		{"UTF-8 Japanese", []byte("こんにちは"), "こんにちは"},
		{"UTF-8 Korean", []byte("안녕하세요"), "안녕하세요"},
		{"UTF-8 Cyrillic", []byte("Привет мир"), "Привет мир"},
		{"UTF-8 mixed", []byte("Hello 世界! Привет!"), "Hello 世界! Привет!"},
		{"UTF-8 emoji", []byte("Hello 👋 World 🌍"), "Hello 👋 World 🌍"},
		{"empty string", []byte(""), ""},
	})
}

func TestEnsureUTF8_Windows1252(t *testing.T) {
	enc := testutil.EncodedSamples
	runEncodingTests(t, []encodingCase{
		{"smart single quote (right)", enc.Win1252_SmartQuoteRight, "Rand\u2019s Opponent"},
		{"en dash", enc.Win1252_EnDash, "2020 \u2013 2024"},
		{"em dash", enc.Win1252_EmDash, "Hello\u2014World"},
		{"left double quote", enc.Win1252_DoubleQuotes, "\u201cHello\u201d"},
		{"trademark", enc.Win1252_Trademark, "Brand\u2122"},
		{"bullet", enc.Win1252_Bullet, "\u2022 Item"},
		{"euro sign", enc.Win1252_Euro, "Price: \u20ac100"},
	})
}

func TestEnsureUTF8_Latin1(t *testing.T) {
	enc := testutil.EncodedSamples
	runEncodingTests(t, []encodingCase{
		{"o with acute", enc.Latin1_OAcute, "Miró - Picasso"},
		{"c with cedilla", enc.Latin1_CCedilla, "Garçon"},
		{"u with umlaut", enc.Latin1_UUmlaut, "München"},
		{"n with tilde", enc.Latin1_NTilde, "España"},
		{"registered trademark", enc.Latin1_Registered, "Laguiole.com ®"},
		{"degree symbol", enc.Latin1_Degree, "25°C"},
	})
}

func TestEnsureUTF8_AsianEncodings(t *testing.T) {
	// ensureUTF8 relies on chardet heuristics without charset hints. Short
	// byte sequences from CJK encodings are typically misidentified, so we
	// can only assert valid UTF-8 output (not exact decoded strings).
	enc := testutil.EncodedSamples
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
			result := ensureUTF8(string(tt.input))
			testutil.AssertValidUTF8(t, result)
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
			testutil.AssertValidUTF8(t, result)
			testutil.AssertContainsAll(t, result, tt.contains)
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
			testutil.AssertValidUTF8(t, result)
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
