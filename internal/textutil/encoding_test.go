package textutil

import (
	"strings"
	"testing"

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/encoding/korean"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/traditionalchinese"

	"github.com/wesm/msgvault/internal/testutil"
)

func TestEnsureUTF8_AlreadyValid(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected string
	}{
		{"ASCII", []byte("Hello, World!"), "Hello, World!"},
		{"UTF-8 Chinese", []byte("你好世界"), "你好世界"},
		{"UTF-8 Japanese", []byte("こんにちは"), "こんにちは"},
		{"UTF-8 Korean", []byte("안녕하세요"), "안녕하세요"},
		{"UTF-8 Cyrillic", []byte("Привет мир"), "Привет мир"},
		{"UTF-8 mixed", []byte("Hello 世界! Привет!"), "Hello 世界! Привет!"},
		{"UTF-8 emoji", []byte("Hello 👋 World 🌍"), "Hello 👋 World 🌍"},
		{"empty string", []byte(""), ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EnsureUTF8(string(tt.input))
			if result != tt.expected {
				t.Errorf("got %q, want %q", result, tt.expected)
			}
			testutil.AssertValidUTF8(t, result)
		})
	}
}

func TestEnsureUTF8_Windows1252(t *testing.T) {
	enc := testutil.EncodedSamples()
	tests := []struct {
		name     string
		input    []byte
		expected string
	}{
		{"smart single quote (right)", enc.Win1252_SmartQuoteRight, "Rand\u2019s Opponent"},
		{"en dash", enc.Win1252_EnDash, "2020 \u2013 2024"},
		{"em dash", enc.Win1252_EmDash, "Hello\u2014World"},
		{"left double quote", enc.Win1252_DoubleQuotes, "\u201cHello\u201d"},
		{"trademark", enc.Win1252_Trademark, "Brand\u2122"},
		{"bullet", enc.Win1252_Bullet, "\u2022 Item"},
		{"euro sign", enc.Win1252_Euro, "Price: \u20ac100"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EnsureUTF8(string(tt.input))
			if result != tt.expected {
				t.Errorf("got %q, want %q", result, tt.expected)
			}
			testutil.AssertValidUTF8(t, result)
		})
	}
}

func TestEnsureUTF8_Latin1(t *testing.T) {
	enc := testutil.EncodedSamples()
	tests := []struct {
		name     string
		input    []byte
		expected string
	}{
		{"o with acute", enc.Latin1_OAcute, "Miró - Picasso"},
		{"c with cedilla", enc.Latin1_CCedilla, "Garçon"},
		{"u with umlaut", enc.Latin1_UUmlaut, "München"},
		{"n with tilde", enc.Latin1_NTilde, "España"},
		{"registered trademark", enc.Latin1_Registered, "Laguiole.com ®"},
		{"degree symbol", enc.Latin1_Degree, "25°C"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EnsureUTF8(string(tt.input))
			if result != tt.expected {
				t.Errorf("got %q, want %q", result, tt.expected)
			}
			testutil.AssertValidUTF8(t, result)
		})
	}
}

func TestEnsureUTF8_AsianEncodings(t *testing.T) {
	// Test that EnsureUTF8 produces valid UTF-8 from Asian-encoded input.
	// We don't assert exact decoded strings because chardet heuristics may vary
	// across library versions. Instead, we verify:
	// 1. Output is valid UTF-8
	// 2. Output is non-empty
	// 3. Output doesn't contain replacement characters (successful decode)
	enc := testutil.EncodedSamples()
	tests := []struct {
		name  string
		input []byte
	}{
		{"Shift-JIS Japanese", enc.ShiftJIS_Long},
		{"GBK Simplified Chinese", enc.GBK_Long},
		{"Big5 Traditional Chinese", enc.Big5_Long},
		{"EUC-KR Korean", enc.EUCKR_Long},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EnsureUTF8(string(tt.input))
			testutil.AssertValidUTF8(t, result)
			if result == "" {
				t.Error("result is empty")
			}
			// Verify no replacement characters (indicates failed decode)
			if strings.ContainsRune(result, '\ufffd') {
				t.Errorf("result contains replacement character, suggesting decode failure: %q", result)
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
			result := EnsureUTF8(string(tt.input))
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
			result := SanitizeUTF8(tt.input)
			if result != tt.expected {
				t.Errorf("SanitizeUTF8(%q) = %q, want %q", tt.input, result, tt.expected)
			}
			testutil.AssertValidUTF8(t, result)
		})
	}
}

func TestGetEncodingByName(t *testing.T) {
	tests := []struct {
		charset    string
		wantNil    bool
		verifyByte byte // A byte that decodes differently in this charset vs ASCII
		wantRune   rune // Expected rune when decoding verifyByte
	}{
		// Windows-1252: 0x92 = right single quote (')
		{"windows-1252", false, 0x92, '\u2019'},
		{"CP1252", false, 0x92, '\u2019'},
		// ISO-8859-1: 0xE9 = é
		{"ISO-8859-1", false, 0xe9, 'é'},
		{"iso-8859-1", false, 0xe9, 'é'},
		{"latin1", false, 0xe9, 'é'},
		// Shift_JIS: two-byte sequence 0x82 0xA0 = あ (hiragana a)
		{"Shift_JIS", false, 0, 0}, // Skip byte verification for multi-byte
		{"shift_jis", false, 0, 0},
		// Other encodings - verify non-nil only
		{"EUC-JP", false, 0, 0},
		{"EUC-KR", false, 0, 0},
		{"GBK", false, 0, 0},
		{"GB2312", false, 0, 0},
		{"Big5", false, 0, 0},
		{"KOI8-R", false, 0, 0},
		// Unknown charset should return nil
		{"unknown-charset", true, 0, 0},
		{"", true, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.charset, func(t *testing.T) {
			enc := GetEncodingByName(tt.charset)
			if tt.wantNil {
				if enc != nil {
					t.Errorf("GetEncodingByName(%q) = %v, want nil", tt.charset, enc)
				}
				return
			}
			if enc == nil {
				t.Fatalf("GetEncodingByName(%q) = nil, want encoding", tt.charset)
			}
			// Verify encoding identity by decoding a characteristic byte
			if tt.verifyByte != 0 {
				decoded, err := enc.NewDecoder().Bytes([]byte{tt.verifyByte})
				if err != nil {
					t.Fatalf("decode failed: %v", err)
				}
				got := []rune(string(decoded))
				if len(got) != 1 || got[0] != tt.wantRune {
					t.Errorf("decoding 0x%02x: got %q, want %q", tt.verifyByte, string(got), string(tt.wantRune))
				}
			}
		})
	}
}

func TestGetEncodingByName_DecodesCorrectly(t *testing.T) {
	// Verify that GetEncodingByName returns encodings that decode test samples correctly.
	enc := testutil.EncodedSamples()
	tests := []struct {
		name     string
		charset  string
		input    []byte
		expected string
	}{
		{"Shift-JIS", "Shift_JIS", enc.ShiftJIS_Long, enc.ShiftJIS_Long_UTF8},
		{"GBK", "GBK", enc.GBK_Long, enc.GBK_Long_UTF8},
		{"Big5", "Big5", enc.Big5_Long, enc.Big5_Long_UTF8},
		{"EUC-KR", "EUC-KR", enc.EUCKR_Long, enc.EUCKR_Long_UTF8},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoding := GetEncodingByName(tt.charset)
			if encoding == nil {
				t.Fatalf("GetEncodingByName(%q) returned nil", tt.charset)
			}
			decoded, err := encoding.NewDecoder().Bytes(tt.input)
			if err != nil {
				t.Fatalf("decode failed: %v", err)
			}
			if string(decoded) != tt.expected {
				t.Errorf("decoded %q, want %q", string(decoded), tt.expected)
			}
		})
	}
}

func TestGetEncodingByName_MatchesExpectedEncodings(t *testing.T) {
	// Verify that charset names map to the correct encoding objects.
	tests := []struct {
		charset  string
		wantName string // Use a test decoding to verify identity
	}{
		// Test that similar charset names return the same encoding
		{"windows-1252", "windows-1252"},
		{"CP1252", "windows-1252"},
		{"cp1252", "windows-1252"},
		{"ISO-8859-1", "iso-8859-1"},
		{"iso-8859-1", "iso-8859-1"},
		{"latin1", "iso-8859-1"},
		{"latin-1", "iso-8859-1"},
	}
	for _, tt := range tests {
		t.Run(tt.charset, func(t *testing.T) {
			enc := GetEncodingByName(tt.charset)
			expected := GetEncodingByName(tt.wantName)
			if enc == nil || expected == nil {
				t.Fatalf("encoding is nil")
			}
			// Verify they decode the same way
			testBytes := []byte{0x80, 0x92, 0xe9, 0xf1}
			got, err := enc.NewDecoder().Bytes(testBytes)
			if err != nil {
				t.Fatalf("decoder error for %q: %v", tt.charset, err)
			}
			want, err := expected.NewDecoder().Bytes(testBytes)
			if err != nil {
				t.Fatalf("decoder error for %q: %v", tt.wantName, err)
			}
			if string(got) != string(want) {
				t.Errorf("%q and %q decode differently: %q vs %q", tt.charset, tt.wantName, got, want)
			}
		})
	}
}

func TestEncodingIdentity(t *testing.T) {
	// Verify that GetEncodingByName returns the correct encoding type
	// by checking that decoding produces expected results for each encoding.
	tests := []struct {
		name     string
		charset  string
		input    []byte
		expected string
	}{
		{
			"Shift_JIS hiragana",
			"Shift_JIS",
			[]byte{0x82, 0xa0, 0x82, 0xa2, 0x82, 0xa4}, // あいう
			"あいう",
		},
		{
			"EUC-JP hiragana",
			"EUC-JP",
			[]byte{0xa4, 0xa2, 0xa4, 0xa4, 0xa4, 0xa6}, // あいう
			"あいう",
		},
		{
			"GBK chinese",
			"GBK",
			[]byte{0xc4, 0xe3, 0xba, 0xc3}, // 你好
			"你好",
		},
		{
			"Big5 chinese",
			"Big5",
			[]byte{0xa7, 0x41, 0xa6, 0x6e}, // 你好
			"你好",
		},
		{
			"EUC-KR korean",
			"EUC-KR",
			[]byte{0xbe, 0xc8, 0xb3, 0xe7}, // 안녕
			"안녕",
		},
		{
			"KOI8-R cyrillic",
			"KOI8-R",
			[]byte{0xf0, 0xf2, 0xe9, 0xf7, 0xe5, 0xf4}, // ПРИВЕТ
			"ПРИВЕТ",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc := GetEncodingByName(tt.charset)
			if enc == nil {
				t.Fatalf("GetEncodingByName(%q) returned nil", tt.charset)
			}
			decoded, err := enc.NewDecoder().Bytes(tt.input)
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}
			if string(decoded) != tt.expected {
				t.Errorf("decoded %q, want %q", string(decoded), tt.expected)
			}
		})
	}
}

func TestGetEncodingByName_ReturnsCorrectType(t *testing.T) {
	// Verify that specific charset names return encodings that decode identically
	// to the expected encoding types. Uses behavior-based comparison rather than
	// pointer equality to be robust against registry wrappers or equivalent encodings.
	tests := []struct {
		charset  string
		expected encoding.Encoding
		input    []byte
	}{
		{"Shift_JIS", japanese.ShiftJIS, []byte{0x82, 0xa0, 0x82, 0xa2}},  // あい
		{"EUC-JP", japanese.EUCJP, []byte{0xa4, 0xa2, 0xa4, 0xa4}},        // あい
		{"EUC-KR", korean.EUCKR, []byte{0xbe, 0xc8, 0xb3, 0xe7}},          // 안녕
		{"GBK", simplifiedchinese.GBK, []byte{0xc4, 0xe3, 0xba, 0xc3}},    // 你好
		{"Big5", traditionalchinese.Big5, []byte{0xa7, 0x41, 0xa6, 0x6e}}, // 你好
	}
	for _, tt := range tests {
		t.Run(tt.charset, func(t *testing.T) {
			enc := GetEncodingByName(tt.charset)
			if enc == nil {
				t.Fatalf("GetEncodingByName(%q) returned nil", tt.charset)
			}
			got, err := enc.NewDecoder().Bytes(tt.input)
			if err != nil {
				t.Fatalf("decoder error: %v", err)
			}
			want, err := tt.expected.NewDecoder().Bytes(tt.input)
			if err != nil {
				t.Fatalf("expected decoder error: %v", err)
			}
			if string(got) != string(want) {
				t.Errorf("GetEncodingByName(%q) decodes %x as %q, expected encoding decodes as %q",
					tt.charset, tt.input, got, want)
			}
		})
	}
}

func TestTruncateRunes(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		maxRunes int
		expected string
	}{
		{"short ASCII", "Hello", 10, "Hello"},
		{"exact length", "Hello", 5, "Hello"},
		{"truncate ASCII", "Hello World", 8, "Hello..."},
		{"empty string", "", 5, ""},
		{"max 3", "Hello", 3, "Hel"},
		{"max 4", "Hello", 4, "H..."},
		{"UTF-8 no truncate", "你好世界", 4, "你好世界"}, // 4 runes, no truncation needed
		{"UTF-8 truncate", "你好世界！", 4, "你..."},
		{"emoji", "Hello 👋 World", 9, "Hello ..."},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := TruncateRunes(tt.input, tt.maxRunes)
			if result != tt.expected {
				t.Errorf("TruncateRunes(%q, %d) = %q, want %q", tt.input, tt.maxRunes, result, tt.expected)
			}
		})
	}
}

func TestFirstLine(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"single line", "Hello World", "Hello World"},
		{"multi line", "First\nSecond\nThird", "First"},
		{"empty string", "", ""},
		{"trailing newline", "Hello\n", "Hello"},
		{"only newline", "\n", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FirstLine(tt.input)
			if result != tt.expected {
				t.Errorf("FirstLine(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}
