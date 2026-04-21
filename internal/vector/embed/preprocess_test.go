package embed

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestPreprocess(t *testing.T) {
	tests := []struct {
		name      string
		subject   string
		body      string
		maxChars  int
		cfg       PreprocessConfig
		checkWant bool // if true, assert out == want (even when want is "")
		want      string
		lenLE     int  // 0 = no check; N = require len(out) <= N
		wantValid bool // require utf8.ValidString(out)
		wantTrunc bool
	}{
		{
			name:      "PlainBody",
			subject:   "Hello",
			body:      "Hi there,\n\nLet's chat tomorrow.",
			maxChars:  1000,
			cfg:       PreprocessConfig{StripQuotes: true, StripSignatures: true},
			checkWant: true,
			want:      "Subject: Hello\n\nHi there,\n\nLet's chat tomorrow.",
			wantTrunc: false,
		},
		{
			name:      "StripsQuotedPreamble",
			subject:   "Re: plan",
			body:      "My reply.\n\nOn 2026-01-01, alice wrote:\n> previous message\n> more quote",
			maxChars:  1000,
			cfg:       PreprocessConfig{StripQuotes: true, StripSignatures: true},
			checkWant: true,
			want:      "Subject: Re: plan\n\nMy reply.",
			wantTrunc: false,
		},
		{
			name:      "StripsStandaloneQuoteLines",
			subject:   "",
			body:      "> nested quote 1\n> nested quote 2\nActual content.",
			maxChars:  1000,
			cfg:       PreprocessConfig{StripQuotes: true, StripSignatures: false},
			checkWant: true,
			want:      "Actual content.",
			wantTrunc: false,
		},
		{
			name:      "StripsSignature",
			subject:   "Hi",
			body:      "Body here.\n-- \nBob\nPhone: ...",
			maxChars:  1000,
			cfg:       PreprocessConfig{StripQuotes: true, StripSignatures: true},
			checkWant: true,
			want:      "Subject: Hi\n\nBody here.",
			wantTrunc: false,
		},
		{
			name:      "SignatureWithoutTrailingSpace",
			subject:   "Hi",
			body:      "Body.\n--\nBob",
			maxChars:  1000,
			cfg:       PreprocessConfig{StripQuotes: false, StripSignatures: true},
			checkWant: true,
			want:      "Subject: Hi\n\nBody.",
			wantTrunc: false,
		},
		{
			name:      "Truncates",
			subject:   "S",
			body:      strings.Repeat("x", 2000),
			maxChars:  100,
			cfg:       PreprocessConfig{},
			lenLE:     100,
			wantTrunc: true,
		},
		{
			// maxChars is a rune count, so 10 snowmen (3 bytes
			// each) should produce a 30-byte result.
			name:      "TruncateAtRuneBoundary",
			subject:   "",
			body:      strings.Repeat("\u2603", 50),
			maxChars:  10,
			cfg:       PreprocessConfig{},
			lenLE:     30,
			wantValid: true,
			wantTrunc: true,
		},
		{
			name:      "EmptySubjectAndBody",
			subject:   "",
			body:      "",
			maxChars:  1000,
			cfg:       PreprocessConfig{},
			checkWant: true,
			want:      "",
			wantTrunc: false,
		},
		{
			name:      "NoConfigPreservesEverything",
			subject:   "Hi",
			body:      "Hello\n> not stripped\n-- \nsig kept",
			maxChars:  1000,
			cfg:       PreprocessConfig{StripQuotes: false, StripSignatures: false},
			checkWant: true,
			want:      "Subject: Hi\n\nHello\n> not stripped\n-- \nsig kept",
			wantTrunc: false,
		},
		{
			name:      "MaxCharsZeroIsUnlimited",
			subject:   "S",
			body:      strings.Repeat("x", 100),
			maxChars:  0,
			cfg:       PreprocessConfig{},
			checkWant: true,
			want:      "Subject: S\n\n" + strings.Repeat("x", 100),
			wantTrunc: false,
		},
		{
			// Each snowman is 3 bytes. 100 bytes of snowmen = ~33
			// runes, well under the 100-rune cap, so no truncation
			// despite byte length comfortably exceeding maxChars
			// under the old byte-counting rule.
			name:      "MultiByteRunesUnderCap",
			subject:   "",
			body:      strings.Repeat("\u2603", 33),
			maxChars:  100,
			cfg:       PreprocessConfig{},
			wantTrunc: false,
			lenLE:     100,
			wantValid: true,
		},
		{
			name:      "StripsNestedQuotes",
			subject:   "",
			body:      ">> deep quote\n>>> deeper quote\nReal content.",
			maxChars:  1000,
			cfg:       PreprocessConfig{StripQuotes: true},
			checkWant: true,
			want:      "Real content.",
			wantTrunc: false,
		},
		{
			name:      "StripsQuoteWithoutSpace",
			subject:   "",
			body:      ">no space after caret\n>another\nKept content.",
			maxChars:  1000,
			cfg:       PreprocessConfig{StripQuotes: true},
			checkWant: true,
			want:      "Kept content.",
			wantTrunc: false,
		},
		{
			name:      "StripsPreambleWithNestedQuotes",
			subject:   "Re: topic",
			body:      "My reply.\n\nOn 2026-04-18, bob wrote:\n>> prior reply\n> and response\n>> more",
			maxChars:  1000,
			cfg:       PreprocessConfig{StripQuotes: true},
			checkWant: true,
			want:      "Subject: Re: topic\n\nMy reply.",
			wantTrunc: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, tr := Preprocess(tt.subject, tt.body, tt.maxChars, tt.cfg)
			if tt.checkWant && out != tt.want {
				t.Errorf("got %q\nwant %q", out, tt.want)
			}
			if tt.lenLE > 0 && len(out) > tt.lenLE {
				t.Errorf("len(out)=%d > %d", len(out), tt.lenLE)
			}
			if tt.wantValid && !utf8.ValidString(out) {
				t.Error("out is not valid UTF-8")
			}
			if tr != tt.wantTrunc {
				t.Errorf("truncated=%v, want %v", tr, tt.wantTrunc)
			}
		})
	}
}
