package embed

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

// PreprocessConfig controls pre-embedding transformations.
type PreprocessConfig struct {
	StripQuotes     bool
	StripSignatures bool
}

var (
	// reReplyPreamble matches "On <date>, <name> wrote:" followed by one
	// or more quoted lines. A quoted line starts with one or more `>`
	// characters, optionally followed by a single space or tab, so nested
	// quotes (">>", ">>>") and clients that omit the space after `>` are
	// both recognised.
	reReplyPreamble = regexp.MustCompile(`(?m)^On [^\n]+wrote:\s*\n(?:>+[ \t]?.*\n?)+`)
	// reSigDelim matches a signature delimiter "\n-- \n" (or "\n--\n") through
	// end of string.
	reSigDelim = regexp.MustCompile(`\n--\s*\n[\s\S]*\z`)
	// reQuoteLine matches standalone quoted lines: one or more `>`
	// characters followed by optional whitespace and the rest of the
	// line. Handles ">", "> foo", ">foo", ">> nested", ">>>deep".
	reQuoteLine = regexp.MustCompile(`(?m)^>+[ \t]?.*\n?`)
)

// Preprocess produces the string fed to the embedder. It optionally strips
// reply quotes and signature blocks, trims whitespace, prepends a
// "Subject: <subject>\n\n" prefix when subject is non-empty, and truncates
// the result to maxChars runes (not bytes — embedders count characters, and
// a byte-based cap would shortchange multi-byte scripts like CJK). Returns
// the preprocessed string and a boolean indicating whether truncation
// occurred. A maxChars value <= 0 disables truncation.
func Preprocess(subject, body string, maxChars int, cfg PreprocessConfig) (string, bool) {
	s := body
	if cfg.StripQuotes {
		s = reReplyPreamble.ReplaceAllString(s, "")
		s = reQuoteLine.ReplaceAllString(s, "")
	}
	if cfg.StripSignatures {
		s = reSigDelim.ReplaceAllString(s, "")
	}
	s = strings.TrimSpace(s)

	var prefix string
	if subject != "" {
		prefix = "Subject: " + subject + "\n\n"
	}
	combined := prefix + s

	if maxChars <= 0 {
		return combined, false
	}
	// Fast path: if every byte is ASCII, len == rune count and we
	// can skip the scan. Otherwise walk runes forward to find the
	// cut point at rune boundary maxChars.
	if len(combined) <= maxChars {
		return combined, false
	}
	byteOffset, runes := 0, 0
	for byteOffset < len(combined) && runes < maxChars {
		_, size := utf8.DecodeRuneInString(combined[byteOffset:])
		byteOffset += size
		runes++
	}
	if runes < maxChars {
		// Fewer runes than the cap — nothing to truncate.
		return combined, false
	}
	if byteOffset == len(combined) {
		// Exactly maxChars runes and no more — no truncation.
		return combined, false
	}
	return combined[:byteOffset], true
}
