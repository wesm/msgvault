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
	// reReplyPreamble matches "On <date>, <name> wrote:" followed by one or
	// more "> " quoted lines, as produced by most email clients when replying.
	reReplyPreamble = regexp.MustCompile(`(?m)^On [^\n]+wrote:\s*\n(?:> .*\n?)+`)
	// reSigDelim matches a signature delimiter "\n-- \n" (or "\n--\n") through
	// end of string.
	reSigDelim = regexp.MustCompile(`\n--\s*\n[\s\S]*\z`)
	// reQuoteLine matches standalone quoted lines beginning with "> ".
	reQuoteLine = regexp.MustCompile(`(?m)^> .*\n?`)
)

// Preprocess produces the string fed to the embedder. It optionally strips
// reply quotes and signature blocks, trims whitespace, prepends a
// "Subject: <subject>\n\n" prefix when subject is non-empty, and truncates
// the result to maxChars bytes on a UTF-8 rune boundary. Returns the
// preprocessed string and a boolean indicating whether truncation occurred.
// A maxChars value <= 0 disables truncation.
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

	if maxChars <= 0 || len(combined) <= maxChars {
		return combined, false
	}
	// Truncate at a UTF-8 rune boundary to avoid emitting invalid bytes.
	cut := maxChars
	for cut > 0 && !utf8.RuneStart(combined[cut]) {
		cut--
	}
	return combined[:cut], true
}
