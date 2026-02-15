package mbox

import (
	"fmt"
	"strings"
	"time"
)

var fromSeparatorDateLayouts = []string{
	"Mon Jan 2 15:04:05 2006",
	"Mon Jan 2 15:04:05 -0700 2006",
	"Mon Jan 2 15:04:05 -07:00 2006",
	"Mon Jan 2 15:04:05 MST 2006",
	"Mon Jan 2 15:04:05 2006 -0700",
	"Mon Jan 2 15:04:05 2006 -07:00",
	"Mon Jan 2 15:04:05 2006 MST",
	"Mon Jan 2 15:04 2006",
	"Mon Jan 2 15:04 -0700 2006",
	"Mon Jan 2 15:04 -07:00 2006",
	"Mon Jan 2 15:04 MST 2006",
	"Mon Jan 2 15:04 2006 -0700",
	"Mon Jan 2 15:04 2006 -07:00",
	"Mon Jan 2 15:04 2006 MST",
	"Jan 2 15:04:05 2006",
	"Jan 2 15:04:05 -0700 2006",
	"Jan 2 15:04:05 -07:00 2006",
	"Jan 2 15:04:05 MST 2006",
	"Jan 2 15:04:05 2006 -0700",
	"Jan 2 15:04:05 2006 -07:00",
	"Jan 2 15:04:05 2006 MST",
	"Jan 2 15:04 2006",
	"Jan 2 15:04 -0700 2006",
	"Jan 2 15:04 -07:00 2006",
	"Jan 2 15:04 MST 2006",
	"Jan 2 15:04 2006 -0700",
	"Jan 2 15:04 2006 -07:00",
	"Jan 2 15:04 2006 MST",
}

var fromSeparatorTZAbbrevOffsets = map[string]int{
	"UTC":  0,
	"GMT":  0,
	"UT":   0,
	"Z":    0,
	"EST":  -5 * 60 * 60,
	"EDT":  -4 * 60 * 60,
	"CST":  -6 * 60 * 60,
	"CDT":  -5 * 60 * 60,
	"MST":  -7 * 60 * 60,
	"MDT":  -6 * 60 * 60,
	"PST":  -8 * 60 * 60,
	"PDT":  -7 * 60 * 60,
	"AKST": -9 * 60 * 60,
	"AKDT": -8 * 60 * 60,
	"HST":  -10 * 60 * 60,
}

func offsetHHMM(offsetSeconds int) string {
	sign := '+'
	if offsetSeconds < 0 {
		sign = '-'
		offsetSeconds = -offsetSeconds
	}
	h := offsetSeconds / (60 * 60)
	m := (offsetSeconds % (60 * 60)) / 60
	return fmt.Sprintf("%c%02d%02d", sign, h, m)
}

func tzOffsetFromAbbrev(abbrev string) (int, bool) {
	abbrev = strings.Trim(abbrev, "()")
	abbrev = strings.ToUpper(abbrev)
	off, ok := fromSeparatorTZAbbrevOffsets[abbrev]
	return off, ok
}

func looksLikeTZToken(token string) bool {
	token = strings.Trim(token, "()")
	if _, ok := tzOffsetFromAbbrev(token); ok {
		return true
	}
	if looksLikeNumericOffset(token) {
		return true
	}
	if token == "" {
		return false
	}
	if token != strings.ToUpper(token) {
		return false
	}
	if len(token) > 5 {
		return false
	}
	for i := 0; i < len(token); i++ {
		c := token[i]
		if (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') {
			return false
		}
	}
	return true
}

func looksLikeNumericOffset(token string) bool {
	if token == "" {
		return false
	}
	if token[0] != '+' && token[0] != '-' {
		return false
	}
	if len(token) == 5 {
		for i := 1; i < len(token); i++ {
			if token[i] < '0' || token[i] > '9' {
				return false
			}
		}
		return true
	}
	if len(token) == 6 && token[3] == ':' {
		if token[1] < '0' || token[1] > '9' || token[2] < '0' || token[2] > '9' || token[4] < '0' || token[4] > '9' || token[5] < '0' || token[5] > '9' {
			return false
		}
		return true
	}
	return false
}

// ParseFromSeparatorDate parses the ctime-like date portion of an mbox "From "
// separator line.
//
// This is intentionally permissive and is used as a heuristic for separator
// detection. In edge cases, an unescaped body line that looks like a separator
// ("From <x> <ctime-like date> ...") can be misclassified; mbox writers should
// escape such body lines (e.g. ">From ").
func ParseFromSeparatorDate(line string) (time.Time, bool) {
	fields := strings.Fields(line)
	// Typical mbox "From " separator: "From <sender> <ctime-like date> [extra...]"
	// Some producers append extra tokens (e.g. "remote from ..."), so only parse the
	// date prefix.
	if len(fields) < 6 || fields[0] != "From" {
		return time.Time{}, false
	}

	for _, layout := range fromSeparatorDateLayouts {
		n := len(strings.Fields(layout))
		if len(fields) < 2+n {
			continue
		}
		dateStr := strings.Join(fields[2:2+n], " ")
		if t, err := time.Parse(layout, dateStr); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// ParseFromSeparatorDateStrict parses the ctime-like date portion of an mbox "From "
// separator line, but only accepts numeric offsets or a small allowlist of
// well-known timezone abbreviations. This avoids treating arbitrary abbreviations
// as UTC.
func ParseFromSeparatorDateStrict(line string) (time.Time, bool) {
	fields := strings.Fields(line)
	if len(fields) < 6 || fields[0] != "From" {
		return time.Time{}, false
	}

	for _, layout := range fromSeparatorDateLayouts {
		n := len(strings.Fields(layout))
		if len(fields) < 2+n {
			continue
		}
		hasTZ := strings.Contains(layout, "MST") || strings.Contains(layout, "-0700") || strings.Contains(layout, "-07:00")
		if !hasTZ && len(fields) > 2+n && looksLikeTZToken(fields[2+n]) {
			continue
		}

		dateFields := fields[2 : 2+n]
		dateStr := strings.Join(dateFields, " ")

		if !strings.Contains(layout, "MST") {
			if t, err := time.Parse(layout, dateStr); err == nil {
				return t, true
			}
			continue
		}

		layoutFields := strings.Fields(layout)
		tzIdx := -1
		for i := range layoutFields {
			if layoutFields[i] == "MST" {
				tzIdx = i
				break
			}
		}
		if tzIdx == -1 {
			continue
		}
		if tzIdx >= len(dateFields) {
			continue
		}
		off, ok := tzOffsetFromAbbrev(dateFields[tzIdx])
		if !ok {
			continue
		}

		patched := append([]string(nil), dateFields...)
		patched[tzIdx] = offsetHHMM(off)
		patchedStr := strings.Join(patched, " ")
		numericLayout := strings.Replace(layout, "MST", "-0700", 1)
		if t, err := time.Parse(numericLayout, patchedStr); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}
