package imessage

import (
	"bytes"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/wesm/msgvault/internal/textimport"
	"howett.net/plist"
)

// appleEpochOffset is the number of seconds between Unix epoch (1970-01-01)
// and Apple/Core Data epoch (2001-01-01).
const appleEpochOffset int64 = 978307200

// appleTimestampToTime converts an Apple epoch timestamp to time.Time.
// macOS High Sierra+ stores dates as nanoseconds since Apple epoch;
// older versions use seconds. We detect the format by checking magnitude.
func appleTimestampToTime(ts int64) time.Time {
	if ts == 0 {
		return time.Time{}
	}
	// Values > 1e12 are nanoseconds (1e12 ns = ~16 minutes from epoch,
	// while 1e12 seconds from epoch = year ~33700).
	if ts > 1_000_000_000_000 {
		sec := ts / 1_000_000_000
		nsec := ts % 1_000_000_000
		return time.Unix(sec+appleEpochOffset, nsec).UTC()
	}
	return time.Unix(ts+appleEpochOffset, 0).UTC()
}

// timeToAppleTimestamp converts a time.Time to an Apple epoch timestamp.
// If useNano is true, returns nanoseconds; otherwise returns seconds.
func timeToAppleTimestamp(t time.Time, useNano bool) int64 {
	appleSec := t.Unix() - appleEpochOffset
	if useNano {
		return appleSec*1_000_000_000 + int64(t.Nanosecond())
	}
	return appleSec
}

// resolveHandle classifies an iMessage handle ID as a phone number,
// email address, or raw identifier (e.g. system handles).
func resolveHandle(handleID string) (phone, email, displayName string) {
	if handleID == "" {
		return "", "", ""
	}
	normalized, err := textimport.NormalizePhone(handleID)
	if err == nil {
		return normalized, "", normalized
	}
	if strings.Contains(handleID, "@") {
		return "", strings.ToLower(handleID), ""
	}
	return "", "", handleID
}

// extractAttributedBodyText extracts the plain text string from chat.db's
// attributedBody column. This column uses one of two serialization formats:
//
//   - NSArchiver "streamtyped" — legacy format, header starts with
//     \x04\x0bstreamtyped. The text is embedded as an NSString with a
//     length prefix after the class hierarchy.
//   - NSKeyedArchiver binary plist — starts with "bplist". The text lives
//     at $objects[rootObj["NS.string"]].
//
// macOS Ventura+ / iOS 16+ stopped populating the plain-text "text"
// column for most iMessages; the content lives exclusively here.
func extractAttributedBodyText(data []byte) string {
	if len(data) == 0 {
		return ""
	}

	// Try streamtyped format first (most common on modern macOS)
	if bytes.HasPrefix(data, []byte("\x04\x0bstreamtyped")) {
		return extractStreamtypedText(data)
	}

	// Try NSKeyedArchiver binary plist
	if bytes.HasPrefix(data, []byte("bplist")) {
		return extractKeyedArchiverText(data)
	}

	return ""
}

// extractStreamtypedText extracts text from NSArchiver streamtyped format.
// The format embeds an NSString with the text content. We scan for the
// NSString class marker, skip past the variable-length encoding prefix,
// and extract the UTF-8 text that follows.
func extractStreamtypedText(data []byte) string {
	// The NSString payload appears after \x84\x01+ followed by a
	// variable-length size prefix, then the raw UTF-8 text.
	marker := []byte("\x84\x01+")
	idx := bytes.Index(data, marker)
	if idx < 0 {
		return ""
	}
	pos := idx + len(marker)
	if pos >= len(data) {
		return ""
	}

	// Skip the length prefix. Single-byte lengths have bit 7 clear.
	// Multi-byte: bit 7 is set, and the remaining bytes encode the
	// length. Rather than trying to decode the exact format, skip
	// all bytes that are clearly part of the prefix (non-printable
	// bytes before the text starts).
	b := data[pos]
	if b&0x80 != 0 {
		// Multi-byte length — skip the flag byte and any following
		// bytes that look like length encoding (values < 0x20 or
		// continuation bytes).
		pos++
		for pos < len(data) && data[pos] < 0x20 {
			pos++
		}
	} else {
		// Single-byte length — just skip it
		pos++
	}

	if pos >= len(data) {
		return ""
	}

	// Extract valid UTF-8 text from pos to the next non-text control
	// sequence. The text ends at a \x00 null, or at a \x84/\x85/\x86
	// archiver control byte.
	end := pos
	for end < len(data) {
		ch := data[end]
		if ch == 0x00 || ch == 0x84 || ch == 0x85 || ch == 0x86 {
			break
		}
		end++
	}

	if end <= pos {
		return ""
	}

	text := string(data[pos:end])

	// Validate UTF-8 and trim any trailing incomplete sequences
	for len(text) > 0 && !utf8.ValidString(text) {
		text = text[:len(text)-1]
	}

	return text
}

// extractKeyedArchiverText extracts text from NSKeyedArchiver binary plist.
func extractKeyedArchiverText(data []byte) string {
	var archive struct {
		Top     map[string]plist.UID `plist:"$top"`
		Objects []interface{}        `plist:"$objects"`
	}
	if _, err := plist.Unmarshal(data, &archive); err != nil {
		return ""
	}

	rootUID, ok := archive.Top["root"]
	if !ok || int(rootUID) >= len(archive.Objects) {
		return ""
	}

	rootObj, ok := archive.Objects[rootUID].(map[string]interface{})
	if !ok {
		return ""
	}

	nsStringUID, ok := rootObj["NS.string"].(plist.UID)
	if !ok {
		return ""
	}
	if int(nsStringUID) >= len(archive.Objects) {
		return ""
	}

	text, ok := archive.Objects[nsStringUID].(string)
	if !ok {
		return ""
	}
	return text
}

// snippet returns the first n characters of s, suitable for message preview.
func snippet(s string, maxLen int) string {
	// Normalize whitespace
	s = strings.Join(strings.Fields(s), " ")
	runes := []rune(s)
	if len(runes) > maxLen {
		return string(runes[:maxLen])
	}
	return s
}
