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
//
// After the marker (\x84\x01+), there is a length prefix:
//   - Single-byte: bit 7 clear (0x00-0x7F), value is the text length
//   - Multi-byte: bit 7 set, followed by length bytes and framing bytes
//     (including 0x92 and other high bytes)
//
// The text content is always clean UTF-8 surrounded by binary framing.
func extractStreamtypedText(data []byte) string {
	marker := []byte("\x84\x01+")
	idx := bytes.Index(data, marker)
	if idx < 0 {
		return ""
	}
	pos := idx + len(marker)
	if pos >= len(data) {
		return ""
	}

	// Decode the length prefix to know exactly how many text bytes follow.
	b := data[pos]
	textLen := -1
	if b&0x80 == 0 {
		// Single-byte length (0x00-0x7F)
		textLen = int(b)
		pos++
	} else {
		// Multi-byte length: flag byte has bit 7 set. Skip the flag
		// and all subsequent non-text framing bytes (high bytes, nulls,
		// and other control bytes) until we find text content.
		pos++
		for pos < len(data) {
			fb := data[pos]
			if (fb >= 0x20 && fb < 0x7F) ||
				(fb >= 0xC2 && fb <= 0xF4) {
				break
			}
			pos++
		}
	}

	if pos >= len(data) {
		return ""
	}

	// If we decoded an exact length, use it directly
	if textLen > 0 {
		end := pos + textLen
		if end > len(data) {
			end = len(data)
		}
		text := string(data[pos:end])
		for len(text) > 0 && !utf8.ValidString(text) {
			text = text[:len(text)-1]
		}
		return text
	}

	// Without a decoded length, extract until an archiver control
	// sequence or null byte. The 0x86 byte reliably marks the end
	// of text in multi-byte format. The 0x84/0x85 bytes can also
	// appear as UTF-8 continuation bytes, so only treat them as
	// terminators when the text so far is already valid UTF-8.
	end := pos
	for end < len(data) {
		ch := data[end]
		if ch == 0x00 || ch == 0x86 {
			break
		}
		if ch == 0x84 || ch == 0x85 {
			if utf8.ValidString(string(data[pos:end])) {
				break
			}
		}
		end++
	}

	if end <= pos {
		return ""
	}

	text := string(data[pos:end])
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
