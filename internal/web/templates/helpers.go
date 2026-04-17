package templates

import (
	"fmt"
	"html"
	"net/url"
	"regexp"
	"time"
)

// formatBytes formats a byte count into a human-readable string.
func formatBytes(b int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)
	switch {
	case b >= GB:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(GB))
	case b >= MB:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(MB))
	case b >= KB:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(KB))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

// formatCount formats a large number with comma separators.
func formatCount(n int64) string {
	if n < 0 {
		return "-" + formatCount(-n)
	}
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}

	s := fmt.Sprintf("%d", n)
	result := make([]byte, 0, len(s)+len(s)/3)
	rem := len(s) % 3
	if rem == 0 {
		rem = 3
	}
	result = append(result, s[:rem]...)
	for i := rem; i < len(s); i += 3 {
		result = append(result, ',')
		result = append(result, s[i:i+3]...)
	}
	return string(result)
}

// addParam appends a query parameter to a URL string.
func addParam(base, key, value string) string {
	if value == "" {
		return base
	}
	u, err := url.Parse(base)
	if err != nil {
		return base
	}
	q := u.Query()
	q.Set(key, value)
	u.RawQuery = q.Encode()
	return u.String()
}

// deleteParam removes a query parameter from a URL string.
func deleteParam(base, key string) string {
	u, err := url.Parse(base)
	if err != nil {
		return base
	}
	q := u.Query()
	q.Del(key)
	u.RawQuery = q.Encode()
	return u.String()
}

// formatMessageDate formats a time for the message list.
func formatMessageDate(t time.Time) string {
	now := time.Now()
	if t.Year() == now.Year() {
		return t.Format("Jan 02 15:04")
	}
	return t.Format("Jan 02, 2006")
}

// formatSyncTime formats a sync timestamp as a relative time string (e.g., "3h ago", "2d ago").
func formatSyncTime(t *time.Time) string {
	if t == nil {
		return "never"
	}
	d := time.Since(*t)
	switch {
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		m := int(d.Minutes())
		if m == 1 {
			return "1m ago"
		}
		return fmt.Sprintf("%dm ago", m)
	case d < 24*time.Hour:
		h := int(d.Hours())
		if h == 1 {
			return "1h ago"
		}
		return fmt.Sprintf("%dh ago", h)
	case d < 30*24*time.Hour:
		days := int(d.Hours() / 24)
		if days == 1 {
			return "1d ago"
		}
		return fmt.Sprintf("%dd ago", days)
	default:
		return t.Format("Jan 02, 2006")
	}
}

// Regexes for HTML-to-text conversion.
var (
	// styleRe and scriptRe strip <style>...</style> and <script>...</script> blocks
	// (including their content) before tag stripping to avoid rendering CSS/JS as text.
	// Go's regexp (RE2) doesn't support backreferences, so we use separate patterns.
	styleRe  = regexp.MustCompile(`(?is)<style[^>]*>.*?</style>`)
	scriptRe = regexp.MustCompile(`(?is)<script[^>]*>.*?</script>`)
	// htmlTagRe matches HTML tags for stripping.
	htmlTagRe = regexp.MustCompile(`<[^>]*>`)
)

// htmlToPlainText strips style/script blocks and all HTML tags, returning plain text.
// Used to extract readable content from HTML email bodies.
func htmlToPlainText(s string) string {
	// Remove style/script blocks first (their content is not displayable text)
	text := styleRe.ReplaceAllString(s, "")
	text = scriptRe.ReplaceAllString(text, "")
	text = htmlTagRe.ReplaceAllString(text, "")
	return html.UnescapeString(text)
}
