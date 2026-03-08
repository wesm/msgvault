package templates

import (
	"fmt"
	"net/url"
	"strings"
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
	sep := "&"
	if !strings.Contains(base, "?") {
		sep = "?"
	}
	return base + sep + url.QueryEscape(key) + "=" + url.QueryEscape(value)
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
