// Package ptr provides generic pointer helpers for tests.
package ptr

import "time"

// Bool returns a pointer to the given bool value.
func Bool(v bool) *bool { return &v }

// Int64 returns a pointer to the given int64 value.
func Int64(v int64) *int64 { return &v }

// String returns a pointer to the given string value.
func String(v string) *string { return &v }

// Time returns a pointer to the given time.Time value.
func Time(v time.Time) *time.Time { return &v }

// Date returns a UTC time for the given year, month, and day.
func Date(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}
