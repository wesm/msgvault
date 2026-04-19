//go:build !sqlite_vec

// Package sqlitevec is a stub when the sqlite_vec build tag is not set.
// The real implementation in ext.go wires up the sqlite-vec extension.
package sqlitevec

import "errors"

// ErrNotBuilt is returned when sqlite-vec features are used in a build
// that did not set the `sqlite_vec` build tag.
var ErrNotBuilt = errors.New(
	"sqlite-vec support not compiled in; rebuild with `go build -tags \"fts5 sqlite_vec\"`")

// RegisterExtension reports that sqlite-vec is unavailable in this build.
func RegisterExtension() error { return ErrNotBuilt }

// DriverName returns the default sqlite3 driver name since sqlite-vec
// is not compiled in.
func DriverName() string { return "sqlite3" }

// Available reports whether this build includes sqlite-vec support.
func Available() bool { return false }
