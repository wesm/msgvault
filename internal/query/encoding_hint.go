package query

import (
	"fmt"
	"strings"
)

// encodingErrorSubstring is the error text emitted by DuckDB when it encounters
// invalid UTF-8 in a Parquet file.
const encodingErrorSubstring = "Invalid string encoding found in Parquet file"

// IsEncodingError reports whether err contains the DuckDB invalid-string-encoding
// error that can be resolved by running `msgvault repair-encoding`.
func IsEncodingError(err error) bool {
	return err != nil && strings.Contains(err.Error(), encodingErrorSubstring)
}

// HintRepairEncoding wraps err with a user-facing hint suggesting
// `msgvault repair-encoding` when the error is an encoding error.
// If err is nil or unrelated, it is returned unchanged.
func HintRepairEncoding(err error) error {
	if !IsEncodingError(err) {
		return err
	}
	return fmt.Errorf("%w\nHint: try running 'msgvault repair-encoding' to fix encoding issues", err)
}
