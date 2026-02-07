// Package mbox implements a streaming reader for MBOX files.
//
// We support typical mboxo/mboxrd exports where each message is preceded by a
// Unix "From " separator line. Body lines that begin with "From " (or with one
// or more leading '>' followed by "From ") are commonly escaped in the file by
// prefixing an additional '>' (mboxrd). When reading, we unescape by removing a
// single leading '>' from any line that matches ^>+From .
package mbox

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strings"
)

// Message is a single message from an MBOX file.
type Message struct {
	// FromLine is the separator line (without trailing newline).
	FromLine string

	// Raw is the RFC 5322 message bytes (headers + body). The separator line
	// is not included. Lines may use either LF or CRLF endings depending on
	// the source file.
	Raw []byte
}

type offsetReader struct {
	r io.Reader
	n int64
}

func (o *offsetReader) Read(p []byte) (int, error) {
	n, err := o.r.Read(p)
	o.n += int64(n)
	return n, err
}

// Reader reads messages from an MBOX stream.
// It is safe for large files: it reads one message at a time.
type Reader struct {
	or *offsetReader
	br *bufio.Reader

	// nextFromLine is the already-read separator line for the next message, if any.
	nextFromLine   string
	nextFromOffset int64
	hasNextFrom    bool
	eof            bool
}

// NewReader creates a new MBOX reader.
func NewReader(r io.Reader) *Reader {
	or := &offsetReader{r: r}
	return &Reader{
		or: or,
		br: bufio.NewReader(or),
	}
}

// Offset reports the current logical read offset (bytes consumed) within the
// underlying stream, accounting for buffered data.
func (r *Reader) Offset() int64 {
	return r.or.n - int64(r.br.Buffered())
}

// NextFromOffset reports the stream offset of the next message's "From " line.
// Valid only after a successful Next() call (or 0 at end-of-file).
func (r *Reader) NextFromOffset() int64 {
	if r.hasNextFrom {
		return r.nextFromOffset
	}
	// If there is no buffered "From " line, the next message would start at the
	// current offset (either EOF or we haven't found the first separator yet).
	return r.Offset()
}

// Next returns the next message from the MBOX stream.
// Returns io.EOF when there are no more messages.
func (r *Reader) Next() (*Message, error) {
	if r.eof {
		return nil, io.EOF
	}

	// Find the separator line for the next message, if we don't already have it.
	if !r.hasNextFrom {
		for {
			lineStart := r.Offset()
			line, err := r.readLine()
			if err != nil {
				if err == io.EOF {
					r.eof = true
					return nil, io.EOF
				}
				return nil, err
			}
			if isFromSeparator(line) {
				r.nextFromLine = strings.TrimRight(line, "\r\n")
				r.nextFromOffset = lineStart
				r.hasNextFrom = true
				break
			}
		}
	}

	// Consume the next separator and start collecting this message.
	fromLine := r.nextFromLine
	r.hasNextFrom = false

	var raw bytes.Buffer

	for {
		lineStart := r.Offset()
		line, err := r.readLineBytes()
		if len(line) > 0 {
			if isFromSeparatorBytes(line) {
				// Found the next message separator; stash it for the next call.
				r.nextFromLine = strings.TrimRight(string(line), "\r\n")
				r.nextFromOffset = lineStart
				r.hasNextFrom = true
				break
			}

			raw.Write(unescapeFromBytes(line))
		}

		if err != nil {
			if err == io.EOF {
				r.eof = true
				break
			}
			return nil, err
		}
	}

	// Empty message bodies are unusual but possible; return them anyway.
	return &Message{
		FromLine: fromLine,
		Raw:      raw.Bytes(),
	}, nil
}

func (r *Reader) readLine() (string, error) {
	b, err := r.readLineBytes()
	return string(b), err
}

func (r *Reader) readLineBytes() ([]byte, error) {
	// ReadBytes returns the bytes including the delimiter. On EOF, it returns
	// the data read and io.EOF.
	b, err := r.br.ReadBytes('\n')
	if err == nil {
		return b, nil
	}
	// If we got some bytes, surface them with the error.
	if len(b) > 0 {
		return b, err
	}
	return nil, err
}

func isFromSeparator(line string) bool {
	return strings.HasPrefix(line, "From ")
}

func isFromSeparatorBytes(line []byte) bool {
	return bytes.HasPrefix(line, []byte("From "))
}

// unescapeFromBytes removes a single leading '>' from any line that matches
// ^>+From  (mboxrd unquoting). This also works for mboxo where only ">From "
// appears for originally "From " lines.
func unescapeFromBytes(line []byte) []byte {
	if len(line) == 0 || line[0] != '>' {
		return line
	}

	// Count leading '>' characters.
	i := 0
	for i < len(line) && line[i] == '>' {
		i++
	}
	// Check if the remainder begins with "From ".
	if i < len(line) && bytes.HasPrefix(line[i:], []byte("From ")) {
		return line[1:]
	}
	return line
}

// Validate scans the stream and returns an error if it doesn't look like an MBOX file.
// It reads up to maxBytes from the stream. This is a heuristic.
func Validate(r io.Reader, maxBytes int64) error {
	if maxBytes <= 0 {
		return fmt.Errorf("maxBytes must be > 0")
	}
	br := bufio.NewReader(io.LimitReader(r, maxBytes))
	for {
		line, err := br.ReadString('\n')
		if isFromSeparator(line) {
			return nil
		}
		if err != nil {
			if err == io.EOF {
				return fmt.Errorf("no \"From \" separators found (not an mbox file?)")
			}
			return err
		}
	}
}
