package download

import (
	"strconv"
	"strings"
)

// ParseByteRange returns the bytes [Start, End], End inclusive, that a Range header requests of a file of size bytes.
// A single range bytes=a-b, bytes=a- or bytes=-n is clamped to the file; Start > End means no bytes, as for a range
// from or past the end of the file, bytes=-0 or an empty file. An absent header, any other unit, malformed or
// overflowing numbers, several ranges or b < a request the whole file, [0, size-1].
func ParseByteRange(header string, size int64) ByteRange {
	whole := ByteRange{Start: 0, End: size - 1}
	spec, isBytes := strings.CutPrefix(header, "bytes=")
	first, last, hasDash := strings.Cut(spec, "-")
	if !isBytes || !hasDash {
		return whole
	}
	if first == "" {
		suffix, err := strconv.ParseUint(last, 10, 63)
		if err != nil {
			return whole
		}
		return ByteRange{Start: max(size-int64(suffix), 0), End: size - 1}
	}
	firstPos, err := strconv.ParseUint(first, 10, 63)
	if err != nil {
		return whole
	}
	rng := ByteRange{Start: int64(firstPos), End: size - 1}
	if last != "" {
		lastPos, err := strconv.ParseUint(last, 10, 63)
		if err != nil || int64(lastPos) < rng.Start {
			return whole
		}
		rng.End = min(int64(lastPos), rng.End)
	}
	return rng
}
