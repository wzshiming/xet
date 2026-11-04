package download

import "testing"

func TestParseByteRange(t *testing.T) {
	const size = 100
	whole := ByteRange{0, size - 1}
	for _, c := range []struct {
		name   string
		header string
		size   int64
		want   ByteRange
	}{
		// Anything but one valid byte range selects the whole file.
		{"absent", "", size, whole},
		{"no bytes= prefix", "10-20", size, whole},
		{"other unit", "items=10-20", size, whole},
		{"no dash", "bytes=10", size, whole},
		{"dash only", "bytes=-", size, whole},
		{"malformed first", "bytes=x-20", size, whole},
		{"malformed last", "bytes=10-y", size, whole},
		{"malformed suffix", "bytes=-y", size, whole},
		{"signed first", "bytes=+10-20", size, whole},
		{"first past 63 bits", "bytes=9223372036854775808-", size, whole},
		{"last past 63 bits", "bytes=10-9223372036854775808", size, whole},
		{"suffix past 63 bits", "bytes=-9223372036854775808", size, whole},
		{"suffix at the 64-bit maximum", "bytes=-18446744073709551615", size, whole},
		{"past 64 bits", "bytes=99999999999999999999-", size, whole},
		{"several ranges", "bytes=10-20,30-40", size, whole},
		{"last before first", "bytes=20-10", size, whole},
		// One range, its end clamped to the last byte.
		{"inside", "bytes=10-20", size, ByteRange{10, 20}},
		{"one byte", "bytes=10-10", size, ByteRange{10, 10}},
		{"clamped", "bytes=10-200", size, ByteRange{10, 99}},
		{"open", "bytes=10-", size, ByteRange{10, 99}},
		{"open at the last byte", "bytes=99-", size, ByteRange{99, 99}},
		{"suffix", "bytes=-30", size, ByteRange{70, 99}},
		{"suffix of the whole file", "bytes=-100", size, ByteRange{0, 99}},
		{"suffix larger than the file", "bytes=-200", size, ByteRange{0, 99}},
		// No bytes: Start > End.
		{"open at the end", "bytes=100-", size, ByteRange{100, 99}},
		{"first past the end", "bytes=150-200", size, ByteRange{150, 99}},
		{"first at the 63-bit maximum", "bytes=9223372036854775807-", size, ByteRange{9223372036854775807, 99}},
		{"suffix zero", "bytes=-0", size, ByteRange{100, 99}},
		{"empty file", "", 0, ByteRange{0, -1}},
		{"empty file, open", "bytes=0-", 0, ByteRange{0, -1}},
		{"empty file, explicit", "bytes=0-0", 0, ByteRange{0, -1}},
		{"empty file, suffix", "bytes=-1", 0, ByteRange{0, -1}},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := ParseByteRange(c.header, c.size); got != c.want {
				t.Errorf("ParseByteRange(%q, %d) = %+v, want %+v", c.header, c.size, got, c.want)
			}
		})
	}
}
