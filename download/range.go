package download

import (
	"slices"
	"strconv"
	"strings"

	"github.com/wzshiming/xet/shard"
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

// trimSegment cuts seg, the segment start bytes into its file, to the chunks holding bytes of rng and returns it
// with the number of bytes cut from its front. seg stays whole unless sh has a CAS block of its xorb agreeing with it.
func trimSegment(sh *shard.Shard, seg shard.FileDataSequenceEntry, start int64, rng ByteRange) (shard.FileDataSequenceEntry, int64) {
	end := start + int64(seg.UnpackedSegBytes)
	if start >= rng.Start && end-1 <= rng.End {
		return seg, 0
	}
	k := slices.IndexFunc(sh.CASInfos, func(b shard.CASBlock) bool { return b.CASHash == seg.CASHash })
	if k < 0 || uint64(seg.ChunkIndexEnd) > uint64(len(sh.CASInfos[k].Chunks)) {
		return seg, 0
	}
	chunks := sh.CASInfos[k].Chunks
	// Chunks [first, last) hold bytes of rng; from and to are their bounds in the file.
	first, last := seg.ChunkIndexStart, seg.ChunkIndexStart
	from, to, at := start, start, start
	for c := seg.ChunkIndexStart; c < seg.ChunkIndexEnd; c++ {
		next := at + int64(chunks[c].UnpackedSegBytes)
		if next <= rng.Start {
			first, from = c+1, next
		}
		if at <= rng.End {
			last, to = c+1, next
		}
		at = next
	}
	if at != end || first >= last {
		return seg, 0
	}
	seg.ChunkIndexStart, seg.ChunkIndexEnd, seg.UnpackedSegBytes = first, last, uint32(to-from)
	return seg, from - start
}
