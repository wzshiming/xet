package encoding_test

import (
	"bytes"
	"fmt"
	"io"
	"slices"
	"testing"
	"testing/iotest"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/test/conformance/rustref"
)

// chunkingCase is one input for TestChunkingConformance; check, when set,
// validates the reference chunk sizes so the case cannot pass vacuously.
type chunkingCase struct {
	name  string
	data  []byte
	check func(t *testing.T, sizes []uint64)
}

// TestChunkingConformance holds the native chunker to the reference on the
// edge cases xet-core's own chunker tests cover, adapted to the fixed chunk
// sizes: empty and sub-minimum inputs, mask hits just below, at, and above
// MinChunkSize and MaxChunkSize, the forced cut at MaxChunkSize, and the
// vectors xet-core pins, which the reference output is checked against first.
// Second chunks obey the minimum-size rule after mask cuts and forced
// MaxChunkSize cuts.
// The reference chunks each input whole and fed in the block sizes xet-core's
// streaming test uses; the native side gets it through both entry points and
// several read shapes; every result must agree, as must the file hash each
// side derives from the whole input.
func TestChunkingConformance(t *testing.T) {
	const tail = maskHitTail

	random1MB := splitMixData(1000000, 0)
	requireBytes(t, random1MB, random1MBSamples)

	var eightMax []uint64
	for i := 1; i <= 8; i++ {
		eightMax = append(eightMax, uint64(i*xet.MaxChunkSize))
	}

	tests := []chunkingCase{
		{name: "empty", check: wantSizes()},
		{name: "1 byte", data: splitMixData(1, 0), check: wantSizes(1)},
		{name: "one below min bytes", data: splitMixData(xet.MinChunkSize-1, 0), check: wantSizes(xet.MinChunkSize - 1)},
		{name: "min bytes", data: splitMixData(xet.MinChunkSize, 0), check: wantSizes(xet.MinChunkSize)},
		{name: "xet-core 1MB random", data: random1MB, check: wantBoundaries(random1MBBoundaries)},
		{name: "xet-core 1MB const", data: bytes.Repeat([]byte{59}, 1000000), check: wantBoundaries(const1MBBoundaries)},
		// xet-core chunks this input in test_chunk_boundaries without pinning
		// it; the offsets are the pinned reference binary's output.
		{name: "256000 random", data: splitMixData(256000, 1), check: wantBoundaries([]uint64{25125, 104497, 235569, 256000})},
		{name: "mask hit one below min", data: maskHitData(xet.MinChunkSize - 1), check: wantSizes(xet.MinChunkSize - 1 + tail)},
		{name: "mask hit at min", data: maskHitData(xet.MinChunkSize), check: wantSizes(xet.MinChunkSize, tail)},
		{name: "mask hit one above min", data: maskHitData(xet.MinChunkSize + 1), check: wantSizes(xet.MinChunkSize+1, tail)},
		{name: "mask hit one below max", data: maskHitData(xet.MaxChunkSize - 1), check: wantSizes(xet.MaxChunkSize-1, tail)},
		{name: "mask hit at max", data: maskHitData(xet.MaxChunkSize), check: wantSizes(xet.MaxChunkSize, tail)},
		{name: "mask hit one above max", data: maskHitData(xet.MaxChunkSize + 1), check: wantSizes(xet.MaxChunkSize, tail+1)},
		{name: "second mask hit one below min", data: maskHitData(2*xet.MinChunkSize, 3*xet.MinChunkSize-1), check: wantSizes(2*xet.MinChunkSize, xet.MinChunkSize-1+tail)},
		{name: "second mask hit at min", data: maskHitData(2*xet.MinChunkSize, 3*xet.MinChunkSize), check: wantSizes(2*xet.MinChunkSize, xet.MinChunkSize, tail)},
		{name: "mask hit one below min after forced max cut", data: maskHitData(xet.MaxChunkSize + xet.MinChunkSize - 1), check: wantSizes(xet.MaxChunkSize, xet.MinChunkSize-1+tail)},
		{name: "mask hit at min after forced max cut", data: maskHitData(xet.MaxChunkSize + xet.MinChunkSize), check: wantSizes(xet.MaxChunkSize, xet.MinChunkSize, tail)},
		{name: "const exactly max", data: bytes.Repeat([]byte{59}, xet.MaxChunkSize), check: wantSizes(xet.MaxChunkSize)},
		{name: "const one over max", data: bytes.Repeat([]byte{59}, xet.MaxChunkSize+1), check: wantSizes(xet.MaxChunkSize, 1)},
		{name: "8 max chunks of zeros", data: make([]byte, 8*xet.MaxChunkSize), check: wantBoundaries(eightMax)},
	}
	for padding, boundaries := range triggeringBoundaries {
		data := triggeringData(triggeringSize, padding)
		requireBytes(t, data, map[int]byte{11111: triggeringSamples[padding]})
		tests = append(tests, chunkingCase{
			name:  fmt.Sprintf("xet-core triggering data padding %d", padding),
			data:  data,
			check: wantBoundaries(boundaries[:]),
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			refChunks := getReferenceChunks(t, tt.data)
			if tt.check != nil {
				tt.check(t, chunkSizes(refChunks))
			}
			for _, blockSize := range referenceBlockSizes {
				t.Run(fmt.Sprintf("reference/%d-byte blocks", blockSize), func(t *testing.T) {
					blocked := getReferenceChunksInBlocks(t, tt.data, blockSize)
					if !slices.Equal(blocked, refChunks) {
						t.Fatalf("reference chunk sizes fed in %d-byte blocks = %v, whole = %v", blockSize, chunkSizes(blocked), chunkSizes(refChunks))
					}
				})
			}
			for readerName, newReader := range nativeReaders {
				for apiName, chunk := range nativeChunkers {
					t.Run(readerName+"/"+apiName, func(t *testing.T) {
						compareChunks(t, chunk(t, newReader(tt.data)), refChunks)
					})
				}
			}
			t.Run("file hash", func(t *testing.T) {
				native := getNativeFileHash(t, getNativeChunks(t, tt.data))
				if reference := getReferenceFileHash(t, tt.data); native != reference {
					t.Fatalf("file hash mismatch: native=%s reference=%s", native, reference)
				}
			})
		})
	}
}

// referenceBlockSizes are the feed sizes xet-core's test_chunk_boundaries
// streams its chunker with.
var referenceBlockSizes = []int{1, 37, 255}

// getReferenceChunksInBlocks splits data with the reference chunker fed
// blockSize bytes at a time.
func getReferenceChunksInBlocks(t *testing.T, data []byte, blockSize int) []chunkEntry {
	t.Helper()
	raw, err := rustref.ChunkDataInBlocks(data, blockSize)
	if err != nil {
		t.Fatalf("reference ChunkDataInBlocks(%d): %v", blockSize, err)
	}
	chunks := make([]chunkEntry, len(raw))
	for i, c := range raw {
		chunks[i] = chunkEntry{hash: c.Hash, size: c.Size}
	}
	return chunks
}

// nativeReaders shape how the native chunker receives an input, from whole
// buffers down to single bytes; 63-byte reads split every 64-byte gearhash
// window across two scans, and the last variant delivers data together with
// EOF.
var nativeReaders = map[string]func([]byte) io.Reader{
	"whole":         func(b []byte) io.Reader { return bytes.NewReader(b) },
	"1 byte":        func(b []byte) io.Reader { return iotest.OneByteReader(bytes.NewReader(b)) },
	"63 bytes":      func(b []byte) io.Reader { return shortReader{r: bytes.NewReader(b), n: 63} },
	"1 to 15 bytes": func(b []byte) io.Reader { return &cyclingReader{r: bytes.NewReader(b)} },
	"half":          func(b []byte) io.Reader { return iotest.HalfReader(bytes.NewReader(b)) },
	"data with EOF": func(b []byte) io.Reader { return iotest.DataErrReader(bytes.NewReader(b)) },
}

// shortReader hands out at most n bytes per Read.
type shortReader struct {
	r io.Reader
	n int
}

func (s shortReader) Read(p []byte) (int, error) {
	if len(p) > s.n {
		p = p[:s.n]
	}
	return s.r.Read(p)
}

// cyclingReader varies the bytes per Read through 1..15, the way xet-core's
// partial-consumption test feeds its chunker.
type cyclingReader struct {
	r io.Reader
	i int
}

func (c *cyclingReader) Read(p []byte) (int, error) {
	c.i = c.i%15 + 1
	if len(p) > c.i {
		p = p[:c.i]
	}
	return c.r.Read(p)
}

// nativeChunker chunks r through one public entry point, failing the test if
// a chunk's reported offset is not the sum of the sizes before it.
type nativeChunker func(t *testing.T, r io.Reader) []chunkEntry

var nativeChunkers = map[string]nativeChunker{"ChunkData": chunkViaChunkData, "Chunker": chunkViaChunker}

func chunkViaChunkData(t *testing.T, r io.Reader) []chunkEntry {
	t.Helper()
	var chunks []chunkEntry
	var next int64
	err := xet.ChunkData(r, func(offset int64, chunk []byte) error {
		if offset != next {
			t.Fatalf("chunk[%d] offset = %d, want %d", len(chunks), offset, next)
		}
		next += int64(len(chunk))
		chunks = append(chunks, chunkEntry{hash: xet.ComputeChunkHash(chunk).String(), size: uint64(len(chunk))})
		return nil
	})
	if err != nil {
		t.Fatalf("native ChunkData: %v", err)
	}
	return chunks
}

func chunkViaChunker(t *testing.T, r io.Reader) []chunkEntry {
	t.Helper()
	var chunks []chunkEntry
	var next int64
	c := xet.NewChunker(r)
	for {
		offset, chunk, err := c.Chunk()
		if err == io.EOF {
			return chunks
		}
		if err != nil {
			t.Fatalf("native Chunker.Chunk: %v", err)
		}
		if offset != next {
			t.Fatalf("chunk[%d] offset = %d, want %d", len(chunks), offset, next)
		}
		next += int64(len(chunk))
		chunks = append(chunks, chunkEntry{hash: xet.ComputeChunkHash(chunk).String(), size: uint64(len(chunk))})
	}
}

// requireBytes checks sentinel bytes against the samples xet-core's tests
// record for the same input.
func requireBytes(t *testing.T, data []byte, want map[int]byte) {
	t.Helper()
	for offset, b := range want {
		if data[offset] != b {
			t.Fatalf("data[%d] = %d, want %d: generator diverges from xet-core", offset, data[offset], b)
		}
	}
}

func wantSizes(sizes ...uint64) func(*testing.T, []uint64) {
	return func(t *testing.T, got []uint64) {
		t.Helper()
		if !slices.Equal(got, sizes) {
			t.Fatalf("reference chunk sizes = %v, want %v", got, sizes)
		}
	}
}

func wantBoundaries(boundaries []uint64) func(*testing.T, []uint64) {
	return func(t *testing.T, sizes []uint64) {
		t.Helper()
		got := make([]uint64, len(sizes))
		var end uint64
		for i, size := range sizes {
			end += size
			got[i] = end
		}
		if !slices.Equal(got, boundaries) {
			t.Fatalf("reference chunk boundaries = %v, want xet-core's %v", got, boundaries)
		}
	}
}
