package gearhash

import (
	"bytes"
	"io"
	"testing"
)

// reassemble concatenates chunks into a single byte slice for comparison.
func reassemble(chunks [][]byte) []byte {
	var out []byte
	for _, c := range chunks {
		out = append(out, c...)
	}
	return out
}

// chunkViaReader calls ChunkData (the io.Reader API) and returns the chunks.
func chunkViaReader(data []byte) ([][]byte, error) {
	var chunks [][]byte
	err := ChunkData(bytes.NewReader(data), func(_ int64, chunk []byte) error {
		cp := make([]byte, len(chunk))
		copy(cp, chunk)
		chunks = append(chunks, cp)
		return nil
	})
	return chunks, err
}

// chunkViaNextBlock calls Chunker.NextBlock with all data at once.
func chunkViaNextBlock(data []byte) [][]byte {
	return NewChunker().NextBlock(data, true)
}

// chunkViaStreaming feeds data to the chunker in small pieces.
func chunkViaStreaming(data []byte, batchSize int) [][]byte {
	chunker := NewChunker()
	var result [][]byte
	for i := 0; i < len(data); i += batchSize {
		end := i + batchSize
		isFinal := end >= len(data)
		if end > len(data) {
			end = len(data)
		}
		for _, chunk := range chunker.NextBlock(data[i:end], isFinal) {
			cp := make([]byte, len(chunk))
			copy(cp, chunk)
			result = append(result, cp)
		}
	}
	return result
}

func TestEmptyInput(t *testing.T) {
	chunker := NewChunker()
	chunks := chunker.NextBlock(nil, true)
	if len(chunks) != 0 {
		t.Fatalf("expected 0 chunks for empty input, got %d", len(chunks))
	}

	chunks, err := chunkViaReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 0 {
		t.Fatalf("ChunkData: expected 0 chunks for empty input, got %d", len(chunks))
	}
}

func TestSmallInput(t *testing.T) {
	// Data smaller than MinChunkSize must be returned as a single chunk.
	data := bytes.Repeat([]byte("x"), MinChunkSize-1)

	chunker := NewChunker()
	chunks := chunker.NextBlock(data, true)
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}
	if !bytes.Equal(chunks[0], data) {
		t.Fatal("chunk content mismatch")
	}
}

func TestMaxChunkEnforced(t *testing.T) {
	// Every chunk must be at most MaxChunkSize bytes.
	data := makePseudorandomData(MaxChunkSize * 5)

	chunker := NewChunker()
	chunks := chunker.NextBlock(data, true)
	if len(chunks) == 0 {
		t.Fatal("expected at least one chunk")
	}
	for i, ch := range chunks {
		if len(ch) > MaxChunkSize {
			t.Errorf("chunk %d: len %d > MaxChunkSize %d", i, len(ch), MaxChunkSize)
		}
	}
	if got := reassemble(chunks); !bytes.Equal(got, data) {
		t.Fatal("reassembly mismatch")
	}
}

func TestMinChunkEnforced(t *testing.T) {
	// Every chunk must be at least MinChunkSize bytes (except possibly the last).
	data := bytes.Repeat([]byte("abcdefgh"), (MaxChunkSize*10)/8)

	chunker := NewChunker()
	chunks := chunker.NextBlock(data, true)
	for i, ch := range chunks[:len(chunks)-1] {
		if len(ch) < MinChunkSize {
			t.Errorf("chunk %d: len %d < MinChunkSize %d", i, len(ch), MinChunkSize)
		}
	}
}

func TestReassembly(t *testing.T) {
	// The concatenation of all chunks must equal the original data.
	sizes := []int{0, 1, MinChunkSize - 1, MinChunkSize, MinChunkSize + 1, MaxChunkSize, MaxChunkSize*3 + 12345}
	for _, sz := range sizes {
		data := makePseudorandomData(sz)

		chunks := chunkViaNextBlock(data)
		if got := reassemble(chunks); !bytes.Equal(got, data) {
			t.Errorf("size %d: reassembly mismatch", sz)
		}
	}
}

func TestChunkDataMatchesNextBlock(t *testing.T) {
	// ChunkData (io.Reader API) must produce the same boundaries as NextBlock.
	sizes := []int{0, 100, MinChunkSize - 1, MinChunkSize, MaxChunkSize - 1, MaxChunkSize * 3}
	for _, sz := range sizes {
		data := makePseudorandomData(sz)

		readerChunks, err := chunkViaReader(data)
		if err != nil {
			t.Fatalf("size %d: ChunkData error: %v", sz, err)
		}
		blockChunks := chunkViaNextBlock(data)

		if len(readerChunks) != len(blockChunks) {
			t.Errorf("size %d: chunk count mismatch: reader=%d block=%d", sz, len(readerChunks), len(blockChunks))
			continue
		}
		for i := range readerChunks {
			if !bytes.Equal(readerChunks[i], blockChunks[i]) {
				t.Errorf("size %d: chunk %d content mismatch", sz, i)
			}
		}
	}
}

func TestStreamingMatchesBatch(t *testing.T) {
	// Feeding data in small batches must produce identical chunks to a single call.
	data := makePseudorandomData(MaxChunkSize*5 + 12345)

	batchSizes := []int{1, 64, 512, 4096, MinChunkSize, MaxChunkSize}
	reference := chunkViaNextBlock(data)

	for _, bs := range batchSizes {
		chunks := chunkViaStreaming(data, bs)
		if len(chunks) != len(reference) {
			t.Errorf("batchSize %d: chunk count mismatch: got %d, want %d", bs, len(chunks), len(reference))
			continue
		}
		for i := range chunks {
			if !bytes.Equal(chunks[i], reference[i]) {
				t.Errorf("batchSize %d: chunk %d content mismatch (len got=%d want=%d)", bs, i, len(chunks[i]), len(reference[i]))
			}
		}
	}
}

func TestFinish(t *testing.T) {
	data := makePseudorandomData(MinChunkSize / 2)

	chunker := NewChunker()
	// Feed data without finalising.
	chunk, consumed := chunker.Next(data, false)
	if chunk != nil {
		t.Fatal("expected no chunk before Finish")
	}
	if consumed != len(data) {
		t.Fatalf("expected %d bytes consumed, got %d", len(data), consumed)
	}
	// Finish should flush the buffer.
	last := chunker.Finish()
	if !bytes.Equal(last, data) {
		t.Fatal("Finish returned wrong data")
	}
	// Finish again on empty state should return nil.
	if extra := chunker.Finish(); extra != nil {
		t.Fatalf("second Finish should return nil, got %d bytes", len(extra))
	}
}

func TestChunkDataOffsets(t *testing.T) {
	// ChunkData must report monotonically increasing offsets that sum to the
	// total input length.
	data := makePseudorandomData(MaxChunkSize*2 + 12345)
	var offsets []int64
	var sizes []int

	err := ChunkData(bytes.NewReader(data), func(offset int64, chunk []byte) error {
		cp := make([]byte, len(chunk))
		copy(cp, chunk)
		offsets = append(offsets, offset)
		sizes = append(sizes, len(chunk))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(offsets) == 0 {
		t.Fatal("no chunks produced")
	}
	if offsets[0] != 0 {
		t.Fatalf("first offset should be 0, got %d", offsets[0])
	}
	for i := 1; i < len(offsets); i++ {
		if offsets[i] != offsets[i-1]+int64(sizes[i-1]) {
			t.Errorf("offset %d: expected %d, got %d", i, offsets[i-1]+int64(sizes[i-1]), offsets[i])
		}
	}
	total := offsets[len(offsets)-1] + int64(sizes[len(sizes)-1])
	if total != int64(len(data)) {
		t.Errorf("total offset+size = %d, want %d", total, len(data))
	}
}

func TestChunkDataReaderError(t *testing.T) {
	errReader := &errorReader{err: io.ErrUnexpectedEOF}
	err := ChunkData(errReader, func(_ int64, _ []byte) error { return nil })
	if err == nil {
		t.Fatal("expected error from ChunkData, got nil")
	}
}

// errorReader is an io.Reader that always returns an error.
type errorReader struct{ err error }

func (e *errorReader) Read(_ []byte) (int, error) { return 0, e.err }

// makePseudorandomData produces a deterministic byte slice of length n using a
// simple xorshift64 generator seeded with a fixed constant. Determinism ensures
// that test failures are reproducible without an external seed flag.
func makePseudorandomData(n int) []byte {
	out := make([]byte, n)
	state := uint64(0x123456789ABCDEF0)
	for i := range out {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		out[i] = byte(state)
	}
	return out
}
