package merklehash

import "testing"

func TestAggregatedEmptyInput(t *testing.T) {
	h := XorbHash(nil)
	if !h.IsZero() {
		t.Errorf("XorbHash(nil) should be zero, got %s", h)
	}
	h = FileHash(nil)
	// FileHash of empty input: aggregatedNodeHash returns zero, then HMAC with zero key.
	// The HMAC of a zero hash with a zero key is deterministic but non-trivially zero.
	// Just verify it doesn't panic.
}

func TestAggregatedSingleElement(t *testing.T) {
	chunk := HashWithSize{
		Hash: ComputeDataHash([]byte("block0")),
		Size: 100,
	}
	h := XorbHash([]HashWithSize{chunk})
	if h.IsZero() {
		t.Error("XorbHash of single element should not be zero")
	}
}

func TestXorbHashDeterminism(t *testing.T) {
	chunks := makeTestChunks(5)
	h1 := XorbHash(chunks)
	h2 := XorbHash(chunks)
	if h1 != h2 {
		t.Error("XorbHash should be deterministic")
	}
}

func TestFileHashDeterminism(t *testing.T) {
	chunks := makeTestChunks(5)
	h1 := FileHash(chunks)
	h2 := FileHash(chunks)
	if h1 != h2 {
		t.Error("FileHash should be deterministic")
	}
}

func TestXorbAndFileHashDiffer(t *testing.T) {
	chunks := makeTestChunks(3)
	xh := XorbHash(chunks)
	fh := FileHash(chunks)
	if xh == fh {
		t.Error("XorbHash and FileHash should produce different results for same input")
	}
}

func TestFileHashWithSaltDiffers(t *testing.T) {
	chunks := makeTestChunks(3)
	var salt1, salt2 [32]byte
	salt2[0] = 1
	h1 := FileHashWithSalt(chunks, salt1)
	h2 := FileHashWithSalt(chunks, salt2)
	if h1 == h2 {
		t.Error("different salts should produce different FileHash results")
	}
}

func TestNextMergeCut(t *testing.T) {
	// 0 or 1 elements
	if nextMergeCut(nil) != 0 {
		t.Error("expected 0 for nil")
	}
	chunks := makeTestChunks(1)
	if nextMergeCut(chunks) != 1 {
		t.Error("expected 1 for single element")
	}
	// 2 elements
	chunks = makeTestChunks(2)
	if nextMergeCut(chunks) != 2 {
		t.Error("expected 2 for two elements")
	}
}

func TestAggregatedManyChunks(t *testing.T) {
	chunks := makeTestChunks(20)
	h := XorbHash(chunks)
	if h.IsZero() {
		t.Error("XorbHash of many chunks should not be zero")
	}
}

func TestAggregatedDoesNotMutateInput(t *testing.T) {
	chunks := makeTestChunks(5)
	orig := make([]HashWithSize, len(chunks))
	copy(orig, chunks)
	XorbHash(chunks)
	for i := range chunks {
		if chunks[i] != orig[i] {
			t.Errorf("XorbHash mutated input at index %d", i)
		}
	}
}

func makeTestChunks(n int) []HashWithSize {
	chunks := make([]HashWithSize, n)
	for i := 0; i < n; i++ {
		chunks[i] = HashWithSize{
			Hash: ComputeDataHash([]byte{byte(i)}),
			Size: uint64((i + 1) * 1024),
		}
	}
	return chunks
}
