package encoding_test

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/test/conformance/rustref"
	"github.com/wzshiming/xet/test/conformance/utils"
	"github.com/wzshiming/xet/xorb"
)

// chunkEntry holds the hash and size for a single chunk.
type chunkEntry struct {
	hash string
	size uint64
}

func TestConformance(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "Empty file",
			data: []byte{},
		},
		{
			name: "Hello World",
			data: []byte("Hello World!"),
		},
		{
			name: "10MB",
			data: utils.MakeRandData(10 * 1024 * 1024),
		},
		{
			name: "10MB repeating",
			data: utils.MakeRepeatData(10 * 1024 * 1024),
		},
		{
			name: "100MB",
			data: utils.MakeRandData(100 * 1024 * 1024),
		},
		{
			name: "100MB repeating",
			data: utils.MakeRepeatData(100 * 1024 * 1024),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nativeChunks := getNativeChunks(t, tt.data)
			refChunks := getReferenceChunks(t, tt.data)

			t.Run("chunking", func(t *testing.T) {
				if len(nativeChunks) != len(refChunks) {
					t.Fatalf("chunk count mismatch: native=%d reference=%d",
						len(nativeChunks), len(refChunks))
				}
				for i := range nativeChunks {
					if nativeChunks[i].hash != refChunks[i].hash {
						t.Errorf("chunk[%d] hash mismatch: native=%s reference=%s",
							i, nativeChunks[i].hash, refChunks[i].hash)
					}
					if nativeChunks[i].size != refChunks[i].size {
						t.Errorf("chunk[%d] size mismatch: native=%d reference=%d",
							i, nativeChunks[i].size, refChunks[i].size)
					}
				}
			})

			t.Run("chunk_hash", func(t *testing.T) {
				reference, err := rustref.HashChunk(tt.data)
				if err != nil {
					t.Fatal(err)
				}
				if native := xet.ComputeChunkHash(tt.data).String(); native != reference {
					t.Errorf("chunk hash mismatch: native=%s reference=%s", native, reference)
				}
			})

			t.Run("aggregate_hashes", func(t *testing.T) {
				compareAggregateHashes(t, nativeChunks)
			})

			t.Run("file", func(t *testing.T) {
				nativeHash := getNativeFileHash(t, nativeChunks)
				refHash := getReferenceFileHash(t, tt.data)
				if nativeHash != refHash {
					t.Errorf("file hash mismatch: native=%s reference=%s",
						nativeHash, refHash)
				}
			})
		})
	}
}

// TestHMACConformance verifies ChunkHash.HMAC matches xet-core's
// MerkleHash::hmac, which keys the chunk hashes in global-dedup shards.
func TestHMACConformance(t *testing.T) {
	var sequential, ones [32]byte
	for i := range sequential {
		sequential[i] = byte(i)
		ones[i] = 0xFF
	}

	hashes := []xet.ChunkHash{
		{}, // zero hash
		xet.ChunkHash(sequential),
		xet.ChunkHash(ones),
		xet.ComputeChunkHash([]byte("Hello World!")),
		xet.ComputeChunkHash(utils.MakeRandData(4096)),
	}
	keys := [][32]byte{
		{}, // zero key: the primitive still computes; only the shard layer treats it as unkeyed
		sequential,
		ones,
		[32]byte(xet.ComputeChunkHash([]byte("per-shard hmac key"))),
	}

	var cases []rustref.HMACCase
	var native []string
	for _, chunkHash := range hashes {
		for _, key := range keys {
			cases = append(cases, rustref.HMACCase{
				Hash: chunkHash.String(),
				Key:  xet.ChunkHash(key).String(),
			})
			native = append(native, chunkHash.HMAC(key).String())
		}
	}

	reference, err := rustref.HashHMAC(cases)
	if err != nil {
		t.Fatal(err)
	}
	if len(reference) != len(cases) {
		t.Fatalf("result count mismatch: got %d want %d", len(reference), len(cases))
	}
	for i := range cases {
		if native[i] != reference[i] {
			t.Errorf("hmac mismatch for hash=%s key=%s: native=%s reference=%s",
				cases[i].Hash, cases[i].Key, native[i], reference[i])
		}
	}
}

func TestXorbConformance(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{name: "Hello World", data: []byte("Hello World!")},
		{name: "1MB", data: utils.MakeRandData(1024 * 1024)},
		{name: "1MB repeating", data: utils.MakeRepeatData(1024 * 1024)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expected := getNativeChunks(t, tt.data)
			for _, withFooter := range []bool{false, true} {
				name := "without_footer"
				if withFooter {
					name = "with_footer"
				}
				t.Run(name, func(t *testing.T) {
					nativeXorb := encodeNativeXorb(t, tt.data, withFooter)
					rustChunks, err := rustref.DecodeXorb(nativeXorb, withFooter)
					if err != nil {
						t.Fatalf("Rust decoding Go xorb: %v", err)
					}
					compareChunkEntries(t, expected, fromRustChunks(rustChunks))

					rustXorb, err := rustref.EncodeXorb(tt.data, withFooter, "auto")
					if err != nil {
						t.Fatalf("Rust encoding xorb: %v", err)
					}
					compareChunkEntries(t, expected, decodeNativeXorb(t, rustXorb, withFooter))
				})
			}
		})
	}
}

// getNativeChunks splits data using the native Go implementation and returns
// the hash and size of each chunk.
func getNativeChunks(t *testing.T, data []byte) []chunkEntry {
	t.Helper()
	var chunks []chunkEntry
	err := xet.ChunkData(bytes.NewReader(data), func(_ int64, chunk []byte) error {
		chunks = append(chunks, chunkEntry{hash: xet.ComputeChunkHash(chunk).String(), size: uint64(len(chunk))})
		return nil
	})
	if err != nil {
		t.Fatalf("native ChunkData: %v", err)
	}
	return chunks
}

// getReferenceChunks splits data using the xet-core (Rust) reference implementation
// and returns the hash and size of each chunk.
func getReferenceChunks(t *testing.T, data []byte) []chunkEntry {
	t.Helper()
	raw, err := rustref.ChunkData(data)
	if err != nil {
		t.Fatalf("reference ChunkData: %v", err)
	}
	chunks := make([]chunkEntry, len(raw))
	for i, c := range raw {
		chunks[i] = chunkEntry{hash: c.Hash, size: c.Size}
	}
	return chunks
}

func compareAggregateHashes(t *testing.T, chunks []chunkEntry) {
	t.Helper()
	hashes, sizes := nativeHashList(t, chunks)
	referenceChunks := make([]rustref.ChunkInfo, len(chunks))
	for i, chunk := range chunks {
		referenceChunks[i] = rustref.ChunkInfo{Hash: chunk.hash, Size: chunk.size}
	}

	tests := []struct {
		name      string
		native    string
		reference func([]rustref.ChunkInfo) (string, error)
	}{
		{name: "xorb", native: xet.ComputeXorbHash(hashes, sizes).String(), reference: rustref.ComputeXorbHash},
		{name: "file", native: xet.ComputeFileHash(hashes, sizes).String(), reference: rustref.ComputeFileHash},
		{name: "range", native: xet.ComputeVerificationHash(hashes).String(), reference: rustref.ComputeRangeHash},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reference, err := test.reference(referenceChunks)
			if err != nil {
				t.Fatal(err)
			}
			if test.native != reference {
				t.Errorf("%s hash mismatch: native=%s reference=%s", test.name, test.native, reference)
			}
		})
	}
}

func nativeHashList(t *testing.T, chunks []chunkEntry) ([]xet.ChunkHash, []uint64) {
	t.Helper()
	hashes := make([]xet.ChunkHash, len(chunks))
	sizes := make([]uint64, len(chunks))
	for i, chunk := range chunks {
		hash, err := xet.ParseChunkHash(chunk.hash)
		if err != nil {
			t.Fatalf("parse hash: %v", err)
		}
		hashes[i] = hash
		sizes[i] = chunk.size
	}
	return hashes, sizes
}

// getNativeFileHash computes the file hash using the native Go implementation.
func getNativeFileHash(t *testing.T, chunks []chunkEntry) string {
	t.Helper()
	hashes, sizes := nativeHashList(t, chunks)
	return xet.ComputeFileHash(hashes, sizes).String()
}

// getReferenceFileHash computes the file hash using the xet-core (Rust) reference
// implementation by writing the data to a temporary file and calling HashFiles.
func getReferenceFileHash(t *testing.T, data []byte) string {
	t.Helper()
	f, err := os.CreateTemp("", "xet-conformance-*")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	defer os.Remove(f.Name())
	if _, err := f.Write(data); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close temp file: %v", err)
	}
	results, err := rustref.HashFiles([]string{f.Name()})
	if err != nil {
		t.Fatalf("reference HashFiles: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("reference HashFiles returned no results")
	}
	return results[0].Hash
}

func encodeNativeXorb(t *testing.T, data []byte, withFooter bool) []byte {
	t.Helper()
	var output bytes.Buffer
	encoder := xorb.NewEncoder(&output, withFooter)
	if err := xet.ChunkData(bytes.NewReader(data), func(_ int64, chunk []byte) error {
		_, err := encoder.Write(chunk)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func decodeNativeXorb(t *testing.T, data []byte, withFooter bool) []chunkEntry {
	t.Helper()
	decoder := xorb.NewDecoder(bytes.NewReader(data), withFooter)
	buffer := make([]byte, xet.MaxChunkSize)
	var chunks []chunkEntry
	for {
		n, err := decoder.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		chunk := buffer[:n]
		chunks = append(chunks, chunkEntry{hash: xet.ComputeChunkHash(chunk).String(), size: uint64(n)})
	}
	return chunks
}

func fromRustChunks(chunks []rustref.ChunkInfo) []chunkEntry {
	result := make([]chunkEntry, len(chunks))
	for i, chunk := range chunks {
		result[i] = chunkEntry{hash: chunk.Hash, size: chunk.Size}
	}
	return result
}

func compareChunkEntries(t *testing.T, expected, actual []chunkEntry) {
	t.Helper()
	if len(expected) != len(actual) {
		t.Fatalf("chunk count mismatch: expected=%d actual=%d", len(expected), len(actual))
	}
	for i := range expected {
		if expected[i] != actual[i] {
			t.Errorf("chunk[%d] mismatch: expected=%+v actual=%+v", i, expected[i], actual[i])
		}
	}
}
