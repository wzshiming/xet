package xorb

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/wzshiming/xet"
)

// encodedTypesForTest encodes the chunks steps names, calling StartFile for each '|', and returns the compression type
// each chunk is stored with, N, L or B for None, LZ4 or ByteGrouping4, after checking that it decodes back.
func encodedTypesForTest(t *testing.T, chunks map[byte][]byte, steps string) string {
	t.Helper()
	var buf bytes.Buffer
	enc := NewEncoder(&buf, false)
	var written [][]byte
	for i := range len(steps) {
		if steps[i] == '|' {
			enc.StartFile()
			continue
		}
		if _, err := enc.Write(chunks[steps[i]]); err != nil {
			t.Fatalf("Write(chunk %c): %v", steps[i], err)
		}
		written = append(written, chunks[steps[i]])
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	var types []byte
	rest := buf.Bytes()
	for i, chunk := range written {
		h, err := ParseChunkHeader(rest)
		if err != nil {
			t.Fatalf("chunk %d header: %v", i, err)
		}
		body := rest[ChunkHeaderSize : ChunkHeaderSize+h.CompressedSize]
		if got, err := DecodeChunk(make([]byte, len(chunk)+1), h, body); err != nil || !bytes.Equal(got, chunk) {
			t.Fatalf("chunk %d decodes to %d bytes, %v; want the %d written", i, len(got), err, len(chunk))
		}
		types = append(types, "NLB"[h.Compression])
		rest = rest[ChunkHeaderSize+h.CompressedSize:]
	}
	return string(types)
}

// zeroRunChunkForTest returns random bytes with a run of zeros that makes compressLZ4's frame exactly saved bytes
// shorter than the chunk.
func zeroRunChunkForTest(t *testing.T, saved int) []byte {
	t.Helper()
	random := selectionChunkForTest(t, "random", 4096)
	for run := range 512 {
		chunk := bytes.Clone(random)
		clear(chunk[100 : 100+run])
		if frame, err := compressLZ4(nil, chunk); err == nil && len(chunk)-len(frame) == saved {
			return chunk
		}
	}
	t.Fatalf("no run of zeros makes LZ4 save exactly %d bytes", saved)
	return nil
}

func TestEncoderStoresSelectionClasses(t *testing.T) {
	for _, class := range selectionClassesForTest {
		chunks := map[byte][]byte{'c': selectionChunkForTest(t, class.name, xet.TargetChunkSize)}
		if got, want := encodedTypesForTest(t, chunks, "c"), string("NLB"[class.ct]); got != want {
			t.Errorf("class %s chunk stored as %s, want %s", class.name, got, want)
		}
	}
}

// A block, the first chunk of a file and up to 31 more of it, takes the scheme its first chunk predicts; LZ4 does not
// shrink bf16-like chunks.
func TestEncoderPredictsCompressionPerBlock(t *testing.T) {
	chunks := map[byte][]byte{
		't': selectionChunkForTest(t, "text", xet.TargetChunkSize),      // predicts LZ4
		'f': selectionChunkForTest(t, "bf16-like", xet.TargetChunkSize), // predicts ByteGrouping4
	}
	for _, c := range []struct{ name, steps, want string }{
		{"chunk 0 decides for chunks 1-31 and chunk 32 decides again", "t" + strings.Repeat("f", 32), "L" + strings.Repeat("N", 31) + "B"},
		{"StartFile decides again", "t|f", "LB"},
		{"StartFile twice starts one block of 32 chunks", "t||" + strings.Repeat("f", 32) + "t", "L" + strings.Repeat("B", 32) + "L"},
		{"StartFile before chunk 0 changes nothing", "||t" + strings.Repeat("f", 32), "L" + strings.Repeat("N", 31) + "B"},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := encodedTypesForTest(t, chunks, c.steps); got != c.want {
				t.Errorf("chunks %s stored as %s, want %s", c.steps, got, c.want)
			}
		})
	}
}

// Each chunk is compressed once, with its block's scheme, and stored as None unless that makes it smaller.
func TestEncoderCompressesEachChunkOnceWithItsBlockScheme(t *testing.T) {
	chunks := map[byte][]byte{
		't': selectionChunkForTest(t, "text", xet.TargetChunkSize), // predicts LZ4
		'g': selectionChunkForTest(t, "bg4", xet.TargetChunkSize),  // predicts ByteGrouping4
		'r': selectionChunkForTest(t, "random", xet.TargetChunkSize),
		's': selectionChunkForTest(t, "text", 1000),
		'e': zeroRunChunkForTest(t, 0), // LZ4 frame exactly as long as the chunk
		'o': zeroRunChunkForTest(t, 1), // LZ4 frame one byte shorter
	}
	lz4Short, _ := compressLZ4(nil, chunks['s'])
	bg4Short, _ := compressByteGrouping4LZ4(nil, chunks['s'])
	lz4Text, _ := compressLZ4(nil, chunks['t'])
	bg4Text, _ := compressByteGrouping4LZ4(nil, chunks['t'])
	if len(lz4Short) >= len(chunks['s']) || len(bg4Short) < len(chunks['s']) || len(lz4Text) >= len(bg4Text) || len(bg4Text) >= len(chunks['t']) {
		t.Fatalf("short text: LZ4 %d, BG4 %d of %d bytes; text: LZ4 %d, BG4 %d of %d bytes; want only LZ4 to shrink short text, and both to shrink text, LZ4 more",
			len(lz4Short), len(bg4Short), len(chunks['s']), len(lz4Text), len(bg4Text), len(chunks['t']))
	}
	for _, c := range []struct{ name, steps, want string }{
		{"LZ4 block keeps only frames shorter than the chunk", "teo", "LNL"},
		{"BG4 block never falls back to LZ4", "gsrt", "BNNB"},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := encodedTypesForTest(t, chunks, c.steps); got != c.want {
				t.Errorf("chunks %s stored as %s, want %s", c.steps, got, c.want)
			}
		})
	}
}

// As in xet-core's serialize_chunk, a failing compressor fails Write, which writes nothing for the chunk, and the encoder.
func TestEncoderFailsWhenCompressionFails(t *testing.T) {
	errInjected := errors.New("injected failure")
	text := selectionChunkForTest(t, "text", xet.TargetChunkSize)      // predicts LZ4
	bf16 := selectionChunkForTest(t, "bf16-like", xet.TargetChunkSize) // predicts ByteGrouping4
	for _, c := range []struct {
		name          string
		failing       CompressionType
		first, failed []byte
	}{
		{"LZ4", CompressionLZ4, bf16, text},
		{"ByteGrouping4", CompressionByteGrouping4, text, bf16},
	} {
		t.Run(c.name, func(t *testing.T) {
			saved := schemeCompressors
			t.Cleanup(func() { schemeCompressors = saved })
			schemeCompressors[c.failing] = func(dst, _ []byte) ([]byte, error) { return dst, errInjected }

			var buf bytes.Buffer
			enc := NewEncoder(&buf, true)
			if _, err := enc.Write(c.first); err != nil {
				t.Fatal(err)
			}
			written := buf.Len()
			enc.StartFile()
			if _, err := enc.Write(c.failed); !errors.Is(err, errInjected) || err == errInjected {
				t.Fatalf("Write(chunk whose %s compression fails) = %v; want that failure, wrapped", c.name, err)
			}
			enc.StartFile()
			if _, err := enc.Write(c.first); !errors.Is(err, errInjected) {
				t.Errorf("Write after the failure = %v, want the failure again", err)
			}
			if err := enc.Close(); !errors.Is(err, errInjected) {
				t.Errorf("Close after the failure = %v, want the failure again", err)
			}
			if buf.Len() != written {
				t.Errorf("encoder wrote %d bytes from the failed Write on, want none", buf.Len()-written)
			}
		})
	}
}
