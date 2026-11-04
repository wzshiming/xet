package xorb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/wzshiming/xet"
)

type decoderModeForTest struct {
	name       string
	data       []byte
	withFooter bool
}

// decoderModesForTest encodes chunks of mixed compressibility, up to MaxChunkSize, with and without a footer.
func decoderModesForTest(t *testing.T) ([][]byte, []decoderModeForTest, xet.XorbHash) {
	t.Helper()
	counters := make([]byte, 64<<10)
	for i := range len(counters) / 4 {
		binary.LittleEndian.PutUint32(counters[4*i:], uint32(i))
	}
	chunks := append(testFooterChunks(5), counters, payloadForTest(xet.MaxChunkSize, 5))
	footered, xorbHash := encodeXorbForTest(t, [4]byte{}, chunks...)
	return chunks, []decoderModeForTest{
		{"footer=true", footered, true},
		{"footer=false", encodeChunkOnlyXorbForTest(t, chunks...), false},
		{"footered_xorb_with_footer=false", footered, false},
	}, xorbHash
}

// drainDecoderForTest reads d with a MaxChunkSize buffer until Read fails and returns the bytes read and that error.
func drainDecoderForTest(d *Decoder) ([]byte, error) {
	var out []byte
	buf := make([]byte, xet.MaxChunkSize)
	for range xet.MaxChunksPerXorb + 1 {
		n, err := d.Read(buf)
		out = append(out, buf[:n]...)
		if err != nil {
			return out, err
		}
	}
	return out, nil
}

func TestDecoderReadMatchesEncodedChunks(t *testing.T) {
	chunks, modes, _ := decoderModesForTest(t)
	want := bytes.Join(chunks, nil)
	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			got, err := drainDecoderForTest(NewDecoder(bytes.NewReader(mode.data), mode.withFooter))
			if err != io.EOF || !bytes.Equal(got, want) {
				t.Fatalf("Decoder read %d bytes then %v; want the %d encoded bytes then io.EOF", len(got), err, len(want))
			}
		})
	}
}

func TestDecoderChunksMatchEncoder(t *testing.T) {
	chunks, modes, _ := decoderModesForTest(t)
	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			d := NewDecoder(bytes.NewReader(mode.data), mode.withFooter)
			if _, err := drainDecoderForTest(d); err != io.EOF {
				t.Fatalf("Decoder ended with %v, want io.EOF", err)
			}
			hashes, sizes := d.Chunks()
			if len(hashes) != len(chunks) || len(sizes) != len(chunks) {
				t.Fatalf("Chunks() = %d hashes and %d sizes, want %d of each", len(hashes), len(sizes), len(chunks))
			}
			for i, c := range chunks {
				if hashes[i] != xet.ComputeChunkHash(c) || sizes[i] != uint64(len(c)) {
					t.Fatalf("Chunks()[%d] = %s/%d, want the Encoder's %s/%d", i, hashes[i], sizes[i], xet.ComputeChunkHash(c), len(c))
				}
			}
		})
	}
}

func TestDecoderSummoryHashMatchesEncoder(t *testing.T) {
	_, modes, xorbHash := decoderModesForTest(t)
	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			d := NewDecoder(bytes.NewReader(mode.data), mode.withFooter)
			if _, err := drainDecoderForTest(d); err != io.EOF {
				t.Fatalf("Decoder ended with %v, want io.EOF", err)
			}
			if got := d.SummoryHash(); got != xorbHash {
				t.Fatalf("SummoryHash() = %s, want Encoder.SummoryHash() %s", got, xorbHash)
			}
		})
	}
}

func TestDecoderChunkOnlyEOF(t *testing.T) {
	chunks := testFooterChunks(3)
	chunkOnly := encodeChunkOnlyXorbForTest(t, chunks...)
	want := bytes.Join(chunks, nil)
	for _, tt := range []struct {
		name string
		data []byte
	}{
		{"clean_end", chunkOnly},
		{"truncated_trailing_header", append(bytes.Clone(chunkOnly), 0, 1, 2)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d := NewDecoder(bytes.NewReader(tt.data), false)
			if got, err := drainDecoderForTest(d); err != io.EOF || !bytes.Equal(got, want) {
				t.Fatalf("Decoder read %d bytes then %v; want the %d chunk bytes then io.EOF", len(got), err, len(want))
			}
			if n, err := d.Read(make([]byte, xet.MaxChunkSize)); n != 0 || err != io.EOF {
				t.Fatalf("Read() after io.EOF = %d, %v; want 0, io.EOF", n, err)
			}
		})
	}
}

func TestDecoderFooterModeRejectsMissingFooter(t *testing.T) {
	chunkOnly := encodeChunkOnlyXorbForTest(t, testFooterChunks(3)...)
	for _, tt := range []struct {
		name string
		data []byte
		want string
	}{
		{"clean_end", chunkOnly, "unexpected EOF: expected footer"},
		{"truncated_trailing_header", append(bytes.Clone(chunkOnly), 0, 1, 2), "failed to read data: unexpected EOF"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := drainDecoderForTest(NewDecoder(bytes.NewReader(tt.data), true))
			if err == nil || err.Error() != tt.want {
				t.Fatalf("Decoder ended with %v, want %q", err, tt.want)
			}
		})
	}
}

func TestDecoderErrorIsSticky(t *testing.T) {
	stream := append([]byte{1, 0, 0, 0, 0, 0, 0, 0}, encodeChunkOnlyXorbForTest(t, []byte("valid chunk"))...)
	d := NewDecoder(bytes.NewReader(stream), false)
	buf := make([]byte, xet.MaxChunkSize)
	_, first := d.Read(buf)
	if first == nil || first == io.EOF {
		t.Fatalf("Read() of a version-1 chunk = %v, want an error", first)
	}
	if n, err := d.Read(buf); n != 0 || !errors.Is(err, first) {
		t.Fatalf("Read() after %q = %d, %v; want 0 and the same error", first, n, err)
	}
}

func TestDecoderOutputTooSmallKeepsChunk(t *testing.T) {
	chunk := make([]byte, 4096)
	frame, _ := chunkFrameForTest(t, CompressionLZ4, chunk)
	d := NewDecoder(bytes.NewReader(frame), false)
	_, err := d.Read(make([]byte, 128))
	if want := "output buffer too small: need 4096 bytes"; err == nil || err.Error() != want {
		t.Fatalf("Read() error = %v, want %q", err, want)
	}
	hashes, sizes := d.Chunks()
	if len(hashes) != 1 || len(sizes) != 1 || hashes[0] != xet.ComputeChunkHash(chunk) || sizes[0] != 4096 {
		t.Fatalf("Chunks() after output-too-small = %v, %v; want the chunk already recorded", hashes, sizes)
	}
}

func TestDecoderKeepsErrorTexts(t *testing.T) {
	oneByteChunk := []byte{0, 1, 0, 0, byte(CompressionNone), 1, 0, 0, 'x'}
	maxChunk, _ := chunkFrameForTest(t, CompressionLZ4, make([]byte, xet.MaxChunkSize))
	oversized := xet.MaxChunkSize + 1
	for _, tt := range []struct {
		name   string
		stream []byte
		bufLen int
		want   string
	}{
		{"unsupported_version", []byte{1, 1, 0, 0, 0, 1, 0, 0, 'x'}, xet.MaxChunkSize, "unsupported chunk version: 1"},
		{"input_buffer_too_small", []byte{0, 2, 0, 0, 0, 2, 0, 0, 'x', 'y'}, 1, "input buffer too small for compressed chunk: need 2 bytes, have 1"},
		{
			"uncompressed_size_above_maximum",
			[]byte{0, 1, 0, 0, 0, byte(oversized), byte(oversized >> 8), byte(oversized >> 16), 'x'}, xet.MaxChunkSize,
			fmt.Sprintf("invalid uncompressed chunk size: %d exceeds maximum %d", oversized, xet.MaxChunkSize),
		},
		{
			"chunk_count_above_maximum",
			bytes.Repeat(oneByteChunk, xet.MaxChunksPerXorb+1), xet.MaxChunkSize,
			fmt.Sprintf("chunk count exceeds maximum %d", xet.MaxChunksPerXorb),
		},
		{
			"raw_payload_above_maximum",
			append(bytes.Repeat(maxChunk, xet.MaxXorbSize/xet.MaxChunkSize), oneByteChunk...), xet.MaxChunkSize,
			fmt.Sprintf("raw payload size exceeds maximum %d", xet.MaxXorbSize),
		},
		{"truncated_chunk_data", []byte{0, 5, 0, 0, 0, 5, 0, 0, 'a', 'b', 'c'}, xet.MaxChunkSize, "failed to read chunk data: unexpected EOF"},
		{"unknown_compression", []byte{0, 1, 0, 0, 3, 1, 0, 0, 'x'}, xet.MaxChunkSize, "decompress chunk: unsupported compression type: 3"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d := NewDecoder(bytes.NewReader(tt.stream), false)
			buf := make([]byte, tt.bufLen)
			var err error
			for range xet.MaxChunksPerXorb + 1 {
				if _, err = d.Read(buf); err != nil {
					break
				}
			}
			if err == nil || err.Error() != tt.want {
				t.Fatalf("Decoder ended with %v, want %q", err, tt.want)
			}
		})
	}
}

// TestDecoderSkip skips every other chunk: the chunks read in between decode
// as usual, Chunks lists only them, and a footer can no longer be checked.
func TestDecoderSkip(t *testing.T) {
	chunks, modes, _ := decoderModesForTest(t)
	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			d := NewDecoder(bytes.NewReader(mode.data), mode.withFooter)
			buf := make([]byte, xet.MaxChunkSize)
			var read [][]byte
			for i, c := range chunks {
				if i%2 == 0 {
					if err := d.Skip(); err != nil {
						t.Fatalf("Skip() of chunk %d = %v", i, err)
					}
					continue
				}
				if n, err := d.Read(buf); err != nil || !bytes.Equal(buf[:n], c) {
					t.Fatalf("Read() of chunk %d = %d bytes, %v; want its %d bytes", i, n, err, len(c))
				}
				read = append(read, c)
			}
			hashes, sizes := d.Chunks()
			if len(hashes) != len(read) || len(sizes) != len(read) {
				t.Fatalf("Chunks() = %d hashes and %d sizes, want %d of each", len(hashes), len(sizes), len(read))
			}
			for i, c := range read {
				if hashes[i] != xet.ComputeChunkHash(c) || sizes[i] != uint64(len(c)) {
					t.Fatalf("Chunks()[%d] = %s/%d, want %s/%d", i, hashes[i], sizes[i], xet.ComputeChunkHash(c), len(c))
				}
			}
			err := d.Skip()
			if mode.withFooter {
				if want := fmt.Sprintf("validate footer: %d chunks were skipped", len(chunks)-len(read)); err == nil || err.Error() != want {
					t.Fatalf("Skip() at the footer = %v, want %q", err, want)
				}
			} else if err != io.EOF {
				t.Fatalf("Skip() at the end = %v, want io.EOF", err)
			}
		})
	}

	oneByteChunk := []byte{0, 1, 0, 0, byte(CompressionNone), 1, 0, 0, 'x'}
	for _, tt := range []struct {
		name   string
		stream []byte
		want   string
	}{
		// Skip never decompresses, so an unknown compression passes.
		{"unknown_compression", []byte{0, 1, 0, 0, 3, 1, 0, 0, 'x'}, "EOF"},
		{"truncated_chunk_data", []byte{0, 5, 0, 0, 0, 5, 0, 0, 'a', 'b', 'c'}, "failed to read chunk data: unexpected EOF"},
		{"compressed_above_buffer", []byte{0, 0, 0, 3, 0, 1, 0, 0}, fmt.Sprintf("input buffer too small for compressed chunk: need %d bytes, have %d", 3<<16, xet.MaxChunkSize+8)},
		{"chunk_count_above_maximum", bytes.Repeat(oneByteChunk, xet.MaxChunksPerXorb+1), fmt.Sprintf("chunk count exceeds maximum %d", xet.MaxChunksPerXorb)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d := NewDecoder(bytes.NewReader(tt.stream), false)
			var err error
			for range xet.MaxChunksPerXorb + 2 {
				if err = d.Skip(); err != nil {
					break
				}
			}
			if err == nil || err.Error() != tt.want {
				t.Fatalf("Skip() ended with %v, want %q", err, tt.want)
			}
		})
	}
}

func TestDecoderRejectsNoneChunkSizeMismatch(t *testing.T) {
	for _, tt := range []struct {
		name   string
		stream []byte
	}{
		{"compressed_above_uncompressed", []byte{0, 5, 0, 0, byte(CompressionNone), 4, 0, 0, 'a', 'b', 'c', 'd', 'e'}},
		{"compressed_below_uncompressed", []byte{0, 4, 0, 0, byte(CompressionNone), 5, 0, 0, 'a', 'b', 'c', 'd'}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewDecoder(bytes.NewReader(tt.stream), false).Read(make([]byte, xet.MaxChunkSize))
			if want := "decompressed size mismatch"; err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("Read() error = %v, want one containing %q", err, want)
			}
		})
	}
}
