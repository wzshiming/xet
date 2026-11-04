package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/xorb"
)

// fakeStore serves one xorb's chunk offsets and bytes; any other Storage call panics on the nil embedded Storage.
type fakeStore struct {
	Storage
	xorb    []byte
	offsets []uint64
}

func (s *fakeStore) GetXorbChunkOffsets(context.Context, xet.XorbHash) ([]uint64, error) {
	return s.offsets, nil
}

func (s *fakeStore) GetXorbReadSeekCloser(context.Context, xet.XorbHash) (io.ReadSeekCloser, error) {
	return &rangeSeeker{Reader: bytes.NewReader(s.xorb)}, nil
}

// pipelineFixtureForTest stores an xorb of a None, an LZ4 and a BG4 chunk, in that order, and returns the chunks and the term covering them.
func pipelineFixtureForTest(t *testing.T) (*fakeStore, [][]byte, shard.FileDataSequenceEntry) {
	t.Helper()
	const n = 64 << 10
	src := rand.NewChaCha8([32]byte{'m', '2'})
	r := rand.New(src)

	none := make([]byte, n)
	_, _ = src.Read(none)

	words := strings.Fields("lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua")
	var lz4 []byte
	for len(lz4) < n {
		lz4 = append(append(lz4, words[r.IntN(len(words))]...), ' ')
	}
	lz4 = lz4[:n]

	// Little-endian uint32s with a random low half and a slowly drifting high half.
	bg4 := make([]byte, n)
	hi := r.Uint32()
	for i := range n / 4 {
		if r.IntN(64) == 0 {
			hi++
		}
		binary.LittleEndian.PutUint32(bg4[4*i:], hi<<16|r.Uint32N(1<<16))
	}

	chunks := [][]byte{none, lz4, bg4}
	var buf bytes.Buffer
	enc := xorb.NewEncoder(&buf, false)
	term := shard.FileDataSequenceEntry{ChunkIndexEnd: uint32(len(chunks))}
	for _, c := range chunks {
		if _, err := enc.Write(c); err != nil {
			t.Fatalf("encode %d-byte chunk: %v", len(c), err)
		}
		term.UnpackedSegBytes += uint32(len(c))
	}
	if err := enc.Close(); err != nil {
		t.Fatalf("close xorb encoder: %v", err)
	}
	term.CASHash = enc.SummoryHash()

	offsets, err := xorb.ScanChunkOffsets(bytes.NewReader(buf.Bytes()))
	if err != nil || len(offsets) != len(chunks) {
		t.Fatalf("ScanChunkOffsets() = %v, %v; want %d chunk ends", offsets, err, len(chunks))
	}
	var start uint64
	for i, want := range []xorb.CompressionType{xorb.CompressionNone, xorb.CompressionLZ4, xorb.CompressionByteGrouping4} {
		if h, err := xorb.ParseChunkHeader(buf.Bytes()[start:]); err != nil || h.Compression != want {
			t.Fatalf("chunk %d header = %+v, %v; want compression type %d", i, h, err, want)
		}
		start = offsets[i]
	}
	return &fakeStore{xorb: buf.Bytes(), offsets: offsets}, chunks, term
}

// assertFramesHoldNoBuffer fails for every window frame still holding a chunk buffer or data.
func assertFramesHoldNoBuffer(t *testing.T, p *pipeline) {
	t.Helper()
	for i := range p.frames {
		if fr := &p.frames[i]; fr.in != nil || fr.out != nil || fr.data != nil {
			t.Errorf("frame %d holds in=%t out=%t and %d data bytes, want its buffers returned and data nil", i, fr.in != nil, fr.out != nil, len(fr.data))
		}
	}
}

func TestPipelineFrameHoldsOnlyTheBufferBackingData(t *testing.T) {
	store, chunks, term := pipelineFixtureForTest(t)
	ctx := t.Context()
	p := startPipeline(ctx, store, []shard.FileDataSequenceEntry{term})
	defer p.stop()
	for i, class := range []string{"none", "lz4", "bg4"} {
		fr, err := p.next(ctx)
		if err != nil {
			t.Fatalf("next() for chunk %d = %v, want it decoded", i, err)
		}
		t.Run("class="+class, func(t *testing.T) {
			if !bytes.Equal(fr.data, chunks[i]) {
				t.Fatalf("frame data = %d bytes, want the chunk's %d bytes", len(fr.data), len(chunks[i]))
			}
			if fr.header.Compression == xorb.CompressionNone {
				if fr.in == nil || fr.out != nil {
					t.Fatalf("None frame holds in=%t out=%t, want only in, whose packed body data aliases", fr.in != nil, fr.out != nil)
				}
				if &fr.data[0] != &fr.in[xorb.ChunkHeaderSize] {
					t.Fatalf("None frame data does not alias in[%d:], the packed body", xorb.ChunkHeaderSize)
				}
				return
			}
			if fr.in != nil || fr.out == nil {
				t.Fatalf("%s frame holds in=%t out=%t, want only out, the decode buffer data aliases", class, fr.in != nil, fr.out != nil)
			}
			if &fr.data[0] != &fr.out[0] {
				t.Fatalf("%s frame data does not alias out[0:]", class)
			}
		})
		p.release(fr)
	}
}

func TestPipelineReleaseReturnsFrameBuffers(t *testing.T) {
	store, chunks, term := pipelineFixtureForTest(t)
	ctx := t.Context()
	p := startPipeline(ctx, store, []shard.FileDataSequenceEntry{term})
	defer p.stop()
	for i := range chunks {
		fr, err := p.next(ctx)
		if err != nil {
			t.Fatalf("next() for chunk %d = %v, want it decoded", i, err)
		}
		p.release(fr)
	}
	// Once every chunk is fetched and decoded, the fetcher and workers return without a stop.
	p.wg.Wait()
	assertFramesHoldNoBuffer(t, p)
}

func TestPipelineStopReturnsFrameBuffersAfterDecodeError(t *testing.T) {
	for _, tc := range []struct {
		name    string
		chunk   int
		corrupt func(packed []byte, term *shard.FileDataSequenceEntry)
		want    string
	}{
		{
			name:  "lz4_frame_magic",
			chunk: 1,
			corrupt: func(packed []byte, _ *shard.FileDataSequenceEntry) {
				packed[xorb.ChunkHeaderSize] ^= 0xFF // first byte of the LZ4 frame magic
			},
			want: "failed to decompress LZ4",
		},
		{
			name:  "none_uncompressed_size",
			chunk: 0,
			corrupt: func(packed []byte, term *shard.FileDataSequenceEntry) {
				// Header bytes 5-7 hold the little-endian uncompressed size; the term shrinks with it.
				size := uint32(packed[5]) | uint32(packed[6])<<8 | uint32(packed[7])<<16
				size--
				packed[5], packed[6], packed[7] = byte(size), byte(size>>8), byte(size>>16)
				term.UnpackedSegBytes--
			},
			want: "decompressed size mismatch",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, _, term := pipelineFixtureForTest(t)
			var start uint64
			if tc.chunk > 0 {
				start = store.offsets[tc.chunk-1]
			}
			tc.corrupt(store.xorb[start:store.offsets[tc.chunk]], &term)

			ctx := t.Context()
			p := startPipeline(ctx, store, []shard.FileDataSequenceEntry{term})
			for i := range tc.chunk {
				fr, err := p.next(ctx)
				if err != nil {
					p.stop()
					t.Fatalf("next() for intact chunk %d = %v, want it decoded", i, err)
				}
				p.release(fr)
			}
			_, err := p.next(ctx)
			p.stop()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("next() for corrupt chunk %d = %v, want an error containing %q", tc.chunk, err, tc.want)
			}
			assertFramesHoldNoBuffer(t, p)
		})
	}
}
