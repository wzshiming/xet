package storagetest

import (
	"context"
	"encoding/binary"
	"io"
	"math"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/xorb"
)

// ChunkClass pairs a chunk generator with the compression type the xorb
// encoder must pick for every chunk it yields, written one chunk per Write.
type ChunkClass struct {
	Name        string
	Compression xorb.CompressionType
	fill        func(src *rand.ChaCha8, p []byte)
}

// ChunkClasses holds one class per compression type.
var ChunkClasses = []ChunkClass{
	{Name: "none", Compression: xorb.CompressionNone, fill: genNone},
	{Name: "lz4", Compression: xorb.CompressionLZ4, fill: genLZ4},
	{Name: "bg4", Compression: xorb.CompressionByteGrouping4, fill: genBG4},
}

// ChunkClassBF16 models bf16 checkpoint weights: byte grouping stores it at
// about 86 % of its size, a real checkpoint at about 90 %. It stays out of
// ChunkClasses, whose classes each stand for one compression type.
var ChunkClassBF16 = ChunkClass{Name: "bf16", Compression: xorb.CompressionByteGrouping4, fill: genBF16}

// Chunk fills p with chunk chunkIndex of xorb xorbIndex. Its seed leaves out
// len(p), so a chunk keeps its bytes in files of any size.
func (c ChunkClass) Chunk(xorbIndex, chunkIndex int, p []byte) {
	c.fill(chunkSource(c.Name, xorbIndex, chunkIndex), p)
}

// Xorbs returns the PutXorbFile input of xorbs xorbs, each of chunksPerXorb
// chunks of chunkSize bytes.
func (c ChunkClass) Xorbs(xorbs, chunksPerXorb, chunkSize int) [][][]byte {
	out := make([][][]byte, xorbs)
	for x := range out {
		out[x] = make([][]byte, chunksPerXorb)
		for i := range out[x] {
			out[x][i] = make([]byte, chunkSize)
			c.Chunk(x, i, out[x][i])
		}
	}
	return out
}

// chunkSource seeds a stream unique to one chunk of one class.
func chunkSource(class string, xorbIndex, chunkIndex int) *rand.ChaCha8 {
	var seed [32]byte
	copy(seed[:], class)
	binary.LittleEndian.PutUint32(seed[24:], uint32(xorbIndex))
	binary.LittleEndian.PutUint32(seed[28:], uint32(chunkIndex))
	return rand.NewChaCha8(seed)
}

func genNone(src *rand.ChaCha8, p []byte) {
	_, _ = src.Read(p)
}

var loremWords = strings.Fields("lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua")

// genLZ4 writes random words, whose repeats byte grouping would split.
func genLZ4(src *rand.ChaCha8, p []byte) {
	r := rand.New(src)
	for i := 0; i < len(p); {
		i += copy(p[i:], loremWords[r.IntN(len(loremWords))])
		if i < len(p) {
			p[i] = ' '
			i++
		}
	}
}

// genBG4 writes LE uint32s with a random low half and a slowly drifting high half.
func genBG4(src *rand.ChaCha8, p []byte) {
	r := rand.New(src)
	hi := r.Uint32()
	for i := range len(p) / 4 {
		if r.IntN(64) == 0 {
			hi++
		}
		binary.LittleEndian.PutUint32(p[4*i:], hi<<16|r.Uint32N(1<<16))
	}
}

// genBF16 writes little-endian bfloat16s of float32 samples with standard
// deviation 2^-6, normal or uniform at even odds. Normal samples alone leave
// LZ4 missing the high-byte planes of about 1 % of chunks, which then store
// raw (4 % of a real bf16 checkpoint's chunks do); the uniform half makes high
// bytes repeat often enough for every chunk to compress.
func genBF16(src *rand.ChaCha8, p []byte) {
	r := rand.New(src)
	for i := 0; i < len(p); i += 2 {
		var z float64
		if r.Float64() < 0.5 {
			z = (2*r.Float64() - 1) * math.Sqrt(3)
		} else {
			z = r.NormFloat64()
		}
		v := toBF16(float32(z * 0x1p-6))
		p[i] = byte(v)
		if i+1 < len(p) {
			p[i+1] = byte(v >> 8)
		}
	}
}

// toBF16 rounds f to the nearest bfloat16, ties to even.
func toBF16(f float32) uint16 {
	b := math.Float32bits(f)
	return uint16((b + 0x7fff + b>>16&1) >> 16)
}

// AssertFileStoredRatio fails tb unless f's xorbs store between lo and hi
// times its size, counting chunk headers, and returns that ratio.
func AssertFileStoredRatio(tb testing.TB, ctx context.Context, st storage.Storage, f File, lo, hi float64) float64 {
	tb.Helper()
	var stored uint64
	for _, h := range f.XorbHashes {
		offsets, err := st.GetXorbChunkOffsets(ctx, h)
		if err != nil {
			tb.Fatal(err)
		}
		if len(offsets) > 0 {
			stored += offsets[len(offsets)-1]
		}
	}
	ratio := float64(stored) / float64(len(f.Content))
	if ratio < lo || ratio > hi {
		tb.Fatalf("stored/raw %.4f (%d of %d bytes) outside [%.2f, %.2f]", ratio, stored, len(f.Content), lo, hi)
	}
	return ratio
}

// AssertFileCompression fails tb unless every chunk of every xorb of f is
// stored with compression want.
func AssertFileCompression(tb testing.TB, ctx context.Context, st storage.Storage, f File, want xorb.CompressionType) {
	tb.Helper()
	for x, h := range f.XorbHashes {
		offsets, err := st.GetXorbChunkOffsets(ctx, h)
		if err != nil {
			tb.Fatal(err)
		}
		rc, err := st.GetXorbReadSeekCloser(ctx, h)
		if err != nil {
			tb.Fatal(err)
		}
		hdrs, err := readChunkHeaders(rc, offsets)
		rc.Close()
		if err != nil {
			tb.Fatalf("xorb %d (%s): %v", x, h.String(), err)
		}
		for i, hdr := range hdrs {
			if hdr.Compression != want {
				tb.Fatalf("xorb %d chunk %d: compression type %d, want %d", x, i, hdr.Compression, want)
			}
		}
	}
}

// readChunkHeaders parses the header at the start of each chunk of an xorb
// whose cumulative packed chunk ends are offsets.
func readChunkHeaders(rs io.ReadSeeker, offsets []uint64) ([]xorb.ChunkHeader, error) {
	hdrs := make([]xorb.ChunkHeader, len(offsets))
	var raw [xorb.ChunkHeaderSize]byte
	var start uint64
	for i, end := range offsets {
		if _, err := rs.Seek(int64(start), io.SeekStart); err != nil {
			return nil, err
		}
		if _, err := io.ReadFull(rs, raw[:]); err != nil {
			return nil, err
		}
		hdr, err := xorb.ParseChunkHeader(raw[:])
		if err != nil {
			return nil, err
		}
		hdrs[i] = hdr
		start = end
	}
	return hdrs, nil
}
