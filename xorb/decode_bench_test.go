package xorb_test

import (
	"bytes"
	"encoding/binary"
	"io"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/xorb"
)

const (
	chunksPerXorb = 256
	chunkSize     = 64 << 10
	xorbSize      = chunksPerXorb * chunkSize
)

// benchClass pairs a chunk generator with the compression type (chunk header
// byte 4) the encoder must pick for every chunk it yields.
type benchClass struct {
	name string
	ct   byte
	gen  func(src *rand.ChaCha8, p []byte)
}

var benchClasses = []benchClass{
	{name: "none", ct: 0, gen: genNone},
	{name: "lz4", ct: 1, gen: genLZ4},
	{name: "bg4", ct: 2, gen: genBG4},
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

// assertChunkTypes walks the encoded chunk headers (layout in decoder.go) and
// fails unless each carries the class's compression type.
func assertChunkTypes(b *testing.B, encoded []byte, c benchClass) {
	b.Helper()
	r := bytes.NewReader(encoded)
	var hdr [8]byte
	for i := range chunksPerXorb {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			b.Fatalf("chunk %d header: %v", i, err)
		}
		if hdr[4] != c.ct {
			b.Fatalf("chunk %d: compression type %d, want %d for class %s", i, hdr[4], c.ct, c.name)
		}
		if _, err := r.Seek(int64(hdr[1])|int64(hdr[2])<<8|int64(hdr[3])<<16, io.SeekCurrent); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecoderRead decodes an in-memory footerless xorb of 256 x 64 KiB
// chunks, per compression class.
func BenchmarkDecoderRead(b *testing.B) {
	for _, c := range benchClasses {
		b.Run("class="+c.name, func(b *testing.B) {
			var encoded bytes.Buffer
			enc := xorb.NewEncoder(&encoded, false)
			chunk := make([]byte, chunkSize)
			for i := range chunksPerXorb {
				c.gen(chunkSource(c.name, 0, i), chunk)
				if _, err := enc.Write(chunk); err != nil {
					b.Fatal(err)
				}
			}
			if err := enc.Close(); err != nil {
				b.Fatal(err)
			}
			assertChunkTypes(b, encoded.Bytes(), c)

			buf := make([]byte, xet.MaxChunkSize)
			b.SetBytes(xorbSize)
			b.ReportAllocs()
			for b.Loop() {
				dec := xorb.NewDecoder(bytes.NewReader(encoded.Bytes()), false)
				var n int64
				for {
					m, err := dec.Read(buf)
					n += int64(m)
					if err == io.EOF {
						break
					}
					if err != nil {
						b.Fatal(err)
					}
				}
				if n != xorbSize {
					b.Fatalf("decoded %d bytes, want %d", n, xorbSize)
				}
			}
		})
	}
}
