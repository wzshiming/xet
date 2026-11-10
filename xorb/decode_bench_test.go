package xorb_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/internal/benchprof"
	"github.com/wzshiming/xet/storage/storagetest"
	"github.com/wzshiming/xet/xorb"
)

const (
	chunksPerXorb = 256
	chunkSize     = 64 << 10
	xorbSize      = chunksPerXorb * chunkSize
)

// BenchmarkEncoder encodes an xorb of 256 x 64 KiB chunks into io.Discard,
// per chunk class and footer mode.
func BenchmarkEncoder(b *testing.B) {
	codecRows(b, func(b *testing.B, c storagetest.ChunkClass, withFooter bool) {
		chunks, _ := codecFixture(b, c, withFooter)
		b.SetBytes(xorbSize)
		b.ReportAllocs()
		stop := benchprof.Start(b)
		for b.Loop() {
			if err := encodeXorb(io.Discard, chunks, withFooter); err != nil {
				b.Fatal(err)
			}
		}
		stop()
	})
}

// BenchmarkDecoder reads back the in-memory xorb BenchmarkEncoder writes, per
// chunk class and footer mode.
func BenchmarkDecoder(b *testing.B) {
	codecRows(b, func(b *testing.B, c storagetest.ChunkClass, withFooter bool) {
		_, encoded := codecFixture(b, c, withFooter)
		buf := make([]byte, xet.MaxChunkSize)
		b.SetBytes(xorbSize)
		b.ReportAllocs()
		stop := benchprof.Start(b)
		for b.Loop() {
			dec := xorb.NewDecoder(bytes.NewReader(encoded), withFooter)
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
		stop()
	})
}

// codecRows runs bench as row class=<name>/footer=<with|without> for each
// chunk class and footer mode.
func codecRows(b *testing.B, bench func(b *testing.B, c storagetest.ChunkClass, withFooter bool)) {
	for _, c := range storagetest.ChunkClasses {
		for _, footer := range []string{"with", "without"} {
			b.Run("class="+c.Name+"/footer="+footer, func(b *testing.B) {
				bench(b, c, footer == "with")
			})
		}
	}
}

// codecFixture returns xorb 0 of class c and its encoding with or without a
// footer, failing b unless every chunk is stored with the class's compression
// type and the encoding decodes back to the chunks.
func codecFixture(b *testing.B, c storagetest.ChunkClass, withFooter bool) (chunks [][]byte, encoded []byte) {
	b.Helper()
	chunks = c.Xorbs(1, chunksPerXorb, chunkSize)[0]
	var buf bytes.Buffer
	if err := encodeXorb(&buf, chunks, withFooter); err != nil {
		b.Fatal(err)
	}
	encoded = buf.Bytes()
	assertChunkTypes(b, encoded, c)
	assertDecodesTo(b, encoded, withFooter, chunks)
	return chunks, encoded
}

// encodeXorb is the encoding BenchmarkEncoder times and codecFixture checks.
func encodeXorb(w io.Writer, chunks [][]byte, withFooter bool) error {
	enc := xorb.NewEncoder(w, withFooter)
	for _, chunk := range chunks {
		if _, err := enc.Write(chunk); err != nil {
			return err
		}
	}
	return enc.Close()
}

// assertChunkTypes walks the encoded chunk headers (layout in decoder.go) and
// fails unless each carries the class's compression type.
func assertChunkTypes(b *testing.B, encoded []byte, c storagetest.ChunkClass) {
	b.Helper()
	r := bytes.NewReader(encoded)
	var hdr [8]byte
	for i := range chunksPerXorb {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			b.Fatalf("chunk %d header: %v", i, err)
		}
		if xorb.CompressionType(hdr[4]) != c.Compression {
			b.Fatalf("chunk %d: compression type %d, want %d for class %s", i, hdr[4], c.Compression, c.Name)
		}
		if _, err := r.Seek(int64(hdr[1])|int64(hdr[2])<<8|int64(hdr[3])<<16, io.SeekCurrent); err != nil {
			b.Fatal(err)
		}
	}
}

// assertDecodesTo fails b unless encoded decodes, one Read per chunk, to
// chunks and the decoder's Chunks lists a hash and a size for each.
func assertDecodesTo(b *testing.B, encoded []byte, withFooter bool, chunks [][]byte) {
	b.Helper()
	dec := xorb.NewDecoder(bytes.NewReader(encoded), withFooter)
	buf := make([]byte, xet.MaxChunkSize)
	n := 0
	for {
		m, err := dec.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			b.Fatalf("chunk %d: %v", n, err)
		}
		if n >= len(chunks) || !bytes.Equal(buf[:m], chunks[n]) {
			b.Fatalf("chunk %d decodes to %d bytes that are not the chunk encoded", n, m)
		}
		n++
	}
	if n != len(chunks) {
		b.Fatalf("decoded %d chunks, want %d", n, len(chunks))
	}
	if hashes, sizes := dec.Chunks(); len(hashes) != len(chunks) || len(sizes) != len(chunks) {
		b.Fatalf("Chunks() lists %d hashes and %d sizes, want %d", len(hashes), len(sizes), len(chunks))
	}
}
