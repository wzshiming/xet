package storage_test

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/local"
	"github.com/wzshiming/xet/storage/storagetest"
)

const (
	xorbsPerFile  = 4
	chunksPerXorb = 256
	chunkSize     = 64 << 10
	fileSize      = xorbsPerFile * chunksPerXorb * chunkSize
	readBufSize   = 32 << 10
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

// fixture is what a class keeps of its stored file once the payloads are dropped.
type fixture struct {
	st     storage.Storage
	digest [32]byte
	xorbs  []xet.XorbHash
}

// buildFixture stores the class's file on a fresh local store, then reads it
// back once untimed to check its size and SHA-256.
func buildFixture(b *testing.B, c benchClass) fixture {
	b.Helper()
	st, err := local.NewStorage(local.WithBasePath(b.TempDir()))
	if err != nil {
		b.Fatal(err)
	}
	xorbs := make([][][]byte, xorbsPerFile)
	for x := range xorbs {
		xorbs[x] = make([][]byte, chunksPerXorb)
		for i := range xorbs[x] {
			xorbs[x][i] = make([]byte, chunkSize)
			c.gen(chunkSource(c.name, x, i), xorbs[x][i])
		}
	}
	f := storagetest.PutXorbFile(b, b.Context(), st, xorbs)
	fx := fixture{st: st, digest: storagetest.SHA256Digest(f.SHA256Hex), xorbs: f.XorbHashes}

	rc, err := st.GetReconstructedFile(b.Context(), fx.digest)
	if err != nil {
		b.Fatal(err)
	}
	defer rc.Close()
	h := sha256.New()
	n, err := readLoop32K(io.TeeReader(rc, h), new([readBufSize]byte))
	if err != nil {
		b.Fatal(err)
	}
	if sum := [32]byte(h.Sum(nil)); n != fileSize || sum != fx.digest {
		b.Fatalf("reconstructed %d bytes with SHA-256 %x, want %d bytes with %x", n, sum, fileSize, fx.digest)
	}
	return fx
}

// assertXorbClass walks the stored xorb's chunk headers (layout in
// xorb/decoder.go) and fails unless each carries the class's compression type.
func assertXorbClass(b *testing.B, st storage.Storage, x int, h xet.XorbHash, c benchClass) {
	b.Helper()
	rc, err := st.GetXorbReadSeekCloser(b.Context(), h)
	if err != nil {
		b.Fatal(err)
	}
	defer rc.Close()
	var hdr [8]byte
	for i := range chunksPerXorb {
		if _, err := io.ReadFull(rc, hdr[:]); err != nil {
			b.Fatalf("xorb %d chunk %d header: %v", x, i, err)
		}
		if hdr[4] != c.ct {
			b.Fatalf("xorb %d chunk %d: compression type %d, want %d for class %s", x, i, hdr[4], c.ct, c.name)
		}
		if _, err := rc.Seek(int64(hdr[1])|int64(hdr[2])<<8|int64(hdr[3])<<16, io.SeekCurrent); err != nil {
			b.Fatal(err)
		}
	}
}

// readLoop32K reads r to EOF through buf, bypassing io.Copy's WriterTo and
// ReaderFrom shortcuts.
func readLoop32K(r io.Reader, buf *[readBufSize]byte) (int64, error) {
	var n int64
	for {
		m, err := r.Read(buf[:])
		n += int64(m)
		if err == io.EOF {
			return n, nil
		}
		if err != nil {
			return n, err
		}
	}
}

// packedSize sums the stored sizes of the xorbs.
func packedSize(b *testing.B, st storage.Storage, xorbs []xet.XorbHash) int64 {
	b.Helper()
	var sum int64
	for _, h := range xorbs {
		rc, err := st.GetXorbReadSeekCloser(b.Context(), h)
		if err != nil {
			b.Fatal(err)
		}
		n, err := rc.Seek(0, io.SeekEnd)
		rc.Close()
		if err != nil {
			b.Fatal(err)
		}
		sum += n
	}
	return sum
}

func readFile(ctx context.Context, fx fixture, buf *[readBufSize]byte) error {
	rc, err := fx.st.GetReconstructedFile(ctx, fx.digest)
	if err != nil {
		return err
	}
	defer rc.Close()
	n, err := readLoop32K(rc, buf)
	if err == nil && n != fileSize {
		err = fmt.Errorf("read %d bytes, want %d", n, fileSize)
	}
	return err
}

// benchmarkReaders times readers concurrent whole-file reads, each through
// its own handle and buffer.
func benchmarkReaders(b *testing.B, fx fixture, readers int) {
	ctx := b.Context()
	bufs := make([][readBufSize]byte, readers)
	errc := make(chan error, readers)
	b.SetBytes(int64(readers) * fileSize)
	b.ReportAllocs()
	for b.Loop() {
		var wg sync.WaitGroup
		for i := range readers {
			wg.Go(func() { errc <- readFile(ctx, fx, &bufs[i]) })
		}
		wg.Wait()
		for range readers {
			if err := <-errc; err != nil {
				b.Fatal(err)
			}
		}
	}
}

// benchmarkCeiling times the same read loop over the raw stored xorbs, which
// is what serving them without decoding would cost.
func benchmarkCeiling(b *testing.B, fx fixture) {
	ctx := b.Context()
	packed := packedSize(b, fx.st, fx.xorbs)
	buf := new([readBufSize]byte)
	b.SetBytes(packed)
	b.ReportAllocs()
	for b.Loop() {
		var n int64
		for _, h := range fx.xorbs {
			rc, err := fx.st.GetXorbReadSeekCloser(ctx, h)
			if err != nil {
				b.Fatal(err)
			}
			m, err := readLoop32K(rc, buf)
			rc.Close()
			if err != nil {
				b.Fatal(err)
			}
			n += m
		}
		if n != packed {
			b.Fatalf("read %d raw xorb bytes, want %d", n, packed)
		}
	}
}

// BenchmarkReconstructedFile reads a 64 MiB file of 4 xorbs x 256 x 64 KiB
// chunks through GetReconstructedFile, per compression class.
func BenchmarkReconstructedFile(b *testing.B) {
	for _, c := range benchClasses {
		b.Run("class="+c.name, func(b *testing.B) {
			fx := buildFixture(b, c)
			for x, h := range fx.xorbs {
				assertXorbClass(b, fx.st, x, h, c)
			}
			for _, readers := range []int{1, 4} {
				b.Run(fmt.Sprintf("readers=%d", readers), func(b *testing.B) { benchmarkReaders(b, fx, readers) })
			}
			if c.name == "none" {
				b.Run("ceiling", func(b *testing.B) { benchmarkCeiling(b, fx) })
			}
		})
	}
}
