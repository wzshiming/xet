package storage_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
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

// fixture is what a class keeps of its stored file once the payloads are dropped.
type fixture struct {
	st     storage.Storage
	sh     *shard.Shard
	digest [32]byte
	xorbs  []xet.XorbHash
}

// buildFixture stores the class's file on a fresh local store, checks that
// every chunk took the class's compression, then reads it back once untimed
// to check its size and SHA-256.
func buildFixture(b *testing.B, c storagetest.ChunkClass) fixture {
	b.Helper()
	st, err := local.NewStorage(local.WithBasePath(b.TempDir()))
	if err != nil {
		b.Fatal(err)
	}
	f := storagetest.PutXorbFile(b, b.Context(), st, c.Xorbs(xorbsPerFile, chunksPerXorb, chunkSize))
	storagetest.AssertFileCompression(b, b.Context(), st, f, c.Compression)
	fx := fixture{st: st, digest: storagetest.SHA256Digest(f.SHA256Hex), xorbs: f.XorbHashes}
	if fx.sh, err = st.GetShardBySHA256(b.Context(), fx.digest); err != nil {
		b.Fatal(err)
	}

	rc, err := storage.NewReconstructedFile(b.Context(), st, fx.sh, fx.digest)
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
	rc, err := storage.NewReconstructedFile(ctx, fx.st, fx.sh, fx.digest)
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
// chunks through NewReconstructedFile, per compression class.
func BenchmarkReconstructedFile(b *testing.B) {
	for _, c := range storagetest.ChunkClasses {
		b.Run("class="+c.Name, func(b *testing.B) {
			fx := buildFixture(b, c)
			for _, readers := range []int{1, 4} {
				b.Run(fmt.Sprintf("readers=%d", readers), func(b *testing.B) { benchmarkReaders(b, fx, readers) })
			}
			if c.Name == "none" {
				b.Run("ceiling", func(b *testing.B) { benchmarkCeiling(b, fx) })
			}
		})
	}
}
