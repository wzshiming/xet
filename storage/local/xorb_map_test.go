package local

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/storagetest"
	"github.com/wzshiming/xet/xorb"
)

// newRangeStore returns a store over a fresh temp dir.
func newRangeStore(t *testing.T, opts ...Option) *Storage {
	t.Helper()
	fs, err := NewStorage(append([]Option{WithBasePath(t.TempDir())}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	return fs
}

// putRandomXorb stores four incompressible 4 KiB chunks drawn from seed and returns the bytes it put.
func putRandomXorb(t *testing.T, fs *Storage, seed byte) ([]byte, xet.XorbHash) {
	t.Helper()
	rng := rand.NewChaCha8([32]byte{seed})
	chunks := make([][]byte, 4)
	for i := range chunks {
		chunks[i] = make([]byte, 4096)
		rng.Read(chunks[i])
	}
	encoded, xorbHash := storagetest.EncodeXorb(t, false, chunks...)
	if _, err := fs.PutXorb(context.Background(), xorbHash, bytes.NewReader(encoded)); err != nil {
		t.Fatal(err)
	}
	return encoded, xorbHash
}

func mustOpen(t *testing.T, fs *Storage, xorbHash xet.XorbHash) io.ReadSeekCloser {
	t.Helper()
	rc, err := fs.GetXorbReadSeekCloser(context.Background(), xorbHash)
	if err != nil {
		t.Fatalf("GetXorbReadSeekCloser: %v", err)
	}
	return rc
}

// drained is what a reader returned up to EOF, or its first error.
type drained struct {
	data []byte
	err  error
}

// drainXorb opens, reads and closes the whole xorb without a *testing.T, so it can run in the background.
func drainXorb(fs *Storage, xorbHash xet.XorbHash) drained {
	rc, err := fs.GetXorbReadSeekCloser(context.Background(), xorbHash)
	if err != nil {
		return drained{err: err}
	}
	data, err := io.ReadAll(rc)
	if closeErr := rc.Close(); err == nil {
		err = closeErr
	}
	return drained{data, err}
}

// inBackground runs call on its own goroutine so await bounds a call that blocks.
func inBackground[T any](call func() T) <-chan T {
	done := make(chan T, 1)
	go func() { done <- call() }()
	return done
}

func assertDrained(t *testing.T, what string, got drained, want []byte) {
	t.Helper()
	if got.err != nil {
		t.Fatalf("%s: %v", what, got.err)
	}
	if !bytes.Equal(got.data, want) {
		t.Fatalf("%s read %d bytes that differ from the %d stored bytes", what, len(got.data), len(want))
	}
}

// assertNext reads the next len(want) bytes of r.
func assertNext(t *testing.T, r io.Reader, want []byte) {
	t.Helper()
	got := make([]byte, len(want))
	n, err := io.ReadFull(r, got)
	assertDrained(t, "open reader", drained{got[:n], err}, want)
}

// assertRest reads r to EOF.
func assertRest(t *testing.T, r io.Reader, want []byte) {
	t.Helper()
	got, err := io.ReadAll(r)
	assertDrained(t, "open reader", drained{got, err}, want)
}

func TestDeleteXorbWhileReaderOpen(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows cannot delete a file with a mapped view")
	}
	ctx := context.Background()
	fs := newRangeStore(t)
	encoded, x := putRandomXorb(t, fs, 1)
	a := mustOpen(t, fs, x)
	defer a.Close()
	assertNext(t, a, encoded[:7])

	if err := await(t, inBackground(func() error { return fs.DeleteXorb(ctx, x) })); err != nil {
		t.Fatalf("DeleteXorb with a reader open: %v", err)
	}
	if ok, err := fs.HasXorb(ctx, x); err != nil || ok {
		t.Fatalf("HasXorb after DeleteXorb = %v, %v; want false, nil", ok, err)
	}
	assertRest(t, a, encoded[7:])
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if mapped, cached := fs.xorbCache.Mapped(), fs.xorbCache.Len(); mapped != 0 || cached != 0 {
		t.Fatalf("after the deleted xorb's last reader closed: xorbCache.Mapped() = %d, Len() = %d; want 0, 0", mapped, cached)
	}
}

func TestZeroByteXorb(t *testing.T) {
	ctx := context.Background()
	fs := newRangeStore(t)
	x := xet.XorbHash{0xe}
	xorbPath := fs.objectPath("xorbs", x.String())
	if err := os.MkdirAll(filepath.Dir(xorbPath), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(xorbPath, nil, 0644); err != nil {
		t.Fatal(err)
	}

	rc := mustOpen(t, fs, x)
	defer rc.Close()
	if cached, mapped := fs.xorbCache.Len(), fs.xorbCache.Mapped(); cached != 1 || mapped != 0 {
		t.Fatalf("cached 0-byte xorb: xorbCache.Len() = %d, Mapped() = %d; want 1, 0", cached, mapped)
	}
	if n, err := rc.Read(make([]byte, 16)); n != 0 || err != io.EOF {
		t.Fatalf("Read of a 0-byte xorb = %d, %v; want 0, io.EOF", n, err)
	}
	if end, err := rc.Seek(0, io.SeekEnd); end != 0 || err != nil {
		t.Fatalf("Seek(0, SeekEnd) of a 0-byte xorb = %d, %v; want 0, nil", end, err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close of a 0-byte xorb reader = %v, want nil", err)
	}

	wantOffsets, wantErr := storage.ReadXorbChunkOffsets(bytes.NewReader(nil))
	if offsets, err := fs.GetXorbChunkOffsets(ctx, x); !slices.Equal(offsets, wantOffsets) || !errors.Is(err, wantErr) {
		t.Fatalf("GetXorbChunkOffsets of a 0-byte xorb = %v, %v; want %v, %v like storage.ReadXorbChunkOffsets of no bytes", offsets, err, wantOffsets, wantErr)
	}
}

func TestXorbIndexBounded(t *testing.T) {
	fs := newRangeStore(t, WithXorbCacheSize(2))
	for seed := range byte(5) {
		encoded, x := putRandomXorb(t, fs, seed+1)
		assertDrained(t, fmt.Sprintf("reader of xorb %d", seed+1), drainXorb(fs, x), encoded)
	}
	if cached, mapped := fs.xorbCache.Len(), fs.xorbCache.Mapped(); cached != 2 || mapped != 2 {
		t.Fatalf("after reading 5 xorbs through a 2-entry cache: xorbCache.Len() = %d, Mapped() = %d; want 2, 2", cached, mapped)
	}
}

func TestXorbReaderMissingXorbIsNotExist(t *testing.T) {
	fs := newRangeStore(t)
	if _, err := fs.GetXorbReadSeekCloser(context.Background(), xet.XorbHash{1}); !errors.Is(err, iofs.ErrNotExist) {
		t.Fatalf("GetXorbReadSeekCloser(missing xorb) error = %v, want fs.ErrNotExist", err)
	}
	if cached, mapped := fs.xorbCache.Len(), fs.xorbCache.Mapped(); cached != 0 || mapped != 0 {
		t.Fatalf("after a failed open: xorbCache.Len() = %d, Mapped() = %d; want 0, 0", cached, mapped)
	}
}

func TestXorbMappedMatchesCacheWhenIdle(t *testing.T) {
	ctx := context.Background()
	fs := newRangeStore(t, WithXorbCacheSize(1))
	encodedX, x := putRandomXorb(t, fs, 1)
	encodedY, y := putRandomXorb(t, fs, 2)
	assertIdle := func(after string, want int) {
		t.Helper()
		if mapped, cached := fs.xorbCache.Mapped(), fs.xorbCache.Len(); mapped != int64(cached) || cached != want {
			t.Fatalf("after %s: xorbCache.Mapped() = %d, Len() = %d; want %d, %d", after, mapped, cached, want, want)
		}
	}

	assertDrained(t, "reader of X", drainXorb(fs, x), encodedX)
	assertIdle("a reader of X closed", 1)
	if _, err := fs.GetXorbChunkOffsets(ctx, x); err != nil {
		t.Fatal(err)
	}
	assertIdle("loading X's chunk offsets", 1)

	a := mustOpen(t, fs, x)
	defer a.Close()
	assertDrained(t, "reader of Y evicting X", await(t, inBackground(func() drained { return drainXorb(fs, y) })), encodedY)
	assertRest(t, a, encodedX)
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	assertIdle("the last reader of evicted X closed", 1)

	for _, xorbHash := range []xet.XorbHash{x, y} {
		if err := fs.DeleteXorb(ctx, xorbHash); err != nil {
			t.Fatal(err)
		}
	}
	assertIdle("deleting every xorb", 0)
}

func TestXorbChunkOffsetsShareOpenReaderMap(t *testing.T) {
	fs := newRangeStore(t)
	encoded, x := putRandomXorb(t, fs, 1)
	wantStart, wantEnd, err := xorb.ChunkDataRange(bytes.NewReader(encoded), 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	a := mustOpen(t, fs, x)
	defer a.Close()
	assertNext(t, a, encoded[:7])

	type byteRange struct {
		start, end int64
		err        error
	}
	got := await(t, inBackground(func() byteRange {
		offsets, err := fs.GetXorbChunkOffsets(context.Background(), x)
		if err != nil {
			return byteRange{err: err}
		}
		start, end, err := xorb.ChunkDataRangeFromOffsets(offsets, 1, 3)
		return byteRange{start, end, err}
	}))
	if got.err != nil || got.start != wantStart || got.end != wantEnd {
		t.Fatalf("chunks [1, 3) from GetXorbChunkOffsets with a reader open = [%d, %d], %v; want [%d, %d]", got.start, got.end, got.err, wantStart, wantEnd)
	}
	if cached, mapped := fs.xorbCache.Len(), fs.xorbCache.Mapped(); cached != 1 || mapped != 1 {
		t.Fatalf("after loading X's chunk offsets with reader A open: xorbCache.Len() = %d, Mapped() = %d; want 1, 1", cached, mapped)
	}
	assertRest(t, a, encoded[7:])
}

// PutFile verifies its shard through the xorb files: no xorb it writes or reads
// enters the cache, and one a reader cached stays there.
func TestPutFileLeavesXorbCacheAlone(t *testing.T) {
	ctx := context.Background()
	fs := newRangeStore(t)
	assertCached := func(after string, want int) {
		t.Helper()
		if cached, mapped := fs.xorbCache.Len(), fs.xorbCache.Mapped(); cached != want || mapped != int64(want) {
			t.Fatalf("after %s: xorbCache.Len() = %d, Mapped() = %d; want %d, %d", after, cached, mapped, want, want)
		}
	}
	data := make([]byte, 2<<20)
	rand.NewChaCha8([32]byte{1}).Read(data)

	first, err := storage.PutFile(ctx, fs, bytes.NewReader(data[:1<<20]))
	if err != nil {
		t.Fatal(err)
	}
	assertCached("PutFile", 0)
	sh, err := fs.GetShard(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	reused := sh.CASInfos[0].CASHash
	if got := drainXorb(fs, reused); got.err != nil {
		t.Fatal(got.err)
	}
	assertCached("reading the first file's xorb", 1)

	// The second file starts with the first, so it has terms in that xorb.
	second, err := storage.PutFile(ctx, fs, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if sh, err = fs.GetShard(ctx, second); err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(sh.Files[0].Entries, func(e shard.FileDataSequenceEntry) bool { return e.CASHash == reused }) {
		t.Fatalf("the second file has no term in the first file's xorb %s", reused.String())
	}
	assertCached("PutFile of a file reusing the cached xorb", 1)
	if !fs.xorbCache.Has(reused) {
		t.Fatal("the reused xorb left the cache during PutFile")
	}
}
