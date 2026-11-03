package storagetest

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/pins"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/xorb"
)

// streamSize spans two xorbs; streamStall lies past every chunk of the first.
const (
	streamSize  = 70 << 20
	streamStall = 66 << 20
)

// dedupSeed draws bytes whose first sampling-eligible chunk past chunk 0 is chunk 12, under 1 MiB in.
const dedupSeed = 186

var (
	errRefused    = errors.New("refused by the test store")
	errReadFailed = errors.New("failed by the test source")
)

// sharedData holds streamSize bytes drawn from dedupSeed, generated once per test binary and never modified.
var sharedData = sync.OnceValue(func() []byte { return randomData(dedupSeed, streamSize) })

// storeFile stores data from a reader positioned past unrelated bytes, checked against an independent chunking.
func storeFile(t *testing.T, ctx context.Context, st storage.Storage, data []byte) File {
	t.Helper()
	f := File{Content: data}
	var sizes []uint64
	if err := xet.ChunkData(bytes.NewReader(data), func(_ int64, chunk []byte) error {
		f.ChunkHashes = append(f.ChunkHashes, xet.ComputeChunkHash(chunk))
		sizes = append(sizes, uint64(len(chunk)))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	want := storage.PutFileResult{FileHash: xet.ComputeFileHash(f.ChunkHashes, sizes), SHA256: digest, Size: int64(len(data))}
	f.SHA256Hex = hex.EncodeToString(digest[:])

	const skipped = "bytes before the file"
	src := io.NewSectionReader(bytes.NewReader(append([]byte(skipped), data...)), int64(len(skipped)), int64(len(data)))
	got, err := storage.PutFile(ctx, st, src)
	if err != nil || got != want {
		t.Fatalf("PutFile() = %s, %v; want %s", resultString(got), err, resultString(want))
	}
	f.FileHash = got.FileHash
	assertFilesCommitted(t, ctx, st, f)
	return f
}

// assertNewChunks checks f's shard packs exactly its chunks missing from stored, once each, in file order.
func assertNewChunks(t *testing.T, ctx context.Context, st storage.Storage, f File, stored []xet.ChunkHash) {
	t.Helper()
	var want []xet.ChunkHash
	for _, h := range f.ChunkHashes {
		if !slices.Contains(stored, h) && !slices.Contains(want, h) {
			want = append(want, h)
		}
	}
	sh, err := st.GetShard(ctx, f.FileHash)
	if err != nil {
		t.Fatalf("GetShard(): %v", err)
	}
	// BuildShard sorts CAS blocks by hash, not by where the file first needs them.
	blocks := slices.SortedFunc(slices.Values(sh.CASInfos), func(a, b shard.CASBlock) int {
		return cmp.Compare(slices.Index(want, a.Chunks[0].ChunkHash), slices.Index(want, b.Chunks[0].ChunkHash))
	})
	var got []xet.ChunkHash
	for _, cb := range blocks {
		for _, chunk := range cb.Chunks {
			got = append(got, chunk.ChunkHash)
		}
		rc, err := st.GetXorbReadSeekCloser(ctx, cb.CASHash)
		if err != nil {
			t.Fatal(err)
		}
		offsets, err := xorb.ReadChunkOffsets(rc)
		_ = rc.Close()
		if err != nil || len(offsets) != len(cb.Chunks) {
			t.Fatalf("xorb %s footer offsets = %d, %v; want %d", cb.CASHash.String(), len(offsets), err, len(cb.Chunks))
		}
	}
	// A fixture reusing no chunk could not tell reuse from packing everything.
	if len(want) == len(f.ChunkHashes) || !slices.Equal(got, want) {
		t.Fatalf("new CAS chunks = %d, want the %d of %d chunks not stored before", len(got), len(want), len(f.ChunkHashes))
	}
}

func countXorbs(t *testing.T, ctx context.Context, st storage.Storage) int {
	t.Helper()
	n := 0
	if err := st.WalkXorbs(ctx, func(string, int64, time.Time) error {
		n++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return n
}

func countShards(t *testing.T, ctx context.Context, st storage.Storage) int {
	t.Helper()
	n := 0
	if err := st.WalkShards(ctx, func(string, int64, time.Time) error {
		n++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return n
}

// PutFile packs only the chunks no stored shard holds, each once.
func testPutFileReusesStoredChunks(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	random := make([]byte, 3<<19)
	_, _ = rand.NewChaCha8([32]byte{}).Read(random)
	// The first file repeats its leading 512 KiB, so chunks also repeat within it.
	first, fresh := slices.Concat(random[:1<<20], random[:1<<19]), random[1<<20:]

	f1 := storeFile(t, ctx, st, first)
	if n := countXorbs(t, ctx, st); n != 1 {
		t.Fatalf("xorbs after PutFile = %d, want 1", n)
	}
	assertNewChunks(t, ctx, st, f1, nil)
	storeFile(t, ctx, st, first)
	if n := countXorbs(t, ctx, st); n != 1 {
		t.Fatalf("xorbs after repeated PutFile = %d, want 1", n)
	}

	// Chunking restarts at boundaries, so f2 repeats f1's chunks up to the shared half's last boundary.
	f2 := storeFile(t, ctx, st, slices.Concat(first[:len(first)/2], fresh))
	if n := countXorbs(t, ctx, st); n != 2 {
		t.Fatalf("xorbs after PutFile of a half-shared file = %d, want 2", n)
	}
	assertNewChunks(t, ctx, st, f2, f1.ChunkHashes)
}

// A dedup hit also serves the chunks still pending in the open group from the stored xorb.
func testPutFileDedupHitTakesPendingChunks(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	fresh, pHead, pTail, _ := dedupPieces(t)
	p := storeFile(t, ctx, st, slices.Concat(pHead, pTail))
	f := storeFile(t, ctx, st, slices.Concat(fresh, pHead, pTail))
	assertNewChunks(t, ctx, st, f, p.ChunkHashes)
	assertTailFrom(t, ctx, st, f, p, 0)
}

// A dedup hit leaves the chunks of an already closed group in that group's xorb.
func testPutFileDedupHitKeepsClosedGroup(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	fresh, pHead, pTail, filler := dedupPieces(t)
	if len(filler) <= xet.MaxXorbSize {
		t.Fatalf("a %d-byte filler cannot close the group holding pHead before the hit", len(filler))
	}
	p := storeFile(t, ctx, st, slices.Concat(pHead, pTail))
	f := storeFile(t, ctx, st, slices.Concat(fresh, pHead, filler, pTail))
	assertNewChunks(t, ctx, st, f, p.ChunkHashes[2:]) // pHead's two chunks stay in the closed group
	assertTailFrom(t, ctx, st, f, p, 2)
}

// dedupPieces cuts sharedData at chunk boundaries: pTail opens with its first sampling-eligible chunk past chunk 0.
func dedupPieces(t *testing.T) (fresh, pHead, pTail, filler []byte) {
	t.Helper()
	data := sharedData()
	var ends []int
	k := -1
	if err := xet.ChunkData(bytes.NewReader(data), func(offset int64, chunk []byte) error {
		if k < 0 && len(ends) > 0 && shard.IsChunkGlobalDedupEligible(xet.ComputeChunkHash(chunk), false, 0) {
			k = len(ends)
		}
		ends = append(ends, int(offset)+len(chunk))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if k < 3 || k+4 >= len(ends) {
		t.Fatalf("first sampling-eligible chunk past chunk 0 is %d of %d; want it at 3 or later, 4 before the last", k, len(ends))
	}
	return data[:ends[k-3]], data[ends[k-3]:ends[k-1]], data[ends[k-1]:ends[k+2]], data[ends[k+2]:ends[len(ends)-2]]
}

// assertTailFrom checks f's last term serves p's chunks from start on out of the xorb p's shard packed.
func assertTailFrom(t *testing.T, ctx context.Context, st storage.Storage, f, p File, start int) {
	t.Helper()
	psh, err := st.GetShard(ctx, p.FileHash)
	if err != nil {
		t.Fatalf("GetShard(): %v", err)
	}
	sh, err := st.GetShard(ctx, f.FileHash)
	if err != nil {
		t.Fatalf("GetShard(): %v", err)
	}
	terms := sh.Files[0].Entries
	got, want := terms[len(terms)-1], psh.Files[0].Entries[0]
	if got.CASHash != want.CASHash || got.ChunkIndexStart != uint32(start) || got.ChunkIndexEnd != want.ChunkIndexEnd {
		t.Fatalf("last term %s [%d, %d), want %s [%d, %d)",
			got.CASHash.String(), got.ChunkIndexStart, got.ChunkIndexEnd, want.CASHash.String(), start, want.ChunkIndexEnd)
	}
}

// An empty source stores nothing: no xorb, no shard, no index entry.
func testPutFileEmpty(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	rec := &recordingStore{Storage: st}
	got, err := storage.PutFile(ctx, rec, bytes.NewReader(nil))
	if want := (storage.PutFileResult{SHA256: sha256.Sum256(nil)}); err != nil || got != want {
		t.Fatalf("PutFile(empty) = %s, %v; want %s", resultString(got), err, resultString(want))
	}
	if calls := rec.history(); calls != "" {
		t.Fatalf("PutFile(empty) made store calls %q, want none", calls)
	}
	if usage, err := st.Usage(ctx); err != nil || usage != (storage.Usage{}) {
		t.Fatalf("Usage() after PutFile(empty) = %+v, %v; want nothing stored", usage, err)
	}
	if _, err := st.GetShard(ctx, xet.FileHash{}); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("GetShard(zero hash) = %v, want not found", err)
	}
}

// On an empty store PutFile packs a two-xorb file as GroupChunkIndicesBySize groups its chunks.
func testPutFileMatchesReferenceShard(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	data := randomData(1, streamSize)
	want, ref := referenceShard(t, data)
	got, err := storage.PutFile(ctx, st, bytes.NewReader(data))
	if err != nil || got != want {
		t.Fatalf("PutFile() = %s, %v; want %s", resultString(got), err, resultString(want))
	}
	sh, err := st.GetShard(ctx, got.FileHash)
	if err != nil {
		t.Fatalf("GetShard(): %v", err)
	}
	if !reflect.DeepEqual(sh.CASInfos, ref.CASInfos) {
		t.Fatalf("stored %d CAS blocks differ from the %d reference blocks", len(sh.CASInfos), len(ref.CASInfos))
	}
	if len(sh.Files) != 1 || !reflect.DeepEqual(sh.Files[0].Entries, ref.Files[0].Entries) {
		t.Fatalf("stored files %+v, want one with entries %+v", sh.Files, ref.Files[0].Entries)
	}
}

// PutFile stores the first xorb while Read stalls past it, then the second, and the shard last.
func testPutFileStoresXorbBeforeEOF(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	data := randomData(1, streamSize)
	want, _ := referenceShard(t, data)
	rec := &recordingStore{Storage: st}
	src := newStreamSource(ctx, data, streamStall)
	done := startPutFile(t, ctx, rec, src)

	waitFor(t, done, "xorb stored while Read stalls", func() bool { return rec.storedXorbs() > 0 })
	src.resume()
	out := awaitPutFile(t, done)
	if out.err != nil || out.res != want {
		t.Fatalf("PutFile() = %s, %v; want %s", resultString(out.res), out.err, resultString(want))
	}
	if calls := rec.history(); calls != "PutXorb PutXorb PutShard" {
		t.Fatalf("store calls %q, want two PutXorb then one PutShard", calls)
	}
	rc, err := st.GetReconstructedFile(ctx, want.SHA256)
	if err != nil {
		t.Fatalf("GetReconstructedFile(): %v", err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("reconstructed %d bytes, %v; want the %d stored", len(got), err, len(data))
	}
}

// A cancel while Read stalls fails PutFile with context.Canceled, keeps the stored xorb and commits nothing more.
func testPutFileCancelMidStream(t *testing.T, b Backend) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := b.New(t)
	rec := &recordingStore{Storage: st}
	src := newStreamSource(ctx, randomData(1, streamSize), streamStall)
	done := startPutFile(t, ctx, rec, src)

	waitFor(t, done, "xorb stored while Read stalls", func() bool { return rec.storedXorbs() > 0 })
	cancel()
	out := awaitPutFile(t, done)
	if !errors.Is(out.err, context.Canceled) {
		t.Fatalf("PutFile() error = %v, want context.Canceled", out.err)
	}
	if out.active != 0 {
		t.Fatalf("%d store or source calls still running when PutFile returned", out.active)
	}
	if calls := rec.history(); calls != "PutXorb" {
		t.Fatalf("store calls %q, want only the PutXorb before the cancel", calls)
	}
	bg := context.Background()
	if n := countShards(t, bg, st); n != 0 {
		t.Fatalf("shards after the cancel = %d, want 0", n)
	}
	if n := countXorbs(t, bg, st); n != 1 {
		t.Fatalf("xorbs after the cancel = %d, want the 1 stored before it", n)
	}
	if n := rec.calls.lateCalls() + src.calls.lateCalls(); n != 0 {
		t.Fatalf("%d store or source calls after PutFile returned", n)
	}
}

// A Read error returns PutFile only after the PutXorb in flight, which ignores ctx, has returned.
func testPutFileReadErrorAwaitsXorbInFlight(t *testing.T, b Backend) {
	ctx := context.Background()
	rec := &recordingStore{Storage: b.New(t), holdXorbs: make(chan struct{})}
	src := newStreamSource(ctx, sharedData(), streamStall)
	src.readErr = errReadFailed
	done := startPutFile(t, ctx, rec, src)
	release := sync.OnceFunc(func() { close(rec.holdXorbs) })
	t.Cleanup(release)

	waitFor(t, done, "held PutXorb with Read at the stall", func() bool {
		return rec.heldXorbs.Load() > 0 && src.delivered.Load() == streamStall
	})
	src.resume()
	// A return within this window left the held PutXorb running.
	select {
	case out := <-done:
		t.Fatalf("PutFile() returned %v with %d calls running, before its held PutXorb returned", out.err, out.active)
	case <-time.After(2 * time.Second):
	}
	release()
	out := awaitPutFile(t, done)
	if !errors.Is(out.err, errReadFailed) || out.active != 0 {
		t.Fatalf("PutFile() error = %v with %d calls running at return; want the Read error and none", out.err, out.active)
	}
}

// A refused PutXorb fails PutFile with that error and commits no shard.
func testPutFileRefusedXorbFails(t *testing.T, b Backend) {
	ctx := context.Background()
	rec := &recordingStore{Storage: b.New(t), putXorbErr: errRefused}
	if _, err := storage.PutFile(ctx, rec, bytes.NewReader(randomData(2, 3<<19))); !errors.Is(err, errRefused) {
		t.Fatalf("PutFile() error = %v, want the refused PutXorb's", err)
	}
	if calls := rec.history(); calls != "PutXorb" {
		t.Fatalf("store calls %q, want only the refused PutXorb", calls)
	}
}

// A ReadAt disagreeing with Read fails PutFile and commits no shard.
func testPutFileReadAtMismatchFails(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	data := randomData(3, 3<<19)
	src := newStreamSource(ctx, data, int64(len(data)))
	src.corrupt = true
	if _, err := storage.PutFile(ctx, st, src); err == nil || !strings.Contains(err.Error(), "xorb hash mismatch") {
		t.Fatalf("PutFile() error = %v, want the xorb hash mismatch PutXorb reports", err)
	}
	if n := countShards(t, ctx, st); n != 0 {
		t.Fatalf("shards after a disagreeing ReadAt = %d, want 0", n)
	}
}

// A rerun after a refused shard re-puts the same xorbs, inserting none, and commits the first run's shard.
func testPutFileRerunRestoresIdenticalXorbs(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	data := randomData(4, 3<<19)
	first := &recordingStore{Storage: st, putShardErr: errRefused}
	if _, err := storage.PutFile(ctx, first, bytes.NewReader(data)); err == nil {
		t.Fatal("PutFile() succeeded although PutShard was refused")
	}

	rerun := &recordingStore{Storage: st}
	got, err := storage.PutFile(ctx, rerun, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("rerun PutFile(): %v", err)
	}
	xorbs, _ := rerun.results()
	for _, x := range xorbs {
		if x.inserted || x.err != nil {
			t.Fatalf("rerun PutXorb() = %v, %v; want false, nil", x.inserted, x.err)
		}
	}
	firstXorbs, built := first.results()
	if !slices.EqualFunc(xorbs, firstXorbs, func(x, y putXorbResult) bool { return x.hash == y.hash }) {
		t.Fatalf("rerun put %d xorbs, want the %d the first run put, under the same hashes", len(xorbs), len(firstXorbs))
	}
	if shardHash, err := st.GetFileIndexEntry(ctx, got.FileHash); err != nil || len(built) != 1 || shardHash != built[0] {
		t.Fatalf("rerun committed shard %q, %v; want the first run's %q", shardHash, err, built)
	}
}

// An xorb reader a backend retained past its PutXorb fails with io.ErrClosedPipe while PutFile still runs and after it returned.
func testPutFileSealsXorbReaders(t *testing.T, b Backend) {
	ctx := context.Background()
	rec := &recordingStore{Storage: b.New(t), retainXorbs: true, holdXorbs: make(chan struct{}), holdAfter: 1}
	data := sharedData()
	src := newStreamSource(ctx, data, int64(len(data)))
	done := startPutFile(t, ctx, rec, src)
	release := sync.OnceFunc(func() { close(rec.holdXorbs) })
	t.Cleanup(release)

	waitFor(t, done, "held second PutXorb", func() bool { return rec.heldXorbs.Load() > 0 })
	rec.mu.Lock()
	first := rec.retained[0]
	rec.mu.Unlock()
	if n, err := first.Read(make([]byte, 1)); n != 0 || !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("Read on the first xorb's retained reader while PutFile runs = %d, %v; want 0, io.ErrClosedPipe: PutFile seals a reader once its PutXorb returned", n, err)
	}
	release()
	if out := awaitPutFile(t, done); out.err == nil {
		t.Fatal("PutFile() succeeded although the xorbs it put were never stored")
	}
	for i, rd := range rec.retained {
		if n, err := rd.Read(make([]byte, 1)); n != 0 || !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("Read on retained xorb reader %d after PutFile returned = %d, %v; want 0, io.ErrClosedPipe: PutFile seals its xorb readers", i, n, err)
		}
	}
	if n := src.calls.lateCalls(); n != 0 {
		t.Fatalf("%d source calls after PutFile returned", n)
	}
}

// PutFile with pins pins each xorb it stores before that xorb's PutXorb and holds none once it returned.
func testPutFilePinsXorbBeforePutXorb(t *testing.T, b Backend) {
	ctx := context.Background()
	table := pins.New[xet.XorbHash]()
	rec := &recordingStore{Storage: b.New(t)}
	rec.onPutXorb = func(h xet.XorbHash) {
		if !table.Pinned(h) {
			t.Errorf("xorb %s not pinned at its PutXorb", h.String())
		}
	}
	if _, err := storage.PutFile(ctx, rec, bytes.NewReader(randomData(5, 3<<19)), storage.WithPins(table)); err != nil {
		t.Fatalf("PutFile(): %v", err)
	}
	xorbs, _ := rec.results()
	if len(xorbs) == 0 {
		t.Fatal("PutFile() put no xorb")
	}
	for _, x := range xorbs {
		if table.Pinned(x.hash) {
			t.Errorf("xorb %s still pinned after PutFile returned", x.hash.String())
		}
	}
}

// PutFile with pins keeps the stored xorb a dedup hit reuses pinned beside its own until it returns.
func testPutFilePinsDedupXorb(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	fresh, pHead, pTail, _ := dedupPieces(t)
	p := storeFile(t, ctx, st, slices.Concat(pHead, pTail))
	psh, err := st.GetShard(ctx, p.FileHash)
	if err != nil {
		t.Fatalf("GetShard(): %v", err)
	}
	reused := psh.CASInfos[0].CASHash

	table := pins.New[xet.XorbHash]()
	rec := &recordingStore{Storage: st, holdShards: make(chan struct{})}
	data := slices.Concat(fresh, pHead, pTail)
	done := startPutFile(t, ctx, rec, newStreamSource(ctx, data, int64(len(data))), storage.WithPins(table))
	release := sync.OnceFunc(func() { close(rec.holdShards) })
	t.Cleanup(release)

	waitFor(t, done, "held PutShard", func() bool {
		_, shards := rec.results()
		return len(shards) > 0
	})
	own, _ := rec.results()
	if !table.Pinned(reused) {
		t.Errorf("reused xorb %s not pinned while PutFile commits its shard", reused.String())
	}
	for _, x := range own {
		if !table.Pinned(x.hash) {
			t.Errorf("own xorb %s not pinned while PutFile commits its shard", x.hash.String())
		}
	}
	release()
	out := awaitPutFile(t, done)
	if out.err != nil {
		t.Fatalf("PutFile(): %v", out.err)
	}
	assertTailFrom(t, ctx, st, File{FileHash: out.res.FileHash}, p, 0)
	if table.Pinned(reused) {
		t.Errorf("reused xorb %s still pinned after PutFile returned", reused.String())
	}
	for _, x := range own {
		if table.Pinned(x.hash) {
			t.Errorf("own xorb %s still pinned after PutFile returned", x.hash.String())
		}
	}
}

// PutFile with pins keeps the stored xorb its probed chunk is found in pinned until it returns, though no pending chunk moves to it.
func testPutFilePinsDedupXorbWithoutPendingChunks(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	fresh, _, pTail, _ := dedupPieces(t)
	p := storeFile(t, ctx, st, pTail)
	psh, err := st.GetShard(ctx, p.FileHash)
	if err != nil {
		t.Fatalf("GetShard(): %v", err)
	}
	reused := psh.CASInfos[0].CASHash

	table := pins.New[xet.XorbHash]()
	rec := &recordingStore{Storage: st, holdShards: make(chan struct{})}
	data := slices.Concat(fresh, pTail)
	done := startPutFile(t, ctx, rec, newStreamSource(ctx, data, int64(len(data))), storage.WithPins(table))
	release := sync.OnceFunc(func() { close(rec.holdShards) })
	t.Cleanup(release)

	waitFor(t, done, "held PutShard", func() bool {
		_, shards := rec.results()
		return len(shards) > 0
	})
	if !table.Pinned(reused) {
		t.Errorf("reused xorb %s, which no pending chunk moved to, not pinned while PutFile commits its shard", reused.String())
	}
	release()
	out := awaitPutFile(t, done)
	if out.err != nil {
		t.Fatalf("PutFile(): %v", out.err)
	}
	assertTailFrom(t, ctx, st, File{FileHash: out.res.FileHash}, p, 0)
	if table.Pinned(reused) {
		t.Errorf("reused xorb %s still pinned after PutFile returned", reused.String())
	}
}

// PutFile with pins keeps every stored xorb a dedup hit reuses pinned until it returns: the one its pending chunks move to and the one its probed chunk is found in.
func testPutFilePinsEveryDedupXorb(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	fresh, pHead, pTail, filler := dedupPieces(t)
	p := storeFile(t, ctx, st, slices.Concat(pHead, filler, pTail))
	psh, err := st.GetShard(ctx, p.FileHash)
	if err != nil {
		t.Fatalf("GetShard(): %v", err)
	}
	if len(psh.CASInfos) != 2 {
		t.Skipf("stored file packs %d xorbs, want 2 so pHead's and pTail's chunks lie in different ones", len(psh.CASInfos))
	}
	// BuildShard sorts CAS blocks by hash; pHead's first chunk opens its xorb.
	head, tail := psh.CASInfos[0].CASHash, psh.CASInfos[1].CASHash
	if psh.CASInfos[1].Chunks[0].ChunkHash == p.ChunkHashes[0] {
		head, tail = tail, head
	}

	table := pins.New[xet.XorbHash]()
	rec := &recordingStore{Storage: st, holdShards: make(chan struct{})}
	data := slices.Concat(fresh, pHead, pTail)
	done := startPutFile(t, ctx, rec, newStreamSource(ctx, data, int64(len(data))), storage.WithPins(table))
	release := sync.OnceFunc(func() { close(rec.holdShards) })
	t.Cleanup(release)

	waitFor(t, done, "held PutShard", func() bool {
		_, shards := rec.results()
		return len(shards) > 0
	})
	if !table.Pinned(head) {
		t.Errorf("reused xorb %s, which pHead's pending chunks moved to, not pinned while PutFile commits its shard", head.String())
	}
	if !table.Pinned(tail) {
		t.Errorf("reused xorb %s, which pTail's probed chunk was found in, not pinned while PutFile commits its shard", tail.String())
	}
	release()
	if out := awaitPutFile(t, done); out.err != nil {
		t.Fatalf("PutFile(): %v", out.err)
	}
	for _, h := range []xet.XorbHash{head, tail} {
		if table.Pinned(h) {
			t.Errorf("reused xorb %s still pinned after PutFile returned", h.String())
		}
	}
}

// PutFile releases every pin it took once it returns, committed or failed.
func testPutFileReleasesPinsOnReturn(t *testing.T, b Backend) {
	ctx := context.Background()
	data := randomData(6, 3<<19)
	table := pins.New[xet.XorbHash]()
	for _, tc := range []struct {
		name                    string
		putXorbErr, putShardErr error
	}{
		{"committed", nil, nil},
		{"refused PutXorb", errRefused, nil},
		{"refused PutShard", nil, errRefused},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &recordingStore{Storage: b.New(t), putXorbErr: tc.putXorbErr, putShardErr: tc.putShardErr}
			rec.onPutXorb = func(h xet.XorbHash) {
				if !table.Pinned(h) {
					t.Errorf("xorb %s not pinned at its PutXorb, so its release goes untested", h.String())
				}
			}
			_, err := storage.PutFile(ctx, rec, bytes.NewReader(data), storage.WithPins(table))
			if want := cmp.Or(tc.putXorbErr, tc.putShardErr); !errors.Is(err, want) {
				t.Fatalf("PutFile() error = %v, want %v", err, want)
			}
			xorbs, _ := rec.results()
			if len(xorbs) == 0 {
				t.Fatal("PutFile() put no xorb")
			}
			for _, x := range xorbs {
				if table.Pinned(x.hash) {
					t.Errorf("xorb %s still pinned after PutFile returned", x.hash.String())
				}
			}
		})
	}
	// These prove only that PutFile without a table, or with a nil one, still stores the file.
	for _, tc := range []struct {
		name string
		opts []storage.PutFileOption
	}{
		{"no option", nil},
		{"WithPins(nil)", []storage.PutFileOption{storage.WithPins(nil)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &recordingStore{Storage: b.New(t)}
			if _, err := storage.PutFile(ctx, rec, bytes.NewReader(data), tc.opts...); err != nil {
				t.Fatalf("PutFile(): %v", err)
			}
			if xorbs, _ := rec.results(); len(xorbs) == 0 {
				t.Fatal("PutFile() put no xorb")
			}
		})
	}
}

func resultString(r storage.PutFileResult) string {
	return fmt.Sprintf("{%s %x %d}", r.FileHash.String(), r.SHA256, r.Size)
}

// randomData returns n bytes drawn from seed.
func randomData(seed byte, n int) []byte {
	data := make([]byte, n)
	_, _ = rand.NewChaCha8([32]byte{seed}).Read(data)
	return data
}

// referenceShard predicts PutFile's result for data and the shard it commits on an empty store.
func referenceShard(t *testing.T, data []byte) (storage.PutFileResult, *shard.Shard) {
	t.Helper()
	var hashes []xet.ChunkHash
	var sizes []uint64
	var chunks []shard.ChunkInfo
	var file shard.FileInfo
	unique := map[xet.ChunkHash]int{}
	if err := xet.ChunkData(bytes.NewReader(data), func(_ int64, chunk []byte) error {
		h := xet.ComputeChunkHash(chunk)
		hashes = append(hashes, h)
		sizes = append(sizes, uint64(len(chunk)))
		i, ok := unique[h]
		if !ok {
			i = len(chunks)
			unique[h] = i
			chunks = append(chunks, shard.ChunkInfo{Hash: h, Size: uint32(len(chunk)), IsNew: true})
		}
		file.ChunkIndices = append(file.ChunkIndices, i)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	chunkSizes := make([]uint32, len(chunks))
	for i, c := range chunks {
		chunkSizes[i] = c.Size
	}
	for _, group := range xorb.GroupChunkIndicesBySize(chunkSizes, xet.MaxXorbSize) {
		var groupHashes []xet.ChunkHash
		var groupSizes []uint64
		for _, i := range group {
			groupHashes = append(groupHashes, chunks[i].Hash)
			groupSizes = append(groupSizes, uint64(chunks[i].Size))
		}
		xorbHash := xet.ComputeXorbHash(groupHashes, groupSizes)
		for j, i := range group {
			chunks[i].XorbHash, chunks[i].ChunkIndex = xorbHash, uint32(j)
		}
	}
	res := storage.PutFileResult{FileHash: xet.ComputeFileHash(hashes, sizes), SHA256: sha256.Sum256(data), Size: int64(len(data))}
	file.Hash, file.SHA256 = res.FileHash, res.SHA256
	return res, shard.BuildShard([]shard.FileInfo{file}, chunks)
}

// callTracker counts calls in flight and calls made after PutFile returned.
type callTracker struct {
	mu       sync.Mutex
	active   int
	late     int
	returned bool
}

func (c *callTracker) enter() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.active++
	if c.returned {
		c.late++
	}
}

func (c *callTracker) exit() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.active--
}

// markReturned records that PutFile returned and reports the calls still running.
func (c *callTracker) markReturned() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.returned = true
	return c.active
}

func (c *callTracker) lateCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.late
}

// recordingStore logs PutXorb and PutShard calls, failing them with putXorbErr or putShardErr when set.
type recordingStore struct {
	storage.Storage
	putXorbErr  error
	putShardErr error
	holdXorbs   chan struct{} // when set, PutXorb waits for it to close, ignoring ctx
	holdAfter   int           // PutXorb calls let past holdXorbs before it holds the rest
	retainXorbs bool          // when set, PutXorb keeps its reader unread and reports an insert, as an asynchronous backend

	onPutXorb  func(xet.XorbHash) // when set, PutXorb calls it on entry
	holdShards chan struct{}      // when set, PutShard waits for it to close, ignoring ctx

	calls     callTracker
	putXorbs  atomic.Int32 // PutXorb calls started
	heldXorbs atomic.Int32 // PutXorb calls that reached holdXorbs
	mu        sync.Mutex
	log       []string // "PutXorb" as each returns, "PutShard" as each starts
	xorbs     []putXorbResult
	shards    []string    // the hash each PutShard argument encodes to
	retained  []io.Reader // the readers PutXorb kept under retainXorbs
}

type putXorbResult struct {
	hash     xet.XorbHash
	inserted bool
	err      error
}

func (r *recordingStore) PutXorb(ctx context.Context, xorbHash xet.XorbHash, rd io.Reader) (bool, error) {
	r.calls.enter()
	defer r.calls.exit()
	if r.onPutXorb != nil {
		r.onPutXorb(xorbHash)
	}
	if started := r.putXorbs.Add(1); r.holdXorbs != nil && int(started) > r.holdAfter {
		r.heldXorbs.Add(1)
		<-r.holdXorbs
	}
	inserted, err := false, r.putXorbErr
	if err == nil && r.retainXorbs {
		inserted = true
	} else if err == nil {
		inserted, err = r.Storage.PutXorb(ctx, xorbHash, rd)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.retainXorbs {
		r.retained = append(r.retained, rd)
	}
	r.log = append(r.log, "PutXorb")
	r.xorbs = append(r.xorbs, putXorbResult{xorbHash, inserted, err})
	return inserted, err
}

func (r *recordingStore) PutShard(ctx context.Context, sh *shard.Shard) (bool, error) {
	r.calls.enter()
	defer r.calls.exit()
	_, shardHash, _ := storage.EncodeShard(sh)
	r.mu.Lock()
	r.log = append(r.log, "PutShard")
	r.shards = append(r.shards, shardHash)
	r.mu.Unlock()
	if r.holdShards != nil {
		<-r.holdShards
	}
	if r.putShardErr != nil {
		return false, r.putShardErr
	}
	return r.Storage.PutShard(ctx, sh)
}

func (r *recordingStore) GetShardByChunkHash(ctx context.Context, chunkHash xet.ChunkHash) (*shard.Shard, error) {
	r.calls.enter()
	defer r.calls.exit()
	return r.Storage.GetShardByChunkHash(ctx, chunkHash)
}

func (r *recordingStore) history() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return strings.Join(r.log, " ")
}

// storedXorbs counts the PutXorb calls that returned without error.
func (r *recordingStore) storedXorbs() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, x := range r.xorbs {
		if x.err == nil {
			n++
		}
	}
	return n
}

func (r *recordingStore) results() ([]putXorbResult, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.xorbs), slices.Clone(r.shards)
}

// streamSource holds Read at stall until resume or ctx ends; ReadAt serves only bytes Read delivered.
type streamSource struct {
	data    []byte
	stall   int64
	ctx     context.Context
	corrupt bool  // ReadAt flips the first byte it returns
	readErr error // once resumed, Read fails with it past stall
	resumed chan struct{}
	resume  func()

	delivered atomic.Int64
	calls     callTracker
}

func newStreamSource(ctx context.Context, data []byte, stall int64) *streamSource {
	s := &streamSource{data: data, stall: stall, ctx: ctx, resumed: make(chan struct{})}
	s.resume = sync.OnceFunc(func() { close(s.resumed) })
	return s
}

func (s *streamSource) Read(p []byte) (int, error) {
	s.calls.enter()
	defer s.calls.exit()
	off, end := s.delivered.Load(), int64(len(s.data))
	if off == end {
		return 0, io.EOF
	}
	select {
	case <-s.resumed:
	default:
		if off < s.stall {
			end = s.stall
		} else {
			select {
			case <-s.resumed:
			case <-s.ctx.Done():
				return 0, s.ctx.Err()
			}
		}
	}
	if s.readErr != nil && off >= s.stall {
		return 0, s.readErr
	}
	n := copy(p, s.data[off:end])
	s.delivered.Add(int64(n))
	return n, nil
}

func (s *streamSource) ReadAt(p []byte, off int64) (int, error) {
	s.calls.enter()
	defer s.calls.exit()
	end := off + int64(len(p))
	if delivered := s.delivered.Load(); off < 0 || end > delivered {
		return 0, fmt.Errorf("ReadAt [%d, %d) past the %d bytes Read delivered", off, end, delivered)
	}
	copy(p, s.data[off:end])
	if s.corrupt && len(p) > 0 {
		p[0] ^= 0xff
	}
	// io.ReaderAt may report io.EOF with a full read ending at the end of the input.
	if end == int64(len(s.data)) {
		return len(p), io.EOF
	}
	return len(p), nil
}

type putFileOutcome struct {
	res    storage.PutFileResult
	err    error
	active int // store and source calls still running when PutFile returned
}

// startPutFile runs PutFile with opts in the background; cleanup resumes src and awaits the return.
func startPutFile(t *testing.T, ctx context.Context, rec *recordingStore, src *streamSource, opts ...storage.PutFileOption) <-chan putFileOutcome {
	t.Helper()
	done := make(chan putFileOutcome, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		res, err := storage.PutFile(ctx, rec, src, opts...)
		done <- putFileOutcome{res, err, rec.calls.markReturned() + src.calls.markReturned()}
	}()
	t.Cleanup(func() {
		src.resume()
		wait := streamWait(t)
		select {
		case <-finished:
		case <-time.After(wait):
			t.Errorf("PutFile still running %v after its source resumed", wait)
		}
	})
	return done
}

// streamWait bounds a wait on PutFile so a hang fails the test before the binary's deadline.
func streamWait(t *testing.T) time.Duration {
	if deadline, ok := t.Deadline(); ok {
		return min(10*time.Minute, time.Until(deadline)*9/10)
	}
	return 10 * time.Minute
}

func awaitPutFile(t *testing.T, done <-chan putFileOutcome) putFileOutcome {
	t.Helper()
	wait := streamWait(t)
	select {
	case out := <-done:
		return out
	case <-time.After(wait):
		t.Fatalf("PutFile did not return within %v", wait)
		return putFileOutcome{}
	}
}

// waitFor polls cond until it holds, failing if PutFile returns first or the wait runs out.
func waitFor(t *testing.T, done <-chan putFileOutcome, what string, cond func() bool) {
	t.Helper()
	wait := streamWait(t)
	deadline := time.After(wait)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for !cond() {
		select {
		case out := <-done:
			t.Fatalf("PutFile returned %s, %v before any %s", resultString(out.res), out.err, what)
		case <-deadline:
			t.Fatalf("no %s within %v", what, wait)
		case <-tick.C:
		}
	}
}
