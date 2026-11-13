package storagetest

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	iofs "io/fs"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/pins"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/xorb"
)

// storeFile stores data with storage.PutFile, from a reader already past
// unrelated bytes, and checks the file commits under the hash an independent
// chunking of data predicts.
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
	want := xet.ComputeFileHash(f.ChunkHashes, sizes)
	digest := sha256.Sum256(data)
	f.SHA256Hex = hex.EncodeToString(digest[:])

	const skipped = "bytes before the file"
	r := bytes.NewReader(append([]byte(skipped), data...))
	if _, err := r.Seek(int64(len(skipped)), io.SeekStart); err != nil {
		t.Fatal(err)
	}
	got, err := storage.PutFile(ctx, st, r)
	if err != nil || got != want {
		t.Fatalf("PutFile() = %s, %v; want %s", got.String(), err, want.String())
	}
	f.FileHash = got
	assertFilesCommitted(t, ctx, st, f)
	return f
}

// assertNewChunks checks f's shard packs exactly f's chunks missing from
// stored, once each and in file order, into xorbs that carry a footer.
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
	var got []xet.ChunkHash
	for _, cb := range sh.CASInfos {
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

// PutFile packs each chunk no stored shard holds once into one xorb; storing
// the file again adds no xorb, and a file sharing its leading half packs only
// the chunks it lacks.
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

	// Chunking restarts its hash at each boundary, so the second file repeats
	// the first's chunks up to the last boundary inside the shared half.
	f2 := storeFile(t, ctx, st, slices.Concat(first[:len(first)/2], fresh))
	if n := countXorbs(t, ctx, st); n != 2 {
		t.Fatalf("xorbs after PutFile of a half-shared file = %d, want 2", n)
	}
	assertNewChunks(t, ctx, st, f2, f1.ChunkHashes)
}

// An empty file stores nothing: no xorb, no shard, no index entry.
func testPutFileEmpty(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	fileHash, err := storage.PutFile(ctx, st, bytes.NewReader(nil))
	if err != nil || fileHash != (xet.FileHash{}) {
		t.Fatalf("PutFile(empty) = %s, %v; want the zero hash", fileHash.String(), err)
	}
	if usage, err := st.Usage(ctx); err != nil || usage != (storage.Usage{}) {
		t.Fatalf("Usage() after PutFile(empty) = %+v, %v; want nothing stored", usage, err)
	}
	if _, err := st.GetShard(ctx, xet.FileHash{}); !errors.Is(err, iofs.ErrNotExist) {
		t.Fatalf("GetShard(zero hash) = %v, want not found", err)
	}
}

// hookedPutStore wraps a store for the PutFile pin cases, recording the xorb of every PutXorb call.
type hookedPutStore struct {
	storage.Storage
	onPutXorb   func(xet.XorbHash) // runs as each PutXorb starts
	holdShards  chan struct{}      // when set, each PutShard signals shardHeld, then waits, ignoring ctx, for it to close
	shardHeld   chan struct{}
	putXorbErr  error // when set, PutXorb fails with it instead of storing
	putShardErr error // when set, PutShard fails with it instead of storing

	mu    sync.Mutex
	xorbs []xet.XorbHash
}

func (s *hookedPutStore) PutXorb(ctx context.Context, xorbHash xet.XorbHash, r io.Reader) (bool, error) {
	s.mu.Lock()
	s.xorbs = append(s.xorbs, xorbHash)
	s.mu.Unlock()
	if s.onPutXorb != nil {
		s.onPutXorb(xorbHash)
	}
	if s.putXorbErr != nil {
		return false, s.putXorbErr
	}
	return s.Storage.PutXorb(ctx, xorbHash, r)
}

func (s *hookedPutStore) PutShard(ctx context.Context, sh *shard.Shard) (bool, error) {
	if s.holdShards != nil {
		select {
		case s.shardHeld <- struct{}{}:
		default:
		}
		<-s.holdShards
	}
	if s.putShardErr != nil {
		return false, s.putShardErr
	}
	return s.Storage.PutShard(ctx, sh)
}

// putXorbs returns the xorbs of the PutXorb calls so far, in call order.
func (s *hookedPutStore) putXorbs() []xet.XorbHash {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.xorbs)
}

// PutFile pins each xorb in the WithPins table before its PutXorb starts and holds none once it returned.
func testPutFilePinsXorbBeforePutXorb(t *testing.T, b Backend) {
	ctx := context.Background()
	table := pins.New[xet.XorbHash]()
	st := &hookedPutStore{Storage: b.New(t)}
	st.onPutXorb = func(h xet.XorbHash) {
		if !table.Pinned(h) {
			t.Errorf("PutXorb(%s) started with the xorb unpinned, want it pinned in the WithPins table first", h)
		}
	}
	data := make([]byte, 3<<19)
	_, _ = rand.NewChaCha8([32]byte{}).Read(data)
	if _, err := storage.PutFile(ctx, st, bytes.NewReader(data), storage.WithPins(table)); err != nil {
		t.Fatalf("PutFile(): %v", err)
	}
	xorbs := st.putXorbs()
	if len(xorbs) == 0 {
		t.Fatal("PutFile() of fresh bytes made no PutXorb")
	}
	for _, h := range xorbs {
		if table.Pinned(h) {
			t.Fatalf("xorb %s still pinned after PutFile() returned, want every pin released", h)
		}
	}
}

// PutFile keeps the stored xorb it dedups against and the xorbs it stores pinned while its shard is pending, and releases them once it returned.
func testPutFilePinsDedupXorb(t *testing.T, b Backend) {
	ctx := context.Background()
	inner := b.New(t)
	random := make([]byte, 3<<19)
	_, _ = rand.NewChaCha8([32]byte{}).Read(random)
	first, fresh := slices.Concat(random[:1<<20], random[:1<<19]), random[1<<20:]
	stored, err := inner.GetShard(ctx, storeFile(t, ctx, inner, first).FileHash)
	if err != nil {
		t.Fatalf("GetShard(): %v", err)
	}
	reused := stored.CASInfos[0].CASHash

	table := pins.New[xet.XorbHash]()
	st := &hookedPutStore{Storage: inner, holdShards: make(chan struct{}), shardHeld: make(chan struct{}, 1)}
	release := sync.OnceFunc(func() { close(st.holdShards) })
	var fileHash xet.FileHash
	var putErr error
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		fileHash, putErr = storage.PutFile(ctx, st, bytes.NewReader(slices.Concat(first[:len(first)/2], fresh)), storage.WithPins(table))
	}()
	t.Cleanup(func() {
		release()
		<-finished
	})
	// Leave headroom so a hang fails here, not at the binary's deadline.
	wait := 10 * time.Minute
	if deadline, ok := t.Deadline(); ok {
		wait = min(wait, time.Until(deadline)*9/10)
	}
	select {
	case <-st.shardHeld:
	case <-finished:
		t.Fatalf("PutFile() returned %v before its PutShard", putErr)
	case <-time.After(wait):
		t.Fatalf("PutFile() reached no PutShard within %v", wait)
	}
	if !table.Pinned(reused) {
		t.Fatalf("reused xorb %s unpinned while the shard is pending, want the stored xorb PutFile dedups against pinned", reused)
	}
	own := st.putXorbs()
	if len(own) == 0 {
		t.Fatal("PutFile() made no PutXorb for the fresh half")
	}
	for _, h := range own {
		if !table.Pinned(h) {
			t.Fatalf("stored xorb %s unpinned while the shard is pending, want it pinned until PutFile returns", h)
		}
	}
	release()
	<-finished
	if putErr != nil {
		t.Fatalf("PutFile(): %v", putErr)
	}
	for _, h := range append(own, reused) {
		if table.Pinned(h) {
			t.Fatalf("xorb %s still pinned after PutFile() returned, want every pin released", h)
		}
	}
	sh, err := inner.GetShard(ctx, fileHash)
	if err != nil {
		t.Fatalf("GetShard(): %v", err)
	}
	if !slices.ContainsFunc(sh.Files[0].Entries, func(e shard.FileDataSequenceEntry) bool { return e.CASHash == reused }) {
		t.Fatalf("the file's shard references no chunk of the stored xorb %s, want its shared half reused", reused)
	}
}

// PutFile releases every pin it took once it returns, whether it stored the file or a PutXorb or the PutShard failed.
func testPutFileReleasesPinsOnReturn(t *testing.T, b Backend) {
	ctx := context.Background()
	data := make([]byte, 1<<18)
	_, _ = rand.NewChaCha8([32]byte{}).Read(data)
	for _, tc := range []struct {
		name                    string
		putXorbErr, putShardErr error
	}{
		{"stored", nil, nil},
		{"PutXorb refused", errors.New("put xorb: refused"), nil},
		{"PutShard refused", nil, errors.New("put shard: refused")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := pins.New[xet.XorbHash]()
			st := &hookedPutStore{Storage: b.New(t), putXorbErr: tc.putXorbErr, putShardErr: tc.putShardErr}
			st.onPutXorb = func(h xet.XorbHash) {
				if !table.Pinned(h) {
					t.Errorf("PutXorb(%s) started with the xorb unpinned: PutFile took no pin to release", h)
				}
			}
			_, err := storage.PutFile(ctx, st, bytes.NewReader(data), storage.WithPins(table))
			if want := cmp.Or(tc.putXorbErr, tc.putShardErr); !errors.Is(err, want) {
				t.Fatalf("PutFile() = %v, want %v", err, want)
			}
			xorbs := st.putXorbs()
			if len(xorbs) == 0 {
				t.Fatal("PutFile() of fresh bytes made no PutXorb")
			}
			for _, h := range xorbs {
				if table.Pinned(h) {
					t.Fatalf("xorb %s still pinned after PutFile() returned, want every pin released", h)
				}
			}
		})
	}
	if _, err := storage.PutFile(ctx, b.New(t), bytes.NewReader(data)); err != nil {
		t.Fatalf("PutFile() without options: %v", err)
	}
	if _, err := storage.PutFile(ctx, b.New(t), bytes.NewReader(data), storage.WithPins(nil)); err != nil {
		t.Fatalf("PutFile() WithPins(nil): %v", err)
	}
}
