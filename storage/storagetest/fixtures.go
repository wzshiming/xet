package storagetest

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/xorb"
)

// NoGrace makes every stored object immediately sweepable.
const NoGrace = -time.Hour

// zeroSHA256Hex is the all-zero digest empty files store as their SHA-256.
const zeroSHA256Hex = "0000000000000000000000000000000000000000000000000000000000000000"

// File captures everything a stored test file owns so sweeps can be
// asserted object by object.
type File struct {
	FileHash    xet.FileHash
	ShardHash   string
	XorbHashes  []xet.XorbHash
	ChunkHashes []xet.ChunkHash
	SHA256Hex   string
	Content     []byte
}

// EncodeXorb serializes chunks as an xorb, with or without footer.
func EncodeXorb(t *testing.T, withFooter bool, chunks ...[]byte) ([]byte, xet.XorbHash) {
	t.Helper()
	var encoded bytes.Buffer
	enc := xorb.NewEncoder(&encoded, withFooter)
	for _, c := range chunks {
		if _, err := enc.Write(c); err != nil {
			t.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return encoded.Bytes(), enc.SummoryHash()
}

// AddFileBlock stores one single-chunk xorb per part and appends the
// matching file and CAS blocks to shardObj, returning the file's hashes.
func AddFileBlock(t *testing.T, ctx context.Context, st storage.Storage, shardObj *shard.Shard, parts [][]byte) (xet.FileHash, []xet.XorbHash, []xet.ChunkHash) {
	t.Helper()
	fileBlock := shard.FileBlock{}
	var xorbHashes []xet.XorbHash
	var chunkHashes []xet.ChunkHash
	var chunkSizes []uint64
	for _, part := range parts {
		encoded, xorbHash := EncodeXorb(t, true, part)
		if _, err := st.PutXorb(ctx, "default", xorbHash, bytes.NewReader(encoded)); err != nil {
			t.Fatal(err)
		}
		chunkHash := xet.ComputeChunkHash(part)
		xorbHashes = append(xorbHashes, xorbHash)
		chunkHashes = append(chunkHashes, chunkHash)
		chunkSizes = append(chunkSizes, uint64(len(part)))
		fileBlock.Entries = append(fileBlock.Entries, shard.FileDataSequenceEntry{
			CASHash: xorbHash, UnpackedSegBytes: uint32(len(part)), ChunkIndexEnd: 1,
		})
		shardObj.AddCASBlock(shard.CASBlock{
			CASHash: xorbHash,
			Chunks:  []shard.CASChunkSequenceEntry{{ChunkHash: chunkHash, UnpackedSegBytes: uint32(len(part))}},
		})
	}
	fileBlock.FileHash = xet.ComputeFileHash(chunkHashes, chunkSizes)
	shardObj.AddFile(fileBlock)
	return fileBlock.FileHash, xorbHashes, chunkHashes
}

// PutFile stores one file chunked as parts (one single-chunk xorb per
// part). Identical parts across files share the same xorb.
func PutFile(t *testing.T, ctx context.Context, st storage.Storage, parts [][]byte) File {
	t.Helper()
	shardObj := shard.NewShard()
	var f File
	f.FileHash, f.XorbHashes, f.ChunkHashes = AddFileBlock(t, ctx, st, shardObj, parts)
	for _, part := range parts {
		f.Content = append(f.Content, part...)
	}
	if _, err := st.PutShard(ctx, shardObj); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(f.Content)
	f.SHA256Hex = hex.EncodeToString(digest[:])

	if err := st.WalkFileIndex(ctx, func(fileHash, shardHash string) error {
		if fileHash == f.FileHash.String() {
			f.ShardHash = shardHash
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if f.ShardHash == "" {
		t.Fatalf("no file index entry for %s", f.FileHash.String())
	}
	return f
}

// UnlinkFile removes both of f's index entries — file and SHA-256 — so a
// sweep can prove the shard dead. Not for empty files (zero digest).
func UnlinkFile(t *testing.T, ctx context.Context, gcs storage.Storage, f File) {
	t.Helper()
	if _, err := storage.NewGC(gcs).Unlink(ctx, f.FileHash); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.NewGC(gcs).UnlinkSHA256(ctx, SHA256Digest(f.SHA256Hex)); err != nil {
		t.Fatal(err)
	}
}

// PutUnlinkedFiles stores one single-part file per content and unlinks
// both its entries, leaving its shard and xorb unreferenced.
func PutUnlinkedFiles(t *testing.T, ctx context.Context, st storage.Storage, contents ...string) []File {
	t.Helper()
	files := make([]File, 0, len(contents))
	for _, content := range contents {
		f := PutFile(t, ctx, st, [][]byte{[]byte(content)})
		UnlinkFile(t, ctx, st, f)
		files = append(files, f)
	}
	return files
}

// LegacyShardBytes serializes a shard the way it was stored before shard
// objects went footerless, and returns the bytes with their object name.
func LegacyShardBytes(t *testing.T, s *shard.Shard, creationTime uint64) ([]byte, string) {
	t.Helper()
	s.SetFooter(time.Unix(int64(creationTime), 0))
	r, err := s.Encode(true)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	return data, hex.EncodeToString(sum[:])
}

// SHA256Digest decodes a hex digest into the [32]byte the Storage API takes.
func SHA256Digest(hexDigest string) [32]byte {
	raw, _ := hex.DecodeString(hexDigest)
	var digest [32]byte
	copy(digest[:], raw)
	return digest
}

func SweptHashes(objs []storage.SweptObject) []string {
	hashes := make([]string, 0, len(objs))
	for _, obj := range objs {
		hashes = append(hashes, obj.Hash)
	}
	slices.Sort(hashes)
	return hashes
}

func sortedSwept(objs []storage.SweptObject) []storage.SweptObject {
	out := slices.Clone(objs)
	slices.SortFunc(out, func(a, b storage.SweptObject) int { return strings.Compare(a.Hash, b.Hash) })
	return out
}

// assertChunkEntriesIntact fails when an aborted shard deletion touched the
// chunk entries a racing commit relies on for dedup.
func assertChunkEntriesIntact(t *testing.T, ctx context.Context, gcs storage.Storage, f File, res *storage.SweepResult) {
	t.Helper()
	if res.DeletedChunkEntries != 0 {
		t.Fatalf("racing commit's chunk entries touched: %+v", res)
	}
	for _, chunkHash := range f.ChunkHashes {
		if got, err := gcs.GetChunkIndexEntry(ctx, chunkHash); err != nil || got != f.ShardHash {
			t.Fatalf("chunk entry = %q, %v; want %q", got, err, f.ShardHash)
		}
	}
}

func AssertFileIntact(t *testing.T, ctx context.Context, st storage.Storage, f File) {
	t.Helper()
	if _, err := st.GetShard(ctx, f.FileHash); err != nil {
		t.Fatalf("GetShard: %v", err)
	}
	rc, err := st.GetReconstructedFile(ctx, "default", SHA256Digest(f.SHA256Hex))
	if err != nil {
		t.Fatalf("GetReconstructedFile: %v", err)
	}
	data, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil || !bytes.Equal(data, f.Content) {
		t.Fatalf("reconstruction corrupted: %v", err)
	}
}

// hookedGCStore wraps a Storage with callbacks fired at sweep-visible points,
// simulating uploads that commit while a sweep is running. A non-zero age
// backdates every modTime the object walks report, simulating aged objects
// on backends whose timestamps cannot be set (S3).
type hookedGCStore struct {
	storage.Storage
	age                time.Duration
	shardModTimes      map[string]time.Time // per-shard walk mtime overrides
	beforeFileEntryGet func()               // consumed on first fire
	beforeShardLoad    func()               // consumed on first fire
	// onFileEntryGet / onSHA256EntryGet fire before every call of the
	// wrapped getter with a 1-based call count, unlike the before* hooks
	// consumed on first fire.
	onFileEntryGet   func(n int)
	fileEntryGets    int
	onSHA256EntryGet func(n int)
	sha256EntryGets  int
	beforeWalkShards func() // fired before every WalkShards delegation
	beforeWalkXorbs  func() // fired before every WalkXorbs delegation
	walkShardsCalls  int    // WalkShards invocations
	loadShardCalls   int    // LoadShard invocations
	cachedShardGets  int    // GetShardByHash invocations (GC must not)
	loadShardErrs    map[string]error
}

// agedStore wraps st so every stored object looks written two hours ago.
func agedStore(st storage.Storage) *hookedGCStore {
	return &hookedGCStore{Storage: st, age: 2 * time.Hour}
}

// walkTime substitutes the aged modTime when aging is enabled.
func (h *hookedGCStore) walkTime(modTime time.Time) time.Time {
	if h.age == 0 {
		return modTime
	}
	return time.Now().Add(-h.age)
}

func (h *hookedGCStore) WalkShards(ctx context.Context, fn func(shardHash string, size int64, modTime time.Time) error) error {
	h.walkShardsCalls++
	if h.beforeWalkShards != nil {
		h.beforeWalkShards()
	}
	return h.Storage.WalkShards(ctx, func(shardHash string, size int64, modTime time.Time) error {
		if t, ok := h.shardModTimes[shardHash]; ok {
			return fn(shardHash, size, t)
		}
		return fn(shardHash, size, h.walkTime(modTime))
	})
}

func (h *hookedGCStore) WalkXorbs(ctx context.Context, fn func(xorbHash string, size int64, modTime time.Time) error) error {
	if h.beforeWalkXorbs != nil {
		h.beforeWalkXorbs()
	}
	return h.Storage.WalkXorbs(ctx, func(xorbHash string, size int64, modTime time.Time) error {
		return fn(xorbHash, size, h.walkTime(modTime))
	})
}

func (h *hookedGCStore) GetFileIndexEntry(ctx context.Context, fileHash xet.FileHash) (string, error) {
	if h.beforeFileEntryGet != nil {
		cb := h.beforeFileEntryGet
		h.beforeFileEntryGet = nil
		cb()
	}
	if h.onFileEntryGet != nil {
		h.fileEntryGets++
		h.onFileEntryGet(h.fileEntryGets)
	}
	return h.Storage.GetFileIndexEntry(ctx, fileHash)
}

func (h *hookedGCStore) GetSHA256IndexEntry(ctx context.Context, sha256Hex string) (string, error) {
	if h.onSHA256EntryGet != nil {
		h.sha256EntryGets++
		h.onSHA256EntryGet(h.sha256EntryGets)
	}
	return h.Storage.GetSHA256IndexEntry(ctx, sha256Hex)
}

func (h *hookedGCStore) GetShardByHash(ctx context.Context, shardHash string) (*shard.Shard, error) {
	h.cachedShardGets++
	return h.Storage.GetShardByHash(ctx, shardHash)
}

func (h *hookedGCStore) LoadShard(ctx context.Context, shardHash string) (*shard.Shard, error) {
	h.loadShardCalls++
	if h.beforeShardLoad != nil {
		cb := h.beforeShardLoad
		h.beforeShardLoad = nil
		cb()
	}
	if err, ok := h.loadShardErrs[shardHash]; ok {
		return nil, err
	}
	return h.Storage.LoadShard(ctx, shardHash)
}

// SortEntries orders expectations the way ListFiles sorts its result:
// original size descending, then SHA-256, then first file hash.
func SortEntries(entries []storage.FileListEntry) {
	slices.SortFunc(entries, func(a, b storage.FileListEntry) int {
		if c := cmp.Compare(b.OriginalSize, a.OriginalSize); c != 0 {
			return c
		}
		if c := cmp.Compare(a.SHA256, b.SHA256); c != 0 {
			return c
		}
		return cmp.Compare(a.FileHashes[0], b.FileHashes[0])
	})
}

// PutListedFile stores one file chunked as parts (one single-chunk xorb per
// part) and returns its hex file hash together with the exact stored size of
// its chunks (a footer-less encode of each part is exactly the packed chunk
// bytes the listing attributes).
func PutListedFile(t *testing.T, ctx context.Context, st storage.Storage, parts [][]byte) (string, uint64) {
	t.Helper()
	shardObj := shard.NewShard()
	fileBlock := shard.FileBlock{}
	var chunkHashes []xet.ChunkHash
	var chunkSizes []uint64
	var storedSize uint64
	for _, part := range parts {
		encoded, xorbHash := EncodeXorb(t, true, part)
		if _, err := st.PutXorb(ctx, "default", xorbHash, bytes.NewReader(encoded)); err != nil {
			t.Fatal(err)
		}
		chunkOnly, _ := EncodeXorb(t, false, part)
		storedSize += uint64(len(chunkOnly))
		chunkHash := xet.ComputeChunkHash(part)
		chunkHashes = append(chunkHashes, chunkHash)
		chunkSizes = append(chunkSizes, uint64(len(part)))
		fileBlock.Entries = append(fileBlock.Entries, shard.FileDataSequenceEntry{
			CASHash: xorbHash, UnpackedSegBytes: uint32(len(part)), ChunkIndexEnd: 1,
		})
		shardObj.AddCASBlock(shard.CASBlock{
			CASHash: xorbHash,
			Chunks:  []shard.CASChunkSequenceEntry{{ChunkHash: chunkHash, UnpackedSegBytes: uint32(len(part))}},
		})
	}
	fileHash := xet.ComputeFileHash(chunkHashes, chunkSizes)
	fileBlock.FileHash = fileHash
	shardObj.AddFile(fileBlock)
	if _, err := st.PutShard(ctx, shardObj); err != nil {
		t.Fatal(err)
	}
	return fileHash.String(), storedSize
}

// CheckFanoutStore verifies that st, opened over data another instance wrote,
// resolves the one-file shard sh through every index and that each walker
// reports the full 64-hex hash. It returns each stored kind mapped to its hash.
func CheckFanoutStore(t *testing.T, ctx context.Context, st storage.Storage, sh *shard.Shard, fileHash xet.FileHash, content []byte) map[string]string {
	t.Helper()
	xorbHash := sh.CASInfos[0].CASHash
	chunkHash := xet.ComputeChunkHash(content)
	digest := sha256.Sum256(content)
	shardHash, err := st.GetFileIndexEntry(ctx, fileHash)
	if err != nil || len(shardHash) != 64 {
		t.Fatalf("GetFileIndexEntry() = %q, %v", shardHash, err)
	}
	if ok, err := st.HasXorb(ctx, "default", xorbHash); err != nil || !ok {
		t.Fatalf("HasXorb() = %v, %v", ok, err)
	}
	if _, err := st.GetShard(ctx, fileHash); err != nil {
		t.Fatalf("GetShard(): %v", err)
	}
	if _, err := st.GetShardByChunkHash(ctx, "default", chunkHash); err != nil {
		t.Fatalf("GetShardByChunkHash(): %v", err)
	}
	if got, err := st.GetFileHashBySHA256(ctx, "default", digest); err != nil || got != fileHash {
		t.Fatalf("GetFileHashBySHA256() = %s, %v", got, err)
	}

	want := map[string]string{
		"xorbs":        xorbHash.String(),
		"shards":       shardHash,
		"index/files":  fileHash.String(),
		"index/chunks": chunkHash.String(),
		"index/sha256": hex.EncodeToString(digest[:]),
	}
	walked := map[string][]string{}
	collect := func(kind string) func(hash string, _ int64, _ time.Time) error {
		return func(hash string, _ int64, _ time.Time) error {
			walked[kind] = append(walked[kind], hash)
			return nil
		}
	}
	collectIndex := func(kind string) func(hash, gotShard string) error {
		return func(hash, gotShard string) error {
			if gotShard != shardHash {
				return fmt.Errorf("%s/%s -> %q, want %q", kind, hash, gotShard, shardHash)
			}
			walked[kind] = append(walked[kind], hash)
			return nil
		}
	}
	if err := st.WalkXorbs(ctx, collect("xorbs")); err != nil {
		t.Fatalf("WalkXorbs(): %v", err)
	}
	if err := st.WalkShards(ctx, collect("shards")); err != nil {
		t.Fatalf("WalkShards(): %v", err)
	}
	if err := st.WalkFileIndex(ctx, collectIndex("index/files")); err != nil {
		t.Fatalf("WalkFileIndex(): %v", err)
	}
	if err := st.WalkSHA256Index(ctx, collectIndex("index/sha256")); err != nil {
		t.Fatalf("WalkSHA256Index(): %v", err)
	}
	for kind, h := range want {
		if kind == "index/chunks" {
			continue // no walker exists for the chunk index
		}
		if got := walked[kind]; len(got) != 1 || got[0] != h {
			t.Errorf("walk %s = %q, want [%s]", kind, got, h)
		}
	}
	return want
}
