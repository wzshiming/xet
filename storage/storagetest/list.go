package storagetest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	iofs "io/fs"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
)

// testListFilesGroupsBySHA256 proves that identical content chunked two
// different ways (two xet file hashes) collapses into one entry whose size is
// counted once, while empty files stay ungrouped with no SHA-256.
func testListFilesGroupsBySHA256(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)

	content := []byte("same content, two different chunkings")
	other := []byte("a different file")

	oneChunk, oneStored := PutListedFile(t, ctx, st, [][]byte{content})
	twoChunks, twoStored := PutListedFile(t, ctx, st, [][]byte{content[:11], content[11:]})
	otherHash, otherStored := PutListedFile(t, ctx, st, [][]byte{other})

	emptyHash := xet.FileHash{}
	emptyShard := shard.NewShard()
	emptyShard.AddFile(shard.FileBlock{FileHash: emptyHash})
	if _, err := st.PutShard(ctx, emptyShard); err != nil {
		t.Fatalf("PutShard(empty file): %v", err)
	}

	got, err := storage.ListFiles(ctx, st.(storage.ListStore))
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}

	contentSHA := sha256.Sum256(content)
	otherSHA := sha256.Sum256(other)
	grouped := []string{oneChunk, twoChunks}
	slices.Sort(grouped)
	// Both chunkings are stored, so the group's unique bytes count the
	// stored chunks of both.
	want := []storage.FileListEntry{
		{FileHashes: []string{emptyHash.String()}},
		{SHA256: hex.EncodeToString(contentSHA[:]), FileHashes: grouped, OriginalSize: uint64(len(content)), UniqueSize: oneStored + twoStored},
		{SHA256: hex.EncodeToString(otherSHA[:]), FileHashes: []string{otherHash}, OriginalSize: uint64(len(other)), UniqueSize: otherStored},
	}
	SortEntries(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ListFiles() = %+v, want %+v", got, want)
	}
}

// testListFilesMarksDanglingEntries covers file-index entries whose shard is
// gone: they stay listed, flagged missing, with no SHA-256 or size.
func testListFilesMarksDanglingEntries(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)

	content := []byte("still resolvable")
	realHash, realStored := PutListedFile(t, ctx, st, [][]byte{content})

	danglingHash := strings.Repeat("ab", 32)
	b.SetIndexEntry(t, st, "index/files", danglingHash, strings.Repeat("cd", 32))

	got, err := storage.ListFiles(ctx, st.(storage.ListStore))
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}

	digest := sha256.Sum256(content)
	want := []storage.FileListEntry{
		{FileHashes: []string{danglingHash}, Missing: true},
		{SHA256: hex.EncodeToString(digest[:]), FileHashes: []string{realHash}, OriginalSize: uint64(len(content)), UniqueSize: realStored},
	}
	SortEntries(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ListFiles() = %+v, want %+v", got, want)
	}
}

// testListFilesToleratesVanishedXorb covers a reconstruction term whose xorb
// is gone: the entry stays listed and the vanished chunks contribute no
// stored bytes.
func testListFilesToleratesVanishedXorb(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)

	partA := []byte("the chunk that stays")
	partB := []byte("the chunk whose xorb vanishes")
	f := PutFile(t, ctx, st, [][]byte{partA, partB})
	vanished := f.XorbHashes[1]
	if err := st.(storage.GCStore).DeleteXorb(ctx, vanished); err != nil {
		t.Fatalf("DeleteXorb: %v", err)
	}

	got, err := storage.ListFiles(ctx, st.(storage.ListStore))
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	storedA, _ := EncodeXorb(t, false, partA)
	want := []storage.FileListEntry{
		{SHA256: f.SHA256Hex, FileHashes: []string{f.FileHash.String()}, OriginalSize: uint64(len(f.Content)), UniqueSize: uint64(len(storedA))},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ListFiles() = %+v, want %+v", got, want)
	}

	if _, err := st.GetXorbReadSeekCloser(ctx, "default", vanished); !errors.Is(err, iofs.ErrNotExist) {
		t.Fatalf("GetXorbReadSeekCloser(vanished) = %v, want fs.ErrNotExist", err)
	}
}

// testListFilesComputesUniqueAndShared covers the stored-size accounting: a
// chunk referenced by two entries counts as shared for both, per-chunk sizes
// are the exact packed bytes from the xorb offset table, and dedup'd terms
// resolve against the xorb regardless of which shard uploaded it.
func testListFilesComputesUniqueAndShared(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)

	partA := []byte("chunk shared by both files")
	partB := []byte("chunk only the second file has")
	fileA := PutFile(t, ctx, st, [][]byte{partA})

	// The second file dedups partA against fileA's xorb, so its shard
	// carries only partB's CAS block.
	shardAB := shard.NewShard()
	fileAB, xorbHashes, _ := AddFileBlock(t, ctx, st, shardAB, [][]byte{partA, partB})
	shardAB.CASInfos = slices.DeleteFunc(shardAB.CASInfos, func(cb shard.CASBlock) bool { return cb.CASHash == xorbHashes[0] })
	if len(shardAB.CASInfos) != 1 {
		t.Fatalf("test setup: %d CAS blocks left, want only partB's", len(shardAB.CASInfos))
	}
	if _, err := st.PutShard(ctx, shardAB); err != nil {
		t.Fatalf("PutShard: %v", err)
	}

	got, err := storage.ListFiles(ctx, st.(storage.ListStore))
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	storedA, _ := EncodeXorb(t, false, partA)
	storedB, _ := EncodeXorb(t, false, partB)
	contentAB := slices.Concat(partA, partB)
	digestAB := sha256.Sum256(contentAB)
	want := []storage.FileListEntry{
		{SHA256: fileA.SHA256Hex, FileHashes: []string{fileA.FileHash.String()}, OriginalSize: uint64(len(partA)), UniqueSize: 0, SharedSize: uint64(len(storedA))},
		{SHA256: hex.EncodeToString(digestAB[:]), FileHashes: []string{fileAB.String()}, OriginalSize: uint64(len(contentAB)), UniqueSize: uint64(len(storedB)), SharedSize: uint64(len(storedA))},
	}
	SortEntries(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ListFiles() = %+v, want %+v", got, want)
	}
}

// testListFilesMarksInvalidChunkMetadata covers a dedup term whose chunk
// index escaped shard-ingest validation: the entry is flagged missing
// instead of sizing per-chunk usage arrays off the bogus index.
func testListFilesMarksInvalidChunkMetadata(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)

	fileHash := xet.FileHash{11}
	sh := shard.NewShard()
	sh.AddFile(shard.FileBlock{
		FileHash: fileHash,
		Entries: []shard.FileDataSequenceEntry{
			{CASHash: xet.XorbHash{1}, UnpackedSegBytes: 100, ChunkIndexStart: math.MaxUint32 - 1, ChunkIndexEnd: math.MaxUint32},
		},
	})
	raw, shardHash, err := storage.EncodeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
	b.PutRawShardObject(t, ctx, st, shardHash, raw)
	b.SetIndexEntry(t, st, "index/files", fileHash.String(), shardHash)
	// The shard loads fine, so a missing flag can only come from the chunk index.
	if _, err := st.(storage.ListStore).GetShardByHash(ctx, shardHash); err != nil {
		t.Fatalf("test setup: GetShardByHash: %v", err)
	}

	got, err := storage.ListFiles(ctx, st.(storage.ListStore))
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	want := []storage.FileListEntry{{FileHashes: []string{fileHash.String()}, Missing: true}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ListFiles() = %+v, want %+v", got, want)
	}
}
