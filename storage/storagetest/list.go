package storagetest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
