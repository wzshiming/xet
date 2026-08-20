package storage

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	iofs "io/fs"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/xorb"
)

// sortEntries orders expectations the way ListFiles sorts its result:
// original size descending, then SHA-256, then first file hash.
func sortEntries(entries []FileListEntry) {
	slices.SortFunc(entries, func(a, b FileListEntry) int {
		if c := cmp.Compare(b.OriginalSize, a.OriginalSize); c != 0 {
			return c
		}
		if c := cmp.Compare(a.SHA256, b.SHA256); c != 0 {
			return c
		}
		return cmp.Compare(a.FileHashes[0], b.FileHashes[0])
	})
}

type listBackend struct {
	name          string
	newStore      func(t *testing.T) Storage
	writeDangling func(t *testing.T, st Storage, fileHash, shardHash string)
}

func listBackends() []listBackend {
	return []listBackend{
		{
			name: "file",
			newStore: func(t *testing.T) Storage {
				st, err := NewFileStorage(WithBasePath(t.TempDir()))
				if err != nil {
					t.Fatal(err)
				}
				return st
			},
			writeDangling: func(t *testing.T, st Storage, fileHash, shardHash string) {
				fs := st.(*FileStorage)
				if err := writeIndexFile(fs.objectPath("index/files", fileHash), []byte(shardHash)); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "s3",
			newStore: func(t *testing.T) Storage {
				return newTestS3Storage(t)
			},
			writeDangling: func(t *testing.T, st Storage, fileHash, shardHash string) {
				ss := st.(*S3Storage)
				if err := ss.putIndexObject(context.Background(), ss.objectKey("index/files", fileHash), []byte(shardHash)); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
}

// putListedFile stores one file chunked as parts (one single-chunk xorb per
// part) and returns its hex file hash together with the exact stored size of
// its chunks (a footer-less encode of each part is exactly the packed chunk
// bytes the listing attributes).
func putListedFile(t *testing.T, ctx context.Context, st Storage, parts [][]byte) (string, uint64) {
	t.Helper()
	shardObj := shard.NewShard()
	fileBlock := shard.FileBlock{}
	var chunkHashes []xet.ChunkHash
	var chunkSizes []uint64
	var storedSize uint64
	for _, part := range parts {
		var encoded bytes.Buffer
		encoder := xorb.NewEncoder(&encoded, true)
		if _, err := encoder.Write(part); err != nil {
			t.Fatal(err)
		}
		if err := encoder.Close(); err != nil {
			t.Fatal(err)
		}
		xorbHash := encoder.SummoryHash()
		if _, err := st.PutXorb(ctx, "default", xorbHash, bytes.NewReader(encoded.Bytes())); err != nil {
			t.Fatal(err)
		}
		var chunkOnly bytes.Buffer
		bare := xorb.NewEncoder(&chunkOnly, false)
		if _, err := bare.Write(part); err != nil {
			t.Fatal(err)
		}
		if err := bare.Close(); err != nil {
			t.Fatal(err)
		}
		storedSize += uint64(chunkOnly.Len())
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

// TestListFilesGroupsBySHA256 proves that identical content chunked two
// different ways (two xet file hashes) collapses into one entry whose size is
// counted once, while empty files stay ungrouped with no SHA-256.
func TestListFilesGroupsBySHA256(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)

			content := []byte("same content, two different chunkings")
			other := []byte("a different file")

			oneChunk, oneStored := putListedFile(t, ctx, st, [][]byte{content})
			twoChunks, twoStored := putListedFile(t, ctx, st, [][]byte{content[:11], content[11:]})
			otherHash, otherStored := putListedFile(t, ctx, st, [][]byte{other})

			emptyHash := xet.FileHash{42}
			emptyShard := shard.NewShard()
			emptyShard.AddFile(shard.FileBlock{FileHash: emptyHash})
			if _, err := st.PutShard(ctx, emptyShard); err != nil {
				t.Fatalf("PutShard(empty file): %v", err)
			}

			got, err := ListFiles(ctx, st.(ListStore))
			if err != nil {
				t.Fatalf("ListFiles: %v", err)
			}

			contentSHA := sha256.Sum256(content)
			otherSHA := sha256.Sum256(other)
			grouped := []string{oneChunk, twoChunks}
			slices.Sort(grouped)
			// Both chunkings are stored, so the group's unique bytes count the
			// stored chunks of both.
			want := []FileListEntry{
				{FileHashes: []string{emptyHash.String()}},
				{SHA256: hex.EncodeToString(contentSHA[:]), FileHashes: grouped, OriginalSize: uint64(len(content)), UniqueSize: oneStored + twoStored},
				{SHA256: hex.EncodeToString(otherSHA[:]), FileHashes: []string{otherHash}, OriginalSize: uint64(len(other)), UniqueSize: otherStored},
			}
			sortEntries(want)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("ListFiles() = %+v, want %+v", got, want)
			}
		})
	}
}

// TestListFilesMarksDanglingEntries covers file-index entries whose shard is
// gone: they stay listed, flagged missing, with no SHA-256 or size.
func TestListFilesMarksDanglingEntries(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)

			content := []byte("still resolvable")
			realHash, realStored := putListedFile(t, ctx, st, [][]byte{content})

			danglingHash := strings.Repeat("ab", 32)
			backend.writeDangling(t, st, danglingHash, strings.Repeat("cd", 32))

			got, err := ListFiles(ctx, st.(ListStore))
			if err != nil {
				t.Fatalf("ListFiles: %v", err)
			}

			digest := sha256.Sum256(content)
			want := []FileListEntry{
				{FileHashes: []string{danglingHash}, Missing: true},
				{SHA256: hex.EncodeToString(digest[:]), FileHashes: []string{realHash}, OriginalSize: uint64(len(content)), UniqueSize: realStored},
			}
			sortEntries(want)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("ListFiles() = %+v, want %+v", got, want)
			}
		})
	}
}

// fakeListStore serves hand-built shards and per-xorb chunk packed sizes,
// letting the accounting tests control stored bytes without real encoding.
type fakeListStore struct {
	refs       [][2]string // fileHash, shardHash
	shards     map[string]*shard.Shard
	chunkSizes map[xet.XorbHash][]uint64 // per-chunk stored (packed) sizes
}

func (f *fakeListStore) WalkFileIndex(_ context.Context, fn func(fileHash, shardHash string) error) error {
	for _, r := range f.refs {
		if err := fn(r[0], r[1]); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeListStore) GetShardByHash(_ context.Context, shardHash string) (*shard.Shard, error) {
	sh, ok := f.shards[shardHash]
	if !ok {
		return nil, iofs.ErrNotExist
	}
	return sh, nil
}

func (f *fakeListStore) GetXorbChunkOffsets(_ context.Context, xorbHash xet.XorbHash) ([]uint64, error) {
	sizes, ok := f.chunkSizes[xorbHash]
	if !ok {
		return nil, iofs.ErrNotExist
	}
	offsets := make([]uint64, len(sizes))
	var total uint64
	for i, size := range sizes {
		total += size
		offsets[i] = total
	}
	return offsets, nil
}

// TestListFilesComputesUniqueAndShared covers the stored-size accounting: a
// chunk referenced by two entries counts as shared for both, per-chunk sizes
// are the exact packed bytes from the xorb offset table, and dedup'd terms
// resolve against the xorb regardless of which shard uploaded it.
func TestListFilesComputesUniqueAndShared(t *testing.T) {
	xorbA := xet.XorbHash{1}
	xorbB := xet.XorbHash{2}
	file1 := xet.FileHash{11}
	file2 := xet.FileHash{12}
	sha1 := shard.NewSHA256Hash([32]byte{101})
	sha2 := shard.NewSHA256Hash([32]byte{102})

	shard1 := shard.NewShard()
	shard1.AddFile(shard.FileBlock{
		FileHash:    file1,
		Entries:     []shard.FileDataSequenceEntry{{CASHash: xorbA, UnpackedSegBytes: 100, ChunkIndexEnd: 1}},
		MetadataExt: &shard.FileMetadataExt{SHA256Hash: sha1},
	})
	shard1.AddCASBlock(shard.CASBlock{
		CASHash:        xorbA,
		Chunks:         []shard.CASChunkSequenceEntry{{UnpackedSegBytes: 100}},
		NumBytesInCAS:  100,
		NumBytesOnDisk: 50,
	})

	// file2 dedups its first chunk against xorbA, whose CAS block lives in shard1.
	shard2 := shard.NewShard()
	shard2.AddFile(shard.FileBlock{
		FileHash: file2,
		Entries: []shard.FileDataSequenceEntry{
			{CASHash: xorbA, UnpackedSegBytes: 100, ChunkIndexEnd: 1},
			{CASHash: xorbB, UnpackedSegBytes: 200, ChunkIndexEnd: 1},
		},
		MetadataExt: &shard.FileMetadataExt{SHA256Hash: sha2},
	})
	shard2.AddCASBlock(shard.CASBlock{
		CASHash:        xorbB,
		Chunks:         []shard.CASChunkSequenceEntry{{UnpackedSegBytes: 200}},
		NumBytesInCAS:  200,
		NumBytesOnDisk: 100,
	})

	st := &fakeListStore{
		refs:   [][2]string{{file1.String(), "s1"}, {file2.String(), "s2"}},
		shards: map[string]*shard.Shard{"s1": shard1, "s2": shard2},
		chunkSizes: map[xet.XorbHash][]uint64{
			xorbA: {50},
			xorbB: {100},
		},
	}

	got, err := ListFiles(context.Background(), st)
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	want := []FileListEntry{
		{SHA256: sha1.String(), FileHashes: []string{file1.String()}, OriginalSize: 100, UniqueSize: 0, SharedSize: 50},
		{SHA256: sha2.String(), FileHashes: []string{file2.String()}, OriginalSize: 300, UniqueSize: 100, SharedSize: 50},
	}
	sortEntries(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ListFiles() = %+v, want %+v", got, want)
	}
}

// TestListFilesMarksInvalidChunkMetadata covers a dedup term whose chunk
// index escaped shard-ingest validation: the entry is flagged missing
// instead of sizing per-chunk usage arrays off the bogus index.
func TestListFilesMarksInvalidChunkMetadata(t *testing.T) {
	file1 := xet.FileHash{11}
	sh := shard.NewShard()
	sh.AddFile(shard.FileBlock{
		FileHash: file1,
		Entries: []shard.FileDataSequenceEntry{
			{CASHash: xet.XorbHash{1}, UnpackedSegBytes: 100, ChunkIndexStart: math.MaxUint32 - 1, ChunkIndexEnd: math.MaxUint32},
		},
	})
	st := &fakeListStore{
		refs:   [][2]string{{file1.String(), "s1"}},
		shards: map[string]*shard.Shard{"s1": sh},
	}

	got, err := ListFiles(context.Background(), st)
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	want := []FileListEntry{{FileHashes: []string{file1.String()}, Missing: true}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ListFiles() = %+v, want %+v", got, want)
	}
}

// TestListFilesToleratesVanishedXorb covers a reconstruction term whose xorb
// is gone: the entry stays listed and the vanished chunks contribute no
// stored bytes.
func TestListFilesToleratesVanishedXorb(t *testing.T) {
	xorbA := xet.XorbHash{1}
	xorbGone := xet.XorbHash{9}
	file1 := xet.FileHash{11}
	sha1 := shard.NewSHA256Hash([32]byte{101})
	sh := shard.NewShard()
	sh.AddFile(shard.FileBlock{
		FileHash: file1,
		Entries: []shard.FileDataSequenceEntry{
			{CASHash: xorbA, UnpackedSegBytes: 100, ChunkIndexEnd: 1},
			{CASHash: xorbGone, UnpackedSegBytes: 200, ChunkIndexEnd: 1},
		},
		MetadataExt: &shard.FileMetadataExt{SHA256Hash: sha1},
	})
	st := &fakeListStore{
		refs:       [][2]string{{file1.String(), "s1"}},
		shards:     map[string]*shard.Shard{"s1": sh},
		chunkSizes: map[xet.XorbHash][]uint64{xorbA: {50}},
	}

	got, err := ListFiles(context.Background(), st)
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	want := []FileListEntry{{SHA256: sha1.String(), FileHashes: []string{file1.String()}, OriginalSize: 300, UniqueSize: 50}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ListFiles() = %+v, want %+v", got, want)
	}
}
