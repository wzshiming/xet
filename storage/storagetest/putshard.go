package storagetest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
)

// addXorbFile stores each element of xorbs as one multi-chunk footer xorb and
// appends to sh their CAS blocks and the file of one whole-xorb term each.
func addXorbFile(tb testing.TB, ctx context.Context, st storage.Storage, sh *shard.Shard, xorbs [][][]byte) File {
	tb.Helper()
	if len(xorbs) == 0 {
		tb.Fatal("no xorbs to store")
	}
	f := File{Content: bytes.Join(slices.Concat(xorbs...), nil)}
	fb := shard.FileBlock{}
	var sizes []uint64
	for i, parts := range xorbs {
		if len(parts) == 0 {
			tb.Fatalf("xorb %d has no chunks", i)
		}
		encoded, xorbHash := EncodeXorb(tb, true, parts...)
		if _, err := st.PutXorb(ctx, xorbHash, bytes.NewReader(encoded)); err != nil {
			tb.Fatal(err)
		}
		f.XorbHashes = append(f.XorbHashes, xorbHash)
		cb := shard.CASBlock{CASHash: xorbHash}
		for _, part := range parts {
			chunkHash := xet.ComputeChunkHash(part)
			f.ChunkHashes = append(f.ChunkHashes, chunkHash)
			sizes = append(sizes, uint64(len(part)))
			cb.Chunks = append(cb.Chunks, shard.CASChunkSequenceEntry{
				ChunkHash: chunkHash, ByteRangeStart: cb.NumBytesInCAS, UnpackedSegBytes: uint32(len(part)),
			})
			cb.NumBytesInCAS += uint32(len(part))
		}
		sh.AddCASBlock(cb)
		fb.Entries = append(fb.Entries, shard.FileDataSequenceEntry{
			CASHash: xorbHash, UnpackedSegBytes: cb.NumBytesInCAS, ChunkIndexEnd: uint32(len(parts)),
		})
	}
	f.FileHash = xet.ComputeFileHash(f.ChunkHashes, sizes)
	fb.FileHash = f.FileHash
	sh.AddFile(fb)
	digest := sha256.Sum256(f.Content)
	f.SHA256Hex = hex.EncodeToString(digest[:])
	return f
}

// addFile appends parts to sh as one file and returns its File.
func addFile(t *testing.T, ctx context.Context, st storage.Storage, sh *shard.Shard, parts [][]byte) File {
	t.Helper()
	f := File{Content: bytes.Join(parts, nil)}
	f.FileHash, f.XorbHashes, f.ChunkHashes = AddFileBlock(t, ctx, st, sh, parts)
	digest := sha256.Sum256(f.Content)
	f.SHA256Hex = hex.EncodeToString(digest[:])
	return f
}

// assertFilesCommitted checks each file resolves by file hash and SHA-256 and reconstructs.
func assertFilesCommitted(t *testing.T, ctx context.Context, st storage.Storage, files ...File) {
	t.Helper()
	for _, f := range files {
		AssertFileIntact(t, ctx, st, f)
		if got, err := st.GetFileHashBySHA256(ctx, SHA256Digest(f.SHA256Hex)); err != nil || got != f.FileHash {
			t.Fatalf("GetFileHashBySHA256(%s) = %s, %v; want %s", f.SHA256Hex, got.String(), err, f.FileHash.String())
		}
	}
}

// addInterleavedFile adds to sh the blocks of xorbs A (3 chunks) and B (1) and a file A[0,2) B[0,1) A[2,3).
func addInterleavedFile(t *testing.T, ctx context.Context, st storage.Storage, sh *shard.Shard) {
	t.Helper()
	a := [][]byte{[]byte("interleaved xorb A chunk 0"), []byte("interleaved xorb A chunk 1, longer"), []byte("interleaved xorb A chunk 2, longest of the three")}
	b := []byte("interleaved xorb B chunk 0")
	blocks := shard.NewShard()
	addXorbFile(t, ctx, st, blocks, [][][]byte{a, {b}})
	sh.AddCASBlock(blocks.CASInfos[0])
	sh.AddCASBlock(blocks.CASInfos[1])
	var chunkHashes []xet.ChunkHash
	var sizes []uint64
	for _, part := range [][]byte{a[0], a[1], b, a[2]} {
		chunkHashes = append(chunkHashes, xet.ComputeChunkHash(part))
		sizes = append(sizes, uint64(len(part)))
	}
	xorbA, xorbB := blocks.CASInfos[0].CASHash, blocks.CASInfos[1].CASHash
	sh.AddFile(shard.FileBlock{FileHash: xet.ComputeFileHash(chunkHashes, sizes), Entries: []shard.FileDataSequenceEntry{
		{CASHash: xorbA, UnpackedSegBytes: uint32(len(a[0]) + len(a[1])), ChunkIndexEnd: 2},
		{CASHash: xorbB, UnpackedSegBytes: uint32(len(b)), ChunkIndexEnd: 1},
		{CASHash: xorbA, UnpackedSegBytes: uint32(len(a[2])), ChunkIndexStart: 2, ChunkIndexEnd: 3},
	}})
}

// phaseEvent is a phase PutShard reported or, with put set, the kind of a Put it sent.
type phaseEvent struct {
	phase       storage.PutShardPhase
	done, total int
	put         string
}

// phaseRecorder logs, in order, the phases given to report and the Puts sent through it.
type phaseRecorder struct {
	storage.Storage
	events []phaseEvent
}

func (r *phaseRecorder) report(phase storage.PutShardPhase, done, total int) {
	r.events = append(r.events, phaseEvent{phase: phase, done: done, total: total})
}

func (r *phaseRecorder) PutXorb(ctx context.Context, xorbHash xet.XorbHash, rd io.Reader) (bool, error) {
	r.events = append(r.events, phaseEvent{put: "xorb"})
	return r.Storage.PutXorb(ctx, xorbHash, rd)
}

func (r *phaseRecorder) PutShard(ctx context.Context, sh *shard.Shard) (string, bool, error) {
	r.events = append(r.events, phaseEvent{put: "shard"})
	return r.Storage.PutShard(ctx, sh)
}

func (r *phaseRecorder) PutChunkIndexEntry(ctx context.Context, chunkHash xet.ChunkHash, shardHash string) (bool, error) {
	r.events = append(r.events, phaseEvent{put: "chunks"})
	return r.Storage.PutChunkIndexEntry(ctx, chunkHash, shardHash)
}

func (r *phaseRecorder) PutSHA256IndexEntry(ctx context.Context, digest [32]byte, shardHash string) (bool, error) {
	r.events = append(r.events, phaseEvent{put: "sha256"})
	return r.Storage.PutSHA256IndexEntry(ctx, digest, shardHash)
}

func (r *phaseRecorder) PutFileIndexEntry(ctx context.Context, fileHash xet.FileHash, shardHash string) (bool, error) {
	r.events = append(r.events, phaseEvent{put: "files"})
	return r.Storage.PutFileIndexEntry(ctx, fileHash, shardHash)
}

// One upload commits all its files in one shard; a file stored earlier must not hide the new ones.
func testPutShardCommitsNewFilesBesideKnownFile(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	partsA := [][]byte{[]byte("file A, uploaded earlier")}
	a := PutFile(t, ctx, st, partsA)

	sh := shard.NewShard()
	addFile(t, ctx, st, sh, partsA)
	fileB := addFile(t, ctx, st, sh, [][]byte{[]byte("file B chunk one"), []byte("file B chunk two")})
	if inserted, err := storage.PutShard(ctx, st, sh, nil); err != nil || !inserted {
		t.Fatalf("PutShard([A,B]) = %v, %v", inserted, err)
	}
	assertFilesCommitted(t, ctx, st, a, fileB)

	if inserted, err := storage.PutShard(ctx, st, sh, nil); err != nil || inserted {
		t.Fatalf("repeated PutShard([A,B]) = %v, %v; want no-op", inserted, err)
	}
	assertFilesCommitted(t, ctx, st, a, fileB)
}

// File entries commit last and in order, so a commit dying between A's and B's leaves B unindexed.
func testPutShardRetryRepairsMissingFileEntry(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	sh := shard.NewShard()
	a := addFile(t, ctx, st, sh, [][]byte{[]byte("file A of an interrupted upload")})
	fileB := addFile(t, ctx, st, sh, [][]byte{[]byte("file B of an interrupted upload")})
	if inserted, err := storage.PutShard(ctx, st, sh, nil); err != nil || !inserted {
		t.Fatalf("PutShard([A,B]) = %v, %v", inserted, err)
	}
	if _, err := storage.NewGC(st).Unlink(ctx, fileB.FileHash); err != nil {
		t.Fatal(err)
	}
	if got, err := st.GetFileIndexEntry(ctx, fileB.FileHash); err != nil || got != "" {
		t.Fatalf("GetFileIndexEntry(B) after unlink = %q, %v", got, err)
	}

	if _, err := storage.PutShard(ctx, st, sh, nil); err != nil {
		t.Fatalf("PutShard([A,B]) retry: %v", err)
	}
	assertFilesCommitted(t, ctx, st, a, fileB)
}

func testPutShardVerifiesFileHash(t *testing.T, b Backend) {
	for _, test := range []struct {
		name    string
		wantErr string
	}{
		{name: "correct"},
		{name: "wrong file hash", wantErr: "file hash mismatch"},
		{name: "wrong SHA-256", wantErr: "SHA-256 mismatch"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			st := b.New(t)
			parts := [][]byte{[]byte("first chunk"), []byte("second chunk, different size")}
			sh := shard.NewShard()
			AddFileBlock(t, ctx, st, sh, parts)
			digest := sha256.Sum256(bytes.Join(parts, nil))
			sh.Files[0].MetadataExt = &shard.FileMetadataExt{SHA256Hash: shard.NewSHA256Hash(digest)}
			sh.Files[0].Flags |= shard.FileWithMetadataExt
			switch test.name {
			case "wrong file hash":
				sh.Files[0].FileHash[0] ^= 1
			case "wrong SHA-256":
				digest[0] ^= 1
				sh.Files[0].MetadataExt.SHA256Hash = shard.NewSHA256Hash(digest)
			}
			inserted, err := storage.PutShard(ctx, st, sh, nil)
			if test.wantErr != "" {
				if inserted || !errors.Is(err, storage.ErrInvalidShard) || !strings.Contains(err.Error(), test.wantErr) {
					t.Errorf("PutShard() = %v, %v, want %q", inserted, err, test.wantErr)
				}
				if _, err := st.GetShard(ctx, sh.Files[0].FileHash); err == nil {
					t.Error("GetShard() succeeded for rejected shard")
				}
				return
			}
			if err != nil || !inserted {
				t.Fatalf("PutShard() = %v, %v", inserted, err)
			}
			if _, err := st.GetShard(ctx, sh.Files[0].FileHash); err != nil {
				t.Fatalf("GetShard(): %v", err)
			}
		})
	}
}

// testPutShardVerifiesCASChunks: PutShard rejects a shard whose CAS chunks disagree with the
// stored xorb, leaving no index entries; an honest retry indexes the file's probe-able chunks.
func testPutShardVerifiesCASChunks(t *testing.T, b Backend) {
	parts := [][]byte{PlainChunk(t, "first"), SampledChunk(t, "second"), PlainChunk(t, "third")}
	for _, test := range []struct {
		name    string
		mutate  func(sh *shard.Shard)
		wantErr string
	}{
		{name: "full CAS blocks"},
		{name: "dedup without CAS blocks", mutate: func(sh *shard.Shard) { sh.CASInfos = nil }},
		{name: "forged chunk hash", mutate: func(sh *shard.Shard) { sh.CASInfos[0].Chunks[1].ChunkHash[0] ^= 1 }, wantErr: "chunk 1 hash"},
		{name: "forged chunk hash in unreferenced block", mutate: func(sh *shard.Shard) { sh.CASInfos[1].Chunks[0].ChunkHash[0] ^= 1 }, wantErr: "chunk 0 hash"},
		{name: "forged chunk size", mutate: func(sh *shard.Shard) { sh.CASInfos[0].Chunks[1].UnpackedSegBytes++ }, wantErr: "chunk 1 has"},
		{name: "undeclared trailing chunk", mutate: func(sh *shard.Shard) { sh.CASInfos[1].Chunks = sh.CASInfos[1].Chunks[:1] }, wantErr: "1 declared chunks"},
		{name: "declared chunk beyond xorb", mutate: func(sh *shard.Shard) {
			cb := &sh.CASInfos[1]
			cb.Chunks = append(cb.Chunks, cb.Chunks[1])
		}, wantErr: "3 declared chunks"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			st := b.New(t)
			sh := shard.NewShard()
			f := addXorbFile(t, ctx, st, sh, [][][]byte{parts})
			unreferenced := shard.NewShard()
			orphan := addXorbFile(t, ctx, st, unreferenced, [][][]byte{{PlainChunk(t, "chunk of an unreferenced xorb"), SampledChunk(t, "its second chunk")}})
			sh.AddCASBlock(unreferenced.CASInfos[0])
			if test.mutate != nil {
				test.mutate(sh)
			}

			inserted, err := storage.PutShard(ctx, st, sh, nil)
			if test.wantErr == "" {
				if err != nil || !inserted {
					t.Fatalf("PutShard() = %v, %v", inserted, err)
				}
				assertFilesCommitted(t, ctx, st, f)
				switch test.name {
				case "full CAS blocks":
					shardHash, err := st.GetFileIndexEntry(ctx, f.FileHash)
					if err != nil {
						t.Fatal(err)
					}
					// No file starts in the unreferenced xorb: only its sampled chunk is probe-able.
					assertChunkIndexEntry(t, ctx, st, orphan.ChunkHashes[1], shardHash)
					assertChunkIndexEntry(t, ctx, st, orphan.ChunkHashes[0], "")
				case "dedup without CAS blocks":
					// The file's first chunk lives in a xorb the shard does not declare.
					assertChunkIndexEntry(t, ctx, st, f.ChunkHashes[0], "")
				}
				return
			}
			if inserted || !errors.Is(err, storage.ErrInvalidShard) || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("PutShard() = %v, %v, want %q", inserted, err, test.wantErr)
			}
			if got, err := st.GetFileIndexEntry(ctx, f.FileHash); err != nil || got != "" {
				t.Fatalf("GetFileIndexEntry() after rejection = %q, %v", got, err)
			}
			if got, err := st.GetSHA256IndexEntry(ctx, SHA256Digest(f.SHA256Hex)); err != nil || got != "" {
				t.Fatalf("GetSHA256IndexEntry() after rejection = %q, %v", got, err)
			}
			for _, cb := range sh.CASInfos {
				for _, chunk := range cb.Chunks {
					if got, err := st.GetChunkIndexEntry(ctx, chunk.ChunkHash); err != nil || got != "" {
						t.Fatalf("GetChunkIndexEntry(%s) after rejection = %q, %v", chunk.ChunkHash.String(), got, err)
					}
				}
			}

			honest := shard.NewShard()
			addXorbFile(t, ctx, st, honest, [][][]byte{parts})
			if inserted, err := storage.PutShard(ctx, st, honest, nil); err != nil || !inserted {
				t.Fatalf("honest PutShard() after rejection = %v, %v", inserted, err)
			}
			assertFilesCommitted(t, ctx, st, f)
			shardHash, err := st.GetFileIndexEntry(ctx, f.FileHash)
			if err != nil {
				t.Fatal(err)
			}
			for _, chunkHash := range f.ChunkHashes[:2] {
				assertChunkIndexEntry(t, ctx, st, chunkHash, shardHash)
				if _, err := st.GetShardByChunkHash(ctx, chunkHash); err != nil {
					t.Fatalf("GetShardByChunkHash(%s) after honest retry: %v", chunkHash.String(), err)
				}
			}
			assertChunkIndexEntry(t, ctx, st, f.ChunkHashes[2], "")
			if _, err := st.GetShardByChunkHash(ctx, f.ChunkHashes[2]); err == nil {
				t.Fatal("GetShardByChunkHash(plain chunk) after honest retry resolved; want not found")
			}
		})
	}
}

// testPutShardIndexesProbeableChunksOnly: PutShard indexes exactly the probe-able chunks — file
// starts, flagged and hash-sampled ones, also in a xorb no file references — and no other chunk,
// so WalkChunkIndex gives exactly their entries.
func testPutShardIndexesProbeableChunksOnly(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	sh := shard.NewShard()
	f := addXorbFile(t, ctx, st, sh, [][][]byte{{PlainChunk(t, "start"), SampledChunk(t, "mid"), PlainChunk(t, "tail"), PlainChunk(t, "flagged")}})
	sh.CASInfos[0].Chunks[3].Flags = shard.ChunkGlobalDedupEligible
	unreferenced := shard.NewShard()
	orphan := addXorbFile(t, ctx, st, unreferenced, [][][]byte{{PlainChunk(t, "orphan-a"), SampledChunk(t, "orphan-b")}})
	sh.AddCASBlock(unreferenced.CASInfos[0])
	if inserted, err := storage.PutShard(ctx, st, sh, nil); err != nil || !inserted {
		t.Fatalf("PutShard() = %v, %v", inserted, err)
	}
	shardHash, err := st.GetFileIndexEntry(ctx, f.FileHash)
	if err != nil || shardHash == "" {
		t.Fatalf("GetFileIndexEntry() = %q, %v", shardHash, err)
	}

	var probeable []string
	for _, c := range []struct {
		name string
		hash xet.ChunkHash
		want string
	}{
		{"file start", f.ChunkHashes[0], shardHash},
		{"sampled", f.ChunkHashes[1], shardHash},
		{"plain", f.ChunkHashes[2], ""},
		{"flagged", f.ChunkHashes[3], shardHash},
		{"unreferenced plain", orphan.ChunkHashes[0], ""},
		{"unreferenced sampled", orphan.ChunkHashes[1], shardHash},
	} {
		if got, err := st.GetChunkIndexEntry(ctx, c.hash); err != nil || got != c.want {
			t.Errorf("GetChunkIndexEntry(%s chunk) = %q, %v; want %q", c.name, got, err, c.want)
		}
		if c.want != "" {
			probeable = append(probeable, c.hash.String()+" -> "+c.want)
		}
	}
	slices.Sort(probeable)
	if got := indexEntries(t, ctx, st, storage.Storage.WalkChunkIndex); !slices.Equal(got, probeable) {
		t.Errorf("WalkChunkIndex() = %q, want exactly the probe-able chunks %q", got, probeable)
	}
	if _, err := st.GetShardByChunkHash(ctx, f.ChunkHashes[2]); err == nil {
		t.Error("GetShardByChunkHash(plain chunk) resolved; want not found")
	}
	if usage, err := st.Usage(ctx); err != nil || usage.ChunkIndex != (storage.ObjectUsage{Count: 4, Bytes: 4 * 64}) {
		t.Errorf("Usage().ChunkIndex = %+v, %v; want 4 entries of 64 bytes", usage.ChunkIndex, err)
	}
}

// A file spanning multi-chunk xorbs commits one whole-xorb term per xorb and
// reads back after a seek from its first term into its second.
func testPutShardCommitsMultiTermFile(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	xorbs := [][][]byte{
		{[]byte("first xorb, chunk 0"), []byte("first xorb, chunk 1, longer"), []byte("first xorb, chunk 2")},
		{[]byte("second xorb, chunk 0"), []byte("second xorb, chunk 1, longer"), []byte("second xorb, chunk 2")},
	}
	f := PutXorbFile(t, ctx, st, xorbs)

	sh, err := st.GetShard(ctx, f.FileHash)
	if err != nil {
		t.Fatalf("GetShard(): %v", err)
	}
	if len(sh.Files) != 1 || len(sh.Files[0].Entries) != 2 || len(sh.CASInfos) != 2 {
		t.Fatalf("shard has %d files, %d CAS blocks; want 1 file of 2 terms, 2 CAS blocks", len(sh.Files), len(sh.CASInfos))
	}
	for i, term := range sh.Files[0].Entries {
		cb := sh.CASInfos[i]
		if term.CASHash != f.XorbHashes[i] || term.ChunkIndexStart != 0 || term.ChunkIndexEnd != 3 || cb.CASHash != f.XorbHashes[i] || len(cb.Chunks) != 3 {
			t.Fatalf("term %d covers chunks [%d, %d) beside a CAS block of %d chunks; want all 3 chunks of xorb %s",
				i, term.ChunkIndexStart, term.ChunkIndexEnd, len(cb.Chunks), f.XorbHashes[i].String())
		}
	}
	assertFilesCommitted(t, ctx, st, f)

	digest := SHA256Digest(f.SHA256Hex)
	sh, err = st.GetShardBySHA256(ctx, digest)
	if err != nil {
		t.Fatalf("GetShardBySHA256(): %v", err)
	}
	file := sh.FindFileBySHA256(digest)
	if file == nil {
		t.Fatal("FindFileBySHA256(): not found")
	}
	rc := storage.NewReconstructedFile(ctx, st, file.Entries)
	defer rc.Close()
	if _, err := io.ReadFull(rc, make([]byte, 1)); err != nil {
		t.Fatalf("Read(): %v", err)
	}
	// Mid second chunk of the second xorb, so reopening its term skips a whole chunk.
	offset := int64(len(bytes.Join(xorbs[0], nil)) + len(xorbs[1][0]) + 1)
	if pos, err := rc.Seek(offset, io.SeekStart); err != nil || pos != offset {
		t.Fatalf("Seek(%d) = %d, %v", offset, pos, err)
	}
	if got, err := io.ReadAll(rc); err != nil || !bytes.Equal(got, f.Content[offset:]) {
		t.Fatalf("read after Seek(%d) = %q, %v; want %q", offset, got, err, f.Content[offset:])
	}
}

// Verifying counts 2 CAS blocks and 3 terms from (0, 5) to (5, 5), then Writing and Indexing follow once.
func testPutShardReportsPhasesInOrder(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	sh := shard.NewShard()
	addInterleavedFile(t, ctx, st, sh)
	rec := &phaseRecorder{}
	if inserted, err := storage.PutShard(ctx, st, sh, rec.report); err != nil || !inserted {
		t.Fatalf("PutShard() = %v, %v", inserted, err)
	}
	want := []phaseEvent{
		{phase: storage.PutShardVerifying, done: 0, total: 5},
		{phase: storage.PutShardVerifying, done: 1, total: 5},
		{phase: storage.PutShardVerifying, done: 2, total: 5},
		{phase: storage.PutShardVerifying, done: 3, total: 5},
		{phase: storage.PutShardVerifying, done: 4, total: 5},
		{phase: storage.PutShardVerifying, done: 5, total: 5},
		{phase: storage.PutShardWriting},
		{phase: storage.PutShardIndexing},
	}
	if !slices.Equal(rec.events, want) {
		t.Fatalf("PutShard() reported %+v, want %+v", rec.events, want)
	}
}

// A shard whose files are all indexed is a no-op that reports no phase.
func testPutShardReportsNothingForKnownShard(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	sh := shard.NewShard()
	addInterleavedFile(t, ctx, st, sh)
	if inserted, err := storage.PutShard(ctx, st, sh, nil); err != nil || !inserted {
		t.Fatalf("PutShard() = %v, %v", inserted, err)
	}
	rec := &phaseRecorder{}
	if inserted, err := storage.PutShard(ctx, st, sh, rec.report); err != nil || inserted {
		t.Fatalf("repeated PutShard() = %v, %v; want not inserted", inserted, err)
	}
	if len(rec.events) != 0 {
		t.Fatalf("repeated PutShard() reported %+v, want nothing", rec.events)
	}
}

// A forged chunk in the second CAS block ends the reports after the first block's, before any Put.
func testPutShardStopsReportingAtInvalidShard(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	sh := shard.NewShard()
	addInterleavedFile(t, ctx, st, sh)
	sh.CASInfos[1].Chunks[0].ChunkHash[0] ^= 1
	rec := &phaseRecorder{Storage: st}
	if inserted, err := storage.PutShard(ctx, rec, sh, rec.report); inserted || !errors.Is(err, storage.ErrInvalidShard) {
		t.Fatalf("PutShard() = %v, %v; want %v", inserted, err, storage.ErrInvalidShard)
	}
	want := []phaseEvent{
		{phase: storage.PutShardVerifying, done: 0, total: 5},
		{phase: storage.PutShardVerifying, done: 1, total: 5},
	}
	if !slices.Equal(rec.events, want) {
		t.Fatalf("PutShard() sent %+v, want %+v", rec.events, want)
	}
}

// Writing precedes the shard object's Put and Indexing the first index Put; indexes go chunks, sha256, files.
func testPutShardReportsWritePhasesBeforeTheirPuts(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	sh := shard.NewShard()
	addInterleavedFile(t, ctx, st, sh)
	rec := &phaseRecorder{Storage: st}
	if inserted, err := storage.PutShard(ctx, rec, sh, rec.report); err != nil || !inserted {
		t.Fatalf("PutShard() = %v, %v", inserted, err)
	}
	got := slices.DeleteFunc(rec.events, func(e phaseEvent) bool { return e.phase == storage.PutShardVerifying })
	want := []phaseEvent{
		{phase: storage.PutShardWriting}, {put: "shard"},
		{phase: storage.PutShardIndexing}, {put: "chunks"}, {put: "sha256"}, {put: "files"},
	}
	if got = slices.Compact(got); !slices.Equal(got, want) {
		t.Fatalf("PutShard() sent %+v after verifying, want %+v", got, want)
	}
}
