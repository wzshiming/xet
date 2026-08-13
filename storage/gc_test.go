package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/xorb"
)

// storedFile records the addresses of one file stored via putTestFile.
type storedFile struct {
	fileHash  xet.FileHash
	chunkHash xet.ChunkHash
	xorbHash  xet.XorbHash
	shardHash string
	sha256    [32]byte
}

// putTestXorb encodes content as a single-chunk xorb and stores it.
func putTestXorb(t *testing.T, fs *FileStorage, content []byte) xet.XorbHash {
	t.Helper()
	var encoded bytes.Buffer
	enc := xorb.NewEncoder(&encoded, true)
	if _, err := enc.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	xorbHash := enc.SummoryHash()
	if _, err := fs.PutXorb(context.Background(), "default", xorbHash, bytes.NewReader(encoded.Bytes())); err != nil {
		t.Fatal(err)
	}
	return xorbHash
}

// putTestFile stores content as one single-chunk xorb plus a shard describing
// the file, mirroring the normal upload pipeline output.
func putTestFile(t *testing.T, fs *FileStorage, content []byte) storedFile {
	t.Helper()
	xorbHash := putTestXorb(t, fs, content)
	chunkHash := xet.ComputeChunkHash(content)
	fileHash := xet.ComputeFileHash([]xet.ChunkHash{chunkHash}, []uint64{uint64(len(content))})
	s := shard.NewShard()
	s.AddFile(shard.FileBlock{
		FileHash: fileHash,
		Entries: []shard.FileDataSequenceEntry{
			{CASHash: xorbHash, UnpackedSegBytes: uint32(len(content)), ChunkIndexEnd: 1},
		},
	})
	s.AddCASBlock(shard.CASBlock{
		CASHash: xorbHash,
		Chunks:  []shard.CASChunkSequenceEntry{{ChunkHash: chunkHash, UnpackedSegBytes: uint32(len(content))}},
	})
	if _, err := fs.PutShard(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(fs.objectPath("files", fileHash.String()))
	if err != nil {
		t.Fatal(err)
	}
	return storedFile{
		fileHash:  fileHash,
		chunkHash: chunkHash,
		xorbHash:  xorbHash,
		shardHash: string(b),
		sha256:    sha256.Sum256(content),
	}
}

func mustExist(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("%s should exist: %v", path, err)
	}
}

func mustNotExist(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("%s should have been removed (err=%v)", path, err)
	}
}

func TestGCStandaloneCollectsOrphans(t *testing.T) {
	basePath := t.TempDir()
	fs, err := NewFileStorage(WithBasePath(basePath))
	if err != nil {
		t.Fatal(err)
	}

	live := putTestFile(t, fs, []byte("live file content"))
	orphanXorb := putTestXorb(t, fs, []byte("orphan xorb, upload never finished"))
	dead := putTestFile(t, fs, []byte("superseded upload"))
	// Simulate a superseded upload: the file index entry is gone but shard,
	// xorb and the chunk/sha256 index entries linger.
	if err := os.Remove(fs.objectPath("files", dead.fileHash.String())); err != nil {
		t.Fatal(err)
	}

	res, err := fs.GC(context.Background(), WithGCGracePeriod(0))
	if err != nil {
		t.Fatal(err)
	}

	if res.LiveFiles != 1 || res.LiveShards != 1 || res.LiveXorbs != 1 {
		t.Fatalf("live counts = %d/%d/%d, want 1/1/1", res.LiveFiles, res.LiveShards, res.LiveXorbs)
	}
	if res.RemovedXorbs != 2 || res.RemovedShards != 1 || res.RemovedChunks != 1 || res.RemovedSHA256s != 1 {
		t.Fatalf("removed counts = xorbs %d, shards %d, chunks %d, sha256 %d; want 2/1/1/1",
			res.RemovedXorbs, res.RemovedShards, res.RemovedChunks, res.RemovedSHA256s)
	}
	if res.ReclaimedBytes <= 0 {
		t.Fatalf("ReclaimedBytes = %d, want > 0", res.ReclaimedBytes)
	}

	mustNotExist(t, fs.objectPath("xorbs", orphanXorb.String()))
	mustNotExist(t, fs.objectPath("shards", dead.shardHash))
	mustNotExist(t, fs.objectPath("xorbs", dead.xorbHash.String()))
	mustNotExist(t, fs.objectPath("chunks", dead.chunkHash.String()))
	mustNotExist(t, fs.objectPath("sha256", shard.NewSHA256Hash(dead.sha256).String()))

	mustExist(t, fs.objectPath("files", live.fileHash.String()))
	mustExist(t, fs.objectPath("shards", live.shardHash))
	mustExist(t, fs.objectPath("xorbs", live.xorbHash.String()))
	mustExist(t, fs.objectPath("chunks", live.chunkHash.String()))
	mustExist(t, fs.objectPath("sha256", shard.NewSHA256Hash(live.sha256).String()))

	// The surviving file must still reconstruct through a fresh storage.
	fs2, err := NewFileStorage(WithBasePath(basePath))
	if err != nil {
		t.Fatal(err)
	}
	rc, err := fs2.GetReconstructedFile(context.Background(), "default", live.sha256)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "live file content" {
		t.Fatalf("reconstructed = %q", got)
	}
}

func TestGCWithExplicitRoots(t *testing.T) {
	fs, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}

	kept := putTestFile(t, fs, []byte("still referenced by the mirror index"))
	dropped := putTestFile(t, fs, []byte("old commit content"))

	res, err := fs.GC(context.Background(),
		WithGCGracePeriod(0),
		WithGCRoots([]xet.FileHash{kept.fileHash}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if res.RemovedFiles != 1 || res.RemovedShards != 1 || res.RemovedXorbs != 1 {
		t.Fatalf("removed counts = files %d, shards %d, xorbs %d; want 1/1/1",
			res.RemovedFiles, res.RemovedShards, res.RemovedXorbs)
	}

	mustNotExist(t, fs.objectPath("files", dropped.fileHash.String()))
	mustNotExist(t, fs.objectPath("shards", dropped.shardHash))
	mustNotExist(t, fs.objectPath("xorbs", dropped.xorbHash.String()))
	mustNotExist(t, fs.objectPath("chunks", dropped.chunkHash.String()))
	mustNotExist(t, fs.objectPath("sha256", shard.NewSHA256Hash(dropped.sha256).String()))

	mustExist(t, fs.objectPath("files", kept.fileHash.String()))
	mustExist(t, fs.objectPath("shards", kept.shardHash))
	mustExist(t, fs.objectPath("xorbs", kept.xorbHash.String()))

	// The dedup path must not resurrect the removed shard.
	if _, err := fs.GetShardByChunkHash(context.Background(), "default", dropped.chunkHash); err == nil {
		t.Fatal("GetShardByChunkHash should fail for a collected chunk")
	}
	if _, err := fs.GetShard(context.Background(), kept.fileHash); err != nil {
		t.Fatalf("kept file lookup: %v", err)
	}
}

func TestGCSharedShardKeepsStorageForNonRootFile(t *testing.T) {
	ctx := context.Background()
	fs, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}

	// Two files described by one shard: collecting one root must keep the
	// shard and every xorb it references, while the other file's own index
	// entries go away.
	contents := [][]byte{[]byte("file one content"), []byte("file two content")}
	s := shard.NewShard()
	var files []storedFile
	for _, content := range contents {
		xorbHash := putTestXorb(t, fs, content)
		chunkHash := xet.ComputeChunkHash(content)
		fileHash := xet.ComputeFileHash([]xet.ChunkHash{chunkHash}, []uint64{uint64(len(content))})
		s.AddFile(shard.FileBlock{
			FileHash: fileHash,
			Entries: []shard.FileDataSequenceEntry{
				{CASHash: xorbHash, UnpackedSegBytes: uint32(len(content)), ChunkIndexEnd: 1},
			},
		})
		s.AddCASBlock(shard.CASBlock{
			CASHash: xorbHash,
			Chunks:  []shard.CASChunkSequenceEntry{{ChunkHash: chunkHash, UnpackedSegBytes: uint32(len(content))}},
		})
		files = append(files, storedFile{
			fileHash:  fileHash,
			chunkHash: chunkHash,
			xorbHash:  xorbHash,
			sha256:    sha256.Sum256(content),
		})
	}
	if _, err := fs.PutShard(ctx, s); err != nil {
		t.Fatal(err)
	}
	shardHashBytes, err := os.ReadFile(fs.objectPath("files", files[0].fileHash.String()))
	if err != nil {
		t.Fatal(err)
	}
	shardHash := string(shardHashBytes)

	res, err := fs.GC(ctx,
		WithGCGracePeriod(0),
		WithGCRoots([]xet.FileHash{files[0].fileHash}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if res.RemovedShards != 0 || res.RemovedXorbs != 0 || res.RemovedChunks != 0 {
		t.Fatalf("shared shard storage was collected: shards %d, xorbs %d, chunks %d",
			res.RemovedShards, res.RemovedXorbs, res.RemovedChunks)
	}

	mustExist(t, fs.objectPath("shards", shardHash))
	mustExist(t, fs.objectPath("xorbs", files[1].xorbHash.String()))
	mustExist(t, fs.objectPath("chunks", files[1].chunkHash.String()))
	mustNotExist(t, fs.objectPath("files", files[1].fileHash.String()))
	mustNotExist(t, fs.objectPath("sha256", shard.NewSHA256Hash(files[1].sha256).String()))
	mustExist(t, fs.objectPath("files", files[0].fileHash.String()))
	mustExist(t, fs.objectPath("sha256", shard.NewSHA256Hash(files[0].sha256).String()))
}

func TestGCGracePeriodProtectsRecentObjects(t *testing.T) {
	fs, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}

	orphanXorb := putTestXorb(t, fs, []byte("just uploaded, shard not registered yet"))
	fresh := putTestFile(t, fs, []byte("ingest finished moments ago"))

	// No roots at all: without the grace period everything would go.
	res, err := fs.GC(context.Background(),
		WithGCGracePeriod(time.Hour),
		WithGCRoots(nil),
	)
	if err != nil {
		t.Fatal(err)
	}
	if total := res.RemovedFiles + res.RemovedShards + res.RemovedXorbs + res.RemovedChunks + res.RemovedSHA256s; total != 0 {
		t.Fatalf("removed %d objects inside the grace period", total)
	}
	// The young file index entry pins its shard and xorb as in-flight work.
	if res.LiveFiles != 1 || res.LiveShards != 1 || res.LiveXorbs != 1 {
		t.Fatalf("live counts = %d/%d/%d, want grace-pinned 1/1/1", res.LiveFiles, res.LiveShards, res.LiveXorbs)
	}
	mustExist(t, fs.objectPath("xorbs", orphanXorb.String()))
	mustExist(t, fs.objectPath("shards", fresh.shardHash))
}

func TestGCDryRunRemovesNothing(t *testing.T) {
	fs, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}

	orphanXorb := putTestXorb(t, fs, []byte("orphan"))
	dead := putTestFile(t, fs, []byte("unreferenced file"))

	res, err := fs.GC(context.Background(),
		WithGCGracePeriod(0),
		WithGCRoots(nil),
		WithGCDryRun(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	if res.RemovedXorbs != 2 || res.RemovedShards != 1 || res.RemovedFiles != 1 {
		t.Fatalf("dry run counts = xorbs %d, shards %d, files %d; want 2/1/1",
			res.RemovedXorbs, res.RemovedShards, res.RemovedFiles)
	}
	mustExist(t, fs.objectPath("xorbs", orphanXorb.String()))
	mustExist(t, fs.objectPath("files", dead.fileHash.String()))
	mustExist(t, fs.objectPath("shards", dead.shardHash))
	mustExist(t, fs.objectPath("xorbs", dead.xorbHash.String()))
}

func TestGCKeepsXorbsDeduplicatedFromDeadShards(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	fs, err := NewFileStorage(WithBasePath(basePath))
	if err != nil {
		t.Fatal(err)
	}

	// File A owns the xorb; file B deduplicated against it, so B's shard
	// references A's xorb only through its file term entries, never through
	// its own CASInfos. Collecting A must keep the xorb alive for B.
	content := []byte("shared deduplicated content")
	a := putTestFile(t, fs, content)

	bContent := []byte("unique b content")
	bXorb := putTestXorb(t, fs, bContent)
	bChunk := xet.ComputeChunkHash(bContent)
	bFileHash := xet.ComputeFileHash(
		[]xet.ChunkHash{a.chunkHash, bChunk},
		[]uint64{uint64(len(content)), uint64(len(bContent))},
	)
	s := shard.NewShard()
	s.AddFile(shard.FileBlock{
		FileHash: bFileHash,
		Entries: []shard.FileDataSequenceEntry{
			{CASHash: a.xorbHash, UnpackedSegBytes: uint32(len(content)), ChunkIndexEnd: 1},
			{CASHash: bXorb, UnpackedSegBytes: uint32(len(bContent)), ChunkIndexEnd: 1},
		},
	})
	// Only the newly uploaded xorb appears in CASInfos, like BuildShard
	// produces after a dedup hit.
	s.AddCASBlock(shard.CASBlock{
		CASHash: bXorb,
		Chunks:  []shard.CASChunkSequenceEntry{{ChunkHash: bChunk, UnpackedSegBytes: uint32(len(bContent))}},
	})
	if _, err := fs.PutShard(ctx, s); err != nil {
		t.Fatal(err)
	}

	res, err := fs.GC(ctx,
		WithGCGracePeriod(0),
		WithGCRoots([]xet.FileHash{bFileHash}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if res.RemovedXorbs != 0 {
		t.Fatalf("RemovedXorbs = %d, deduplicated xorb was collected", res.RemovedXorbs)
	}
	mustExist(t, fs.objectPath("xorbs", a.xorbHash.String()))
	mustNotExist(t, fs.objectPath("shards", a.shardHash))
	mustNotExist(t, fs.objectPath("files", a.fileHash.String()))

	// B must still reconstruct byte for byte through a fresh storage.
	fs2, err := NewFileStorage(WithBasePath(basePath))
	if err != nil {
		t.Fatal(err)
	}
	bSHA := sha256.Sum256(append(append([]byte{}, content...), bContent...))
	rc, err := fs2.GetReconstructedFile(ctx, "default", bSHA)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, append(append([]byte{}, content...), bContent...)) {
		t.Fatalf("reconstructed = %q", got)
	}
}

func TestGCSweepsStaleTempFiles(t *testing.T) {
	basePath := t.TempDir()
	fs, err := NewFileStorage(WithBasePath(basePath))
	if err != nil {
		t.Fatal(err)
	}

	staleShardTemp := filepath.Join(basePath, "shards", ".shard-12345")
	if err := os.WriteFile(staleShardTemp, []byte("interrupted"), 0644); err != nil {
		t.Fatal(err)
	}
	xorbTempDir := filepath.Join(basePath, "xorbs", "ab")
	if err := os.MkdirAll(xorbTempDir, 0755); err != nil {
		t.Fatal(err)
	}
	staleXorbTemp := filepath.Join(xorbTempDir, "cdef.tmp")
	if err := os.WriteFile(staleXorbTemp, []byte("interrupted"), 0644); err != nil {
		t.Fatal(err)
	}
	stray := filepath.Join(basePath, "xorbs", "README")
	if err := os.WriteFile(stray, []byte("not garbage"), 0644); err != nil {
		t.Fatal(err)
	}

	res, err := fs.GC(context.Background(), WithGCGracePeriod(0))
	if err != nil {
		t.Fatal(err)
	}
	if res.RemovedTemps != 2 {
		t.Fatalf("RemovedTemps = %d, want 2", res.RemovedTemps)
	}
	mustNotExist(t, staleShardTemp)
	mustNotExist(t, staleXorbTemp)
	mustExist(t, stray)
}
