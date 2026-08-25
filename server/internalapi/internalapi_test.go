package internalapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/xorb"
)

// putTestFile stores content as one single-chunk xorb plus its shard and
// returns the xet file hash.
func putTestFile(t *testing.T, ctx context.Context, stor storage.Storage, content []byte) xet.FileHash {
	t.Helper()
	var encoded bytes.Buffer
	encoder := xorb.NewEncoder(&encoded, true)
	if _, err := encoder.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	xorbHash := encoder.SummoryHash()
	if _, err := stor.PutXorb(ctx, "default", xorbHash, bytes.NewReader(encoded.Bytes())); err != nil {
		t.Fatal(err)
	}
	chunkHash := xet.ComputeChunkHash(content)
	fileHash := xet.ComputeFileHash([]xet.ChunkHash{chunkHash}, []uint64{uint64(len(content))})
	shardObj := shard.NewShard()
	shardObj.AddCASBlock(shard.CASBlock{
		CASHash: xorbHash,
		Chunks:  []shard.CASChunkSequenceEntry{{ChunkHash: chunkHash, UnpackedSegBytes: uint32(len(content))}},
	})
	shardObj.AddFile(shard.FileBlock{
		FileHash: fileHash,
		Entries: []shard.FileDataSequenceEntry{
			{CASHash: xorbHash, UnpackedSegBytes: uint32(len(content)), ChunkIndexEnd: 1},
		},
	})
	if _, err := stor.PutShard(ctx, shardObj); err != nil {
		t.Fatal(err)
	}
	return fileHash
}

func TestListFilesEndpoint(t *testing.T) {
	ctx := context.Background()
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}

	content := []byte("internal file listing content")
	fileHash := putTestFile(t, ctx, stor, content)

	handler := NewHandler(WithStorage(stor))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/internal/files", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	var entries []storage.FileListEntry
	if err := json.NewDecoder(rec.Body).Decode(&entries); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	digest := sha256.Sum256(content)
	want := []storage.FileListEntry{{
		SHA256:       hex.EncodeToString(digest[:]),
		FileHashes:   []string{fileHash.String()},
		OriginalSize: uint64(len(content)),
	}}
	if len(entries) != 1 || entries[0].SHA256 != want[0].SHA256 ||
		len(entries[0].FileHashes) != 1 || entries[0].FileHashes[0] != want[0].FileHashes[0] ||
		entries[0].OriginalSize != want[0].OriginalSize || entries[0].Missing {
		t.Fatalf("entries = %+v, want %+v", entries, want)
	}
}

// listlessStorage implements storage.Storage but not storage.ListStore.
type listlessStorage struct{ storage.Storage }

func TestListFilesEndpointNotImplemented(t *testing.T) {
	handler := NewHandler(WithStorage(listlessStorage{}))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/internal/files", nil))
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotImplemented)
	}
}

func TestHandlerFallsThroughToNext(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	handler := NewHandler(WithNext(next))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/shards", nil))
	if rec.Code != http.StatusTeapot {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusTeapot)
	}
}

func TestUnlinkFileEndpoint(t *testing.T) {
	ctx := context.Background()
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	fileHash := putTestFile(t, ctx, stor, []byte("unlink endpoint content"))
	handler := NewHandler(WithStorage(stor))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/xet/"+fileHash.String(), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var resp map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp["file_hash"] != fileHash.String() || resp["removed"] != true {
		t.Fatalf("response = %v", resp)
	}
	if _, err := stor.GetShard(ctx, fileHash); err == nil {
		t.Fatal("file still resolves after unlink")
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/xet/"+fileHash.String(), nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("second unlink status = %d, want %d", rec.Code, http.StatusNotFound)
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/xet/not-a-hash", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid hash status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestGCSweepEndpoint(t *testing.T) {
	ctx := context.Background()
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	fileHash := putTestFile(t, ctx, stor, []byte("sweep endpoint content"))
	handler := NewHandler(WithStorage(stor))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/xet/"+fileHash.String(), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("unlink status = %d", rec.Code)
	}

	// Dry run reports the orphans but removes nothing.
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?dry_run=true&grace=0", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("dry run status = %d: %s", rec.Code, rec.Body.String())
	}
	var result storage.SweepResult
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("decode dry run: %v", err)
	}
	if !result.DryRun || len(result.SweptShards) != 1 || len(result.SweptXorbs) != 1 {
		t.Fatalf("dry run result = %+v", result)
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?grace=0", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("sweep status = %d: %s", rec.Code, rec.Body.String())
	}
	result = storage.SweepResult{}
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("decode sweep: %v", err)
	}
	if result.DryRun || len(result.SweptShards) != 1 || len(result.SweptXorbs) != 1 {
		t.Fatalf("sweep result = %+v", result)
	}

	// Everything is gone: a second sweep finds nothing.
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?grace=0", nil))
	result = storage.SweepResult{}
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("decode second sweep: %v", err)
	}
	if len(result.SweptShards) != 0 || len(result.SweptXorbs) != 0 {
		t.Fatalf("second sweep result = %+v", result)
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?grace=bogus", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bogus grace status = %d, want %d", rec.Code, http.StatusBadRequest)
	}

	// A negative grace is rejected rather than silently disabling the window.
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?grace=-5m", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("negative grace status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestGCEndpointsNotImplemented(t *testing.T) {
	handler := NewHandler(WithStorage(listlessStorage{}))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/xet/"+strings.Repeat("ab", 32), nil))
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("unlink status = %d, want %d", rec.Code, http.StatusNotImplemented)
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep", nil))
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("sweep status = %d, want %d", rec.Code, http.StatusNotImplemented)
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/compact", nil))
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("compact status = %d, want %d", rec.Code, http.StatusNotImplemented)
	}
}

// snapshotStore captures every stored shard, xorb, and file-index entry.
func snapshotStore(t *testing.T, ctx context.Context, gcs storage.GCStore) map[string]string {
	t.Helper()
	snap := map[string]string{}
	err := gcs.WalkShards(ctx, func(hash string, size int64, modTime time.Time) error {
		snap["shard/"+hash] = fmt.Sprintf("%d@%d", size, modTime.UnixNano())
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = gcs.WalkXorbs(ctx, func(hash string, size int64, modTime time.Time) error {
		snap["xorb/"+hash] = fmt.Sprintf("%d@%d", size, modTime.UnixNano())
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = gcs.WalkFileIndex(ctx, func(fileHash, shardHash string) error {
		snap["file/"+fileHash] = shardHash
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return snap
}

func TestGCCompactEndpoint(t *testing.T) {
	ctx := context.Background()
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	fileA := putTestFile(t, ctx, stor, []byte("compact endpoint content a"))
	fileB := putTestFile(t, ctx, stor, []byte("compact endpoint content b"))
	handler := NewHandler(WithStorage(stor))

	// Dry run reports the plan but writes nothing.
	before := snapshotStore(t, ctx, stor)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/compact?dry_run=true", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("dry run status = %d: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	var result storage.CompactResult
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("decode dry run: %v", err)
	}
	if !result.DryRun || result.SourceXorbs != 2 || result.EstimatedNewXorbs != 1 || result.XorbsWritten != 0 {
		t.Fatalf("dry run result = %+v", result)
	}
	if after := snapshotStore(t, ctx, stor); !reflect.DeepEqual(before, after) {
		t.Fatalf("store changed across a dry run:\nbefore: %v\nafter:  %v", before, after)
	}

	// The real run merges the two single-chunk xorbs into one dense xorb.
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/compact", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("compact status = %d: %s", rec.Code, rec.Body.String())
	}
	result = storage.CompactResult{}
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("decode compact: %v", err)
	}
	if result.DryRun || result.XorbsWritten != 1 || result.ShardsRewritten != 2 || result.FilesRepointed != 2 {
		t.Fatalf("compact result = %+v", result)
	}
	for _, fileHash := range []xet.FileHash{fileA, fileB} {
		if _, err := stor.GetShard(ctx, fileHash); err != nil {
			t.Fatalf("GetShard(%s) after compact: %v", fileHash.String(), err)
		}
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/compact?dry_run=bogus", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bogus dry_run status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

// blockingStore stalls the first WalkFileIndex until released, keeping the handler's GC single-flight lock held.
type blockingStore struct {
	*storage.FileStorage
	enterOnce sync.Once
	entered   chan struct{}
	release   chan struct{}
}

func (b *blockingStore) WalkFileIndex(ctx context.Context, fn func(fileHash, shardHash string) error) error {
	b.enterOnce.Do(func() { close(b.entered) })
	<-b.release
	return b.FileStorage.WalkFileIndex(ctx, fn)
}

func TestGCCompactEndpointBusy(t *testing.T) {
	ctx := context.Background()
	fs, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	putTestFile(t, ctx, fs, []byte("busy compact content"))
	bs := &blockingStore{FileStorage: fs, entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(bs.release) }) }
	t.Cleanup(release)
	handler := NewHandler(WithStorage(bs))

	// A sweep stalls inside its first walk, holding the GC lock.
	sweepDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?grace=0", nil))
		sweepDone <- rec
	}()
	<-bs.entered

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/compact", nil))
	if rec.Code != http.StatusConflict {
		t.Fatalf("compact while sweeping status = %d, want %d: %s", rec.Code, http.StatusConflict, rec.Body.String())
	}

	release()
	if rec := <-sweepDone; rec.Code != http.StatusOK {
		t.Fatalf("sweep status = %d: %s", rec.Code, rec.Body.String())
	}

	// The lock is free again: the same request now succeeds.
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/compact", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("compact after sweep status = %d: %s", rec.Code, rec.Body.String())
	}
}
