package internalapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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
	fs, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}

	content := []byte("internal file listing content")
	fileHash := putTestFile(t, ctx, fs, content)

	handler := NewHandler(WithStorage(fs))
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
	fs, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	fileHash := putTestFile(t, ctx, fs, []byte("unlink endpoint content"))
	handler := NewHandler(WithStorage(fs))

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
	if _, err := fs.GetShard(ctx, fileHash); err == nil {
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

func TestUnlinkSHA256Endpoint(t *testing.T) {
	ctx := context.Background()
	fs, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("sha256 unlink endpoint content")
	putTestFile(t, ctx, fs, content)
	digest := sha256.Sum256(content)
	shaHex := hex.EncodeToString(digest[:])
	handler := NewHandler(WithStorage(fs))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/sha256/"+shaHex, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var resp map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp["sha256"] != shaHex || resp["removed"] != true {
		t.Fatalf("response = %v", resp)
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/sha256/"+shaHex, nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("second unlink status = %d, want %d", rec.Code, http.StatusNotFound)
	}

	// Under anchor=sha256 the remaining file entry does not anchor: the
	// shard and its xorb are reclaimed.
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?grace=0&anchor=sha256", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("sweep status = %d: %s", rec.Code, rec.Body.String())
	}
	var result storage.SweepResult
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("decode sweep: %v", err)
	}
	if len(result.SweptShards) != 1 || len(result.SweptXorbs) != 1 {
		t.Fatalf("sweep result = %+v", result)
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?grace=0&anchor=sha256", nil))
	result = storage.SweepResult{}
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("decode second sweep: %v", err)
	}
	if len(result.SweptShards) != 0 || len(result.SweptXorbs) != 0 {
		t.Fatalf("second sweep result = %+v", result)
	}
}

func TestUnlinkSHA256EndpointRejectsBadDigests(t *testing.T) {
	fs, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(WithStorage(fs))

	for _, hash := range []string{
		"abc",                    // too short
		strings.Repeat("ab", 33), // too long
		strings.Repeat("zz", 32), // not hex
		strings.Repeat("00", 32), // all-zero empty-file marker
	} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/sha256/"+hash, nil))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%q status = %d, want %d", hash, rec.Code, http.StatusBadRequest)
		}
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/sha256/"+strings.Repeat("ab", 32), nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown digest status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestGCSweepEndpoint(t *testing.T) {
	ctx := context.Background()
	fs, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	fileHash := putTestFile(t, ctx, fs, []byte("sweep endpoint content"))
	handler := NewHandler(WithStorage(fs))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/xet/"+fileHash.String(), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("unlink status = %d", rec.Code)
	}

	// Dry run reports the orphans but removes nothing. anchor=files keeps
	// this a legacy-mode flow where the file unlink alone orphans the shard.
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?dry_run=true&grace=0&anchor=files", nil))
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
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?grace=0&anchor=files", nil))
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
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?grace=0&anchor=files", nil))
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

// TestGCSweepEndpointStepped drains a two-file store one dead object per
// request and checks the cumulative progress reporting.
func TestGCSweepEndpointStepped(t *testing.T) {
	ctx := context.Background()
	fs, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(WithStorage(fs))
	for i, content := range [][]byte{[]byte("stepped sweep one"), []byte("stepped sweep two")} {
		fileHash := putTestFile(t, ctx, fs, content)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/xet/"+fileHash.String(), nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("unlink %d status = %d", i, rec.Code)
		}
	}

	var result storage.SweepResult
	steps := 0
	remaining := -1
	for {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?grace=0&max=1&anchor=files", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("step %d status = %d: %s", steps, rec.Code, rec.Body.String())
		}
		result = storage.SweepResult{}
		if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
			t.Fatalf("decode step %d: %v", steps, err)
		}
		steps++
		if got := result.RemainingShards + result.RemainingXorbs; remaining >= 0 && got >= remaining {
			t.Fatalf("step %d remaining = %d, want < %d", steps, got, remaining)
		} else {
			remaining = got
		}
		if result.Done {
			break
		}
		if steps > 10 {
			t.Fatalf("cycle not done after %d steps: %+v", steps, result)
		}
	}
	if steps < 2 {
		t.Fatalf("steps = %d, want >= 2", steps)
	}
	// The final step reports the whole cycle's work.
	if len(result.SweptShards) != 2 || len(result.SweptXorbs) != 2 || result.RemainingShards != 0 || result.RemainingXorbs != 0 {
		t.Fatalf("final result = %+v", result)
	}

	// Nothing is left for a full pass.
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?grace=0&anchor=files", nil))
	result = storage.SweepResult{}
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("decode full pass: %v", err)
	}
	if len(result.SweptShards) != 0 || len(result.SweptXorbs) != 0 {
		t.Fatalf("full pass after drain = %+v", result)
	}
}

func TestGCSweepEndpointRejectsInvalidStepParams(t *testing.T) {
	fs, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(WithStorage(fs))
	for _, query := range []string{
		"max=bogus", "max=-1",
		"budget=bogus", "budget=-5s",
		"clean_chunks=bogus",
	} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?"+query, nil))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want %d", query, rec.Code, http.StatusBadRequest)
		}
	}
}

// TestGCSweepEndpointCleanChunks: clean_chunks=true reaches the sweep
// options — an entry orphaned by out-of-band shard loss survives a default
// sweep and is cleaned by a flagged one.
func TestGCSweepEndpointCleanChunks(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	fs, err := storage.NewFileStorage(storage.WithBasePath(dir))
	if err != nil {
		t.Fatal(err)
	}
	fileHash := putTestFile(t, ctx, fs, []byte("clean chunks endpoint"))
	shardHash, _, err := fs.GetFileIndexEntry(ctx, fileHash)
	if err != nil || shardHash == "" {
		t.Fatalf("file index entry = %q, %v", shardHash, err)
	}
	var chunkHashes []string
	if err := fs.WalkChunkIndex(ctx, func(chunkHash, _ string, _ time.Time) error {
		chunkHashes = append(chunkHashes, chunkHash)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(chunkHashes) == 0 {
		t.Fatal("no chunk entries stored")
	}
	// The shard object vanishes out-of-band, orphaning the chunk entries.
	if err := os.Remove(filepath.Join(dir, "shards", shardHash[:2], shardHash[2:])); err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(WithStorage(fs))

	// Without the flag the orphaned entries survive a full sweep.
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?grace=0", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("default sweep status = %d: %s", rec.Code, rec.Body.String())
	}
	var result storage.SweepResult
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.DeletedChunkEntries != 0 || result.RepointedChunkEntries != 0 {
		t.Fatalf("default sweep touched chunk entries: %+v", result)
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?grace=0&clean_chunks=true", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("clean_chunks sweep status = %d: %s", rec.Code, rec.Body.String())
	}
	result = storage.SweepResult{}
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.DeletedChunkEntries != len(chunkHashes) {
		t.Fatalf("DeletedChunkEntries = %d, want %d", result.DeletedChunkEntries, len(chunkHashes))
	}
	for _, hexHash := range chunkHashes {
		ch, err := xet.ParseChunkHash(hexHash)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := fs.GetChunkIndexEntry(ctx, ch); err != nil || got != "" {
			t.Fatalf("chunk entry %s = %q, %v; want removed", hexHash, got, err)
		}
	}
}

// TestGCSweepEndpointServerGrace: WithGCGrace supplies the window for
// requests omitting grace; an explicit parameter still overrides it.
func TestGCSweepEndpointServerGrace(t *testing.T) {
	ctx := context.Background()

	// Disabled server default: a plain request reclaims fresh objects.
	fs, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(WithStorage(fs), WithGCGrace(-1))
	fileHash := putTestFile(t, ctx, fs, []byte("server grace content"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/xet/"+fileHash.String(), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("unlink status = %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?anchor=files", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("sweep status = %d: %s", rec.Code, rec.Body.String())
	}
	var result storage.SweepResult
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("decode sweep: %v", err)
	}
	if len(result.SweptShards) != 1 || len(result.SweptXorbs) != 1 {
		t.Fatalf("sweep with disabled server grace = %+v", result)
	}

	// Unset option: a plain request keeps the default window and shields
	// the fresh objects.
	fs2, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	handler = NewHandler(WithStorage(fs2))
	fileHash = putTestFile(t, ctx, fs2, []byte("server grace default"))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/xet/"+fileHash.String(), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("unlink status = %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?anchor=files", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("default sweep status = %d: %s", rec.Code, rec.Body.String())
	}
	result = storage.SweepResult{}
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("decode default sweep: %v", err)
	}
	if len(result.SweptShards) != 0 || len(result.SweptXorbs) != 0 {
		t.Fatalf("default-window sweep = %+v", result)
	}

	// An explicit parameter overrides the disabled server default.
	handler = NewHandler(WithStorage(fs2), WithGCGrace(-1))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?grace=1h&anchor=files", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("override sweep status = %d: %s", rec.Code, rec.Body.String())
	}
	result = storage.SweepResult{}
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("decode override sweep: %v", err)
	}
	if len(result.SweptShards) != 0 || len(result.SweptXorbs) != 0 {
		t.Fatalf("override sweep = %+v", result)
	}
}

// TestGCSweepEndpointDefaultAnchor: under the default anchor full removal
// takes both unlinks — the file unlink alone leaves the sha256 entry
// anchoring the shard.
func TestGCSweepEndpointDefaultAnchor(t *testing.T) {
	ctx := context.Background()
	fs, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("default anchor content")
	fileHash := putTestFile(t, ctx, fs, content)
	digest := sha256.Sum256(content)
	handler := NewHandler(WithStorage(fs))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/xet/"+fileHash.String(), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("unlink status = %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?grace=0", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("sweep status = %d: %s", rec.Code, rec.Body.String())
	}
	var result storage.SweepResult
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("decode sweep: %v", err)
	}
	if len(result.SweptShards) != 0 || len(result.SweptXorbs) != 0 {
		t.Fatalf("sweep after file unlink alone = %+v", result)
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/sha256/"+hex.EncodeToString(digest[:]), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("sha256 unlink status = %d: %s", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?grace=0", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("second sweep status = %d: %s", rec.Code, rec.Body.String())
	}
	result = storage.SweepResult{}
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("decode second sweep: %v", err)
	}
	if len(result.SweptShards) != 1 || len(result.SweptXorbs) != 1 {
		t.Fatalf("sweep after both unlinks = %+v", result)
	}
}

// TestGCSweepEndpointAnchorParam: the anchor parameter selects the sweep
// anchor, WithGCAnchor supplies it when omitted, and unknown or misspelled
// values are rejected.
func TestGCSweepEndpointAnchorParam(t *testing.T) {
	ctx := context.Background()
	fs, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(WithStorage(fs))

	for _, v := range []string{"bogus", "Files", "SHA256", "BOTH", "file"} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?anchor="+v, nil))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("anchor=%s status = %d, want %d", v, rec.Code, http.StatusBadRequest)
		}
	}

	// An explicit anchor=both matches the default: after a file unlink the
	// sha256 entry still anchors the shard.
	fileHash := putTestFile(t, ctx, fs, []byte("anchor param content"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/xet/"+fileHash.String(), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("unlink status = %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?grace=0&anchor=both", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("anchor=both sweep status = %d: %s", rec.Code, rec.Body.String())
	}
	var result storage.SweepResult
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("decode anchor=both sweep: %v", err)
	}
	if len(result.SweptShards) != 0 || len(result.SweptXorbs) != 0 {
		t.Fatalf("anchor=both sweep = %+v", result)
	}

	// WithGCAnchor supplies the anchor for requests omitting the parameter;
	// an explicit parameter still overrides the server default.
	filesHandler := NewHandler(WithStorage(fs), WithGCAnchor(storage.AnchorFiles))
	rec = httptest.NewRecorder()
	filesHandler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?grace=0&anchor=both", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("override sweep status = %d: %s", rec.Code, rec.Body.String())
	}
	result = storage.SweepResult{}
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("decode override sweep: %v", err)
	}
	if len(result.SweptShards) != 0 || len(result.SweptXorbs) != 0 {
		t.Fatalf("override sweep = %+v", result)
	}
	rec = httptest.NewRecorder()
	filesHandler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?grace=0", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("server-default sweep status = %d: %s", rec.Code, rec.Body.String())
	}
	result = storage.SweepResult{}
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("decode server-default sweep: %v", err)
	}
	if len(result.SweptShards) != 1 || len(result.SweptXorbs) != 1 {
		t.Fatalf("server-default anchor sweep = %+v", result)
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
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/sha256/"+strings.Repeat("ab", 32), nil))
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("sha256 unlink status = %d, want %d", rec.Code, http.StatusNotImplemented)
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/shards/"+strings.Repeat("ab", 32), nil))
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("shard delete status = %d, want %d", rec.Code, http.StatusNotImplemented)
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep", nil))
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("sweep status = %d, want %d", rec.Code, http.StatusNotImplemented)
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/internal/gc/status", nil))
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("gc status = %d, want %d", rec.Code, http.StatusNotImplemented)
	}
}

// gcHookedStorage overrides one GC delete call of a FileStorage so tests
// can observe its context or fail it.
type gcHookedStorage struct {
	*storage.FileStorage
	onDeleteXorb func(ctx context.Context, xorbHash xet.XorbHash) error
}

func (h *gcHookedStorage) DeleteXorb(ctx context.Context, xorbHash xet.XorbHash) error {
	if h.onDeleteXorb != nil {
		if err := h.onDeleteXorb(ctx, xorbHash); err != nil {
			return err
		}
	}
	return h.FileStorage.DeleteXorb(ctx, xorbHash)
}

// TestGCSweepEndpointDetachedFromRequestContext: a client vanishing
// mid-step must not cancel the sweep — the handler hands SweepStep a
// context detached from the request's cancellation.
func TestGCSweepEndpointDetachedFromRequestContext(t *testing.T) {
	ctx := context.Background()
	fs, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	hooked := &gcHookedStorage{FileStorage: fs}
	handler := NewHandler(WithStorage(hooked))

	fileHash := putTestFile(t, ctx, fs, []byte("detached sweep content"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/xet/"+fileHash.String(), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("unlink status = %d", rec.Code)
	}

	reqCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	deletes := 0
	var deleteCtxErr error
	hooked.onDeleteXorb = func(callCtx context.Context, _ xet.XorbHash) error {
		deletes++
		cancel() // the client vanishes mid-delete
		deleteCtxErr = callCtx.Err()
		return nil
	}
	rec = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?grace=0&anchor=files", nil).WithContext(reqCtx)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("sweep status = %d: %s", rec.Code, rec.Body.String())
	}
	if deletes != 1 {
		t.Fatalf("deletes = %d, want 1", deletes)
	}
	if deleteCtxErr != nil {
		t.Fatalf("backend delete saw context error %v, want the step detached from the request context", deleteCtxErr)
	}
	var result storage.SweepResult
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("decode sweep: %v", err)
	}
	if !result.Done || len(result.SweptShards) != 1 || len(result.SweptXorbs) != 1 {
		t.Fatalf("sweep result = %+v, want the full pass despite the canceled request", result)
	}
}

// TestGCStatusEndpoint: GET /internal/gc/status reports the idle state, a
// parked cycle with its phase and queues, and the last step's result.
func TestGCStatusEndpoint(t *testing.T) {
	ctx := context.Background()
	fs, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(WithStorage(fs))

	getStatus := func() storage.GCStatus {
		t.Helper()
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/internal/gc/status", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("status code = %d: %s", rec.Code, rec.Body.String())
		}
		if got := rec.Header().Get("Content-Type"); got != "application/json" {
			t.Fatalf("Content-Type = %q, want application/json", got)
		}
		var s storage.GCStatus
		if err := json.NewDecoder(rec.Body).Decode(&s); err != nil {
			t.Fatalf("decode status: %v", err)
		}
		return s
	}

	if s := getStatus(); s.Running || s.Parked || s.LastResult != nil {
		t.Fatalf("idle status = %+v, want zero state", s)
	}

	for i, content := range [][]byte{[]byte("status endpoint one"), []byte("status endpoint two")} {
		fileHash := putTestFile(t, ctx, fs, content)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/xet/"+fileHash.String(), nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("unlink %d status = %d", i, rec.Code)
		}
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?grace=0&max=1&anchor=files", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("sweep status = %d: %s", rec.Code, rec.Body.String())
	}
	s := getStatus()
	if s.Running || !s.Parked || s.ParkedPhase != "shards" || s.RemainingShards != 1 || s.Marked.IsZero() {
		t.Fatalf("parked status = %+v, want a parked shard-phase cycle with one shard left", s)
	}
	if s.LastResult == nil || s.LastResult.Done || len(s.LastResult.SweptShards) != 1 {
		t.Fatalf("parked LastResult = %+v, want the first step's progress", s.LastResult)
	}

	for steps := 0; ; steps++ {
		if steps > 10 {
			t.Fatal("cycle not done after 10 steps")
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?grace=0&max=1&anchor=files", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("step status = %d: %s", rec.Code, rec.Body.String())
		}
		var result storage.SweepResult
		if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
			t.Fatalf("decode step: %v", err)
		}
		if result.Done {
			break
		}
	}
	s = getStatus()
	if s.Running || s.Parked || s.LastResult == nil || !s.LastResult.Done {
		t.Fatalf("final status = %+v, want idle with a done LastResult", s)
	}
	if len(s.LastResult.SweptShards) != 2 || len(s.LastResult.SweptXorbs) != 2 {
		t.Fatalf("final LastResult = %+v, want the full cycle accumulated", s.LastResult)
	}
}

// TestDeleteShardEndpoint: DELETE /internal/shards/{hash} removes a corrupt
// shard object outright, refuses a live referenced one until force=true,
// and rejects malformed input.
func TestDeleteShardEndpoint(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	fs, err := storage.NewFileStorage(storage.WithBasePath(dir))
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(WithStorage(fs))

	shardOf := func(fileHash xet.FileHash) string {
		t.Helper()
		shardHash, _, err := fs.GetFileIndexEntry(ctx, fileHash)
		if err != nil || shardHash == "" {
			t.Fatalf("file index entry = %q, %v", shardHash, err)
		}
		return shardHash
	}
	deleteShard := func(path string) (*httptest.ResponseRecorder, map[string]any) {
		t.Helper()
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, path, nil))
		var resp map[string]any
		if strings.HasPrefix(rec.Header().Get("Content-Type"), "application/json") {
			if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
				t.Fatalf("decode %s response: %v", path, err)
			}
		}
		return rec, resp
	}

	// A corrupt stored shard is the remediation target: removed without force.
	corruptHash := shardOf(putTestFile(t, ctx, fs, []byte("corrupt shard endpoint")))
	if err := os.WriteFile(filepath.Join(dir, "shards", corruptHash[:2], corruptHash[2:]), bytes.Repeat([]byte{0xff}, 128), 0644); err != nil {
		t.Fatal(err)
	}
	rec, resp := deleteShard("/internal/shards/" + corruptHash)
	if rec.Code != http.StatusOK {
		t.Fatalf("corrupt delete status = %d: %s", rec.Code, rec.Body.String())
	}
	if resp["removed"] != true || resp["was_readable"] != false {
		t.Fatalf("corrupt delete response = %v", resp)
	}

	// A live referenced shard needs force.
	liveHash := shardOf(putTestFile(t, ctx, fs, []byte("live shard endpoint")))
	rec, resp = deleteShard("/internal/shards/" + liveHash)
	if rec.Code != http.StatusConflict {
		t.Fatalf("referenced delete status = %d, want %d: %s", rec.Code, http.StatusConflict, rec.Body.String())
	}
	if resp["removed"] != false || resp["was_readable"] != true || resp["referenced"] != true || resp["hint"] == nil {
		t.Fatalf("referenced delete response = %v", resp)
	}
	if _, err := fs.LoadShard(ctx, liveHash); err != nil {
		t.Fatalf("refused shard must stay loadable: %v", err)
	}
	rec, resp = deleteShard("/internal/shards/" + liveHash + "?force=true")
	if rec.Code != http.StatusOK {
		t.Fatalf("forced delete status = %d: %s", rec.Code, rec.Body.String())
	}
	if resp["removed"] != true || resp["was_readable"] != true {
		t.Fatalf("forced delete response = %v", resp)
	}

	if rec, _ := deleteShard("/internal/shards/not-a-hash"); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad hash status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if rec, _ := deleteShard("/internal/shards/" + strings.Repeat("ab", 32)); rec.Code != http.StatusNotFound {
		t.Fatalf("absent shard status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	if rec, _ := deleteShard("/internal/shards/" + strings.Repeat("ab", 32) + "?force=garbage"); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad force status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}
