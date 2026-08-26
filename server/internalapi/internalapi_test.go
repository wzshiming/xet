package internalapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?grace=0&max=1", nil))
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
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?grace=0", nil))
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
	} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?"+query, nil))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want %d", query, rec.Code, http.StatusBadRequest)
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
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep", nil))
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
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep", nil))
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
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?grace=1h", nil))
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
}
