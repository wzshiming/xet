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

func TestUnlinkSHA256Endpoint(t *testing.T) {
	ctx := context.Background()
	fs, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("sha256 unlink endpoint content")
	fileHash := putTestFile(t, ctx, fs, content)
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

	// The remaining file entry still anchors the shard: a graceless sweep
	// reclaims nothing and the file hash keeps resolving.
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
		t.Fatalf("sweep after sha256 unlink alone = %+v", result)
	}
	if _, err := fs.GetShard(ctx, fileHash); err != nil {
		t.Fatalf("file hash stopped resolving after sha256 unlink: %v", err)
	}

	// Unlinking the file hash too leaves nothing anchoring the shard.
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/xet/"+fileHash.String(), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("file unlink status = %d: %s", rec.Code, rec.Body.String())
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
	content := []byte("sweep endpoint content")
	fileHash := putTestFile(t, ctx, fs, content)
	digest := sha256.Sum256(content)
	handler := NewHandler(WithStorage(fs))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/xet/"+fileHash.String(), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("unlink status = %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/sha256/"+hex.EncodeToString(digest[:]), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("sha256 unlink status = %d: %s", rec.Code, rec.Body.String())
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
// request. Every request runs an independent pass that re-marks from
// scratch and reports only its own work, so the union of the steps covers
// the whole store.
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
		digest := sha256.Sum256(content)
		rec = httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/sha256/"+hex.EncodeToString(digest[:]), nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("sha256 unlink %d status = %d", i, rec.Code)
		}
	}

	var result storage.SweepResult
	sweptShards, sweptXorbs := 0, 0
	steps := 0
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
		if got := len(result.SweptShards) + len(result.SweptXorbs); got != 1 {
			t.Fatalf("step %d swept %d objects, want exactly 1", steps, got)
		}
		sweptShards += len(result.SweptShards)
		sweptXorbs += len(result.SweptXorbs)
		if result.Done {
			break
		}
		if steps > 10 {
			t.Fatalf("stepping not done after %d steps: %+v", steps, result)
		}
	}
	// Two shards plus two xorbs, one dead object per step.
	if steps != 4 {
		t.Fatalf("steps = %d, want 4", steps)
	}
	if sweptShards != 2 || sweptXorbs != 2 {
		t.Fatalf("union swept = %d shards, %d xorbs, want 2 and 2", sweptShards, sweptXorbs)
	}
	if result.RemainingShards != 0 || result.RemainingXorbs != 0 {
		t.Fatalf("final step remaining = %d/%d, want 0/0", result.RemainingShards, result.RemainingXorbs)
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

// TestGCSweepEndpointRejectsInvalidAnchor: unknown or misspelled anchor
// values are rejected at the boundary before any store access.
func TestGCSweepEndpointRejectsInvalidAnchor(t *testing.T) {
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
}

// TestGCSweepEndpointServerGrace: WithGCGrace supplies the window for
// requests omitting grace; an explicit parameter still overrides it.
func TestGCSweepEndpointServerGrace(t *testing.T) {
	ctx := context.Background()

	unlinkBoth := func(t *testing.T, handler *Handler, fileHash xet.FileHash, content []byte) {
		t.Helper()
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/xet/"+fileHash.String(), nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("unlink status = %d", rec.Code)
		}
		digest := sha256.Sum256(content)
		rec = httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/sha256/"+hex.EncodeToString(digest[:]), nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("sha256 unlink status = %d: %s", rec.Code, rec.Body.String())
		}
	}

	// Disabled server default: a plain request reclaims fresh objects.
	fs, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(WithStorage(fs), WithGCGrace(-1))
	content := []byte("server grace content")
	unlinkBoth(t, handler, putTestFile(t, ctx, fs, content), content)
	rec := httptest.NewRecorder()
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
	content = []byte("server grace default")
	unlinkBoth(t, handler, putTestFile(t, ctx, fs2, content), content)
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

// TestGCSweepEndpointNeedsBothUnlinks: full removal of a non-empty file
// takes both unlinks — after the file unlink alone the sha256 entry still
// anchors the shard through a graceless sweep.
func TestGCSweepEndpointNeedsBothUnlinks(t *testing.T) {
	ctx := context.Background()
	fs, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("needs both unlinks content")
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

// TestGCSweepEndpointServerAnchor: WithGCAnchor supplies the anchor for
// requests omitting the parameter — under a sha256 server default the
// sha256 unlink alone reclaims — while an explicit anchor=both still
// overrides it back to needing both unlinks.
func TestGCSweepEndpointServerAnchor(t *testing.T) {
	ctx := context.Background()
	fs, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(WithStorage(fs), WithGCAnchor(storage.AnchorSHA256))

	// Server default sha256: the sha256 unlink alone frees the shard for a
	// plain sweep.
	content := []byte("server anchor content one")
	putTestFile(t, ctx, fs, content)
	digest := sha256.Sum256(content)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/sha256/"+hex.EncodeToString(digest[:]), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("sha256 unlink status = %d: %s", rec.Code, rec.Body.String())
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
	if len(result.SweptShards) != 1 || len(result.SweptXorbs) != 1 {
		t.Fatalf("server-default sha256 sweep = %+v", result)
	}

	// An explicit anchor=both overrides the server default: after the
	// sha256 unlink alone the file entry still anchors the shard.
	content = []byte("server anchor content two")
	fileHash := putTestFile(t, ctx, fs, content)
	digest = sha256.Sum256(content)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/sha256/"+hex.EncodeToString(digest[:]), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("second sha256 unlink status = %d: %s", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?grace=0&anchor=both", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("anchor=both sweep status = %d: %s", rec.Code, rec.Body.String())
	}
	result = storage.SweepResult{}
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("decode anchor=both sweep: %v", err)
	}
	if len(result.SweptShards) != 0 || len(result.SweptXorbs) != 0 {
		t.Fatalf("anchor=both sweep after sha256 unlink alone = %+v", result)
	}

	// Unlinking the file hash too completes removal under anchor=both,
	// proving the earlier non-reclaim was the anchor override at work.
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/xet/"+fileHash.String(), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("file unlink status = %d: %s", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?grace=0&anchor=both", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("final sweep status = %d: %s", rec.Code, rec.Body.String())
	}
	result = storage.SweepResult{}
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("decode final sweep: %v", err)
	}
	if len(result.SweptShards) != 1 || len(result.SweptXorbs) != 1 {
		t.Fatalf("anchor=both sweep after both unlinks = %+v", result)
	}
}

// TestGCSweepEndpointSHA256AnchorLFS: the sha256 anchor serves stores
// managed exclusively by SHA-256, e.g. Git-LFS backends — DELETE
// /internal/files/sha256/{hash} alone lets an anchor=sha256 sweep reclaim
// the shard and xorbs, deleting the stale file entry with the shard.
func TestGCSweepEndpointSHA256AnchorLFS(t *testing.T) {
	ctx := context.Background()
	fs, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("sha256 anchor lfs content")
	fileHash := putTestFile(t, ctx, fs, content)
	digest := sha256.Sum256(content)
	handler := NewHandler(WithStorage(fs))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/internal/files/sha256/"+hex.EncodeToString(digest[:]), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("sha256 unlink status = %d: %s", rec.Code, rec.Body.String())
	}

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
		t.Fatalf("anchor=sha256 sweep = %+v", result)
	}
	if result.DeletedFileEntries < 1 {
		t.Fatalf("DeletedFileEntries = %d, want >= 1", result.DeletedFileEntries)
	}

	// The stale file entry went with the shard: nothing lists and the file
	// hash no longer resolves.
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/internal/files", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("list status = %d", rec.Code)
	}
	var entries []storage.FileListEntry
	if err := json.NewDecoder(rec.Body).Decode(&entries); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("listed %d files after sweep, want 0", len(entries))
	}
	if _, err := fs.GetShard(ctx, fileHash); err == nil {
		t.Fatal("file hash still resolves after anchor=sha256 sweep")
	}
}

// blockingGCStorage parks a sweep inside its first mark walk until released
// so a concurrent request can observe the busy GC.
type blockingGCStorage struct {
	*storage.FileStorage
	enter   chan struct{}
	release chan struct{}
}

func (b *blockingGCStorage) WalkFileIndex(ctx context.Context, fn func(fileHash, shardHash string) error) error {
	b.enter <- struct{}{}
	<-b.release
	return b.FileStorage.WalkFileIndex(ctx, fn)
}

// TestGCSweepEndpointBusy: while one sweep runs, a concurrent request fails
// fast with 409 instead of queueing.
func TestGCSweepEndpointBusy(t *testing.T) {
	fs, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	blocking := &blockingGCStorage{
		FileStorage: fs,
		enter:       make(chan struct{}),
		release:     make(chan struct{}),
	}
	handler := NewHandler(WithStorage(blocking))

	first := make(chan int, 1)
	go func() {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?grace=0", nil))
		first <- rec.Code
	}()
	<-blocking.enter // the sweep is parked mid-pass, holding the GC

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep?grace=0", nil))
	if rec.Code != http.StatusConflict {
		t.Fatalf("busy sweep status = %d, want %d", rec.Code, http.StatusConflict)
	}

	close(blocking.release)
	if code := <-first; code != http.StatusOK {
		t.Fatalf("first sweep status = %d, want %d", code, http.StatusOK)
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
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/internal/gc/sweep", nil))
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("sweep status = %d, want %d", rec.Code, http.StatusNotImplemented)
	}
}
