package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/xorb"
)

// putGCTestFile stores a single-chunk file and returns its SHA-256 hex.
func putGCTestFile(t *testing.T, stor storage.Storage, data []byte) string {
	t.Helper()
	ctx := context.Background()

	var encoded bytes.Buffer
	encoder := xorb.NewEncoder(&encoded, true)
	if _, err := encoder.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	xorbHash := encoder.SummoryHash()
	if _, err := stor.PutXorb(ctx, "default", xorbHash, bytes.NewReader(encoded.Bytes())); err != nil {
		t.Fatal(err)
	}

	chunkHash := xet.ComputeChunkHash(data)
	s := shard.NewShard()
	s.AddFile(shard.FileBlock{
		FileHash: xet.ComputeFileHash([]xet.ChunkHash{chunkHash}, []uint64{uint64(len(data))}),
		Entries: []shard.FileDataSequenceEntry{{
			CASHash: xorbHash, UnpackedSegBytes: uint32(len(data)), ChunkIndexEnd: 1,
		}},
	})
	s.AddCASBlock(shard.CASBlock{
		CASHash: xorbHash,
		Chunks:  []shard.CASChunkSequenceEntry{{ChunkHash: chunkHash, UnpackedSegBytes: uint32(len(data))}},
	})
	s.SetFooter()
	if _, err := stor.PutShard(ctx, s); err != nil {
		t.Fatal(err)
	}

	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

// gcAuth configures the token auth the GC endpoints refuse to run without.
func gcAuth() Option {
	return WithAuthFunc(func(tok string) bool { return tok == "gc-secret" })
}

func gcRequest(method, target string) *http.Request {
	req := httptest.NewRequest(method, target, nil)
	req.Header.Set("Authorization", "Bearer gc-secret")
	return req
}

func TestGCDeleteFileThenSweep(t *testing.T) {
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	sha256Hex := putGCTestFile(t, stor, []byte("gc endpoint test data"))
	handler := NewHandler(WithStorage(stor), gcAuth())

	do := func(method, target string) *http.Response {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, gcRequest(method, target))
		return rec.Result()
	}

	// The file is served before deletion.
	if resp := do(http.MethodGet, "/xet-bridge/"+sha256Hex); resp.StatusCode != http.StatusOK {
		t.Fatalf("bridge before delete = %d", resp.StatusCode)
	}

	resp := do(http.MethodDelete, "/internal/files/sha256/"+sha256Hex)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delete status = %d", resp.StatusCode)
	}
	var res storage.UnlinkResult
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if res.SHA256 != sha256Hex || !res.RemovedFileIndex || !res.RemovedSHA256Index {
		t.Fatalf("delete response = %+v", res)
	}

	if resp := do(http.MethodGet, "/xet-bridge/"+sha256Hex); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("bridge after delete = %d, want 404", resp.StatusCode)
	}
	if resp := do(http.MethodDelete, "/internal/files/sha256/"+sha256Hex); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("second delete = %d, want 404", resp.StatusCode)
	}

	// grace=0s disables the grace window so fresh objects are collectable.
	resp = do(http.MethodPost, "/internal/gc/sweep?grace=0s")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("sweep status = %d", resp.StatusCode)
	}
	var report storage.SweepReport
	if err := json.NewDecoder(resp.Body).Decode(&report); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if report.RemovedShards != 1 || report.RemovedXorbs != 1 || report.RemovedChunkIndexEntries != 1 {
		t.Fatalf("sweep report = %+v", report)
	}

	// Dry-run on the now-empty store reports nothing to do.
	resp = do(http.MethodPost, "/internal/gc/sweep?grace=0s&dry_run=true")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("dry-run sweep status = %d", resp.StatusCode)
	}
	report = storage.SweepReport{}
	if err := json.NewDecoder(resp.Body).Decode(&report); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if !report.DryRun || report.RemovedShards != 0 || report.RemovedXorbs != 0 {
		t.Fatalf("dry-run report = %+v", report)
	}
}

func TestGCDeleteFileCallsHookAndDistinguishesHashKinds(t *testing.T) {
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	sha256Hex := putGCTestFile(t, stor, []byte("hook test data"))

	var hookSHA256, hookFileHash string
	handler := NewHandler(WithStorage(stor), gcAuth(), WithFileRemovedHook(func(_ context.Context, sh, fh string) error {
		hookSHA256, hookFileHash = sh, fh
		return nil
	}))

	// A SHA-256 sent to the xet-hash route must not resolve.
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, gcRequest(http.MethodDelete, "/internal/files/xet/"+sha256Hex))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("delete via xet route = %d, want 404", rec.Code)
	}
	if hookSHA256 != "" {
		t.Fatal("hook called for failed delete")
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, gcRequest(http.MethodDelete, "/internal/files/sha256/"+sha256Hex))
	if rec.Code != http.StatusOK {
		t.Fatalf("delete via sha256 route = %d", rec.Code)
	}
	if hookSHA256 != sha256Hex || hookFileHash == "" {
		t.Fatalf("hook got sha256 %q, fileHash %q", hookSHA256, hookFileHash)
	}
}

func TestGCEndpointsRequireAuth(t *testing.T) {
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	sha256Hex := putGCTestFile(t, stor, []byte("auth test data"))
	targets := []struct{ method, path string }{
		{http.MethodDelete, "/internal/files/sha256/" + sha256Hex},
		{http.MethodPost, "/internal/gc/sweep"},
	}

	// Without an AuthFunc the destructive endpoints are disabled outright.
	noAuth := NewHandler(WithStorage(stor))
	for _, target := range targets {
		rec := httptest.NewRecorder()
		noAuth.ServeHTTP(rec, httptest.NewRequest(target.method, target.path, nil))
		if rec.Code != http.StatusForbidden {
			t.Fatalf("%s %s without AuthFunc = %d, want 403", target.method, target.path, rec.Code)
		}
	}

	handler := NewHandler(WithStorage(stor), WithAuthFunc(func(tok string) bool { return tok == "secret" }))
	for _, target := range targets {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(target.method, target.path, nil))
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s %s without token = %d, want 401", target.method, target.path, rec.Code)
		}
	}

	req := httptest.NewRequest(http.MethodDelete, "/internal/files/sha256/"+sha256Hex, nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("delete with token = %d, want 200", rec.Code)
	}
}

func TestGCSweepSingleFlight(t *testing.T) {
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(WithStorage(stor), gcAuth())
	handler.sweepActive.Store(true)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, gcRequest(http.MethodPost, "/internal/gc/sweep"))
	if rec.Code != http.StatusConflict {
		t.Fatalf("concurrent sweep = %d, want 409", rec.Code)
	}
}

// nonCollectorStorage satisfies Storage without the GC primitives.
type nonCollectorStorage struct{ storage.Storage }

func TestGCEndpointsRejectNonCollectorStorage(t *testing.T) {
	handler := NewHandler(WithStorage(nonCollectorStorage{}), gcAuth())

	for _, target := range []struct{ method, path string }{
		{http.MethodDelete, "/internal/files/sha256/" + hex.EncodeToString(make([]byte, 32))},
		{http.MethodDelete, "/internal/files/xet/" + hex.EncodeToString(make([]byte, 32))},
		{http.MethodPost, "/internal/gc/sweep"},
	} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, gcRequest(target.method, target.path))
		if rec.Code != http.StatusNotImplemented {
			t.Fatalf("%s %s = %d, want 501", target.method, target.path, rec.Code)
		}
	}
}
