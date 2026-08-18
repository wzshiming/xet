package admin

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/xorb"
)

// putGCTestFile stores a single-chunk file, returning its SHA-256 hex and xet
// file hash.
func putGCTestFile(t *testing.T, stor storage.Storage, data []byte) (sha256Hex, fileHash string) {
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
	fh := xet.ComputeFileHash([]xet.ChunkHash{chunkHash}, []uint64{uint64(len(data))})
	s := shard.NewShard()
	s.AddFile(shard.FileBlock{
		FileHash: fh,
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
	return hex.EncodeToString(digest[:]), fh.String()
}

// newTestHandler builds an admin handler in front of the CAS handler, with the
// token auth the admin endpoints refuse to run without.
func newTestHandler(stor storage.Storage, opts ...Option) *Handler {
	opts = append([]Option{
		WithStorage(stor),
		WithAuthFunc(func(tok string) bool { return tok == "admin-secret" }),
		WithNext(server.NewHandler(server.WithStorage(stor))),
	}, opts...)
	return NewHandler(opts...)
}

func adminRequest(method, target string) *http.Request {
	req := httptest.NewRequest(method, target, nil)
	req.Header.Set("Authorization", "Bearer admin-secret")
	return req
}

func TestGCDeleteFileThenSweep(t *testing.T) {
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	sha256Hex, fileHash := putGCTestFile(t, stor, []byte("gc endpoint test data"))
	handler := newTestHandler(stor)

	do := func(method, target string) *http.Response {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, adminRequest(method, target))
		return rec.Result()
	}

	// The file is served before deletion.
	if resp := do(http.MethodGet, "/xet-bridge/"+sha256Hex); resp.StatusCode != http.StatusOK {
		t.Fatalf("bridge before delete = %d", resp.StatusCode)
	}

	resp := do(http.MethodDelete, "/internal/files/xet/"+fileHash)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delete status = %d", resp.StatusCode)
	}
	var res storage.UnlinkResult
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if res.SHA256 != sha256Hex || res.FileHash != fileHash || !res.RemovedFileIndex {
		t.Fatalf("delete response = %+v", res)
	}

	if resp := do(http.MethodGet, "/xet-bridge/"+sha256Hex); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("bridge after delete = %d, want 404", resp.StatusCode)
	}
	if resp := do(http.MethodDelete, "/internal/files/xet/"+fileHash); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("second delete = %d, want 404", resp.StatusCode)
	}

	// grace=0s disables the grace window so fresh objects are collectable.
	// The SHA-256 index entry falls only here, together with the shard.
	resp = do(http.MethodPost, "/internal/gc/sweep?grace=0s")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("sweep status = %d", resp.StatusCode)
	}
	var report storage.SweepReport
	if err := json.NewDecoder(resp.Body).Decode(&report); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if report.RemovedShards != 1 || report.RemovedXorbs != 1 ||
		report.RemovedChunkIndexEntries != 1 || report.RemovedSHA256IndexEntries != 1 {
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

func TestGCDeleteFileHasNoSHA256Route(t *testing.T) {
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	sha256Hex, fileHash := putGCTestFile(t, stor, []byte("route test data"))
	handler := newTestHandler(stor)

	// A SHA-256 sent to the xet-hash route must not resolve.
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, adminRequest(http.MethodDelete, "/internal/files/xet/"+sha256Hex))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("delete via xet route with sha256 = %d, want 404", rec.Code)
	}

	// Deleting by SHA-256 is deliberately not offered: one SHA-256 can map to
	// several xet hashes, so the route must not exist at all.
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, adminRequest(http.MethodDelete, "/internal/files/sha256/"+sha256Hex))
	if rec.Code == http.StatusOK {
		t.Fatalf("delete via sha256 route = %d, want non-200", rec.Code)
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, adminRequest(http.MethodDelete, "/internal/files/xet/"+fileHash))
	if rec.Code != http.StatusOK {
		t.Fatalf("delete via xet route = %d", rec.Code)
	}
}

func TestAdminEndpointsRequireAuth(t *testing.T) {
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	_, fileHash := putGCTestFile(t, stor, []byte("auth test data"))
	targets := []struct{ method, path string }{
		{http.MethodDelete, "/internal/files/xet/" + fileHash},
		{http.MethodPost, "/internal/gc/sweep"},
		{http.MethodGet, "/internal/files"},
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

	req := httptest.NewRequest(http.MethodDelete, "/internal/files/xet/"+fileHash, nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("delete with token = %d, want 200", rec.Code)
	}
}

// blockingStore parks WalkFileIndex until released, keeping a sweep in
// flight deterministically.
type blockingStore struct {
	storage.SweepStore
	entered chan struct{}
	release chan struct{}
}

func (b *blockingStore) WalkFileIndex(ctx context.Context, fn func(fileHash, shardHash string, modTime time.Time) error) error {
	close(b.entered)
	<-b.release
	return b.SweepStore.WalkFileIndex(ctx, fn)
}

func TestGCSweepsShareSingleFlight(t *testing.T) {
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	bs := &blockingStore{SweepStore: stor, entered: make(chan struct{}), release: make(chan struct{})}
	handler := NewHandler(
		WithGC(storage.NewGC(bs)),
		WithAuthFunc(func(tok string) bool { return tok == "admin-secret" }),
	)

	sweepDone := make(chan int, 1)
	go func() {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, adminRequest(http.MethodPost, "/internal/gc/sweep"))
		sweepDone <- rec.Code
	}()
	<-bs.entered

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, adminRequest(http.MethodPost, "/internal/gc/sweep"))
	if rec.Code != http.StatusConflict {
		t.Fatalf("POST sweep during sweep = %d, want 409", rec.Code)
	}

	close(bs.release)
	if code := <-sweepDone; code != http.StatusOK {
		t.Fatalf("blocked sweep finished with %d", code)
	}
}

// nonSweepStorage satisfies Storage without the GC primitives.
type nonSweepStorage struct{ storage.Storage }

func TestAdminEndpointsRejectNonSweepStorage(t *testing.T) {
	handler := newTestHandler(nonSweepStorage{})

	for _, target := range []struct{ method, path string }{
		{http.MethodDelete, "/internal/files/xet/" + hex.EncodeToString(make([]byte, 32))},
		{http.MethodPost, "/internal/gc/sweep"},
		{http.MethodGet, "/internal/files"},
	} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, adminRequest(target.method, target.path))
		if rec.Code != http.StatusNotImplemented {
			t.Fatalf("%s %s = %d, want 501", target.method, target.path, rec.Code)
		}
	}
}
