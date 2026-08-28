package e2e_test

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
	"testing"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/server/internalapi"
	"github.com/wzshiming/xet/storage"
)

// TestGCUnlinkSweepLifecycle runs the whole GC story over the real HTTP
// stack wired like cmd/xetd (internal endpoints in front of the CAS routes):
// upload with the xet client, unlink, sweep with and without the grace
// window, verify the survivors byte for byte, and re-upload the swept
// content. Its sweeps pin anchor=files (legacy file-entry rooting) so
// unlinking the file hash alone frees the shard; the default anchor is
// covered by TestGCSweepDefaultAnchorLifecycle.
func TestGCUnlinkSweepLifecycle(t *testing.T) {
	for _, backend := range gcBackends() {
		t.Run(backend.name, func(t *testing.T) {
			runGCLifecycle(t, backend.newStore(t))
		})
	}
}

// TestGCSweepDefaultAnchorLifecycle covers the default-anchor ("both")
// contract over HTTP: after DELETE /internal/files/xet/{hash} the sha256
// entry alone keeps the shard alive through a graceless sweep, and only
// DELETE /internal/files/sha256/{hash} lets the next sweep reclaim it.
func TestGCSweepDefaultAnchorLifecycle(t *testing.T) {
	for _, backend := range gcBackends() {
		t.Run(backend.name, func(t *testing.T) {
			runGCDefaultAnchorLifecycle(t, backend.newStore(t))
		})
	}
}

type gcBackend struct {
	name     string
	newStore func(t *testing.T) storage.Storage
}

func gcBackends() []gcBackend {
	return []gcBackend{
		{name: "file", newStore: func(t *testing.T) storage.Storage {
			stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
			if err != nil {
				t.Fatal(err)
			}
			return stor
		}},
		{name: "s3", newStore: newGofakeS3Storage},
	}
}

func runGCLifecycle(t *testing.T, stor storage.Storage) {
	ctx := context.Background()
	srv := httptest.NewServer(internalapi.NewHandler(
		internalapi.WithStorage(stor),
		internalapi.WithNext(server.NewHandler(server.WithStorage(stor))),
	))
	defer srv.Close()

	keepData := deterministicData(3*128*1024 + 7919)
	dropData := invertedData(2*128*1024 + 101)
	smallData := []byte("gc lifecycle small file")

	uploader := newGCClient(t, srv.URL)
	keep, err := uploader.UploadFile(ctx, bytes.NewReader(keepData))
	if err != nil {
		t.Fatalf("upload keep: %v", err)
	}
	drop, err := uploader.UploadFile(ctx, bytes.NewReader(dropData))
	if err != nil {
		t.Fatalf("upload drop: %v", err)
	}
	small, err := uploader.UploadFile(ctx, bytes.NewReader(smallData))
	if err != nil {
		t.Fatalf("upload small: %v", err)
	}

	// Everything serves through file hash and SHA-256 before any GC.
	assertDownload(t, ctx, srv.URL, keep, keepData)
	assertDownload(t, ctx, srv.URL, drop, dropData)
	assertDownload(t, ctx, srv.URL, small, smallData)
	assertBridge(t, srv.URL, dropData, http.StatusOK)
	if got := len(gcListFiles(t, srv.URL)); got != 3 {
		t.Fatalf("listed %d files, want 3", got)
	}

	// Unlink removes the file-hash lookup at once; the SHA-256 path keeps
	// serving until a sweep collects the shard.
	resp := doRequest(t, http.MethodDelete, srv.URL+"/internal/files/xet/"+drop.String(), nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unlink status = %d", resp.StatusCode)
	}
	assertReconstructionStatus(t, srv.URL, drop, http.StatusNotFound)
	assertBridge(t, srv.URL, dropData, http.StatusOK)
	if got := len(gcListFiles(t, srv.URL)); got != 2 {
		t.Fatalf("listed %d files after unlink, want 2", got)
	}

	// The default grace window shields the freshly written objects.
	res := gcSweep(t, srv.URL, "?anchor=files")
	if len(res.SweptShards) != 0 || len(res.SweptXorbs) != 0 {
		t.Fatalf("in-grace sweep removed objects: %+v", res)
	}
	if res.SkippedInGrace < 2 {
		t.Fatalf("SkippedInGrace = %d, want >= 2", res.SkippedInGrace)
	}
	assertBridge(t, srv.URL, dropData, http.StatusOK)

	// A sweep without grace reclaims the dead shard and its xorbs.
	res = gcSweep(t, srv.URL, "?grace=0&anchor=files")
	if len(res.SweptShards) != 1 || len(res.SweptXorbs) < 1 || res.ReclaimedBytes <= 0 {
		t.Fatalf("sweep result = %+v, want 1 shard, >= 1 xorbs, bytes > 0", res)
	}

	// The swept file is gone on every path; the survivors are intact.
	assertReconstructionStatus(t, srv.URL, drop, http.StatusNotFound)
	assertBridge(t, srv.URL, dropData, http.StatusNotFound)
	if _, err := tryDownload(t, ctx, srv.URL, drop); err == nil {
		t.Fatal("swept file still downloads")
	}
	entries := gcListFiles(t, srv.URL)
	if len(entries) != 2 {
		t.Fatalf("listed %d files after sweep, want 2", len(entries))
	}
	for _, entry := range entries {
		if entry.Missing {
			t.Fatalf("surviving entry marked missing: %+v", entry)
		}
	}
	assertDownload(t, ctx, srv.URL, keep, keepData)
	assertDownload(t, ctx, srv.URL, small, smallData)

	// A second sweep converges to nothing.
	res = gcSweep(t, srv.URL, "?grace=0&anchor=files")
	if len(res.SweptShards) != 0 || len(res.SweptXorbs) != 0 {
		t.Fatalf("second sweep found leftovers: %+v", res)
	}

	// Re-uploading the swept content resurrects it end to end.
	again, err := newGCClient(t, srv.URL).UploadFile(ctx, bytes.NewReader(dropData))
	if err != nil {
		t.Fatalf("re-upload: %v", err)
	}
	if again != drop {
		t.Fatalf("re-upload hash = %s, want %s", again.String(), drop.String())
	}
	assertDownload(t, ctx, srv.URL, drop, dropData)
	assertBridge(t, srv.URL, dropData, http.StatusOK)
	if got := len(gcListFiles(t, srv.URL)); got != 3 {
		t.Fatalf("listed %d files after re-upload, want 3", got)
	}
}

func runGCDefaultAnchorLifecycle(t *testing.T, stor storage.Storage) {
	ctx := context.Background()
	srv := httptest.NewServer(internalapi.NewHandler(
		internalapi.WithStorage(stor),
		internalapi.WithNext(server.NewHandler(server.WithStorage(stor))),
	))
	defer srv.Close()

	content := deterministicData(2*128*1024 + 4099)
	digest := sha256.Sum256(content)
	shaHex := hex.EncodeToString(digest[:])

	fileHash, err := newGCClient(t, srv.URL).UploadFile(ctx, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	assertDownload(t, ctx, srv.URL, fileHash, content)
	assertBridge(t, srv.URL, content, http.StatusOK)
	if got := len(gcListFiles(t, srv.URL)); got != 1 {
		t.Fatalf("listed %d files, want 1", got)
	}

	// Unlinking the file hash leaves the sha256 entry in place.
	resp := doRequest(t, http.MethodDelete, srv.URL+"/internal/files/xet/"+fileHash.String(), nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unlink status = %d", resp.StatusCode)
	}
	assertReconstructionStatus(t, srv.URL, fileHash, http.StatusNotFound)
	if got := len(gcListFiles(t, srv.URL)); got != 0 {
		t.Fatalf("listed %d files after unlink, want 0", got)
	}

	// Under the default anchor the sha256 entry alone keeps the shard
	// alive even without the grace window, and the content keeps serving
	// over the SHA-256 bridge.
	res := gcSweep(t, srv.URL, "?grace=0")
	if len(res.SweptShards) != 0 || len(res.SweptXorbs) != 0 {
		t.Fatalf("default-anchor sweep removed sha-anchored objects: %+v", res)
	}
	assertBridge(t, srv.URL, content, http.StatusOK)

	// The first sha256 unlink hits, the second 404s.
	resp = doRequest(t, http.MethodDelete, srv.URL+"/internal/files/sha256/"+shaHex, nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("sha256 unlink status = %d", resp.StatusCode)
	}
	resp = doRequest(t, http.MethodDelete, srv.URL+"/internal/files/sha256/"+shaHex, nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("second sha256 unlink status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
	assertBridge(t, srv.URL, content, http.StatusNotFound)

	// With both anchors gone the same sweep reclaims the shard and xorbs.
	res = gcSweep(t, srv.URL, "?grace=0")
	if len(res.SweptShards) != 1 || len(res.SweptXorbs) < 1 || res.ReclaimedBytes <= 0 {
		t.Fatalf("sweep result = %+v, want 1 shard, >= 1 xorbs, bytes > 0", res)
	}
	assertBridge(t, srv.URL, content, http.StatusNotFound)
	if _, err := tryDownload(t, ctx, srv.URL, fileHash); err == nil {
		t.Fatal("swept file still downloads")
	}
	if got := len(gcListFiles(t, srv.URL)); got != 0 {
		t.Fatalf("listed %d files after sweep, want 0", got)
	}

	// A repeat sweep converges to nothing.
	res = gcSweep(t, srv.URL, "?grace=0")
	if len(res.SweptShards) != 0 || len(res.SweptXorbs) != 0 {
		t.Fatalf("repeat sweep found leftovers: %+v", res)
	}
}

// newGofakeS3Storage builds S3Storage through the production option path
// against an in-process gofakes3 double.
func newGofakeS3Storage(t *testing.T) storage.Storage {
	const bucket = "xet-e2e-gc"
	backend := s3mem.New()
	if err := backend.CreateBucket(bucket); err != nil {
		t.Fatal(err)
	}
	s3Server := httptest.NewServer(gofakes3.New(backend).Server())
	t.Cleanup(s3Server.Close)

	// gofakes3 cannot parse the aws-chunked bodies the SDK sends when
	// default request checksums are on.
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
	t.Setenv("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")

	stor, err := storage.NewS3Storage(context.Background(),
		storage.WithS3Bucket(bucket),
		storage.WithS3Prefix("xet-data"),
		storage.WithS3Endpoint(s3Server.URL),
		storage.WithS3PathStyle(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	return stor
}

// invertedData never shares chunks with deterministicData streams.
func invertedData(size int) []byte {
	data := deterministicData(size)
	for i := range data {
		data[i] ^= 0xFF
	}
	return data
}

func newGCClient(t *testing.T, baseURL string) *client.Client {
	t.Helper()
	c, err := client.NewClient(client.WithBaseURL(baseURL), client.WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// tryDownload fetches fileHash with a fresh client and cache, so nothing can
// be served from a previous download.
func tryDownload(t *testing.T, ctx context.Context, baseURL string, fileHash xet.FileHash) ([]byte, error) {
	t.Helper()
	out, err := os.Create(filepath.Join(t.TempDir(), "out"))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	if err := newGCClient(t, baseURL).DownloadFile(ctx, fileHash, out); err != nil {
		return nil, err
	}
	return os.ReadFile(out.Name())
}

func assertDownload(t *testing.T, ctx context.Context, baseURL string, fileHash xet.FileHash, want []byte) {
	t.Helper()
	got, err := tryDownload(t, ctx, baseURL, fileHash)
	if err != nil {
		t.Fatalf("download %s: %v", fileHash.String(), err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("downloaded %d bytes, want %d", len(got), len(want))
	}
}

func assertBridge(t *testing.T, baseURL string, content []byte, wantStatus int) {
	t.Helper()
	digest := sha256.Sum256(content)
	resp := doRequest(t, http.MethodGet, baseURL+"/xet-bridge/"+hex.EncodeToString(digest[:]), nil)
	if wantStatus == http.StatusOK {
		assertResponse(t, resp, http.StatusOK, content)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != wantStatus {
		t.Fatalf("bridge status = %d, want %d", resp.StatusCode, wantStatus)
	}
}

func assertReconstructionStatus(t *testing.T, baseURL string, fileHash xet.FileHash, wantStatus int) {
	t.Helper()
	resp := doRequest(t, http.MethodGet, baseURL+"/v1/reconstructions/"+fileHash.String(), nil)
	defer resp.Body.Close()
	if resp.StatusCode != wantStatus {
		t.Fatalf("reconstruction status = %d, want %d", resp.StatusCode, wantStatus)
	}
}

func gcListFiles(t *testing.T, baseURL string) []storage.FileListEntry {
	t.Helper()
	resp := doRequest(t, http.MethodGet, baseURL+"/internal/files", nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list status = %d", resp.StatusCode)
	}
	var entries []storage.FileListEntry
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		t.Fatal(err)
	}
	return entries
}

func gcSweep(t *testing.T, baseURL, query string) storage.SweepResult {
	t.Helper()
	resp := doRequest(t, http.MethodPost, baseURL+"/internal/gc/sweep"+query, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("sweep status = %d", resp.StatusCode)
	}
	var res storage.SweepResult
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		t.Fatal(err)
	}
	return res
}
