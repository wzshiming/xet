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
// content.
func TestGCUnlinkSweepLifecycle(t *testing.T) {
	backends := []struct {
		name     string
		newStore func(t *testing.T) storage.Storage
	}{
		{name: "file", newStore: func(t *testing.T) storage.Storage {
			stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
			if err != nil {
				t.Fatal(err)
			}
			return stor
		}},
		{name: "s3", newStore: newGofakeS3Storage},
	}
	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) {
			runGCLifecycle(t, backend.newStore(t))
		})
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
	res := gcSweep(t, srv.URL, "")
	if len(res.SweptShards) != 0 || len(res.SweptXorbs) != 0 {
		t.Fatalf("in-grace sweep removed objects: %+v", res)
	}
	if res.SkippedInGrace < 2 {
		t.Fatalf("SkippedInGrace = %d, want >= 2", res.SkippedInGrace)
	}
	assertBridge(t, srv.URL, dropData, http.StatusOK)

	// A sweep without grace reclaims the dead shard and its xorbs.
	res = gcSweep(t, srv.URL, "?grace=0")
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
	res = gcSweep(t, srv.URL, "?grace=0")
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
