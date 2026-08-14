package e2e_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage"
)

// TestS3StorageUploadRestartAndDownload runs the full upload/download cycle
// against an in-process S3 double (gofakes3) through the production option
// path. Both storage instances share one backend, so the restart below
// proves nothing is served from memory.
func TestS3StorageUploadRestartAndDownload(t *testing.T) {
	ctx := context.Background()

	const bucket = "xet-e2e"
	backend := s3mem.New()
	if err := backend.CreateBucket(bucket); err != nil {
		t.Fatal(err)
	}
	s3Server := httptest.NewServer(gofakes3.New(backend).Server())
	defer s3Server.Close()
	endpoint := s3Server.URL

	// NewS3Storage resolves credentials and checksum behavior from the AWS
	// env chain; gofakes3 cannot parse the aws-chunked bodies the SDK sends
	// when default request checksums are on.
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
	t.Setenv("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")

	// Constructed through the production option path, not an injected client.
	newStorage := func() storage.Storage {
		stor, err := storage.NewS3Storage(ctx,
			storage.WithS3Bucket(bucket),
			storage.WithS3Prefix("xet-data"),
			storage.WithS3Endpoint(endpoint),
			storage.WithS3PathStyle(true),
		)
		if err != nil {
			t.Fatal(err)
		}
		return stor
	}

	uploadServer := httptest.NewServer(server.NewHandler(server.WithStorage(newStorage())))
	uploadClient, err := client.NewClient(
		client.WithBaseURL(uploadServer.URL),
		client.WithCacheDir(t.TempDir()),
	)
	if err != nil {
		t.Fatal(err)
	}

	files := []struct {
		name string
		data []byte
		hash xet.FileHash
	}{
		{name: "small", data: []byte("Hello, S3!")},
		{name: "multi-chunk", data: deterministicData(3*128*1024 + 7919)},
		{name: "large", data: deterministicData(4 * 1024 * 1024)},
	}
	for i := range files {
		hash, err := uploadClient.UploadFile(ctx, bytes.NewReader(files[i].data))
		if err != nil {
			t.Fatalf("upload %s: %v", files[i].name, err)
		}
		files[i].hash = hash
	}
	uploadServer.Close()

	// Fresh storage over the same bucket: nothing may be served from memory.
	downloadServer := httptest.NewServer(server.NewHandler(server.WithStorage(newStorage())))
	defer downloadServer.Close()
	downloadClient, err := client.NewClient(
		client.WithBaseURL(downloadServer.URL),
		client.WithCacheDir(t.TempDir()),
	)
	if err != nil {
		t.Fatal(err)
	}

	for _, file := range files {
		t.Run(file.name, func(t *testing.T) {
			// Reconstruction must hand out presigned S3 URLs pointing at the
			// object store, not at the CAS server.
			var reconstruction struct {
				FetchInfo map[string][]struct {
					URL string `json:"url"`
				} `json:"fetch_info"`
			}
			resp := doRequest(t, http.MethodGet, downloadServer.URL+"/v1/reconstructions/"+file.hash.String(), nil)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("reconstruction status = %d", resp.StatusCode)
			}
			if err := json.NewDecoder(resp.Body).Decode(&reconstruction); err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if len(reconstruction.FetchInfo) == 0 {
				t.Fatal("reconstruction has no fetch info")
			}
			for _, entries := range reconstruction.FetchInfo {
				for _, entry := range entries {
					if !strings.HasPrefix(entry.URL, endpoint+"/") || !strings.Contains(entry.URL, "X-Amz-Signature") {
						t.Fatalf("fetch URL = %q, want presigned URL at %s", entry.URL, endpoint)
					}
				}
			}

			// Full xet download path: reconstruction plus ranged xorb terms.
			out, err := os.Create(filepath.Join(t.TempDir(), "out"))
			if err != nil {
				t.Fatal(err)
			}
			defer out.Close()
			if err := downloadClient.DownloadFile(ctx, file.hash, out); err != nil {
				t.Fatalf("download: %v", err)
			}
			got, err := os.ReadFile(out.Name())
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, file.data) {
				t.Fatalf("downloaded %d bytes, want %d", len(got), len(file.data))
			}

			// Bridge path: complete file and a byte range.
			digest := sha256.Sum256(file.data)
			url := downloadServer.URL + "/xet-bridge/" + hex.EncodeToString(digest[:])
			resp = doRequest(t, http.MethodGet, url, nil)
			assertResponse(t, resp, http.StatusOK, file.data)

			if len(file.data) > 1024 {
				start, end := 100, 1023
				headers := http.Header{"Range": []string{fmt.Sprintf("bytes=%d-%d", start, end)}}
				resp = doRequest(t, http.MethodGet, url, headers)
				assertResponse(t, resp, http.StatusPartialContent, file.data[start:end+1])
			}
		})
	}
}
