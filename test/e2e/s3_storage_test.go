package e2e_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage"
)

// TestS3StorageUploadRestartAndDownload runs the full upload/download cycle
// against a real S3-compatible store (MinIO in CI). It is skipped unless
// XET_TEST_S3_ENDPOINT is set; credentials come from the AWS env chain.
func TestS3StorageUploadRestartAndDownload(t *testing.T) {
	endpoint := os.Getenv("XET_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("XET_TEST_S3_ENDPOINT not set")
	}
	ctx := context.Background()

	bucket := fmt.Sprintf("xet-e2e-%d", time.Now().UnixNano())
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		t.Fatal(err)
	}
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	if _, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}

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
			resp := doRequest(t, http.MethodGet, url, nil)
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
