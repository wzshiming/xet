package server_test

import (
	"bytes"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	xetgo "github.com/wzshiming/xet-go"
	"github.com/wzshiming/xet/pkg/server"
)

// testCase describes a single upload/download round-trip test.
type testCase struct {
	name string
	data []byte
}

func TestServerUploadDownload(t *testing.T) {
	tests := []testCase{
		{
			name: "Hello World",
			data: []byte("Hello World!"),
		},
		{
			name: "1KB",
			data: makeBinaryData(1024),
		},
		{
			name: "10KB",
			data: makeBinaryData(10 * 1024),
		},
		{
			name: "100KB",
			data: makeBinaryData(100 * 1024),
		},
		{
			name: "1MB",
			data: makeBinaryData(1024 * 1024),
		},
		{
			name: "10MB",
			data: makeBinaryData(10 * 1024 * 1024),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runUploadDownload(t, tt.data)
		})
	}
}

// runUploadDownload starts a fresh server for the test, uploads the given data
// using the xet-go reference client, downloads it back, and verifies the
// content is byte-for-byte identical to the original.
func runUploadDownload(t *testing.T, data []byte) {
	t.Helper()

	// Create a temporary directory for server storage.
	storageDir := t.TempDir()

	storage, err := server.NewFileStorage(server.FileStorageOptions{
		BasePath: storageDir,
	})
	if err != nil {
		t.Fatalf("create storage: %v", err)
	}

	// Start the server; the httptest server URL is used as the CAS endpoint.
	// Use a short idle timeout so that xet-go's Rust HTTP connection pool
	// closes idle connections promptly (within ~2 s after all requests are
	// served), allowing the internal Tokio runtime to shut down cleanly and
	// DownloadFiles to return.
	srv := server.NewServer(server.ServerOptions{
		Storage: storage,
	})
	ts := httptest.NewUnstartedServer(srv)
	ts.Config.IdleTimeout = 2 * time.Second
	ts.Start()
	defer ts.Close()

	// Update the storage base URL so reconstruction responses point to the
	// running test server.
	storage.SetBaseURL(ts.URL)

	// Write the test data to a temporary file for the reference client.
	srcFile, err := os.CreateTemp("", "xet-server-conformance-src-*")
	if err != nil {
		t.Fatalf("create source temp file: %v", err)
	}
	defer os.Remove(srcFile.Name())

	if _, err := srcFile.Write(data); err != nil {
		t.Fatalf("write source temp file: %v", err)
	}
	if err := srcFile.Close(); err != nil {
		t.Fatalf("close source temp file: %v", err)
	}

	// Upload the file using the xet-go reference client.
	uploadResults, err := xetgo.UploadFiles(
		[]string{srcFile.Name()},
		ts.URL,
		nil,   // no auth token
		nil,   // no pre-computed SHA-256
		false, // compute SHA-256
	)
	if err != nil {
		t.Fatalf("UploadFiles: %v", err)
	}
	if len(uploadResults) == 0 {
		t.Fatal("UploadFiles returned no results")
	}
	fileHash := uploadResults[0].Hash

	// Prepare a destination file for the download.
	dstFile, err := os.CreateTemp("", "xet-server-conformance-dst-*")
	if err != nil {
		t.Fatalf("create destination temp file: %v", err)
	}
	dstPath := dstFile.Name()
	dstFile.Close()
	defer os.Remove(dstPath)

	// Download the file using the xet-go reference client.
	_, err = xetgo.DownloadFiles(
		[]xetgo.DownloadRequest{
			{
				DestinationPath: dstPath,
				Hash:            fileHash,
				FileSize:        int64(len(data)),
			},
		},
		ts.URL,
		nil, // no auth token
	)
	if err != nil {
		t.Fatalf("DownloadFiles: %v", err)
	}

	// Read the downloaded file and compare with the original.
	downloaded, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatalf("read downloaded file: %v", err)
	}

	if !bytes.Equal(data, downloaded) {
		t.Errorf("downloaded content does not match original: got %d bytes, want %d bytes",
			len(downloaded), len(data))
	}
}

// makeBinaryData creates a deterministic byte sequence of the given size.
func makeBinaryData(size int) []byte {
	result := make([]byte, size)
	for i := range result {
		result[i] = byte(i % 256)
	}
	return result
}
