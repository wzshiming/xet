// Package e2e provides end-to-end tests for the XET server using the
// xet-go client (https://github.com/wzshiming/xet-go), which wraps the
// reference xet-core Rust implementation via CGo.
//
// These tests start a real XET server on a local port, upload files
// through the xet-go client, download them back, and verify the round-trip.
package e2e_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	xetgo "github.com/wzshiming/xet-go"
	"github.com/wzshiming/xet/pkg/server"
)

// startServer starts the XET server on a random local port and returns the
// endpoint URL.  The caller is responsible for closing the returned listener
// to stop the server.
func startServer(t *testing.T) (endpoint string, close func()) {
	t.Helper()

	storageDir := t.TempDir()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	addr := listener.Addr().String()
	baseURL := "http://" + addr

	storage, err := server.NewFileStorage(server.FileStorageOptions{
		BasePath: storageDir,
		BaseURL:  baseURL,
	})
	if err != nil {
		listener.Close()
		t.Fatalf("create storage: %v", err)
	}

	srv := server.NewServer(server.ServerOptions{
		Storage: storage,
	})

	go http.Serve(listener, srv) //nolint:errcheck

	return baseURL, func() { listener.Close() }
}

// writeFile writes data to a temporary file and returns its path.
func writeFile(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write file %s: %v", path, err)
	}
	return path
}

// TestServerE2EUploadDownload uploads a small file through xet-go and downloads
// it back, verifying the content is identical.
func TestServerE2EUploadDownload(t *testing.T) {
	endpoint, close := startServer(t)
	defer close()

	uploadDir := t.TempDir()
	downloadDir := t.TempDir()

	content := []byte("Hello, XET e2e test!")
	srcPath := writeFile(t, uploadDir, "hello.dat", content)

	// Upload via the reference xet-core client.
	results, err := xetgo.UploadFiles([]string{srcPath}, endpoint, nil, nil, true)
	if err != nil {
		t.Fatalf("UploadFiles: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 upload result, got %d", len(results))
	}

	hash := results[0].Hash
	fileSize := int64(results[0].FileSize)

	if hash == "" {
		t.Fatal("upload returned an empty hash")
	}
	if fileSize != int64(len(content)) {
		t.Fatalf("expected file_size=%d, got %d", len(content), fileSize)
	}

	// Download via the reference xet-core client.
	dstPath := filepath.Join(downloadDir, "hello.dat")
	_, err = xetgo.DownloadFiles([]xetgo.DownloadRequest{
		{DestinationPath: dstPath, Hash: hash, FileSize: fileSize},
	}, endpoint, nil)
	if err != nil {
		t.Fatalf("DownloadFiles: %v", err)
	}

	got, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatalf("read downloaded file: %v", err)
	}

	if !bytes.Equal(content, got) {
		t.Fatalf("content mismatch:\n  want: %q\n  got:  %q", content, got)
	}
}

// TestServerE2EMultipleFiles uploads several files and downloads them back,
// verifying each one.
func TestServerE2EMultipleFiles(t *testing.T) {
	endpoint, close := startServer(t)
	defer close()

	uploadDir := t.TempDir()
	downloadDir := t.TempDir()

	files := []struct {
		name    string
		content []byte
	}{
		{"a.dat", []byte("file-a content")},
		{"b.dat", []byte("file-b content with a bit more data for variety")},
		{"c.dat", bytes.Repeat([]byte("chunk"), 1000)},
	}

	// Write all source files.
	var srcPaths []string
	for _, f := range files {
		srcPaths = append(srcPaths, writeFile(t, uploadDir, f.name, f.content))
	}

	// Upload all files in one batch.
	results, err := xetgo.UploadFiles(srcPaths, endpoint, nil, nil, true)
	if err != nil {
		t.Fatalf("UploadFiles: %v", err)
	}
	if len(results) != len(files) {
		t.Fatalf("expected %d upload results, got %d", len(files), len(results))
	}

	// Download and verify each file.
	for i, f := range files {
		hash := results[i].Hash
		fileSize := int64(results[i].FileSize)

		dstPath := filepath.Join(downloadDir, f.name)
		_, err = xetgo.DownloadFiles([]xetgo.DownloadRequest{
			{DestinationPath: dstPath, Hash: hash, FileSize: fileSize},
		}, endpoint, nil)
		if err != nil {
			t.Fatalf("DownloadFiles[%d] %s: %v", i, f.name, err)
		}

		got, err := os.ReadFile(dstPath)
		if err != nil {
			t.Fatalf("read downloaded file %s: %v", f.name, err)
		}

		if !bytes.Equal(f.content, got) {
			t.Fatalf("content mismatch for %s", f.name)
		}
	}
}

// TestServerE2ELargeFile uploads a file larger than the minimum chunk size
// (8 KiB) so that chunking actually occurs, then downloads and verifies it.
func TestServerE2ELargeFile(t *testing.T) {
	endpoint, close := startServer(t)
	defer close()

	uploadDir := t.TempDir()
	downloadDir := t.TempDir()

	// 512 KiB of pseudo-random data — guaranteed to span multiple chunks.
	rng := rand.New(rand.NewSource(42))
	content := make([]byte, 512*1024)
	rng.Read(content)

	srcPath := writeFile(t, uploadDir, "large.dat", content)

	results, err := xetgo.UploadFiles([]string{srcPath}, endpoint, nil, nil, true)
	if err != nil {
		t.Fatalf("UploadFiles: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 upload result, got %d", len(results))
	}

	hash := results[0].Hash
	fileSize := int64(results[0].FileSize)

	dstPath := filepath.Join(downloadDir, "large.dat")
	_, err = xetgo.DownloadFiles([]xetgo.DownloadRequest{
		{DestinationPath: dstPath, Hash: hash, FileSize: fileSize},
	}, endpoint, nil)
	if err != nil {
		t.Fatalf("DownloadFiles: %v", err)
	}

	got, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatalf("read downloaded file: %v", err)
	}

	if !bytes.Equal(content, got) {
		t.Fatalf("content mismatch for large file (len want=%d, got=%d)", len(content), len(got))
	}
}

// TestServerE2EDeduplication uploads the same file twice, verifying that the
// second upload is recognised as a duplicate (hash is stable).
func TestServerE2EDeduplication(t *testing.T) {
	endpoint, close := startServer(t)
	defer close()

	uploadDir := t.TempDir()

	content := []byte("deduplicated content — same bytes, same hash")
	srcPath := writeFile(t, uploadDir, "dedup.dat", content)

	first, err := xetgo.UploadFiles([]string{srcPath}, endpoint, nil, nil, true)
	if err != nil {
		t.Fatalf("first UploadFiles: %v", err)
	}
	if len(first) != 1 {
		t.Fatalf("expected 1 result, got %d", len(first))
	}

	second, err := xetgo.UploadFiles([]string{srcPath}, endpoint, nil, nil, true)
	if err != nil {
		t.Fatalf("second UploadFiles: %v", err)
	}
	if len(second) != 1 {
		t.Fatalf("expected 1 result, got %d", len(second))
	}

	if first[0].Hash != second[0].Hash {
		t.Fatalf("hash mismatch between uploads: %q vs %q", first[0].Hash, second[0].Hash)
	}
}

// TestServerE2EEmptyFile uploads and downloads an empty file, verifying the
// edge case is handled correctly.
func TestServerE2EEmptyFile(t *testing.T) {
	endpoint, close := startServer(t)
	defer close()

	uploadDir := t.TempDir()
	downloadDir := t.TempDir()

	srcPath := writeFile(t, uploadDir, "empty.dat", []byte{})

	results, err := xetgo.UploadFiles([]string{srcPath}, endpoint, nil, nil, true)
	if err != nil {
		t.Fatalf("UploadFiles: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 upload result, got %d", len(results))
	}

	hash := results[0].Hash
	if hash == "" {
		t.Fatal("upload returned an empty hash for empty file")
	}
	if results[0].FileSize != 0 {
		t.Fatalf("expected file_size=0, got %d", results[0].FileSize)
	}

	dstPath := filepath.Join(downloadDir, "empty.dat")
	_, err = xetgo.DownloadFiles([]xetgo.DownloadRequest{
		{DestinationPath: dstPath, Hash: hash, FileSize: 0},
	}, endpoint, nil)
	if err != nil {
		t.Fatalf("DownloadFiles: %v", err)
	}

	got, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatalf("read downloaded file: %v", err)
	}

	if len(got) != 0 {
		t.Fatalf("expected empty file, got %d bytes", len(got))
	}
}

// TestServerE2EHashConsistency computes the hash of a file via xet-go's
// HashFiles (no upload) and verifies that a subsequent UploadFiles produces
// the same hash.
func TestServerE2EHashConsistency(t *testing.T) {
	endpoint, close := startServer(t)
	defer close()

	uploadDir := t.TempDir()

	content := []byte("hash consistency check content")
	srcPath := writeFile(t, uploadDir, "hash.dat", content)

	hashResults, err := xetgo.HashFiles([]string{srcPath})
	if err != nil {
		t.Fatalf("HashFiles: %v", err)
	}
	if len(hashResults) != 1 {
		t.Fatalf("expected 1 hash result, got %d", len(hashResults))
	}
	expectedHash := hashResults[0].Hash

	uploadResults, err := xetgo.UploadFiles([]string{srcPath}, endpoint, nil, nil, true)
	if err != nil {
		t.Fatalf("UploadFiles: %v", err)
	}
	if len(uploadResults) != 1 {
		t.Fatalf("expected 1 upload result, got %d", len(uploadResults))
	}

	if uploadResults[0].Hash != expectedHash {
		t.Fatalf("hash mismatch: HashFiles=%q UploadFiles=%q", expectedHash, uploadResults[0].Hash)
	}

	_ = fmt.Sprintf("file hash: %s", expectedHash)
}
