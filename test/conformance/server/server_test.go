package server_test

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/wzshiming/xet"
	xetgo "github.com/wzshiming/xet-go"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage"
)

// TestServerUploadDownloadConformance tests that files uploaded through the native
// Go client can be verified with the xet-go reference implementation, that files
// can be uploaded using the xet-go client, and that files can be downloaded using
// both the native client and the xet-go client.
func TestServerUploadDownloadConformance(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
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
		{
			name: "100MB",
			data: makeBinaryData(100 * 1024 * 1024),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temporary directory for storage
			storageDir := t.TempDir()

			// Start test HTTP server first (without creating storage yet)
			// We'll create storage after we know the server URL
			var stor storage.Storage
			var srv *server.Handler
			var httpSrv *httptest.Server

			// Create a placeholder handler that will be replaced
			httpSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if srv != nil {
					srv.ServeHTTP(w, r)
				} else {
					http.Error(w, "server not initialized", http.StatusInternalServerError)
				}
			}))
			defer httpSrv.Close()

			// Now create storage with the correct base URL
			var err error
			stor, err = storage.NewFileStorage(
				storage.WithBasePath(storageDir),
				storage.WithBaseURL(httpSrv.URL),
			)
			if err != nil {
				t.Fatalf("Failed to create storage: %v", err)
			}

			srv = server.NewHandler(server.WithStorage(stor))

			// Create native client
			nativeClient := client.NewClient(client.ClientOptions{
				BaseURL:   httpSrv.URL,
				Namespace: "default",
			})

			t.Run("upload_with_xetgo", func(t *testing.T) {
				// Create temp directory and write test file
				tempDir := t.TempDir()
				uploadFile := filepath.Join(tempDir, "upload.bin")
				if err := os.WriteFile(uploadFile, tt.data, 0644); err != nil {
					t.Fatalf("Failed to write upload file: %v", err)
				}

				// Upload using xet-go client
				uploadResults, err := xetgo.UploadFiles(
					[]string{uploadFile},
					httpSrv.URL,
					nil,   // token
					nil,   // sha256s (computed automatically)
					false, // skipSHA256
				)
				if err != nil {
					t.Fatalf("Failed to upload file with xet-go: %v", err)
				}

				if len(uploadResults) != 1 {
					t.Fatalf("Expected 1 upload result, got %d", len(uploadResults))
				}

				xetgoHash := uploadResults[0].Hash
				t.Logf("xet-go uploaded file with hash %s", xetgoHash)

				// Parse the hash for download
				fileHash, err := xet.ParseHash(xetgoHash)
				if err != nil {
					t.Fatalf("Failed to parse hash from xet-go: %v", err)
				}

				// Download using native client to verify
				downloadSession := nativeClient.DownloadSession()
				reader, _, err := downloadSession.DownloadFile(context.Background(), fileHash)
				if err != nil {
					t.Fatalf("Failed to download file with native client: %v", err)
				}

				downloadedData, err := io.ReadAll(reader)
				if err != nil {
					t.Fatalf("Failed to read downloaded data: %v", err)
				}

				// Verify downloaded content matches original
				if !bytes.Equal(downloadedData, tt.data) {
					t.Errorf("Downloaded data does not match original (got %d bytes, want %d bytes)",
						len(downloadedData), len(tt.data))
				}

				t.Logf("Successfully uploaded with xet-go and downloaded with native client")
			})

			t.Run("download_with_xetgo", func(t *testing.T) {
				// First upload the file using native client
				tempDir := t.TempDir()
				uploadFile := filepath.Join(tempDir, "upload.bin")
				if err := os.WriteFile(uploadFile, tt.data, 0644); err != nil {
					t.Fatalf("Failed to write upload file: %v", err)
				}

				// Compute file info for upload
				f, err := os.Open(uploadFile)
				if err != nil {
					t.Fatalf("Failed to open upload file: %v", err)
				}

				uploadSession := nativeClient.UploadSession()
				fileHashes, err := uploadSession.UploadFiles(context.Background(), f)
				if err != nil {
					t.Fatalf("Failed to upload file: %v", err)
				}

				fileHash := fileHashes[0]

				// Download using xet-go client
				downloadFile := filepath.Join(tempDir, "download-xetgo.bin")
				downloadReq := []xetgo.DownloadRequest{
					{
						DestinationPath: downloadFile,
						Hash:            fileHash.String(),
						FileSize:        int64(len(tt.data)),
					},
				}

				// Use xet-go to download from our server
				downloaded, err := xetgo.DownloadFiles(downloadReq, httpSrv.URL, nil)
				if err != nil {
					t.Fatalf("Failed to download file with xet-go: %v", err)
				}

				if len(downloaded) != 1 {
					t.Fatalf("Expected 1 downloaded file, got %d", len(downloaded))
				}

				// Verify downloaded content matches original
				downloadedData, err := os.ReadFile(downloadFile)
				if err != nil {
					t.Fatalf("Failed to read downloaded file: %v", err)
				}

				if !bytes.Equal(downloadedData, tt.data) {
					t.Errorf("Downloaded data (xet-go) does not match original (got %d bytes, want %d bytes)",
						len(downloadedData), len(tt.data))
				}

				t.Logf("Successfully downloaded file using xet-go client with hash %s", fileHash.String())
			})
		})

		t.Run(tt.name, func(t *testing.T) {
			// Create temporary directory for storage
			storageDir := t.TempDir()

			// Start test HTTP server first (without creating storage yet)
			// We'll create storage after we know the server URL
			var stor storage.Storage
			var srv *server.Handler
			var httpSrv *httptest.Server

			// Create a placeholder handler that will be replaced
			httpSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if srv != nil {
					srv.ServeHTTP(w, r)
				} else {
					http.Error(w, "server not initialized", http.StatusInternalServerError)
				}
			}))
			defer httpSrv.Close()

			// Now create storage with the correct base URL
			var err error
			stor, err = storage.NewFileStorage(
				storage.WithBasePath(storageDir),
				storage.WithBaseURL(httpSrv.URL),
			)
			if err != nil {
				t.Fatalf("Failed to create storage: %v", err)
			}

			srv = server.NewHandler(server.WithStorage(stor))

			// Create native client
			nativeClient := client.NewClient(client.ClientOptions{
				BaseURL:   httpSrv.URL,
				Namespace: "default",
			})

			t.Run("upload", func(t *testing.T) {
				// Write test file to upload
				tempDir := t.TempDir()
				testFile := filepath.Join(tempDir, "test.bin")
				if err := os.WriteFile(testFile, tt.data, 0644); err != nil {
					t.Fatalf("Failed to write test file: %v", err)
				}

				// Compute file info for upload
				f, err := os.Open(testFile)
				if err != nil {
					t.Fatalf("Failed to open test file: %v", err)
				}

				// Upload using native client
				uploadSession := nativeClient.UploadSession()
				fileHashes, err := uploadSession.UploadFiles(context.Background(), f)
				if err != nil {
					t.Fatalf("Failed to upload file: %v", err)
				}

				// Verify the file hash using xet-go reference implementation
				refResults, err := xetgo.HashFiles([]string{testFile})
				if err != nil {
					t.Fatalf("Failed to hash file with xet-go: %v", err)
				}

				if len(refResults) == 0 {
					t.Fatal("xet-go returned no results")
				}

				// Compare hashes
				nativeHash := fileHashes[0].String()
				refHash := refResults[0].Hash

				if nativeHash != refHash {
					t.Errorf("Hash mismatch: native=%s reference=%s", nativeHash, refHash)
				}

				t.Logf("Successfully uploaded file with hash %s", nativeHash)
			})

			// Test download for all file sizes now that multi-xorb support is fixed
			t.Run("download", func(t *testing.T) {
				// First upload the file
				tempDir := t.TempDir()
				uploadFile := filepath.Join(tempDir, "upload.bin")
				if err := os.WriteFile(uploadFile, tt.data, 0644); err != nil {
					t.Fatalf("Failed to write upload file: %v", err)
				}

				// Compute file info for upload
				f, err := os.Open(uploadFile)
				if err != nil {
					t.Fatalf("Failed to open upload file: %v", err)
				}

				uploadSession := nativeClient.UploadSession()
				fileHashes, err := uploadSession.UploadFiles(context.Background(), f)
				if err != nil {
					t.Fatalf("Failed to upload file: %v", err)
				}

				fileHash := fileHashes[0]

				// Download using native client
				downloadSession := nativeClient.DownloadSession()
				reader, _, err := downloadSession.DownloadFile(context.Background(), fileHash)
				if err != nil {
					t.Fatalf("Failed to download file: %v", err)
				}

				downloadedData, err := io.ReadAll(reader)
				if err != nil {
					t.Fatalf("Failed to read downloaded data: %v", err)
				}

				// Verify downloaded content matches original
				if !bytes.Equal(downloadedData, tt.data) {
					t.Errorf("Downloaded data does not match original (got %d bytes, want %d bytes)",
						len(downloadedData), len(tt.data))
				}

				// Write downloaded data to file for verification
				downloadFile := filepath.Join(tempDir, "download.bin")
				if err := os.WriteFile(downloadFile, downloadedData, 0644); err != nil {
					t.Fatalf("Failed to write downloaded file: %v", err)
				}

				// Verify using xet-go that the downloaded file has the correct hash
				refResults, err := xetgo.HashFiles([]string{downloadFile})
				if err != nil {
					t.Fatalf("Failed to hash downloaded file with xet-go: %v", err)
				}

				if len(refResults) == 0 {
					t.Fatal("xet-go returned no results for downloaded file")
				}

				expectedHash := fileHash.String()
				actualHash := refResults[0].Hash

				if actualHash != expectedHash {
					t.Errorf("Downloaded file hash mismatch: got=%s want=%s", actualHash, expectedHash)
				}

				t.Logf("Successfully downloaded and verified file with hash %s", expectedHash)
			})
		})
	}
}

var seed = rand.NewSource(0)

// makeBinaryData creates a deterministic byte sequence of the given size.
func makeBinaryData(size int) []byte {
	result := make([]byte, size)
	for i := range result {
		result[i] = byte(seed.Int63() % 256)
	}
	return result
}
