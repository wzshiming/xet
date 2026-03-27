package server_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/wzshiming/xet"
	xetgo "github.com/wzshiming/xet-go"
	"github.com/wzshiming/xet/pkg/client"
	"github.com/wzshiming/xet/pkg/client/download"
	"github.com/wzshiming/xet/pkg/client/upload"
	"github.com/wzshiming/xet/pkg/server"
)

// TestServerUploadDownloadConformance tests that files uploaded through the native
// Go client can be verified with the xet-go reference implementation, and that
// files can be downloaded using the xet-go client.
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
			var storage server.Storage
			var srv *server.Server
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
			storage, err = server.NewFileStorage(server.FileStorageOptions{
				BasePath: storageDir,
				BaseURL:  httpSrv.URL,
			})
			if err != nil {
				t.Fatalf("Failed to create storage: %v", err)
			}

			srv = server.NewServer(server.ServerOptions{
				Storage: storage,
			})

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
				fileInfo, err := upload.ComputeFileInfo(f)
				if err != nil {
					f.Close()
					t.Fatalf("Failed to compute file info: %v", err)
				}
				f.Close()
				fileInfo.Path = testFile

				// Upload using native client
				uploadSession := upload.NewSession(upload.SessionOptions{
					Client: nativeClient,
				})
				err = uploadSession.UploadFiles(context.Background(), []upload.FileUploadInfo{fileInfo})
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
				nativeHash := fileInfo.FileHash.String()
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
				fileInfo, err := upload.ComputeFileInfo(f)
				if err != nil {
					f.Close()
					t.Fatalf("Failed to compute file info: %v", err)
				}
				f.Close()
				fileInfo.Path = uploadFile

				uploadSession := upload.NewSession(upload.SessionOptions{
					Client: nativeClient,
				})
				err = uploadSession.UploadFiles(context.Background(), []upload.FileUploadInfo{fileInfo})
				if err != nil {
					t.Fatalf("Failed to upload file: %v", err)
				}

				fileHash := fileInfo.FileHash

				// Download using native client
				downloadSession := download.NewSession(download.SessionOptions{
					Client: nativeClient,
				})
				downloadedData, err := downloadSession.DownloadFile(context.Background(), fileHash)
				if err != nil {
					t.Fatalf("Failed to download file: %v", err)
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
				fileInfo, err := upload.ComputeFileInfo(f)
				if err != nil {
					f.Close()
					t.Fatalf("Failed to compute file info: %v", err)
				}
				f.Close()
				fileInfo.Path = uploadFile

				uploadSession := upload.NewSession(upload.SessionOptions{
					Client: nativeClient,
				})
				err = uploadSession.UploadFiles(context.Background(), []upload.FileUploadInfo{fileInfo})
				if err != nil {
					t.Fatalf("Failed to upload file: %v", err)
				}

				fileHash := fileInfo.FileHash

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
	}
}

// TestServerUploadVerifyChunking tests that the chunking performed during upload
// matches the xet-go reference implementation.
func TestServerUploadVerifyChunking(t *testing.T) {
	// Create test data
	testData := makeBinaryData(100 * 1024) // 100KB

	// Create temporary directory for storage
	storageDir := t.TempDir()

	// Create storage and server
	storage, err := server.NewFileStorage(server.FileStorageOptions{
		BasePath: storageDir,
		BaseURL:  "http://localhost:8080",
	})
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	srv := server.NewServer(server.ServerOptions{
		Storage: storage,
	})

	// Start test HTTP server
	httpSrv := startTestServer(t, srv)
	defer httpSrv.Close()

	// Chunk using native implementation
	var nativeChunks []chunkInfo
	err = xet.ChunkData(bytes.NewReader(testData), func(_ int64, chunk xet.ChunkBytes) error {
		nativeChunks = append(nativeChunks, chunkInfo{
			hash: chunk.Hash().String(),
			size: chunk.Size(),
		})
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to chunk data with native implementation: %v", err)
	}

	// Chunk using xet-go reference implementation
	refChunks, err := xetgo.ChunkData(testData)
	if err != nil {
		t.Fatalf("Failed to chunk data with xet-go: %v", err)
	}

	// Compare chunk count
	if len(nativeChunks) != len(refChunks) {
		t.Errorf("Chunk count mismatch: native=%d reference=%d",
			len(nativeChunks), len(refChunks))
	}

	// Compare individual chunks
	minLen := len(nativeChunks)
	if len(refChunks) < minLen {
		minLen = len(refChunks)
	}

	for i := 0; i < minLen; i++ {
		if nativeChunks[i].hash != refChunks[i].Hash {
			t.Errorf("Chunk[%d] hash mismatch: native=%s reference=%s",
				i, nativeChunks[i].hash, refChunks[i].Hash)
		}
		if nativeChunks[i].size != refChunks[i].Size {
			t.Errorf("Chunk[%d] size mismatch: native=%d reference=%d",
				i, nativeChunks[i].size, refChunks[i].Size)
		}
	}

	t.Logf("Successfully verified chunking matches between native and xet-go (%d chunks)", len(nativeChunks))
}

// Helper types and functions

type chunkInfo struct {
	hash string
	size uint64
}

// makeBinaryData creates a deterministic byte sequence of the given size.
func makeBinaryData(size int) []byte {
	result := make([]byte, size)
	for i := range result {
		result[i] = byte(i % 256)
	}
	return result
}

// startTestServer starts an HTTP test server with the given handler.
func startTestServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(func() {
		srv.Close()
	})
	return srv
}
