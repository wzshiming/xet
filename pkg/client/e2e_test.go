package client_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/wzshiming/xet/pkg/client"
	"github.com/wzshiming/xet/pkg/client/download"
	"github.com/wzshiming/xet/pkg/client/upload"
)

// TestClientE2EWithServer tests the full upload/download flow with a real xetd server
func TestClientE2EWithServer(t *testing.T) {
	// Build xetd binary
	xetdBinary := filepath.Join(t.TempDir(), "xetd")
	buildCmd := exec.Command("go", "build", "-o", xetdBinary, "../../cmd/xetd")
	if output, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build xetd: %v\n%s", err, output)
	}

	// Create temporary storage directory
	storageDir := t.TempDir()

	// Start xetd server
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverAddr := "localhost:18080"
	baseURL := fmt.Sprintf("http://%s", serverAddr)
	serverCmd := exec.CommandContext(ctx, xetdBinary,
		"-addr", serverAddr,
		"-storage", storageDir,
		"-base-url", baseURL,
	)
	serverCmd.Stdout = os.Stdout
	serverCmd.Stderr = os.Stderr

	if err := serverCmd.Start(); err != nil {
		t.Fatalf("Failed to start xetd server: %v", err)
	}

	// Wait for server to start
	time.Sleep(500 * time.Millisecond)

	// Ensure server is killed at the end
	defer func() {
		cancel()
		serverCmd.Wait()
	}()

	// Test cases with different file sizes
	testCases := []struct {
		name string
		data []byte
	}{
		{
			name: "Small file - Hello World",
			data: []byte("Hello World!"),
		},
		{
			name: "Medium file - 1KB",
			data: makeRepeatedData(1024),
		},
		{
			name: "Large file - 100KB",
			data: makeRepeatedData(100 * 1024),
		},
		{
			name: "Very large file - 1MB",
			data: makeRepeatedData(1024 * 1024),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create client
			c := client.NewClient(client.ClientOptions{
				BaseURL:   fmt.Sprintf("http://%s", serverAddr),
				Namespace: "default",
			})

			// Compute file info
			fileInfo := upload.ComputeFileInfo(tc.data)

			// Create upload session and upload file
			uploadSession := upload.NewSession(upload.SessionOptions{
				Client:            c,
				EnableGlobalDedup: false,
			})

			err := uploadSession.UploadFiles(context.Background(), []upload.FileUploadInfo{fileInfo})
			if err != nil {
				t.Fatalf("Upload failed: %v", err)
			}

			t.Logf("Successfully uploaded file with hash %s (%d bytes)", fileInfo.FileHash.String(), len(tc.data))

			// Create download session and download file
			downloadSession := download.NewSession(download.SessionOptions{
				Client:        c,
				EnableCaching: false,
			})

			downloadedData, err := downloadSession.DownloadFile(context.Background(), fileInfo.FileHash)
			if err != nil {
				t.Fatalf("Download failed: %v", err)
			}

			// Verify downloaded data matches original
			if !bytes.Equal(downloadedData, tc.data) {
				t.Errorf("Downloaded data does not match original:\nOriginal size: %d\nDownloaded size: %d",
					len(tc.data), len(downloadedData))
			}

			t.Logf("Successfully downloaded and verified file (%d bytes)", len(downloadedData))
		})
	}
}

// TestClientE2ECacheConsistency tests that caching works correctly
func TestClientE2ECacheConsistency(t *testing.T) {
	// Build xetd binary
	xetdBinary := filepath.Join(t.TempDir(), "xetd")
	buildCmd := exec.Command("go", "build", "-o", xetdBinary, "../../cmd/xetd")
	if output, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build xetd: %v\n%s", err, output)
	}

	// Create temporary storage directory
	storageDir := t.TempDir()

	// Start xetd server
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverAddr := "localhost:18081"
	baseURL := fmt.Sprintf("http://%s", serverAddr)
	serverCmd := exec.CommandContext(ctx, xetdBinary,
		"-addr", serverAddr,
		"-storage", storageDir,
		"-base-url", baseURL,
	)
	serverCmd.Stdout = os.Stdout
	serverCmd.Stderr = os.Stderr

	if err := serverCmd.Start(); err != nil {
		t.Fatalf("Failed to start xetd server: %v", err)
	}

	// Wait for server to start
	time.Sleep(500 * time.Millisecond)

	// Ensure server is killed at the end
	defer func() {
		cancel()
		serverCmd.Wait()
	}()

	// Create test data
	testData := makeRepeatedData(100 * 1024)

	// Create client
	c := client.NewClient(client.ClientOptions{
		BaseURL:   fmt.Sprintf("http://%s", serverAddr),
		Namespace: "default",
	})

	// Upload file
	fileInfo := upload.ComputeFileInfo(testData)
	uploadSession := upload.NewSession(upload.SessionOptions{
		Client:            c,
		EnableGlobalDedup: false,
	})

	err := uploadSession.UploadFiles(context.Background(), []upload.FileUploadInfo{fileInfo})
	if err != nil {
		t.Fatalf("Upload failed: %v", err)
	}

	// Test 1: Download with caching enabled
	t.Run("WithCache", func(t *testing.T) {
		downloadSession := download.NewSession(download.SessionOptions{
			Client:        c,
			EnableCaching: true,
		})

		// First download - should populate cache
		data1, err := downloadSession.DownloadFile(context.Background(), fileInfo.FileHash)
		if err != nil {
			t.Fatalf("First download failed: %v", err)
		}

		if !bytes.Equal(data1, testData) {
			t.Errorf("First download data mismatch")
		}

		// Second download - should use cache
		data2, err := downloadSession.DownloadFile(context.Background(), fileInfo.FileHash)
		if err != nil {
			t.Fatalf("Second download failed: %v", err)
		}

		if !bytes.Equal(data2, testData) {
			t.Errorf("Second download data mismatch")
		}

		if !bytes.Equal(data1, data2) {
			t.Errorf("Cached data differs from original")
		}

		t.Logf("Cache consistency verified: both downloads match")
	})

	// Test 2: Download without caching
	t.Run("WithoutCache", func(t *testing.T) {
		downloadSession := download.NewSession(download.SessionOptions{
			Client:        c,
			EnableCaching: false,
		})

		data, err := downloadSession.DownloadFile(context.Background(), fileInfo.FileHash)
		if err != nil {
			t.Fatalf("Download without cache failed: %v", err)
		}

		if !bytes.Equal(data, testData) {
			t.Errorf("Download without cache data mismatch")
		}

		t.Logf("Download without cache successful")
	})
}

// TestClientE2ERangeRequest tests partial file downloads
func TestClientE2ERangeRequest(t *testing.T) {
	// Build xetd binary
	xetdBinary := filepath.Join(t.TempDir(), "xetd")
	buildCmd := exec.Command("go", "build", "-o", xetdBinary, "../../cmd/xetd")
	if output, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build xetd: %v\n%s", err, output)
	}

	// Create temporary storage directory
	storageDir := t.TempDir()

	// Start xetd server
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverAddr := "localhost:18082"
	baseURL := fmt.Sprintf("http://%s", serverAddr)
	serverCmd := exec.CommandContext(ctx, xetdBinary,
		"-addr", serverAddr,
		"-storage", storageDir,
		"-base-url", baseURL,
	)
	serverCmd.Stdout = os.Stdout
	serverCmd.Stderr = os.Stderr

	if err := serverCmd.Start(); err != nil {
		t.Fatalf("Failed to start xetd server: %v", err)
	}

	// Wait for server to start
	time.Sleep(500 * time.Millisecond)

	// Ensure server is killed at the end
	defer func() {
		cancel()
		serverCmd.Wait()
	}()

	// Create test data
	testData := makeRepeatedData(10 * 1024) // 10KB

	// Create client
	c := client.NewClient(client.ClientOptions{
		BaseURL:   fmt.Sprintf("http://%s", serverAddr),
		Namespace: "default",
	})

	// Upload file
	fileInfo := upload.ComputeFileInfo(testData)
	uploadSession := upload.NewSession(upload.SessionOptions{
		Client:            c,
		EnableGlobalDedup: false,
	})

	err := uploadSession.UploadFiles(context.Background(), []upload.FileUploadInfo{fileInfo})
	if err != nil {
		t.Fatalf("Upload failed: %v", err)
	}

	// Test range requests
	downloadSession := download.NewSession(download.SessionOptions{
		Client:        c,
		EnableCaching: false,
	})

	testCases := []struct {
		name   string
		start  int64
		length int64
	}{
		{"First 100 bytes", 0, 100},
		{"Middle 100 bytes", 5000, 100},
		{"Last 100 bytes", int64(len(testData) - 100), 100},
		{"First 1KB", 0, 1024},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rangeData, err := downloadSession.DownloadFileRange(context.Background(), fileInfo.FileHash, tc.start, tc.length)
			if err != nil {
				t.Fatalf("Range download failed: %v", err)
			}

			expectedData := testData[tc.start : tc.start+tc.length]
			if !bytes.Equal(rangeData, expectedData) {
				t.Errorf("Range data mismatch:\nExpected length: %d\nGot length: %d\nFirst 20 bytes of expected: %v\nFirst 20 bytes of got: %v",
					len(expectedData), len(rangeData), expectedData[:min(20, len(expectedData))], rangeData[:min(20, len(rangeData))])
				return
			}

			t.Logf("Range request successful: bytes %d-%d", tc.start, tc.start+tc.length)
		})
	}
}

// TestClientE2EGlobalDeduplication tests global deduplication
func TestClientE2EGlobalDeduplication(t *testing.T) {
	// Build xetd binary
	xetdBinary := filepath.Join(t.TempDir(), "xetd")
	buildCmd := exec.Command("go", "build", "-o", xetdBinary, "../../cmd/xetd")
	if output, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build xetd: %v\n%s", err, output)
	}

	// Create temporary storage directory
	storageDir := t.TempDir()

	// Start xetd server
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverAddr := "localhost:18083"
	baseURL := fmt.Sprintf("http://%s", serverAddr)
	serverCmd := exec.CommandContext(ctx, xetdBinary,
		"-addr", serverAddr,
		"-storage", storageDir,
		"-base-url", baseURL,
	)
	serverCmd.Stdout = os.Stdout
	serverCmd.Stderr = os.Stderr

	if err := serverCmd.Start(); err != nil {
		t.Fatalf("Failed to start xetd server: %v", err)
	}

	// Wait for server to start
	time.Sleep(500 * time.Millisecond)

	// Ensure server is killed at the end
	defer func() {
		cancel()
		serverCmd.Wait()
	}()

	// Create client
	c := client.NewClient(client.ClientOptions{
		BaseURL:   fmt.Sprintf("http://%s", serverAddr),
		Namespace: "default",
	})

	// Create test data - use same content for multiple files
	commonData := makeRepeatedData(50 * 1024)
	file1Data := append([]byte("File1:"), commonData...)
	file2Data := append([]byte("File2:"), commonData...)

	// Upload first file with global dedup enabled
	uploadSession := upload.NewSession(upload.SessionOptions{
		Client:            c,
		EnableGlobalDedup: true,
	})

	fileInfo1 := upload.ComputeFileInfo(file1Data)
	err := uploadSession.UploadFiles(context.Background(), []upload.FileUploadInfo{fileInfo1})
	if err != nil {
		t.Fatalf("First upload failed: %v", err)
	}

	t.Logf("Uploaded first file: %s", fileInfo1.FileHash.String())

	// Upload second file - should deduplicate common chunks
	fileInfo2 := upload.ComputeFileInfo(file2Data)
	err = uploadSession.UploadFiles(context.Background(), []upload.FileUploadInfo{fileInfo2})
	if err != nil {
		t.Fatalf("Second upload failed: %v", err)
	}

	t.Logf("Uploaded second file: %s", fileInfo2.FileHash.String())

	// Download both files and verify
	downloadSession := download.NewSession(download.SessionOptions{
		Client:        c,
		EnableCaching: false,
	})

	data1, err := downloadSession.DownloadFile(context.Background(), fileInfo1.FileHash)
	if err != nil {
		t.Fatalf("Download file 1 failed: %v", err)
	}

	if !bytes.Equal(data1, file1Data) {
		t.Errorf("File 1 data mismatch")
	}

	data2, err := downloadSession.DownloadFile(context.Background(), fileInfo2.FileHash)
	if err != nil {
		t.Fatalf("Download file 2 failed: %v", err)
	}

	if !bytes.Equal(data2, file2Data) {
		t.Errorf("File 2 data mismatch")
	}

	t.Logf("Global deduplication test passed: both files downloaded correctly")
}

// makeRepeatedData creates test data of the specified size
func makeRepeatedData(size int) []byte {
	pattern := []byte("XET Protocol Test Pattern - Conformance Testing - ")
	result := make([]byte, size)
	for i := 0; i < size; i++ {
		result[i] = pattern[i%len(pattern)]
	}
	return result
}
