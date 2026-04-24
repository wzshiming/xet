package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/wzshiming/xet"
	xetgo "github.com/wzshiming/xet-go"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/download"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/test/conformance/utils"
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
			name: "Empty file",
			data: []byte{},
		},
		{
			name: "Hello World",
			data: []byte("Hello World!"),
		},
		{
			name: "10MB",
			data: utils.MakeRandData(10 * 1024 * 1024),
		},
		{
			name: "10MB repeating",
			data: utils.MakeRepeatData(10 * 1024 * 1024),
		},
		{
			name: "100MB",
			data: utils.MakeRandData(100 * 1024 * 1024),
		},
		{
			name: "100MB repeating",
			data: utils.MakeRepeatData(100 * 1024 * 1024),
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
			nativeClient, err := client.NewClient(client.WithBaseURL(httpSrv.URL))
			if err != nil {
				t.Fatalf("create native client: %v", err)
			}

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
				nativeDownloadFile := filepath.Join(tempDir, "native-download.bin")
				nativeFile, err := os.Create(nativeDownloadFile)
				if err != nil {
					t.Fatalf("Failed to create native download file: %v", err)
				}
				err = nativeClient.DownloadFile(context.Background(), fileHash, nativeFile)
				nativeFile.Close()
				if err != nil {
					t.Fatalf("Failed to download file with native client: %v", err)
				}

				downloadedData, err := os.ReadFile(nativeDownloadFile)
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

				fileHash, err := nativeClient.UploadFile(context.Background(), f)
				if err != nil {
					t.Fatalf("Failed to upload file: %v", err)
				}

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
			nativeClient, err := client.NewClient(client.WithBaseURL(httpSrv.URL))
			if err != nil {
				t.Fatalf("create native client: %v", err)
			}

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
				fileHash, err := nativeClient.UploadFile(context.Background(), f)
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
				nativeHash := fileHash.String()
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

				fileHash, err := nativeClient.UploadFile(context.Background(), f)
				if err != nil {
					t.Fatalf("Failed to upload file: %v", err)
				}

				// Download using native client
				downloadFile := filepath.Join(tempDir, "download.bin")
				dlFile, err := os.Create(downloadFile)
				if err != nil {
					t.Fatalf("Failed to create download file: %v", err)
				}
				err = nativeClient.DownloadFile(context.Background(), fileHash, dlFile)
				dlFile.Close()
				if err != nil {
					t.Fatalf("Failed to download file: %v", err)
				}

				downloadedData, err := os.ReadFile(downloadFile)
				if err != nil {
					t.Fatalf("Failed to read downloaded data: %v", err)
				}

				// Verify downloaded content matches original
				if !bytes.Equal(downloadedData, tt.data) {
					t.Errorf("Downloaded data does not match original (got %d bytes, want %d bytes)",
						len(downloadedData), len(tt.data))
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

func TestServerBatchDedupChunkIndexConformance(t *testing.T) {
	storageDir := t.TempDir()

	var stor storage.Storage
	var srv *server.Handler
	var httpSrv *httptest.Server

	httpSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if srv != nil {
			srv.ServeHTTP(w, r)
			return
		}
		http.Error(w, "server not initialized", http.StatusInternalServerError)
	}))
	defer httpSrv.Close()

	var err error
	stor, err = storage.NewFileStorage(
		storage.WithBasePath(storageDir),
		storage.WithBaseURL(httpSrv.URL),
	)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	srv = server.NewHandler(server.WithStorage(stor))
	nativeClient, err := client.NewClient(client.WithBaseURL(httpSrv.URL))
	if err != nil {
		t.Fatalf("create native client: %v", err)
	}

	data := utils.MakeRandData(2 * 1024 * 1024)
	uploadFile := filepath.Join(t.TempDir(), "batch-dedup.bin")
	if err := os.WriteFile(uploadFile, data, 0644); err != nil {
		t.Fatalf("write upload file: %v", err)
	}

	f, err := os.Open(uploadFile)
	if err != nil {
		t.Fatalf("open upload file: %v", err)
	}
	fileHash, err := nativeClient.UploadFile(context.Background(), f)
	if err != nil {
		_ = f.Close()
		t.Fatalf("upload failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close upload file: %v", err)
	}

	if fileHash == (xet.Hash{}) {
		t.Fatalf("expected a valid file hash, got empty hash")
	}

	shardObj, err := stor.GetShardByFileHash(context.Background(), fileHash)
	if err != nil {
		t.Fatalf("get shard by file hash: %v", err)
	}

	var targetChunk xet.Hash
	var expectedXorb xet.Hash
	var expectedIndex uint32
	found := false
	for _, cas := range shardObj.CASInfos {
		if len(cas.Chunks) < 2 {
			continue
		}
		targetChunk = cas.Chunks[1].ChunkHash
		expectedXorb = cas.CASHash
		expectedIndex = 1
		found = true
		break
	}
	if !found {
		t.Skip("uploaded shard did not contain a CAS block with at least two chunks")
	}

	body, err := json.Marshal(map[string]any{"chunk_hashes": []string{targetChunk.String()}})
	if err != nil {
		t.Fatalf("marshal request body: %v", err)
	}

	resp, err := http.Post(httpSrv.URL+"/v1/chunks/default:query", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("batch dedup query failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("batch dedup status=%d body=%s", resp.StatusCode, string(b))
	}

	var batchResp struct {
		Results []struct {
			ChunkHash  string `json:"chunk_hash"`
			Found      bool   `json:"found"`
			XorbHash   string `json:"xorb_hash"`
			ChunkIndex uint32 `json:"chunk_index"`
		} `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&batchResp); err != nil {
		t.Fatalf("decode batch response: %v", err)
	}

	if len(batchResp.Results) != 1 {
		t.Fatalf("expected one batch result, got %d", len(batchResp.Results))
	}
	result := batchResp.Results[0]
	if !result.Found {
		t.Fatalf("expected found=true for chunk %s", targetChunk.String())
	}
	if result.XorbHash != expectedXorb.String() {
		t.Fatalf("unexpected xorb hash: got %s want %s", result.XorbHash, expectedXorb.String())
	}
	if result.ChunkIndex != expectedIndex {
		t.Fatalf("unexpected chunk index: got %d want %d", result.ChunkIndex, expectedIndex)
	}
}

// TestServerBatchGetReconstructionConformance tests the batch reconstruction endpoint
// GET /reconstructions?file_id=<h1>&file_id=<h2>...
// Verifies the response structure, correct content, partial-unknown handling, and
// that the xet-go reference client can download multiple files using this endpoint.
func TestServerBatchGetReconstructionConformance(t *testing.T) {
	storageDir := t.TempDir()

	var stor storage.Storage
	var srv *server.Handler
	var httpSrv *httptest.Server

	httpSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if srv != nil {
			srv.ServeHTTP(w, r)
			return
		}
		http.Error(w, "server not initialized", http.StatusInternalServerError)
	}))
	defer httpSrv.Close()

	var err error
	stor, err = storage.NewFileStorage(
		storage.WithBasePath(storageDir),
		storage.WithBaseURL(httpSrv.URL),
	)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	srv = server.NewHandler(server.WithStorage(stor))
	nativeClient, err := client.NewClient(client.WithBaseURL(httpSrv.URL))
	if err != nil {
		t.Fatalf("create native client: %v", err)
	}

	// Upload three files of different types so the batch spans multiple xorbs.
	datasets := [][]byte{
		[]byte("small file content for batch reconstruction test"),
		utils.MakeRandData(2 * 1024 * 1024),
		utils.MakeRepeatData(2 * 1024 * 1024),
	}
	hashes := make([]xet.Hash, len(datasets))
	for i, data := range datasets {
		hash, err := nativeClient.UploadFile(context.Background(), bytes.NewReader(data))
		if err != nil {
			t.Fatalf("upload dataset %d: %v", i, err)
		}
		hashes[i] = hash
	}

	t.Run("all_files_returned", func(t *testing.T) {
		url := httpSrv.URL + "/reconstructions?"
		for i, h := range hashes {
			if i > 0 {
				url += "&"
			}
			url += "file_id=" + h.String()
		}

		resp, err := http.Get(url)
		if err != nil {
			t.Fatalf("batch request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("batch status=%d body=%s", resp.StatusCode, string(body))
		}

		var batchResp struct {
			Files     map[string]interface{} `json:"files"`
			FetchInfo map[string]interface{} `json:"fetch_info"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&batchResp); err != nil {
			t.Fatalf("decode batch response: %v", err)
		}

		for i, h := range hashes {
			if _, ok := batchResp.Files[h.String()]; !ok {
				t.Errorf("dataset %d (hash %s) missing from batch response files", i, h.String())
			}
		}
		if len(batchResp.FetchInfo) == 0 {
			t.Error("expected non-empty fetch_info in batch response")
		}
		t.Logf("✓ Batch endpoint returned %d files and %d fetch_info entries",
			len(batchResp.Files), len(batchResp.FetchInfo))
	})

	t.Run("empty_request_returns_empty_maps", func(t *testing.T) {
		resp, err := http.Get(httpSrv.URL + "/reconstructions")
		if err != nil {
			t.Fatalf("empty batch request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("empty batch status=%d body=%s", resp.StatusCode, string(body))
		}

		var batchResp struct {
			Files     map[string]interface{} `json:"files"`
			FetchInfo map[string]interface{} `json:"fetch_info"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&batchResp); err != nil {
			t.Fatalf("decode empty batch response: %v", err)
		}

		if len(batchResp.Files) != 0 {
			t.Errorf("expected empty files map, got %d entries", len(batchResp.Files))
		}
		if len(batchResp.FetchInfo) != 0 {
			t.Errorf("expected empty fetch_info, got %d entries", len(batchResp.FetchInfo))
		}
	})

	t.Run("single_file_batch", func(t *testing.T) {
		url := httpSrv.URL + "/reconstructions?file_id=" + hashes[0].String()
		resp, err := http.Get(url)
		if err != nil {
			t.Fatalf("single-file batch request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("single-file batch status=%d body=%s", resp.StatusCode, string(body))
		}

		var batchResp struct {
			Files     map[string]interface{} `json:"files"`
			FetchInfo map[string]interface{} `json:"fetch_info"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&batchResp); err != nil {
			t.Fatalf("decode single-file batch response: %v", err)
		}

		if _, ok := batchResp.Files[hashes[0].String()]; !ok {
			t.Errorf("expected file %s in batch response", hashes[0].String())
		}
		if len(batchResp.FetchInfo) == 0 {
			t.Error("expected fetch_info for single non-empty file")
		}
	})

	t.Run("unknown_files_skipped", func(t *testing.T) {
		// Mix a known hash with a fabricated non-existent hash.
		var unknownHash xet.Hash
		for i := range unknownHash {
			unknownHash[i] = 0xcd
		}

		url := httpSrv.URL + "/reconstructions?file_id=" + hashes[0].String() + "&file_id=" + unknownHash.String()
		resp, err := http.Get(url)
		if err != nil {
			t.Fatalf("partial batch request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("partial batch status=%d body=%s", resp.StatusCode, string(body))
		}

		var batchResp struct {
			Files     map[string]interface{} `json:"files"`
			FetchInfo map[string]interface{} `json:"fetch_info"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&batchResp); err != nil {
			t.Fatalf("decode partial batch response: %v", err)
		}

		if _, ok := batchResp.Files[hashes[0].String()]; !ok {
			t.Errorf("expected known file %s in response", hashes[0].String())
		}
		if _, ok := batchResp.Files[unknownHash.String()]; ok {
			t.Errorf("unexpected unknown file %s in response", unknownHash.String())
		}
	})

	t.Run("xetgo_downloads_multiple_files", func(t *testing.T) {
		// xet-go downloads multiple files in a single DownloadFiles call,
		// which exercises our batch reconstruction endpoint.
		tempDir := t.TempDir()
		fileNames := []string{"xetgo-0.bin", "xetgo-1.bin", "xetgo-2.bin"}
		downloadReqs := make([]xetgo.DownloadRequest, len(datasets))
		for i, h := range hashes {
			downloadReqs[i] = xetgo.DownloadRequest{
				DestinationPath: filepath.Join(tempDir, fileNames[i]),
				Hash:            h.String(),
				FileSize:        int64(len(datasets[i])),
			}
		}

		downloaded, err := xetgo.DownloadFiles(downloadReqs, httpSrv.URL, nil)
		if err != nil {
			t.Fatalf("xet-go DownloadFiles failed: %v", err)
		}
		if len(downloaded) != len(datasets) {
			t.Fatalf("expected %d downloaded results, got %d", len(datasets), len(downloaded))
		}

		for i, data := range datasets {
			got, err := os.ReadFile(downloadReqs[i].DestinationPath)
			if err != nil {
				t.Errorf("read downloaded file %d: %v", i, err)
				continue
			}
			if !bytes.Equal(got, data) {
				t.Errorf("downloaded file %d content mismatch: got %d bytes want %d bytes",
					i, len(got), len(data))
			}
		}
		t.Logf("✓ xet-go successfully downloaded %d files via batch endpoint", len(datasets))
	})

	// Verify that the native client's DownloadFiles also reconstructs content correctly.
	t.Run("native_client_download_files", func(t *testing.T) {
		readers, sizes, err := nativeClient.DownloadFiles(context.Background(), hashes)
		if err != nil {
			t.Fatalf("DownloadFiles failed: %v", err)
		}
		if len(readers) != len(datasets) {
			t.Fatalf("expected %d readers, got %d", len(datasets), len(readers))
		}

		for i, data := range datasets {
			if readers[i] == nil {
				t.Errorf("reader %d is nil", i)
				continue
			}
			if sizes[i] != int64(len(data)) {
				t.Errorf("file %d size: got %d want %d", i, sizes[i], int64(len(data)))
			}
			got, err := io.ReadAll(readers[i])
			if err != nil {
				t.Errorf("read file %d: %v", i, err)
				continue
			}
			if !bytes.Equal(got, data) {
				t.Errorf("file %d content mismatch: got %d bytes want %d bytes", i, len(got), len(data))
			}
		}
		t.Logf("✓ Native DownloadFiles reconstructed correct content for %d files", len(datasets))
	})

	// Verify that the batch response is semantically equivalent to N individual V1
	// reconstruction responses — the same terms and a matching fetch_info superset.
	t.Run("batch_response_matches_individual_v1_responses", func(t *testing.T) {
		// Fetch the batch response for all files.
		batchURL := httpSrv.URL + "/reconstructions?"
		for i, h := range hashes {
			if i > 0 {
				batchURL += "&"
			}
			batchURL += "file_id=" + h.String()
		}
		batchHTTPResp, err := http.Get(batchURL)
		if err != nil {
			t.Fatalf("batch request failed: %v", err)
		}
		defer batchHTTPResp.Body.Close()

		var batchResp download.BatchReconstructionResponse
		if err := json.NewDecoder(batchHTTPResp.Body).Decode(&batchResp); err != nil {
			t.Fatalf("decode batch response: %v", err)
		}

		// For each file, fetch the individual V1 response and compare terms.
		for i, h := range hashes {
			v1Resp, err := http.Get(httpSrv.URL + "/v1/reconstructions/" + h.String())
			if err != nil {
				t.Fatalf("v1 reconstruction %d: %v", i, err)
			}
			defer v1Resp.Body.Close()

			var singleResp download.ReconstructionResponseV1
			if err := json.NewDecoder(v1Resp.Body).Decode(&singleResp); err != nil {
				t.Fatalf("decode v1 response %d: %v", i, err)
			}

			batchTerms, ok := batchResp.Files[h.String()]
			if !ok {
				t.Errorf("file %d (%s) missing from batch response", i, h)
				continue
			}

			// STRICT: terms must be identical.
			if len(batchTerms) != len(singleResp.Terms) {
				t.Errorf("file %d terms count: batch=%d individual=%d", i, len(batchTerms), len(singleResp.Terms))
				continue
			}
			for j := range batchTerms {
				bt := batchTerms[j]
				st := singleResp.Terms[j]
				if bt.Hash != st.Hash {
					t.Errorf("file %d term %d hash: batch=%s individual=%s", i, j, bt.Hash, st.Hash)
				}
				if bt.UnpackedLength != st.UnpackedLength {
					t.Errorf("file %d term %d UnpackedLength: batch=%d individual=%d",
						i, j, bt.UnpackedLength, st.UnpackedLength)
				}
				if bt.Range != st.Range {
					t.Errorf("file %d term %d Range: batch=%+v individual=%+v", i, j, bt.Range, st.Range)
				}
			}

			// STRICT: every xorb referenced by the individual response must be
			// present in the batch fetch_info.
			for xorbHash := range singleResp.FetchInfo {
				if _, ok := batchResp.FetchInfo[xorbHash]; !ok {
					t.Errorf("file %d: xorb %s missing from batch fetch_info", i, xorbHash)
				}
			}
		}
		t.Logf("✓ Batch response terms match individual V1 responses for %d files", len(hashes))
	})

	// Verify that xet-go and the native client reconstruct identical content when
	// downloading the same files and that both get correct data.
	t.Run("xetgo_and_native_reconstruct_same_content", func(t *testing.T) {
		tempDir := t.TempDir()

		// xet-go downloads all files.
		xetgoReqs := make([]xetgo.DownloadRequest, len(datasets))
		for i, h := range hashes {
			xetgoReqs[i] = xetgo.DownloadRequest{
				DestinationPath: filepath.Join(tempDir, "xetgo-"+h.String()+".bin"),
				Hash:            h.String(),
				FileSize:        int64(len(datasets[i])),
			}
		}
		if _, err := xetgo.DownloadFiles(xetgoReqs, httpSrv.URL, nil); err != nil {
			t.Fatalf("xet-go DownloadFiles failed: %v", err)
		}

		// Native client downloads all files via DownloadFiles (batch endpoint).
		nativeClient, err := client.NewClient(client.WithBaseURL(httpSrv.URL))
		if err != nil {
			t.Fatalf("create native client: %v", err)
		}

		readers, sizes, err := nativeClient.DownloadFiles(context.Background(), hashes)
		if err != nil {
			t.Fatalf("native DownloadFiles failed: %v", err)
		}

		for i, data := range datasets {
			// xet-go content check.
			xetgoGot, err := os.ReadFile(xetgoReqs[i].DestinationPath)
			if err != nil {
				t.Errorf("read xet-go file %d: %v", i, err)
			} else if !bytes.Equal(xetgoGot, data) {
				t.Errorf("xet-go file %d content mismatch: got %d bytes want %d bytes",
					i, len(xetgoGot), len(data))
			}

			// Native content check.
			if readers[i] == nil {
				t.Errorf("native reader %d is nil", i)
				continue
			}
			if sizes[i] != int64(len(data)) {
				t.Errorf("native file %d size: got %d want %d", i, sizes[i], int64(len(data)))
			}
			nativeGot, err := io.ReadAll(readers[i])
			if err != nil {
				t.Errorf("read native file %d: %v", i, err)
				continue
			}
			if !bytes.Equal(nativeGot, data) {
				t.Errorf("native file %d content mismatch: got %d bytes want %d bytes",
					i, len(nativeGot), len(data))
			}

			// xet-go and native must agree on content.
			if err == nil && !bytes.Equal(xetgoGot, nativeGot) {
				t.Errorf("file %d: xet-go and native content differ", i)
			}
		}
		t.Logf("✓ xet-go and native both reconstructed correct content for %d files", len(datasets))
	})
}
