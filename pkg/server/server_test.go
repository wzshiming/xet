package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wzshiming/xet/pkg/client"
	"github.com/wzshiming/xet/pkg/client/upload"
	"github.com/wzshiming/xet/pkg/gearhash"
	"github.com/wzshiming/xet/pkg/shard"
	"github.com/wzshiming/xet/pkg/xorb"
)

func TestServerXorbUpload(t *testing.T) {
	// Create storage
	storage, err := NewFileStorage(FileStorageOptions{
		BasePath: t.TempDir(),
		BaseURL:  "http://localhost:8080",
	})
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Create server
	srv := NewServer(ServerOptions{
		Storage: storage,
	})

	// Create test data and xorb
	testData := []byte("Hello, XET Protocol! This is a test file.")
	chunks := chunkData(t, testData)

	xorbObj := xorb.NewXorb()
	for _, chunk := range chunks {
		if err := xorbObj.AddChunk(chunk.Data); err != nil {
			t.Fatalf("Failed to add chunk: %v", err)
		}
	}

	xorbData, err := xorbObj.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize xorb: %v", err)
	}

	// Test xorb upload
	req := httptest.NewRequest(http.MethodPost, "/api/v1/xorbs/default/"+xorbObj.Hash.String(), bytes.NewReader(xorbData))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var response client.XorbUploadResponse
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if !response.WasInserted {
		t.Errorf("Expected xorb to be inserted")
	}

	// Test uploading the same xorb again
	req = httptest.NewRequest(http.MethodPost, "/api/v1/xorbs/default/"+xorbObj.Hash.String(), bytes.NewReader(xorbData))
	req.Header.Set("Content-Type", "application/octet-stream")
	w = httptest.NewRecorder()

	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", w.Code)
	}

	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response.WasInserted {
		t.Errorf("Expected xorb to not be inserted again")
	}
}

func TestServerShardUpload(t *testing.T) {
	// Create storage
	storage, err := NewFileStorage(FileStorageOptions{
		BasePath: t.TempDir(),
		BaseURL:  "http://localhost:8080",
	})
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Create server
	srv := NewServer(ServerOptions{
		Storage: storage,
	})

	// Create test data
	testData := []byte("Hello, XET Protocol! This is a test file for shard upload.")
	fileInfo, err := upload.ComputeFileInfo(testData)
	if err != nil {
		t.Fatalf("Failed to compute file info: %v", err)
	}

	// Create chunks and xorb manually
	chunks := chunkData(t, testData)
	xorbObj := xorb.NewXorb()
	for _, chunk := range chunks {
		if err := xorbObj.AddChunk(chunk.Data); err != nil {
			t.Fatalf("Failed to add chunk: %v", err)
		}
	}

	// Build a shard
	testShard := shard.NewShard()
	testShard.AddFile(shard.FileBlock{
		FileHash: fileInfo.FileHash,
		Flags:    0,
		Entries: []shard.FileDataSequenceEntry{
			{
				CASHash:          xorbObj.Hash,
				CASFlags:         0,
				UnpackedSegBytes: uint32(len(testData)),
				ChunkIndexStart:  0,
				ChunkIndexEnd:    uint32(len(xorbObj.Chunks)),
			},
		},
	})

	// Add CAS block
	casBlock := shard.CASBlock{
		CASHash:  xorbObj.Hash,
		CASFlags: 0,
		Chunks:   []shard.CASChunkSequenceEntry{},
	}

	// Add chunks to CAS block
	var byteOffset uint32
	for _, chunk := range xorbObj.Chunks {
		casBlock.Chunks = append(casBlock.Chunks, shard.CASChunkSequenceEntry{
			ChunkHash:        chunk.Hash,
			ByteRangeStart:   byteOffset,
			UnpackedSegBytes: uint32(len(chunk.UncompressedData)),
			Flags:            shard.ChunkGlobalDedupEligible,
		})
		byteOffset += uint32(len(chunk.UncompressedData))
	}
	testShard.AddCASBlock(casBlock)

	// Serialize shard (without footer for upload)
	shardData, err := testShard.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize shard: %v", err)
	}

	// Test shard upload
	req := httptest.NewRequest(http.MethodPost, "/api/v1/shards", bytes.NewReader(shardData))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var response client.ShardUploadResponse
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response.Result != 1 {
		t.Errorf("Expected result 1 (inserted), got %d", response.Result)
	}
}

func TestServerGetReconstruction(t *testing.T) {
	// Create storage
	storage, err := NewFileStorage(FileStorageOptions{
		BasePath: t.TempDir(),
		BaseURL:  "http://localhost:8080",
	})
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Create server
	srv := NewServer(ServerOptions{
		Storage: storage,
	})

	// Create test data and upload xorb + shard
	testData := []byte("Hello, XET Protocol! This is a test file for reconstruction.")
	fileInfo, err := upload.ComputeFileInfo(testData)
	if err != nil {
		t.Fatalf("Failed to compute file info: %v", err)
	}

	// Create chunks and xorb manually
	chunks := chunkData(t, testData)
	xorbObj := xorb.NewXorb()
	for _, chunk := range chunks {
		if err := xorbObj.AddChunk(chunk.Data); err != nil {
			t.Fatalf("Failed to add chunk: %v", err)
		}
	}

	// Upload xorb first
	xorbData, err := xorbObj.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize xorb: %v", err)
	}

	_, err = storage.StoreXorb(context.Background(), "default", xorbObj.Hash, xorbData)
	if err != nil {
		t.Fatalf("Failed to store xorb: %v", err)
	}

	// Build and upload shard
	testShard := shard.NewShard()
	testShard.AddFile(shard.FileBlock{
		FileHash: fileInfo.FileHash,
		Flags:    0,
		Entries: []shard.FileDataSequenceEntry{
			{
				CASHash:          xorbObj.Hash,
				CASFlags:         0,
				UnpackedSegBytes: uint32(len(testData)),
				ChunkIndexStart:  0,
				ChunkIndexEnd:    uint32(len(xorbObj.Chunks)),
			},
		},
	})

	// Add CAS block
	casBlock := shard.CASBlock{
		CASHash:  xorbObj.Hash,
		CASFlags: 0,
		Chunks:   []shard.CASChunkSequenceEntry{},
	}

	var byteOffset uint32
	for _, chunk := range xorbObj.Chunks {
		casBlock.Chunks = append(casBlock.Chunks, shard.CASChunkSequenceEntry{
			ChunkHash:        chunk.Hash,
			ByteRangeStart:   byteOffset,
			UnpackedSegBytes: uint32(len(chunk.UncompressedData)),
			Flags:            shard.ChunkGlobalDedupEligible,
		})
		byteOffset += uint32(len(chunk.UncompressedData))
	}
	testShard.AddCASBlock(casBlock)

	_, err = storage.StoreShard(context.Background(), testShard)
	if err != nil {
		t.Fatalf("Failed to store shard: %v", err)
	}

	// Test reconstruction query
	req := httptest.NewRequest(http.MethodGet, "/api/v1/reconstructions/"+fileInfo.FileHash.String(), nil)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var response client.ReconstructionResponse
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(response.Terms) == 0 {
		t.Errorf("Expected at least one term in response")
	}

	if len(response.FetchInfo) == 0 {
		t.Errorf("Expected fetch info in response")
	}
}

func TestServerXorbDownload(t *testing.T) {
	// Create storage
	storage, err := NewFileStorage(FileStorageOptions{
		BasePath: t.TempDir(),
		BaseURL:  "http://localhost:8080",
	})
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Create server
	srv := NewServer(ServerOptions{
		Storage: storage,
	})

	// Create test data and xorb
	testData := []byte("Hello, XET Protocol! This is a test file for download.")
	chunks := chunkData(t, testData)

	xorbObj := xorb.NewXorb()
	for _, chunk := range chunks {
		if err := xorbObj.AddChunk(chunk.Data); err != nil {
			t.Fatalf("Failed to add chunk: %v", err)
		}
	}

	xorbData, err := xorbObj.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize xorb: %v", err)
	}

	// Store xorb
	_, err = storage.StoreXorb(context.Background(), "default", xorbObj.Hash, xorbData)
	if err != nil {
		t.Fatalf("Failed to store xorb: %v", err)
	}

	// Test xorb download
	req := httptest.NewRequest(http.MethodGet, "/api/v1/xorbs/default/"+xorbObj.Hash.String()+"/data", nil)
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", w.Code)
	}

	downloadedData, err := io.ReadAll(w.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	if !bytes.Equal(downloadedData, xorbData) {
		t.Errorf("Downloaded xorb data does not match original")
	}
}

func TestServerAuthentication(t *testing.T) {
	// Create storage
	storage, err := NewFileStorage(FileStorageOptions{
		BasePath: t.TempDir(),
		BaseURL:  "http://localhost:8080",
	})
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Create server with authentication
	expectedToken := "test-token-123"
	srv := NewServer(ServerOptions{
		Storage: storage,
		AuthFn: func(token string) bool {
			return token == expectedToken
		},
	})

	// Create test xorb
	testData := []byte("Hello, XET!")
	chunks := chunkData(t, testData)
	xorbObj := xorb.NewXorb()
	for _, chunk := range chunks {
		xorbObj.AddChunk(chunk.Data)
	}
	xorbData, _ := xorbObj.Serialize()

	// Test without authentication - should fail
	req := httptest.NewRequest(http.MethodPost, "/api/v1/xorbs/default/"+xorbObj.Hash.String(), bytes.NewReader(xorbData))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()

	srv.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected status 401, got %d", w.Code)
	}

	// Test with wrong token - should fail
	req = httptest.NewRequest(http.MethodPost, "/api/v1/xorbs/default/"+xorbObj.Hash.String(), bytes.NewReader(xorbData))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Authorization", "Bearer wrong-token")
	w = httptest.NewRecorder()

	srv.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected status 401, got %d", w.Code)
	}

	// Test with correct token - should succeed
	req = httptest.NewRequest(http.MethodPost, "/api/v1/xorbs/default/"+xorbObj.Hash.String(), bytes.NewReader(xorbData))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Authorization", "Bearer "+expectedToken)
	w = httptest.NewRecorder()

	srv.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}

type chunk struct {
	Data   []byte
	Offset int64
}

func chunkData(t *testing.T, data []byte) []chunk {
	t.Helper()

	var chunks []chunk
	err := gearhash.ChunkData(bytes.NewReader(data), func(offset int64, dataChunk []byte) error {
		buf := make([]byte, len(dataChunk))
		copy(buf, dataChunk)
		chunks = append(chunks, chunk{Data: buf, Offset: offset})
		return nil
	})
	if err != nil {
		t.Fatalf("ChunkData failed: %v", err)
	}

	return chunks
}
