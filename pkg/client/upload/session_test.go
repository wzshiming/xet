package upload

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wzshiming/xet/pkg/client"
	"github.com/wzshiming/xet/pkg/xet"
)

func TestNewSession(t *testing.T) {
	client := client.NewClient(client.ClientOptions{
		BaseURL: "https://example.com",
	})

	session := NewSession(SessionOptions{
		Client:            client,
		TargetXorbSize:    100 * 1024 * 1024,
		EnableGlobalDedup: true,
	})

	if session.targetXorbSize != 100*1024*1024 {
		t.Errorf("Expected targetXorbSize 100MB, got %d", session.targetXorbSize)
	}
	if !session.enableGlobalDedup {
		t.Error("Expected enableGlobalDedup to be true")
	}
	if session.client == nil {
		t.Error("Expected client to be set")
	}
	if session.localChunkCache == nil {
		t.Error("Expected localChunkCache to be initialized")
	}
}

func TestNewSessionDefaults(t *testing.T) {
	client := client.NewClient(client.ClientOptions{
		BaseURL: "https://example.com",
	})

	session := NewSession(SessionOptions{
		Client: client,
	})

	if session.targetXorbSize != 64*1024*1024 {
		t.Errorf("Expected default targetXorbSize 64MB, got %d", session.targetXorbSize)
	}
}

func TestLocalDeduplication(t *testing.T) {
	client := client.NewClient(client.ClientOptions{
		BaseURL: "https://example.com",
	})

	session := NewSession(SessionOptions{
		Client:            client,
		EnableGlobalDedup: false,
	})

	// First chunk should be new
	chunk1 := []byte("Hello, World!")
	hash1 := xet.ComputeChunkHash(chunk1)
	result1 := session.deduplicateChunk(context.Background(), hash1, chunk1)

	if !result1.IsNew {
		t.Error("Expected first chunk to be new")
	}
	if result1.ChunkHash != hash1 {
		t.Error("Expected chunk hash to match")
	}

	// Verify chunk is in cache
	if len(session.localChunkCache) != 1 {
		t.Errorf("Expected 1 chunk in cache, got %d", len(session.localChunkCache))
	}

	// Second occurrence should return the same result from cache
	result2 := session.deduplicateChunk(context.Background(), hash1, chunk1)

	if result2 != result1 {
		t.Error("Expected same deduplication result from cache")
	}
	if result2.ChunkHash != hash1 {
		t.Error("Expected chunk hash to match")
	}

	// Different chunk should be new
	chunk2 := []byte("Different content")
	hash2 := xet.ComputeChunkHash(chunk2)
	result3 := session.deduplicateChunk(context.Background(), hash2, chunk2)

	if !result3.IsNew {
		t.Error("Expected different chunk to be new")
	}
	if result3.ChunkHash != hash2 {
		t.Error("Expected chunk hash to match")
	}
	if len(session.localChunkCache) != 2 {
		t.Errorf("Expected 2 chunks in cache, got %d", len(session.localChunkCache))
	}
}

func TestUploadFilesSimple(t *testing.T) {
	// Create test server
	uploadedShards := 0
	uploadedXorbs := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/shards" {
			uploadedShards++
			resp := client.ShardUploadResponse{Result: 1}
			json.NewEncoder(w).Encode(resp)
		} else if r.Method == http.MethodPost {
			uploadedXorbs++
			resp := client.XorbUploadResponse{WasInserted: true}
			json.NewEncoder(w).Encode(resp)
		}
	}))
	defer server.Close()

	client := client.NewClient(client.ClientOptions{
		BaseURL: server.URL,
	})

	session := NewSession(SessionOptions{
		Client:            client,
		EnableGlobalDedup: false,
	})

	// Upload a simple file
	testData := []byte("Hello, XET Protocol! This is a test file.")
	chunkHash := xet.ComputeChunkHash(testData)
	fileHash := xet.ComputeFileHash(chunkHash[:])

	files := []FileUploadInfo{
		{
			Path:     "test.txt",
			Data:     testData,
			FileHash: fileHash,
		},
	}

	err := session.UploadFiles(context.Background(), files)
	if err != nil {
		t.Fatalf("UploadFiles failed: %v", err)
	}

	// Verify that xorbs and shards were uploaded
	if uploadedXorbs == 0 {
		t.Error("Expected at least one xorb to be uploaded")
	}
	if uploadedShards == 0 {
		t.Error("Expected at least one shard to be uploaded")
	}
}

func TestUploadFilesWithDeduplication(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/shards" {
			resp := client.ShardUploadResponse{Result: 1}
			json.NewEncoder(w).Encode(resp)
		} else if r.Method == http.MethodPost {
			resp := client.XorbUploadResponse{WasInserted: true}
			json.NewEncoder(w).Encode(resp)
		}
	}))
	defer server.Close()

	client := client.NewClient(client.ClientOptions{
		BaseURL: server.URL,
	})

	session := NewSession(SessionOptions{
		Client:            client,
		EnableGlobalDedup: false,
	})

	// Upload two files with identical content
	testData := []byte("Duplicate content for testing deduplication")
	chunkHash := xet.ComputeChunkHash(testData)
	fileHash := xet.ComputeFileHash(chunkHash[:])

	files := []FileUploadInfo{
		{
			Path:     "file1.txt",
			Data:     testData,
			FileHash: fileHash,
		},
		{
			Path:     "file2.txt",
			Data:     testData,
			FileHash: fileHash,
		},
	}

	err := session.UploadFiles(context.Background(), files)
	if err != nil {
		t.Fatalf("UploadFiles failed: %v", err)
	}

	// Verify local deduplication cache
	if len(session.localChunkCache) == 0 {
		t.Error("Expected chunks to be in local cache")
	}
}

func TestUploadEmptyFile(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/shards" {
			resp := client.ShardUploadResponse{Result: 1}
			json.NewEncoder(w).Encode(resp)
		}
	}))
	defer server.Close()

	client := client.NewClient(client.ClientOptions{
		BaseURL: server.URL,
	})

	session := NewSession(SessionOptions{
		Client: client,
	})

	// Upload an empty file
	files := []FileUploadInfo{
		{
			Path:     "empty.txt",
			Data:     []byte{},
			FileHash: xet.Hash{},
		},
	}

	err := session.UploadFiles(context.Background(), files)
	if err != nil {
		t.Fatalf("UploadFiles failed: %v", err)
	}
}
