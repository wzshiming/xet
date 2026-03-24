package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wzshiming/xet/pkg/shard"
	"github.com/wzshiming/xet/pkg/xet"
)

func TestNewClient(t *testing.T) {
	client := NewClient(ClientOptions{
		BaseURL:   "https://example.com/",
		Token:     "test-token",
		Namespace: "test-ns",
		Timeout:   10 * time.Second,
	})

	if client.baseURL != "https://example.com" {
		t.Errorf("Expected baseURL 'https://example.com', got '%s'", client.baseURL)
	}
	if client.token != "test-token" {
		t.Errorf("Expected token 'test-token', got '%s'", client.token)
	}
	if client.namespace != "test-ns" {
		t.Errorf("Expected namespace 'test-ns', got '%s'", client.namespace)
	}
}

func TestNewClientDefaults(t *testing.T) {
	client := NewClient(ClientOptions{
		BaseURL: "https://example.com",
	})

	if client.namespace != "default" {
		t.Errorf("Expected default namespace 'default', got '%s'", client.namespace)
	}
	if client.httpClient.Timeout != 30*time.Second {
		t.Errorf("Expected default timeout 30s, got %v", client.httpClient.Timeout)
	}
}

func TestGetReconstruction(t *testing.T) {
	// Create a test hash
	testHash := xet.Hash([32]byte{0xa1, 0xb2, 0xc3, 0xd4, 0xe5, 0xf6, 0xa7, 0xb8, 0xc9, 0xd0, 0xe1, 0xf2, 0xa3, 0xb4, 0xc5, 0xd6, 0xe7, 0xf8, 0xa9, 0xb0, 0xc1, 0xd2, 0xe3, 0xf4, 0xa5, 0xb6, 0xc7, 0xd8, 0xe9, 0xf0, 0xa1, 0xb2})
	expectedPath := "/api/v1/reconstructions/" + testHash.String()

	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != expectedPath {
			t.Errorf("Unexpected path: %s, expected: %s", r.URL.Path, expectedPath)
		}
		if r.Method != http.MethodGet {
			t.Errorf("Expected GET method, got %s", r.Method)
		}
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Errorf("Expected Authorization header 'Bearer test-token', got '%s'", r.Header.Get("Authorization"))
		}

		resp := ReconstructionResponse{
			OffsetIntoFirstRange: 0,
			Terms: []Term{
				{
					Hash:           "chunk1",
					UnpackedLength: 1024,
					Range:          ChunkRange{Start: 0, End: 1},
				},
			},
			FetchInfo: map[string][]FetchInfoEntry{
				"xorb1": {
					{
						Range:    ChunkRange{Start: 0, End: 1},
						URL:      "https://example.com/xorb1",
						URLRange: ByteRange{Start: 0, End: 1023},
					},
				},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewClient(ClientOptions{
		BaseURL: server.URL,
		Token:   "test-token",
	})

	reconstruction, err := client.GetReconstruction(context.Background(), testHash)
	if err != nil {
		t.Fatalf("GetReconstruction failed: %v", err)
	}

	if reconstruction.OffsetIntoFirstRange != 0 {
		t.Errorf("Expected OffsetIntoFirstRange 0, got %d", reconstruction.OffsetIntoFirstRange)
	}
	if len(reconstruction.Terms) != 1 {
		t.Errorf("Expected 1 term, got %d", len(reconstruction.Terms))
	}
	if reconstruction.Terms[0].Hash != "chunk1" {
		t.Errorf("Expected term hash 'chunk1', got '%s'", reconstruction.Terms[0].Hash)
	}
}

func TestGetReconstructionError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("file not found"))
	}))
	defer server.Close()

	client := NewClient(ClientOptions{
		BaseURL: server.URL,
	})

	hash := xet.Hash{}
	_, err := client.GetReconstruction(context.Background(), hash)
	if err == nil {
		t.Fatal("Expected error for 404 response")
	}
}

func TestGetReconstructionRange(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "bytes=1000-2000" {
			t.Errorf("Expected Range header 'bytes=1000-2000', got '%s'", rangeHeader)
		}

		w.WriteHeader(http.StatusPartialContent)
		resp := ReconstructionResponse{
			OffsetIntoFirstRange: 1000,
			Terms:                []Term{},
			FetchInfo:            map[string][]FetchInfoEntry{},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewClient(ClientOptions{
		BaseURL: server.URL,
	})

	hash := xet.Hash{}
	reconstruction, err := client.GetReconstructionRange(context.Background(), hash, 1000, 2000)
	if err != nil {
		t.Fatalf("GetReconstructionRange failed: %v", err)
	}

	if reconstruction.OffsetIntoFirstRange != 1000 {
		t.Errorf("Expected OffsetIntoFirstRange 1000, got %d", reconstruction.OffsetIntoFirstRange)
	}
}

func TestUploadXorb(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("Expected POST method, got %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/octet-stream" {
			t.Errorf("Expected Content-Type 'application/octet-stream', got '%s'", r.Header.Get("Content-Type"))
		}
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Errorf("Expected Authorization header 'Bearer test-token', got '%s'", r.Header.Get("Authorization"))
		}

		body, _ := io.ReadAll(r.Body)
		if string(body) != "xorb-data" {
			t.Errorf("Expected body 'xorb-data', got '%s'", string(body))
		}

		resp := XorbUploadResponse{
			WasInserted: true,
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewClient(ClientOptions{
		BaseURL: server.URL,
		Token:   "test-token",
	})

	hash := xet.Hash{}
	xorbData := []byte("xorb-data")
	resp, err := client.UploadXorb(context.Background(), hash, xorbData)
	if err != nil {
		t.Fatalf("UploadXorb failed: %v", err)
	}

	if !resp.WasInserted {
		t.Error("Expected WasInserted to be true")
	}
}

func TestUploadShard(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/shards" {
			t.Errorf("Unexpected path: %s", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Errorf("Expected POST method, got %s", r.Method)
		}

		body, _ := io.ReadAll(r.Body)
		if string(body) != "shard-data" {
			t.Errorf("Expected body 'shard-data', got '%s'", string(body))
		}

		resp := ShardUploadResponse{
			Result: 1,
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewClient(ClientOptions{
		BaseURL: server.URL,
	})

	shardData := []byte("shard-data")
	resp, err := client.UploadShard(context.Background(), shardData)
	if err != nil {
		t.Fatalf("UploadShard failed: %v", err)
	}

	if resp.Result != 1 {
		t.Errorf("Expected Result 1, got %d", resp.Result)
	}
}

func TestQueryChunkDeduplicationNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := NewClient(ClientOptions{
		BaseURL: server.URL,
	})

	hash := xet.Hash{}
	result, err := client.QueryChunkDeduplication(context.Background(), hash)
	if err != nil {
		t.Fatalf("QueryChunkDeduplication failed: %v", err)
	}

	if result != nil {
		t.Error("Expected nil result for 404")
	}
}

func TestQueryChunkDeduplicationFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return a minimal valid shard
		shard := shard.NewShard()
		serialized, _ := shard.Serialize()
		w.Write(serialized)
	}))
	defer server.Close()

	client := NewClient(ClientOptions{
		BaseURL: server.URL,
	})

	hash := xet.Hash{}
	result, err := client.QueryChunkDeduplication(context.Background(), hash)
	if err != nil {
		t.Fatalf("QueryChunkDeduplication failed: %v", err)
	}

	if result == nil {
		t.Error("Expected non-nil result")
	}
}

func TestDownloadXorbData(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("Expected GET method, got %s", r.Method)
		}

		w.Write([]byte("xorb-content"))
	}))
	defer server.Close()

	client := NewClient(ClientOptions{
		BaseURL: "https://unused.example.com",
	})

	data, err := client.DownloadXorbData(context.Background(), server.URL, nil)
	if err != nil {
		t.Fatalf("DownloadXorbData failed: %v", err)
	}

	if string(data) != "xorb-content" {
		t.Errorf("Expected data 'xorb-content', got '%s'", string(data))
	}
}

func TestDownloadXorbDataWithRange(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "bytes=100-200" {
			t.Errorf("Expected Range header 'bytes=100-200', got '%s'", rangeHeader)
		}

		w.WriteHeader(http.StatusPartialContent)
		w.Write([]byte("partial-content"))
	}))
	defer server.Close()

	client := NewClient(ClientOptions{
		BaseURL: "https://unused.example.com",
	})

	byteRange := &ByteRange{Start: 100, End: 200}
	data, err := client.DownloadXorbData(context.Background(), server.URL, byteRange)
	if err != nil {
		t.Fatalf("DownloadXorbData failed: %v", err)
	}

	if string(data) != "partial-content" {
		t.Errorf("Expected data 'partial-content', got '%s'", string(data))
	}
}
