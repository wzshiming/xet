package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/pkg/xorb"
)

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

		// Read the body and verify it's a serialized xorb
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("Failed to read body: %v", err)
		}

		// Try to deserialize it to verify it's valid
		_, err = xorb.DeserializeBytes(body, false)
		if err != nil {
			t.Errorf("Failed to deserialize uploaded xorb: %v", err)
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

	// Create a test xorb
	xorbObj := xorb.NewXorb()
	testData := []byte("test chunk data")
	chunkData := xet.ChunkBytes(testData)
	err := xorbObj.AddChunk(chunkData)
	if err != nil {
		t.Fatalf("Failed to add chunk: %v", err)
	}

	resp, err := client.UploadXorb(context.Background(), xorbObj)
	if err != nil {
		t.Fatalf("UploadXorb failed: %v", err)
	}

	if !resp.WasInserted {
		t.Error("Expected WasInserted to be true")
	}
}

func TestDownloadXorb(t *testing.T) {
	// Create a test xorb
	xorbObj := xorb.NewXorb()
	testData := []byte("test chunk data for download")
	chunkData := xet.ChunkBytes(testData)
	err := xorbObj.AddChunk(chunkData)
	if err != nil {
		t.Fatalf("Failed to add chunk: %v", err)
	}

	// Serialize the xorb
	serialized, err := xorb.SerializeBytes(xorbObj, false)
	if err != nil {
		t.Fatalf("Failed to serialize xorb: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("Expected GET method, got %s", r.Method)
		}

		w.Write(serialized)
	}))
	defer server.Close()

	client := NewClient(ClientOptions{
		BaseURL: "https://unused.example.com",
	})

	downloadedXorb, err := client.DownloadXorb(context.Background(), server.URL)
	if err != nil {
		t.Fatalf("DownloadXorb failed: %v", err)
	}

	// Verify the downloaded xorb has the same hash
	if downloadedXorb.Hash != xorbObj.Hash {
		t.Errorf("Expected hash %s, got %s", xorbObj.Hash.String(), downloadedXorb.Hash.String())
	}

	// Verify the chunks match
	if len(downloadedXorb.Chunks) != len(xorbObj.Chunks) {
		t.Errorf("Expected %d chunks, got %d", len(xorbObj.Chunks), len(downloadedXorb.Chunks))
	}

	if len(downloadedXorb.Chunks) > 0 {
		if string(downloadedXorb.Chunks[0].UncompressedData) != string(testData) {
			t.Errorf("Expected chunk data %q, got %q", testData, downloadedXorb.Chunks[0].UncompressedData)
		}
	}
}

func TestDownloadXorbWithRange(t *testing.T) {
	// Create a test xorb with multiple chunks
	xorbObj := xorb.NewXorb()
	testData1 := []byte("first chunk data")
	testData2 := []byte("second chunk data")

	err := xorbObj.AddChunk(xet.ChunkBytes(testData1))
	if err != nil {
		t.Fatalf("Failed to add first chunk: %v", err)
	}
	err = xorbObj.AddChunk(xet.ChunkBytes(testData2))
	if err != nil {
		t.Fatalf("Failed to add second chunk: %v", err)
	}

	// Serialize only chunks (chunks-only format)
	serialized, err := xorb.SerializeBytes(xorbObj, true)
	if err != nil {
		t.Fatalf("Failed to serialize xorb: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHeader := r.Header.Get("Range")
		if rangeHeader == "" {
			t.Error("Expected Range header to be set")
		}

		w.WriteHeader(http.StatusPartialContent)
		w.Write(serialized)
	}))
	defer server.Close()

	client := NewClient(ClientOptions{
		BaseURL: "https://unused.example.com",
	})

	downloadedXorb, err := client.DownloadXorb(context.Background(), server.URL, WithRange(0, int64(len(serialized)-1)))
	if err != nil {
		t.Fatalf("DownloadXorb with range failed: %v", err)
	}

	// Verify the chunks were downloaded
	if len(downloadedXorb.Chunks) != 2 {
		t.Errorf("Expected 2 chunks, got %d", len(downloadedXorb.Chunks))
	}

	if len(downloadedXorb.Chunks) == 2 {
		if string(downloadedXorb.Chunks[0].UncompressedData) != string(testData1) {
			t.Errorf("Expected first chunk data %q, got %q", testData1, downloadedXorb.Chunks[0].UncompressedData)
		}
		if string(downloadedXorb.Chunks[1].UncompressedData) != string(testData2) {
			t.Errorf("Expected second chunk data %q, got %q", testData2, downloadedXorb.Chunks[1].UncompressedData)
		}
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

	data, err := client.DownloadXorbData(context.Background(), server.URL)
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

	data, err := client.DownloadXorbData(context.Background(), server.URL, WithRange(100, 200))
	if err != nil {
		t.Fatalf("DownloadXorbData failed: %v", err)
	}

	if string(data) != "partial-content" {
		t.Errorf("Expected data 'partial-content', got '%s'", string(data))
	}
}
