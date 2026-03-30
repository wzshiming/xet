package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/upload"
	"github.com/wzshiming/xet/xorb"
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

		// Try to deserialize it to verify it's valid
		_, err := xorb.Decode(r.Body, false)
		if err != nil {
			t.Errorf("Failed to deserialize uploaded xorb: %v", err)
		}

		resp := upload.XorbUploadResponse{
			WasInserted: true,
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewClient(WithBaseURL(server.URL), WithToken("test-token"))

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

func TestDownloadXorbWithAuthorization(t *testing.T) {
	// Create a test xorb
	xorbObj := xorb.NewXorb()
	testData := []byte("test chunk data")
	chunkData := xet.ChunkBytes(testData)
	err := xorbObj.AddChunk(chunkData)
	if err != nil {
		t.Fatalf("Failed to add chunk: %v", err)
	}

	// Encode it
	encodedReader, err := upload.EncodeXorb(xorbObj)
	if err != nil {
		t.Fatalf("Failed to encode xorb: %v", err)
	}

	// Read the encoded data into a byte slice
	var encoded []byte
	buf := make([]byte, 4096)
	for {
		n, err := encodedReader.Read(buf)
		if n > 0 {
			encoded = append(encoded, buf[:n]...)
		}
		if err != nil {
			break
		}
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("Expected GET method, got %s", r.Method)
		}
		// Verify Authorization header is present
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Errorf("Expected Authorization header 'Bearer test-token', got '%s'", r.Header.Get("Authorization"))
		}

		// Respond with partial content if Range header is present
		if r.Header.Get("Range") != "" {
			w.WriteHeader(http.StatusPartialContent)
		} else {
			w.WriteHeader(http.StatusOK)
		}
		// Write the encoded xorb
		w.Write(encoded)
	}))
	defer server.Close()

	client := NewClient(WithBaseURL(server.URL), WithToken("test-token"))

	t.Run("without_range", func(t *testing.T) {
		_, err := client.DownloadXorb(context.Background(), server.URL)
		if err != nil {
			t.Fatalf("DownloadXorb failed: %v", err)
		}
	})

	t.Run("with_range", func(t *testing.T) {
		_, err := client.DownloadXorb(context.Background(), server.URL, WithRange(0, 100))
		if err != nil {
			t.Fatalf("DownloadXorb with Range failed: %v", err)
		}
	})
}

