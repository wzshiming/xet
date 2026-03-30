package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/pkg/upload"
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
