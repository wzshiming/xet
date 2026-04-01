package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
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

func TestDownloadXorbUsesPersistentCache(t *testing.T) {
	xorbObj := xorb.NewXorb()
	if err := xorbObj.AddChunk(xet.ChunkBytes([]byte("cache-me"))); err != nil {
		t.Fatalf("add chunk: %v", err)
	}
	encoded, err := xorb.Encode(xorbObj, false)
	if err != nil {
		t.Fatalf("encode xorb: %v", err)
	}
	payload, err := io.ReadAll(encoded)
	if err != nil {
		t.Fatalf("read payload: %v", err)
	}

	var hits int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(payload)
	}))
	defer server.Close()

	client := NewClient(WithBaseURL(server.URL))
	client.cacheDirPath = t.TempDir()

	first, err := client.DownloadXorb(context.Background(), server.URL+"/xorb", nil)
	if err != nil {
		t.Fatalf("first download: %v", err)
	}
	second, err := client.DownloadXorb(context.Background(), server.URL+"/xorb", nil)
	if err != nil {
		t.Fatalf("second download: %v", err)
	}

	if first.Hash != second.Hash {
		t.Fatalf("cached xorb mismatch: %s vs %s", first.Hash.String(), second.Hash.String())
	}
	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Fatalf("expected one network hit due to cache reuse, got %d", got)
	}
}
