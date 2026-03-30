package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wzshiming/xet"
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
