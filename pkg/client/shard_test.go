package client

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/pkg/shard"
)

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
		reader, _ := shard.Serialize()
		io.Copy(w, reader)
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
