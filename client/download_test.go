package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/upload"
	"github.com/wzshiming/xet/xorb"
)

func TestClientAdapterPreservesAuthorizationWithRange(t *testing.T) {
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
		// Verify both Authorization and Range headers are present
		authHeader := r.Header.Get("Authorization")
		rangeHeader := r.Header.Get("Range")

		if authHeader != "Bearer test-token" {
			t.Errorf("Expected Authorization header 'Bearer test-token', got '%s'", authHeader)
		}

		if rangeHeader != "bytes=0-100" {
			t.Errorf("Expected Range header 'bytes=0-100', got '%s'", rangeHeader)
		}

		// Respond with partial content
		w.WriteHeader(http.StatusPartialContent)
		w.Write(encoded)
	}))
	defer server.Close()

	client := NewClient(WithBaseURL(server.URL), WithToken("test-token"))

	// Create adapter (this is what DownloadSession uses internally)
	adapter := &clientAdapter{client: client}

	// Call adapter's DownloadXorb with Range header
	header := http.Header{
		"Range": []string{"bytes=0-100"},
	}

	_, err = adapter.DownloadXorb(context.Background(), server.URL, header)
	if err != nil {
		t.Fatalf("clientAdapter.DownloadXorb failed: %v", err)
	}
}
