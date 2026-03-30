package client

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/pkg/reconstruction"
)

func TestGetReconstruction(t *testing.T) {
	// Create a test hash
	testHash := xet.Hash([32]byte{0xa1, 0xb2, 0xc3, 0xd4, 0xe5, 0xf6, 0xa7, 0xb8, 0xc9, 0xd0, 0xe1, 0xf2, 0xa3, 0xb4, 0xc5, 0xd6, 0xe7, 0xf8, 0xa9, 0xb0, 0xc1, 0xd2, 0xe3, 0xf4, 0xa5, 0xb6, 0xc7, 0xd8, 0xe9, 0xf0, 0xa1, 0xb2})
	expectedPath := "/v1/reconstructions/" + testHash.String()

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

		resp := reconstruction.ReconstructionResponse{
			OffsetIntoFirstRange: 0,
			Terms: []reconstruction.Term{
				{
					Hash:           "chunk1",
					UnpackedLength: 1024,
					Range:          reconstruction.ChunkRange{Start: 0, End: 1},
				},
			},
			FetchInfo: map[string][]reconstruction.FetchInfoEntry{
				"xorb1": {
					{
						Range:    reconstruction.ChunkRange{Start: 0, End: 1},
						URL:      "https://example.com/xorb1",
						URLRange: reconstruction.ByteRange{Start: 0, End: 1023},
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

	reconstruction, err := client.GetReconstructionV1(context.Background(), testHash)
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
	_, err := client.GetReconstructionV1(context.Background(), hash)
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
		resp := reconstruction.ReconstructionResponse{
			OffsetIntoFirstRange: 1000,
			Terms:                []reconstruction.Term{},
			FetchInfo:            map[string][]reconstruction.FetchInfoEntry{},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewClient(ClientOptions{
		BaseURL: server.URL,
	})

	hash := xet.Hash{}
	reconstruction, err := client.GetReconstructionV1(context.Background(), hash, WithRange(1000, 2000))
	if err != nil {
		t.Fatalf("GetReconstructionRange failed: %v", err)
	}

	if reconstruction.OffsetIntoFirstRange != 1000 {
		t.Errorf("Expected OffsetIntoFirstRange 1000, got %d", reconstruction.OffsetIntoFirstRange)
	}
}

func TestGetReconstructionV2(t *testing.T) {
	testHash := xet.Hash([32]byte{0xa1, 0xb2, 0xc3, 0xd4, 0xe5, 0xf6, 0xa7, 0xb8, 0xc9, 0xd0, 0xe1, 0xf2, 0xa3, 0xb4, 0xc5, 0xd6, 0xe7, 0xf8, 0xa9, 0xb0, 0xc1, 0xd2, 0xe3, 0xf4, 0xa5, 0xb6, 0xc7, 0xd8, 0xe9, 0xf0, 0xa1, 0xb2})
	expectedPath := "/v2/reconstructions/" + testHash.String()

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

		resp := reconstruction.ReconstructionResponseV2{
			OffsetIntoFirstRange: 0,
			Terms: []reconstruction.Term{
				{
					Hash:           "xorb1",
					UnpackedLength: 1024,
					Range:          reconstruction.ChunkRange{Start: 0, End: 1},
				},
			},
			Xorbs: map[string][]reconstruction.XorbMultiRangeFetch{
				"xorb1": {
					{
						URL: "https://example.com/xorb1",
						Ranges: []reconstruction.XorbRangeDescriptor{
							{
								Chunks: reconstruction.ChunkRange{Start: 0, End: 1},
								Bytes:  reconstruction.ByteRange{Start: 0, End: 1023},
							},
						},
					},
				},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	c := NewClient(ClientOptions{
		BaseURL: server.URL,
		Token:   "test-token",
	})

	reconstruction, err := c.GetReconstructionV2(context.Background(), testHash)
	if err != nil {
		t.Fatalf("GetReconstructionV2 failed: %v", err)
	}

	if reconstruction.OffsetIntoFirstRange != 0 {
		t.Errorf("Expected OffsetIntoFirstRange 0, got %d", reconstruction.OffsetIntoFirstRange)
	}
	if len(reconstruction.Terms) != 1 {
		t.Errorf("Expected 1 term, got %d", len(reconstruction.Terms))
	}
	if reconstruction.Terms[0].Hash != "xorb1" {
		t.Errorf("Expected term hash 'xorb1', got '%s'", reconstruction.Terms[0].Hash)
	}
	if len(reconstruction.Xorbs) != 1 {
		t.Errorf("Expected 1 xorb entry, got %d", len(reconstruction.Xorbs))
	}
}

func TestGetReconstructionV2Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("file not found"))
	}))
	defer server.Close()

	c := NewClient(ClientOptions{
		BaseURL: server.URL,
	})

	hash := xet.Hash{}
	_, err := c.GetReconstructionV2(context.Background(), hash)
	if err == nil {
		t.Fatal("Expected error for 404 response")
	}
}

func TestGetReconstructionRangeV2(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "bytes=1000-2000" {
			t.Errorf("Expected Range header 'bytes=1000-2000', got '%s'", rangeHeader)
		}

		w.WriteHeader(http.StatusPartialContent)
		resp := reconstruction.ReconstructionResponseV2{
			OffsetIntoFirstRange: 1000,
			Terms:                []reconstruction.Term{},
			Xorbs:                map[string][]reconstruction.XorbMultiRangeFetch{},
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	c := NewClient(ClientOptions{
		BaseURL: server.URL,
	})

	hash := xet.Hash{}
	reconstruction, err := c.GetReconstructionV2(context.Background(), hash, WithRange(1000, 2000))
	if err != nil {
		t.Fatalf("GetReconstructionRangeV2 failed: %v", err)
	}

	if reconstruction.OffsetIntoFirstRange != 1000 {
		t.Errorf("Expected OffsetIntoFirstRange 1000, got %d", reconstruction.OffsetIntoFirstRange)
	}
}
