package download

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wzshiming/xet/pkg/api"
	"github.com/wzshiming/xet/pkg/xet"
	"github.com/wzshiming/xet/pkg/xorb"
)

func TestNewSession(t *testing.T) {
	client := api.NewClient(api.ClientOptions{
		BaseURL: "https://example.com",
	})

	session := NewSession(SessionOptions{
		Client:        client,
		EnableCaching: true,
	})

	if session.client == nil {
		t.Error("Expected client to be set")
	}
	if session.chunkCache == nil {
		t.Error("Expected chunk cache to be initialized")
	}
}

func TestNewSessionNoCaching(t *testing.T) {
	client := api.NewClient(api.ClientOptions{
		BaseURL: "https://example.com",
	})

	session := NewSession(SessionOptions{
		Client:        client,
		EnableCaching: false,
	})

	if session.chunkCache != nil {
		t.Error("Expected chunk cache to be nil when caching is disabled")
	}
}

func TestDownloadFileSimple(t *testing.T) {
	// Create test data
	testData := []byte("Hello, XET Protocol!")

	// Create xorb
	xorbObj := xorb.NewXorb()
	xorbObj.AddChunk(testData)
	xorbData, _ := xorbObj.Serialize()

	// Create test server
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/xorb-data" {
			// Serve xorb data
			w.Write(xorbData)
		} else {
			// Serve reconstruction response
			resp := api.ReconstructionResponse{
				OffsetIntoFirstRange: 0,
				Terms: []api.Term{
					{
						Hash:           xorbObj.Hash.String(),
						UnpackedLength: uint64(len(testData)),
						Range:          api.ChunkRange{Start: 0, End: 1},
					},
				},
				FetchInfo: map[string][]api.FetchInfoEntry{
					xorbObj.Hash.String(): {
						{
							Range:    api.ChunkRange{Start: 0, End: 1},
							URL:      server.URL + "/xorb-data",
							URLRange: api.ByteRange{Start: 0, End: int64(len(xorbData) - 1)},
						},
					},
				},
			}
			json.NewEncoder(w).Encode(resp)
		}
	}))
	defer server.Close()

	client := api.NewClient(api.ClientOptions{
		BaseURL: server.URL,
	})

	session := NewSession(SessionOptions{
		Client:        client,
		EnableCaching: false,
	})

	fileHash := xet.Hash{} // Dummy file hash for testing
	result, err := session.DownloadFile(context.Background(), fileHash)
	if err != nil {
		t.Fatalf("DownloadFile failed: %v", err)
	}

	if string(result) != string(testData) {
		t.Errorf("Expected data '%s', got '%s'", string(testData), string(result))
	}
}

func TestDownloadFileWithCaching(t *testing.T) {
	// Create test data
	testData := []byte("Test data for caching")
	chunkHash := xet.ComputeChunkHash(testData)

	// Create xorb
	xorbObj := xorb.NewXorb()
	xorbObj.AddChunk(testData)
	xorbData, _ := xorbObj.Serialize()

	// Create test server
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/xorb-data" {
			w.Write(xorbData)
		} else {
			resp := api.ReconstructionResponse{
				OffsetIntoFirstRange: 0,
				Terms: []api.Term{
					{
						Hash:           xorbObj.Hash.String(),
						UnpackedLength: uint64(len(testData)),
						Range:          api.ChunkRange{Start: 0, End: 1},
					},
				},
				FetchInfo: map[string][]api.FetchInfoEntry{
					xorbObj.Hash.String(): {
						{
							Range:    api.ChunkRange{Start: 0, End: 1},
							URL:      server.URL + "/xorb-data",
							URLRange: api.ByteRange{Start: 0, End: int64(len(xorbData) - 1)},
						},
					},
				},
			}
			json.NewEncoder(w).Encode(resp)
		}
	}))
	defer server.Close()

	client := api.NewClient(api.ClientOptions{
		BaseURL: server.URL,
	})

	session := NewSession(SessionOptions{
		Client:        client,
		EnableCaching: true,
	})

	// Download file
	fileHash := xet.Hash{}
	result, err := session.DownloadFile(context.Background(), fileHash)
	if err != nil {
		t.Fatalf("DownloadFile failed: %v", err)
	}

	if string(result) != string(testData) {
		t.Errorf("Expected data '%s', got '%s'", string(testData), string(result))
	}

	// Verify chunk is cached
	cachedData, ok := session.GetCachedChunk(chunkHash)
	if !ok {
		t.Error("Expected chunk to be cached")
	}
	if string(cachedData) != string(testData) {
		t.Errorf("Expected cached data '%s', got '%s'", string(testData), string(cachedData))
	}
}

func TestDownloadFileRange(t *testing.T) {
	// Create test data
	testData := []byte("0123456789ABCDEFGHIJ")

	// Create xorb
	xorbObj := xorb.NewXorb()
	xorbObj.AddChunk(testData)
	xorbData, _ := xorbObj.Serialize()

	// Create test server
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/xorb-data" {
			w.Write(xorbData)
		} else {
			// For range request, return partial reconstruction
			resp := api.ReconstructionResponse{
				OffsetIntoFirstRange: 5, // Start at byte 5
				Terms: []api.Term{
					{
						Hash:           xorbObj.Hash.String(),
						UnpackedLength: uint64(len(testData)),
						Range:          api.ChunkRange{Start: 0, End: 1},
					},
				},
				FetchInfo: map[string][]api.FetchInfoEntry{
					xorbObj.Hash.String(): {
						{
							Range:    api.ChunkRange{Start: 0, End: 1},
							URL:      server.URL + "/xorb-data",
							URLRange: api.ByteRange{Start: 0, End: int64(len(xorbData) - 1)},
						},
					},
				},
			}
			json.NewEncoder(w).Encode(resp)
		}
	}))
	defer server.Close()

	client := api.NewClient(api.ClientOptions{
		BaseURL: server.URL,
	})

	session := NewSession(SessionOptions{
		Client:        client,
		EnableCaching: false,
	})

	fileHash := xet.Hash{}
	result, err := session.DownloadFileRange(context.Background(), fileHash, 5, 10)
	if err != nil {
		t.Fatalf("DownloadFileRange failed: %v", err)
	}

	expected := "56789ABCDE"
	if string(result) != expected {
		t.Errorf("Expected data '%s', got '%s'", expected, string(result))
	}
}

func TestClearCache(t *testing.T) {
	client := api.NewClient(api.ClientOptions{
		BaseURL: "https://example.com",
	})

	session := NewSession(SessionOptions{
		Client:        client,
		EnableCaching: true,
	})

	// Add some data to cache
	testHash := xet.Hash{1, 2, 3}
	session.chunkCache[testHash] = []byte("test")

	if len(session.chunkCache) != 1 {
		t.Errorf("Expected 1 item in cache, got %d", len(session.chunkCache))
	}

	// Clear cache
	session.ClearCache()

	if len(session.chunkCache) != 0 {
		t.Errorf("Expected cache to be empty, got %d items", len(session.chunkCache))
	}
}

func TestGetCachedChunkNotFound(t *testing.T) {
	client := api.NewClient(api.ClientOptions{
		BaseURL: "https://example.com",
	})

	session := NewSession(SessionOptions{
		Client:        client,
		EnableCaching: true,
	})

	testHash := xet.Hash{1, 2, 3}
	_, ok := session.GetCachedChunk(testHash)
	if ok {
		t.Error("Expected chunk to not be found in empty cache")
	}
}

func TestDownloadFileMultipleChunks(t *testing.T) {
	// Create test data with multiple chunks
	chunk1Data := []byte("First chunk data. ")
	chunk2Data := []byte("Second chunk data.")
	chunk1Hash := xet.ComputeChunkHash(chunk1Data)
	chunk2Hash := xet.ComputeChunkHash(chunk2Data)

	// Create xorb with multiple chunks
	xorbObj := xorb.NewXorb()
	xorbObj.AddChunk(chunk1Data)
	xorbObj.AddChunk(chunk2Data)
	xorbData, _ := xorbObj.Serialize()

	// Create test server
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/xorb-data" {
			w.Write(xorbData)
		} else {
			resp := api.ReconstructionResponse{
				OffsetIntoFirstRange: 0,
				Terms: []api.Term{
					{
						Hash:           xorbObj.Hash.String(),
						UnpackedLength: uint64(len(chunk1Data) + len(chunk2Data)),
						Range:          api.ChunkRange{Start: 0, End: 2}, // Both chunks
					},
				},
				FetchInfo: map[string][]api.FetchInfoEntry{
					xorbObj.Hash.String(): {
						{
							Range:    api.ChunkRange{Start: 0, End: 2},
							URL:      server.URL + "/xorb-data",
							URLRange: api.ByteRange{Start: 0, End: int64(len(xorbData) - 1)},
						},
					},
				},
			}
			json.NewEncoder(w).Encode(resp)
		}
	}))
	defer server.Close()

	client := api.NewClient(api.ClientOptions{
		BaseURL: server.URL,
	})

	session := NewSession(SessionOptions{
		Client:        client,
		EnableCaching: true,
	})

	fileHash := xet.Hash{}
	result, err := session.DownloadFile(context.Background(), fileHash)
	if err != nil {
		t.Fatalf("DownloadFile failed: %v", err)
	}

	expected := string(chunk1Data) + string(chunk2Data)
	if string(result) != expected {
		t.Errorf("Expected data '%s', got '%s'", expected, string(result))
	}

	// Verify both chunks are cached
	_, ok1 := session.GetCachedChunk(chunk1Hash)
	_, ok2 := session.GetCachedChunk(chunk2Hash)
	if !ok1 || !ok2 {
		t.Error("Expected both chunks to be cached")
	}
}
