package client

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/pkg/xorb"
)

// Helper function to create a test xorb with sample chunks
func createTestXorb(t *testing.T, chunkData [][]byte) (xet.Hash, []byte, []byte) {
	t.Helper()

	x := xorb.NewXorb()
	for _, data := range chunkData {
		chunk := xet.ChunkBytes(data)
		if err := x.AddChunk(chunk); err != nil {
			t.Fatalf("Failed to add chunk: %v", err)
		}
	}

	// Serialize full xorb with footer
	serialized, err := xorb.SerializeBytes(x, false)
	if err != nil {
		t.Fatalf("Failed to serialize xorb: %v", err)
	}

	// Also create chunks-only serialization for range tests
	chunksOnly, err := xorb.SerializeBytes(x, true)
	if err != nil {
		t.Fatalf("Failed to serialize chunks only: %v", err)
	}

	return x.Hash, serialized, chunksOnly
}

func TestDownloadFile(t *testing.T) {
	// Create test data
	chunk1Data := []byte("Hello, ")
	chunk2Data := []byte("World!")
	expectedData := append(chunk1Data, chunk2Data...)

	xorbHash, xorbSerialized, _ := createTestXorb(t, [][]byte{chunk1Data, chunk2Data})

	// Create test server
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/reconstructions/"+xorbHash.String() {
			// Return reconstruction response
			resp := ReconstructionResponse{
				OffsetIntoFirstRange: 0,
				Terms: []Term{
					{
						Hash:           xorbHash.String(),
						UnpackedLength: uint64(len(expectedData)),
						Range:          ChunkRange{Start: 0, End: 2},
					},
				},
				FetchInfo: map[string][]FetchInfoEntry{
					xorbHash.String(): {
						{
							Range:    ChunkRange{Start: 0, End: 2},
							URL:      server.URL + "/xorbs/" + xorbHash.String(),
							URLRange: ByteRange{Start: 0, End: 0}, // Zero indicates full download
						},
					},
				},
			}
			json.NewEncoder(w).Encode(resp)
		} else if r.URL.Path == "/xorbs/"+xorbHash.String() {
			// Return xorb data
			w.Write(xorbSerialized)
		} else {
			http.Error(w, "Not found", http.StatusNotFound)
		}
	}))
	defer server.Close()

	// Create client and session
	c := NewClient(ClientOptions{
		BaseURL: server.URL,
	})
	session := NewDownloadSession(DownloadSessionOptions{
		Client: c,
	})

	// Download file
	reader, _, err := session.DownloadFileV1(context.Background(), xorbHash)
	if err != nil {
		t.Fatalf("DownloadFile failed: %v", err)
	}

	// Read downloaded data
	downloadedData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read downloaded data: %v", err)
	}

	// Verify data
	if !bytes.Equal(downloadedData, expectedData) {
		t.Errorf("Downloaded data mismatch: got %q, want %q", downloadedData, expectedData)
	}
}

func TestDownloadFileRange(t *testing.T) {
	// Create test data with multiple chunks
	chunk1Data := []byte("AAAAAAA") // 7 bytes
	chunk2Data := []byte("BBBBBBB") // 7 bytes
	chunk3Data := []byte("CCCCCCC") // 7 bytes
	allData := append(append(chunk1Data, chunk2Data...), chunk3Data...)

	xorbHash, xorbSerialized, _ := createTestXorb(t, [][]byte{chunk1Data, chunk2Data, chunk3Data})

	// Test downloading a range that spans chunks 1-2 (middle section)
	rangeStart := int64(7)                                // Start of chunk 2
	rangeEnd := int64(13)                                 // End of chunk 2 (exclusive: end of byte 13)
	expectedRangeData := allData[rangeStart : rangeEnd+1] // +1 because rangeEnd is inclusive in Range header

	// Track whether range header was sent correctly
	var rangeHeaderSent string
	var xorbRangeHeaderSent string

	// Create test server
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/reconstructions/"+xorbHash.String() {
			// Check if Range header was sent
			rangeHeaderSent = r.Header.Get("Range")

			// Return partial reconstruction response
			// Server should return only chunk 2 (index 1) with offset
			resp := ReconstructionResponse{
				OffsetIntoFirstRange: 0, // No offset within the first chunk of the range
				Terms: []Term{
					{
						Hash:           xorbHash.String(),
						UnpackedLength: uint64(len(chunk2Data)),
						Range:          ChunkRange{Start: 0, End: 1}, // Only chunk 2, but indexed from 0 in the partial xorb
					},
				},
				FetchInfo: map[string][]FetchInfoEntry{
					xorbHash.String(): {
						{
							Range:    ChunkRange{Start: 0, End: 1}, // Matching the term range
							URL:      server.URL + "/xorbs/" + xorbHash.String(),
							URLRange: ByteRange{Start: 100, End: 200}, // Non-zero indicates range download
						},
					},
				},
			}
			w.WriteHeader(http.StatusPartialContent)
			json.NewEncoder(w).Encode(resp)
		} else if r.URL.Path == "/xorbs/"+xorbHash.String() {
			// Check if Range header was sent for xorb download
			xorbRangeHeaderSent = r.Header.Get("Range")

			// When range is requested, return chunks-only data
			if xorbRangeHeaderSent != "" {
				w.WriteHeader(http.StatusPartialContent)
				// The server would return the requested byte range from the xorb
				// For this test, we need to calculate which chunks are in the range
				// and return just those chunks serialized
				//
				// In reality, the URLRange (100-200) would map to specific bytes in the xorb
				// For this simplified test, we'll just return the chunks-only serialization
				// of the requested chunks (chunk 1, index 1)

				// Create a temporary xorb with just chunk 2
				tempXorb := xorb.NewXorb()
				chunk := xet.ChunkBytes(chunk2Data)
				tempXorb.AddChunk(chunk)
				tempChunksOnly, _ := xorb.SerializeBytes(tempXorb, true)
				w.Write(tempChunksOnly)
			} else {
				w.Write(xorbSerialized)
			}
		} else {
			http.Error(w, "Not found", http.StatusNotFound)
		}
	}))
	defer server.Close()

	// Create client and session
	c := NewClient(ClientOptions{
		BaseURL: server.URL,
	})
	session := NewDownloadSession(DownloadSessionOptions{
		Client: c,
	})

	// Download file range
	reader, _, err := session.DownloadFileV1(context.Background(), xorbHash, WithRange(rangeStart, rangeEnd))
	if err != nil {
		t.Fatalf("DownloadFileRange failed: %v", err)
	}

	// Read downloaded data
	downloadedData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read downloaded data: %v", err)
	}

	// Verify data
	if !bytes.Equal(downloadedData, expectedRangeData) {
		t.Errorf("Downloaded range data mismatch: got %q (len=%d), want %q (len=%d)",
			downloadedData, len(downloadedData), expectedRangeData, len(expectedRangeData))
	}

	// Verify Range headers were sent
	if rangeHeaderSent == "" {
		t.Error("Expected Range header to be sent to reconstruction endpoint")
	}
	if xorbRangeHeaderSent == "" {
		t.Error("Expected Range header to be sent to xorb download endpoint")
	}

	t.Logf("Range header sent to reconstruction: %s", rangeHeaderSent)
	t.Logf("Range header sent to xorb download: %s", xorbRangeHeaderSent)
}

func TestDownloadFileWithCache(t *testing.T) {
	// Create test data
	chunk1Data := []byte("Cached ")
	chunk2Data := []byte("Chunks!")
	expectedData := append(chunk1Data, chunk2Data...)

	xorbHash, xorbSerialized, _ := createTestXorb(t, [][]byte{chunk1Data, chunk2Data})

	// Create test server
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/reconstructions/"+xorbHash.String() {
			resp := ReconstructionResponse{
				OffsetIntoFirstRange: 0,
				Terms: []Term{
					{
						Hash:           xorbHash.String(),
						UnpackedLength: uint64(len(expectedData)),
						Range:          ChunkRange{Start: 0, End: 2},
					},
				},
				FetchInfo: map[string][]FetchInfoEntry{
					xorbHash.String(): {
						{
							Range:    ChunkRange{Start: 0, End: 2},
							URL:      server.URL + "/xorbs/" + xorbHash.String(),
							URLRange: ByteRange{Start: 0, End: 0},
						},
					},
				},
			}
			json.NewEncoder(w).Encode(resp)
		} else if r.URL.Path == "/xorbs/"+xorbHash.String() {
			w.Write(xorbSerialized)
		} else {
			http.Error(w, "Not found", http.StatusNotFound)
		}
	}))
	defer server.Close()

	// Create client and session with caching enabled
	c := NewClient(ClientOptions{
		BaseURL: server.URL,
	})
	session := NewDownloadSession(DownloadSessionOptions{
		Client:        c,
		EnableCaching: true,
	})

	// Download file
	reader, _, err := session.DownloadFileV1(context.Background(), xorbHash)
	if err != nil {
		t.Fatalf("DownloadFile failed: %v", err)
	}

	// Read downloaded data
	downloadedData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read downloaded data: %v", err)
	}

	// Verify data
	if !bytes.Equal(downloadedData, expectedData) {
		t.Errorf("Downloaded data mismatch: got %q, want %q", downloadedData, expectedData)
	}

	// Verify cache was populated
	if len(session.chunkCache) != 2 {
		t.Errorf("Expected 2 chunks in cache, got %d", len(session.chunkCache))
	}

	// Verify cached chunk data
	chunk1Hash := xet.ChunkBytes(chunk1Data).Hash()
	chunk2Hash := xet.ChunkBytes(chunk2Data).Hash()

	cachedChunk1, ok := session.chunkCache[chunk1Hash]
	if !ok {
		t.Error("Chunk 1 not found in cache")
	} else if !bytes.Equal(cachedChunk1, chunk1Data) {
		t.Errorf("Cached chunk 1 data mismatch")
	}

	cachedChunk2, ok := session.chunkCache[chunk2Hash]
	if !ok {
		t.Error("Chunk 2 not found in cache")
	} else if !bytes.Equal(cachedChunk2, chunk2Data) {
		t.Errorf("Cached chunk 2 data mismatch")
	}
}

func TestDownloadFileWithOffset(t *testing.T) {
	// Create test data
	chunk1Data := []byte("AAAAAAA") // 7 bytes
	chunk2Data := []byte("BBBBBBB") // 7 bytes
	allData := append(chunk1Data, chunk2Data...)

	xorbHash, xorbSerialized, _ := createTestXorb(t, [][]byte{chunk1Data, chunk2Data})

	// Test downloading with offset into first chunk
	offset := int64(3)
	expectedData := allData[offset:]

	// Create test server
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/reconstructions/"+xorbHash.String() {
			resp := ReconstructionResponse{
				OffsetIntoFirstRange: offset,
				Terms: []Term{
					{
						Hash:           xorbHash.String(),
						UnpackedLength: uint64(len(allData)),
						Range:          ChunkRange{Start: 0, End: 2},
					},
				},
				FetchInfo: map[string][]FetchInfoEntry{
					xorbHash.String(): {
						{
							Range:    ChunkRange{Start: 0, End: 2},
							URL:      server.URL + "/xorbs/" + xorbHash.String(),
							URLRange: ByteRange{Start: 0, End: 0},
						},
					},
				},
			}
			json.NewEncoder(w).Encode(resp)
		} else if r.URL.Path == "/xorbs/"+xorbHash.String() {
			w.Write(xorbSerialized)
		} else {
			http.Error(w, "Not found", http.StatusNotFound)
		}
	}))
	defer server.Close()

	// Create client and session
	c := NewClient(ClientOptions{
		BaseURL: server.URL,
	})
	session := NewDownloadSession(DownloadSessionOptions{
		Client: c,
	})

	// Download file
	reader, _, err := session.DownloadFileV1(context.Background(), xorbHash)
	if err != nil {
		t.Fatalf("DownloadFile failed: %v", err)
	}

	// Read downloaded data
	downloadedData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read downloaded data: %v", err)
	}

	// Verify data
	if !bytes.Equal(downloadedData, expectedData) {
		t.Errorf("Downloaded data with offset mismatch: got %q (len=%d), want %q (len=%d)",
			downloadedData, len(downloadedData), expectedData, len(expectedData))
	}
}

func TestDownloadFileWithLength(t *testing.T) {
	chunk1Data := []byte("Hello, ")
	chunk2Data := []byte("World!")
	expectedData := append(chunk1Data, chunk2Data...)

	xorbHash, xorbSerialized, _ := createTestXorb(t, [][]byte{chunk1Data, chunk2Data})

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/reconstructions/"+xorbHash.String() {
			resp := ReconstructionResponse{
				OffsetIntoFirstRange: 0,
				Terms: []Term{
					{
						Hash:           xorbHash.String(),
						UnpackedLength: uint64(len(expectedData)),
						Range:          ChunkRange{Start: 0, End: 2},
					},
				},
				FetchInfo: map[string][]FetchInfoEntry{
					xorbHash.String(): {
						{
							Range:    ChunkRange{Start: 0, End: 2},
							URL:      server.URL + "/xorbs/" + xorbHash.String(),
							URLRange: ByteRange{Start: 0, End: 0},
						},
					},
				},
			}
			json.NewEncoder(w).Encode(resp)
		} else if r.URL.Path == "/xorbs/"+xorbHash.String() {
			w.Write(xorbSerialized)
		} else {
			http.Error(w, "Not found", http.StatusNotFound)
		}
	}))
	defer server.Close()

	c := NewClient(ClientOptions{
		BaseURL: server.URL,
	})
	session := NewDownloadSession(DownloadSessionOptions{
		Client: c,
	})

	reader, length, err := session.DownloadFileV1(context.Background(), xorbHash)
	if err != nil {
		t.Fatalf("DownloadFileWithLength failed: %v", err)
	}

	if length != int64(len(expectedData)) {
		t.Fatalf("Expected length %d, got %d", len(expectedData), length)
	}

	downloadedData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read downloaded data: %v", err)
	}

	if !bytes.Equal(downloadedData, expectedData) {
		t.Errorf("Downloaded data mismatch: got %q, want %q", downloadedData, expectedData)
	}
}

func TestDownloadFileRangeWithLength(t *testing.T) {
	chunk1Data := []byte("AAAAAAA") // 7 bytes
	chunk2Data := []byte("BBBBBBB") // 7 bytes
	chunk3Data := []byte("CCCCCCC") // 7 bytes
	allData := append(append(chunk1Data, chunk2Data...), chunk3Data...)

	rangeStart := int64(7)
	rangeEnd := int64(13)
	expectedRangeData := allData[rangeStart : rangeEnd+1]

	xorbHash, xorbSerialized, _ := createTestXorb(t, [][]byte{chunk1Data, chunk2Data, chunk3Data})

	var rangeHeaderSent string
	var xorbRangeHeaderSent string

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/reconstructions/"+xorbHash.String() {
			rangeHeaderSent = r.Header.Get("Range")
			resp := ReconstructionResponse{
				OffsetIntoFirstRange: 0,
				Terms: []Term{
					{
						Hash:           xorbHash.String(),
						UnpackedLength: uint64(len(chunk2Data)),
						Range:          ChunkRange{Start: 0, End: 1},
					},
				},
				FetchInfo: map[string][]FetchInfoEntry{
					xorbHash.String(): {
						{
							Range:    ChunkRange{Start: 0, End: 1},
							URL:      server.URL + "/xorbs/" + xorbHash.String(),
							URLRange: ByteRange{Start: 100, End: 200},
						},
					},
				},
			}
			w.WriteHeader(http.StatusPartialContent)
			json.NewEncoder(w).Encode(resp)
		} else if r.URL.Path == "/xorbs/"+xorbHash.String() {
			xorbRangeHeaderSent = r.Header.Get("Range")

			if xorbRangeHeaderSent != "" {
				w.WriteHeader(http.StatusPartialContent)

				tempXorb := xorb.NewXorb()
				chunk := xet.ChunkBytes(chunk2Data)
				tempXorb.AddChunk(chunk)
				tempChunksOnly, _ := xorb.SerializeBytes(tempXorb, true)
				w.Write(tempChunksOnly)
			} else {
				w.Write(xorbSerialized)
			}
		} else {
			http.Error(w, "Not found", http.StatusNotFound)
		}
	}))
	defer server.Close()

	c := NewClient(ClientOptions{
		BaseURL: server.URL,
	})
	session := NewDownloadSession(DownloadSessionOptions{
		Client: c,
	})

	reader, length, err := session.DownloadFileV1(context.Background(), xorbHash, WithRange(rangeStart, rangeEnd))
	if err != nil {
		t.Fatalf("DownloadFile failed: %v", err)
	}

	if length != int64(len(expectedRangeData)) {
		t.Fatalf("Expected range length %d, got %d", len(expectedRangeData), length)
	}

	downloadedData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read downloaded data: %v", err)
	}

	if !bytes.Equal(downloadedData, expectedRangeData) {
		t.Errorf("Downloaded range data mismatch: got %q (len=%d), want %q (len=%d)",
			downloadedData, len(downloadedData), expectedRangeData, len(expectedRangeData))
	}

	if rangeHeaderSent == "" {
		t.Error("Expected Range header to be sent to reconstruction endpoint")
	}
	if xorbRangeHeaderSent == "" {
		t.Error("Expected Range header to be sent to xorb download endpoint")
	}
}

func TestDownloadFileV2(t *testing.T) {
	chunk1Data := []byte("Hello, ")
	chunk2Data := []byte("World!")
	expectedData := append(chunk1Data, chunk2Data...)

	xorbHash, xorbSerialized, _ := createTestXorb(t, [][]byte{chunk1Data, chunk2Data})

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v2/reconstructions/"+xorbHash.String() {
			resp := ReconstructionResponseV2{
				OffsetIntoFirstRange: 0,
				Terms: []Term{
					{
						Hash:           xorbHash.String(),
						UnpackedLength: uint64(len(expectedData)),
						Range:          ChunkRange{Start: 0, End: 2},
					},
				},
				Xorbs: map[string][]XorbMultiRangeFetch{
					xorbHash.String(): {
						{
							URL: server.URL + "/xorbs/" + xorbHash.String(),
							Ranges: []XorbRangeDescriptor{
								{
									Chunks: ChunkRange{Start: 0, End: 2},
									Bytes:  ByteRange{Start: 0, End: 0}, // Zero indicates full download
								},
							},
						},
					},
				},
			}
			json.NewEncoder(w).Encode(resp)
		} else if r.URL.Path == "/xorbs/"+xorbHash.String() {
			w.Write(xorbSerialized)
		} else {
			http.Error(w, "Not found", http.StatusNotFound)
		}
	}))
	defer server.Close()

	c := NewClient(ClientOptions{
		BaseURL: server.URL,
	})
	session := NewDownloadSession(DownloadSessionOptions{
		Client: c,
	})

	reader, _, err := session.DownloadFileV2(context.Background(), xorbHash)
	if err != nil {
		t.Fatalf("DownloadFileV2 failed: %v", err)
	}

	downloadedData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read downloaded data: %v", err)
	}

	if !bytes.Equal(downloadedData, expectedData) {
		t.Errorf("Downloaded data mismatch: got %q, want %q", downloadedData, expectedData)
	}
}

func TestDownloadFileWithLengthV2(t *testing.T) {
	chunk1Data := []byte("Hello, ")
	chunk2Data := []byte("World!")
	expectedData := append(chunk1Data, chunk2Data...)

	xorbHash, xorbSerialized, _ := createTestXorb(t, [][]byte{chunk1Data, chunk2Data})

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v2/reconstructions/"+xorbHash.String() {
			resp := ReconstructionResponseV2{
				OffsetIntoFirstRange: 0,
				Terms: []Term{
					{
						Hash:           xorbHash.String(),
						UnpackedLength: uint64(len(expectedData)),
						Range:          ChunkRange{Start: 0, End: 2},
					},
				},
				Xorbs: map[string][]XorbMultiRangeFetch{
					xorbHash.String(): {
						{
							URL: server.URL + "/xorbs/" + xorbHash.String(),
							Ranges: []XorbRangeDescriptor{
								{
									Chunks: ChunkRange{Start: 0, End: 2},
									Bytes:  ByteRange{Start: 0, End: 0},
								},
							},
						},
					},
				},
			}
			json.NewEncoder(w).Encode(resp)
		} else if r.URL.Path == "/xorbs/"+xorbHash.String() {
			w.Write(xorbSerialized)
		} else {
			http.Error(w, "Not found", http.StatusNotFound)
		}
	}))
	defer server.Close()

	c := NewClient(ClientOptions{
		BaseURL: server.URL,
	})
	session := NewDownloadSession(DownloadSessionOptions{
		Client: c,
	})

	reader, length, err := session.DownloadFileV2(context.Background(), xorbHash)
	if err != nil {
		t.Fatalf("DownloadFileWithLengthV2 failed: %v", err)
	}

	if length != int64(len(expectedData)) {
		t.Fatalf("Expected length %d, got %d", len(expectedData), length)
	}

	downloadedData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read downloaded data: %v", err)
	}

	if !bytes.Equal(downloadedData, expectedData) {
		t.Errorf("Downloaded data mismatch: got %q, want %q", downloadedData, expectedData)
	}
}

func TestDownloadFileRangeV2(t *testing.T) {
	chunk1Data := []byte("AAAAAAA") // 7 bytes
	chunk2Data := []byte("BBBBBBB") // 7 bytes
	chunk3Data := []byte("CCCCCCC") // 7 bytes
	allData := append(append(chunk1Data, chunk2Data...), chunk3Data...)

	rangeStart := int64(7)
	rangeEnd := int64(13)
	expectedRangeData := allData[rangeStart : rangeEnd+1]

	xorbHash, _, _ := createTestXorb(t, [][]byte{chunk1Data, chunk2Data, chunk3Data})

	var rangeHeaderSent string
	var xorbRangeHeaderSent string

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v2/reconstructions/"+xorbHash.String() {
			rangeHeaderSent = r.Header.Get("Range")
			resp := ReconstructionResponseV2{
				OffsetIntoFirstRange: 0,
				Terms: []Term{
					{
						Hash:           xorbHash.String(),
						UnpackedLength: uint64(len(chunk2Data)),
						Range:          ChunkRange{Start: 0, End: 1},
					},
				},
				Xorbs: map[string][]XorbMultiRangeFetch{
					xorbHash.String(): {
						{
							URL: server.URL + "/xorbs/" + xorbHash.String(),
							Ranges: []XorbRangeDescriptor{
								{
									Chunks: ChunkRange{Start: 0, End: 1},
									Bytes:  ByteRange{Start: 100, End: 200},
								},
							},
						},
					},
				},
			}
			w.WriteHeader(http.StatusPartialContent)
			json.NewEncoder(w).Encode(resp)
		} else if r.URL.Path == "/xorbs/"+xorbHash.String() {
			xorbRangeHeaderSent = r.Header.Get("Range")
			if xorbRangeHeaderSent != "" {
				w.WriteHeader(http.StatusPartialContent)
				tempXorb := xorb.NewXorb()
				chunk := xet.ChunkBytes(chunk2Data)
				tempXorb.AddChunk(chunk)
				tempChunksOnly, _ := xorb.SerializeBytes(tempXorb, true)
				w.Write(tempChunksOnly)
			} else {
				http.Error(w, "Not found", http.StatusNotFound)
			}
		} else {
			http.Error(w, "Not found", http.StatusNotFound)
		}
	}))
	defer server.Close()

	c := NewClient(ClientOptions{
		BaseURL: server.URL,
	})
	session := NewDownloadSession(DownloadSessionOptions{
		Client: c,
	})

	reader, _, err := session.DownloadFileV2(context.Background(), xorbHash, WithRange(rangeStart, rangeEnd))
	if err != nil {
		t.Fatalf("DownloadFileRangeV2 failed: %v", err)
	}

	downloadedData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read downloaded data: %v", err)
	}

	if !bytes.Equal(downloadedData, expectedRangeData) {
		t.Errorf("Downloaded range data mismatch: got %q (len=%d), want %q (len=%d)",
			downloadedData, len(downloadedData), expectedRangeData, len(expectedRangeData))
	}

	if rangeHeaderSent == "" {
		t.Error("Expected Range header to be sent to reconstruction endpoint")
	}
	if xorbRangeHeaderSent == "" {
		t.Error("Expected Range header to be sent to xorb download endpoint")
	}
}

func TestDownloadFileRangeWithLengthV2(t *testing.T) {
	chunk1Data := []byte("AAAAAAA") // 7 bytes
	chunk2Data := []byte("BBBBBBB") // 7 bytes
	chunk3Data := []byte("CCCCCCC") // 7 bytes
	allData := append(append(chunk1Data, chunk2Data...), chunk3Data...)

	rangeStart := int64(7)
	rangeEnd := int64(13)
	expectedRangeData := allData[rangeStart : rangeEnd+1]

	xorbHash, _, _ := createTestXorb(t, [][]byte{chunk1Data, chunk2Data, chunk3Data})

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v2/reconstructions/"+xorbHash.String() {
			resp := ReconstructionResponseV2{
				OffsetIntoFirstRange: 0,
				Terms: []Term{
					{
						Hash:           xorbHash.String(),
						UnpackedLength: uint64(len(chunk2Data)),
						Range:          ChunkRange{Start: 0, End: 1},
					},
				},
				Xorbs: map[string][]XorbMultiRangeFetch{
					xorbHash.String(): {
						{
							URL: server.URL + "/xorbs/" + xorbHash.String(),
							Ranges: []XorbRangeDescriptor{
								{
									Chunks: ChunkRange{Start: 0, End: 1},
									Bytes:  ByteRange{Start: 100, End: 200},
								},
							},
						},
					},
				},
			}
			w.WriteHeader(http.StatusPartialContent)
			json.NewEncoder(w).Encode(resp)
		} else if r.URL.Path == "/xorbs/"+xorbHash.String() {
			rangeHeader := r.Header.Get("Range")
			if rangeHeader != "" {
				w.WriteHeader(http.StatusPartialContent)
				tempXorb := xorb.NewXorb()
				chunk := xet.ChunkBytes(chunk2Data)
				tempXorb.AddChunk(chunk)
				tempChunksOnly, _ := xorb.SerializeBytes(tempXorb, true)
				w.Write(tempChunksOnly)
			} else {
				http.Error(w, "Not found", http.StatusNotFound)
			}
		} else {
			http.Error(w, "Not found", http.StatusNotFound)
		}
	}))
	defer server.Close()

	c := NewClient(ClientOptions{
		BaseURL: server.URL,
	})
	session := NewDownloadSession(DownloadSessionOptions{
		Client: c,
	})

	reader, length, err := session.DownloadFileV2(context.Background(), xorbHash, WithRange(rangeStart, rangeEnd))
	if err != nil {
		t.Fatalf("DownloadFileV2 failed: %v", err)
	}

	if length != int64(len(expectedRangeData)) {
		t.Fatalf("Expected range length %d, got %d", len(expectedRangeData), length)
	}

	downloadedData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read downloaded data: %v", err)
	}

	if !bytes.Equal(downloadedData, expectedRangeData) {
		t.Errorf("Downloaded range data mismatch: got %q (len=%d), want %q (len=%d)",
			downloadedData, len(downloadedData), expectedRangeData, len(expectedRangeData))
	}
}

func TestDownloadFileV2WithOffset(t *testing.T) {
	chunk1Data := []byte("AAAAAAA") // 7 bytes
	chunk2Data := []byte("BBBBBBB") // 7 bytes
	allData := append(chunk1Data, chunk2Data...)

	offset := int64(3)
	expectedData := allData[offset:]

	xorbHash, xorbSerialized, _ := createTestXorb(t, [][]byte{chunk1Data, chunk2Data})

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v2/reconstructions/"+xorbHash.String() {
			resp := ReconstructionResponseV2{
				OffsetIntoFirstRange: offset,
				Terms: []Term{
					{
						Hash:           xorbHash.String(),
						UnpackedLength: uint64(len(allData)),
						Range:          ChunkRange{Start: 0, End: 2},
					},
				},
				Xorbs: map[string][]XorbMultiRangeFetch{
					xorbHash.String(): {
						{
							URL: server.URL + "/xorbs/" + xorbHash.String(),
							Ranges: []XorbRangeDescriptor{
								{
									Chunks: ChunkRange{Start: 0, End: 2},
									Bytes:  ByteRange{Start: 0, End: 0},
								},
							},
						},
					},
				},
			}
			json.NewEncoder(w).Encode(resp)
		} else if r.URL.Path == "/xorbs/"+xorbHash.String() {
			w.Write(xorbSerialized)
		} else {
			http.Error(w, "Not found", http.StatusNotFound)
		}
	}))
	defer server.Close()

	c := NewClient(ClientOptions{
		BaseURL: server.URL,
	})
	session := NewDownloadSession(DownloadSessionOptions{
		Client: c,
	})

	reader, length, err := session.DownloadFileV2(context.Background(), xorbHash)
	if err != nil {
		t.Fatalf("DownloadFileV2 failed: %v", err)
	}

	if length != int64(len(expectedData)) {
		t.Fatalf("Expected length %d, got %d", len(expectedData), length)
	}

	downloadedData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read downloaded data: %v", err)
	}

	if !bytes.Equal(downloadedData, expectedData) {
		t.Errorf("Downloaded data with offset mismatch: got %q (len=%d), want %q (len=%d)",
			downloadedData, len(downloadedData), expectedData, len(expectedData))
	}
}

func TestDownloadFileV2WithCache(t *testing.T) {
	chunk1Data := []byte("Cached ")
	chunk2Data := []byte("Chunks!")
	expectedData := append(chunk1Data, chunk2Data...)

	xorbHash, xorbSerialized, _ := createTestXorb(t, [][]byte{chunk1Data, chunk2Data})

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v2/reconstructions/"+xorbHash.String() {
			resp := ReconstructionResponseV2{
				OffsetIntoFirstRange: 0,
				Terms: []Term{
					{
						Hash:           xorbHash.String(),
						UnpackedLength: uint64(len(expectedData)),
						Range:          ChunkRange{Start: 0, End: 2},
					},
				},
				Xorbs: map[string][]XorbMultiRangeFetch{
					xorbHash.String(): {
						{
							URL: server.URL + "/xorbs/" + xorbHash.String(),
							Ranges: []XorbRangeDescriptor{
								{
									Chunks: ChunkRange{Start: 0, End: 2},
									Bytes:  ByteRange{Start: 0, End: 0},
								},
							},
						},
					},
				},
			}
			json.NewEncoder(w).Encode(resp)
		} else if r.URL.Path == "/xorbs/"+xorbHash.String() {
			w.Write(xorbSerialized)
		} else {
			http.Error(w, "Not found", http.StatusNotFound)
		}
	}))
	defer server.Close()

	c := NewClient(ClientOptions{
		BaseURL: server.URL,
	})
	session := NewDownloadSession(DownloadSessionOptions{
		Client:        c,
		EnableCaching: true,
	})

	reader, _, err := session.DownloadFileV2(context.Background(), xorbHash)
	if err != nil {
		t.Fatalf("DownloadFileV2 failed: %v", err)
	}

	downloadedData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read downloaded data: %v", err)
	}

	if !bytes.Equal(downloadedData, expectedData) {
		t.Errorf("Downloaded data mismatch: got %q, want %q", downloadedData, expectedData)
	}

	if len(session.chunkCache) != 2 {
		t.Errorf("Expected 2 chunks in cache, got %d", len(session.chunkCache))
	}

	chunk1Hash := xet.ChunkBytes(chunk1Data).Hash()
	chunk2Hash := xet.ChunkBytes(chunk2Data).Hash()

	cachedChunk1, ok := session.chunkCache[chunk1Hash]
	if !ok {
		t.Error("Chunk 1 not found in cache")
	} else if !bytes.Equal(cachedChunk1, chunk1Data) {
		t.Errorf("Cached chunk 1 data mismatch")
	}

	cachedChunk2, ok := session.chunkCache[chunk2Hash]
	if !ok {
		t.Error("Chunk 2 not found in cache")
	} else if !bytes.Equal(cachedChunk2, chunk2Data) {
		t.Errorf("Cached chunk 2 data mismatch")
	}
}

func TestDownloadFileWithOffsetLength(t *testing.T) {
	chunk1Data := []byte("AAAAAAA") // 7 bytes
	chunk2Data := []byte("BBBBBBB") // 7 bytes
	allData := append(chunk1Data, chunk2Data...)

	offset := int64(3)
	expectedData := allData[offset:]

	xorbHash, xorbSerialized, _ := createTestXorb(t, [][]byte{chunk1Data, chunk2Data})

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/reconstructions/"+xorbHash.String() {
			resp := ReconstructionResponse{
				OffsetIntoFirstRange: offset,
				Terms: []Term{
					{
						Hash:           xorbHash.String(),
						UnpackedLength: uint64(len(allData)),
						Range:          ChunkRange{Start: 0, End: 2},
					},
				},
				FetchInfo: map[string][]FetchInfoEntry{
					xorbHash.String(): {
						{
							Range:    ChunkRange{Start: 0, End: 2},
							URL:      server.URL + "/xorbs/" + xorbHash.String(),
							URLRange: ByteRange{Start: 0, End: 0},
						},
					},
				},
			}
			json.NewEncoder(w).Encode(resp)
		} else if r.URL.Path == "/xorbs/"+xorbHash.String() {
			w.Write(xorbSerialized)
		} else {
			http.Error(w, "Not found", http.StatusNotFound)
		}
	}))
	defer server.Close()

	c := NewClient(ClientOptions{
		BaseURL: server.URL,
	})
	session := NewDownloadSession(DownloadSessionOptions{
		Client: c,
	})

	reader, length, err := session.DownloadFileV1(context.Background(), xorbHash)
	if err != nil {
		t.Fatalf("DownloadFileWithLength failed: %v", err)
	}

	if length != int64(len(expectedData)) {
		t.Fatalf("Expected length %d, got %d", len(expectedData), length)
	}

	downloadedData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read downloaded data: %v", err)
	}

	if !bytes.Equal(downloadedData, expectedData) {
		t.Errorf("Downloaded data with offset mismatch: got %q (len=%d), want %q (len=%d)",
			downloadedData, len(downloadedData), expectedData, len(expectedData))
	}
}
