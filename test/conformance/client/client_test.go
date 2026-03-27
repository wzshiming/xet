package client_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/wzshiming/xet"
	xetgo "github.com/wzshiming/xet-go"
	"github.com/wzshiming/xet/pkg/client"
	"github.com/wzshiming/xet/pkg/client/download"
	"github.com/wzshiming/xet/pkg/client/upload"
	"github.com/wzshiming/xet/pkg/server"
	"github.com/wzshiming/xet/pkg/xorb"
)

// RequestRecord captures details of an HTTP request
type RequestRecord struct {
	Method      string
	Path        string
	Headers     http.Header
	Body        []byte
	ClientType  string // "xet-go" or "native"
	RequestID   string // Unique identifier for matching corresponding requests
}

// RecordingProxy is an HTTP proxy that records all requests
type RecordingProxy struct {
	backend  http.Handler
	mu       sync.Mutex
	requests []RequestRecord
}

// NewRecordingProxy creates a new recording proxy
func NewRecordingProxy(backend http.Handler) *RecordingProxy {
	return &RecordingProxy{
		backend:  backend,
		requests: make([]RequestRecord, 0),
	}
}

// ServeHTTP implements http.Handler
func (p *RecordingProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Read the body
	var bodyBytes []byte
	var err error
	if r.Body != nil {
		bodyBytes, err = io.ReadAll(r.Body)
		r.Body.Close()
		if err != nil {
			// Surface read failures deterministically instead of forwarding
			http.Error(w, "failed to read request body", http.StatusBadGateway)
			return
		}
		// Restore the body for the backend
		r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	}

	// Determine client type from User-Agent or custom header
	clientType := "unknown"
	if userAgent := r.Header.Get("User-Agent"); userAgent != "" {
		if strings.Contains(userAgent, "xet-go") {
			clientType = "xet-go"
		} else {
			clientType = "native"
		}
	}
	// Allow explicit client type header for testing
	if ct := r.Header.Get("X-Client-Type"); ct != "" {
		clientType = ct
	}

	// Generate request ID based on method and path
	requestID := fmt.Sprintf("%s:%s", r.Method, r.URL.Path)

	// Record the request
	p.mu.Lock()
	p.requests = append(p.requests, RequestRecord{
		Method:     r.Method,
		Path:       r.URL.Path,
		Headers:    r.Header.Clone(),
		Body:       append([]byte{}, bodyBytes...), // Copy the body
		ClientType: clientType,
		RequestID:  requestID,
	})
	p.mu.Unlock()

	// Forward to backend
	p.backend.ServeHTTP(w, r)
}

// GetRequests returns all recorded requests
func (p *RecordingProxy) GetRequests() []RequestRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]RequestRecord{}, p.requests...)
}

// ClearRequests clears all recorded requests
func (p *RecordingProxy) ClearRequests() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.requests = make([]RequestRecord, 0)
}

// TestClientUploadDownloadRequestConformance tests that xet-go and native clients
// generate compatible HTTP requests for upload and download operations
func TestClientUploadDownloadRequestConformance(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "Hello World",
			data: []byte("Hello World!"),
		},
		{
			name: "1KB",
			data: makeBinaryData(1024),
		},
		{
			name: "10KB",
			data: makeBinaryData(10 * 1024),
		},
		{
			name: "100KB",
			data: makeBinaryData(100 * 1024),
		},
		{
			name: "1MB",
			data: makeBinaryData(1024 * 1024),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("upload_conformance", func(t *testing.T) {
				// Create separate temp directories for each client's storage
				xetgoStorageDir := t.TempDir()
				nativeStorageDir := t.TempDir()

				// Setup server for xet-go
				var xetgoStorage server.Storage
				var xetgoSrv *server.Server
				var xetgoProxy *RecordingProxy
				var xetgoHttpSrv *httptest.Server

				xetgoHttpSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if xetgoProxy != nil {
						xetgoProxy.ServeHTTP(w, r)
					} else {
						http.Error(w, "server not initialized", http.StatusInternalServerError)
					}
				}))
				defer xetgoHttpSrv.Close()

				var err error
				xetgoStorage, err = server.NewFileStorage(server.FileStorageOptions{
					BasePath: xetgoStorageDir,
					BaseURL:  xetgoHttpSrv.URL,
				})
				if err != nil {
					t.Fatalf("Failed to create xet-go storage: %v", err)
				}

				xetgoSrv = server.NewServer(server.ServerOptions{
					Storage: xetgoStorage,
				})
				xetgoProxy = NewRecordingProxy(xetgoSrv)

				// Upload with xet-go
				tempDir := t.TempDir()
				xetgoFile := filepath.Join(tempDir, "xetgo-upload.bin")
				if err := os.WriteFile(xetgoFile, tt.data, 0644); err != nil {
					t.Fatalf("Failed to write xet-go upload file: %v", err)
				}

				uploadResults, err := xetgo.UploadFiles(
					[]string{xetgoFile},
					xetgoHttpSrv.URL,
					nil,   // token
					nil,   // sha256s (computed automatically)
					false, // skipSHA256
				)
				if err != nil {
					t.Fatalf("Failed to upload file with xet-go: %v", err)
				}

				if len(uploadResults) != 1 {
					t.Fatalf("Expected 1 upload result, got %d", len(uploadResults))
				}

				xetgoRequests := xetgoProxy.GetRequests()

				// Setup server for native client
				var nativeStorage server.Storage
				var nativeSrv *server.Server
				var nativeProxy *RecordingProxy
				var nativeHttpSrv *httptest.Server

				nativeHttpSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if nativeProxy != nil {
						nativeProxy.ServeHTTP(w, r)
					} else {
						http.Error(w, "server not initialized", http.StatusInternalServerError)
					}
				}))
				defer nativeHttpSrv.Close()

				nativeStorage, err = server.NewFileStorage(server.FileStorageOptions{
					BasePath: nativeStorageDir,
					BaseURL:  nativeHttpSrv.URL,
				})
				if err != nil {
					t.Fatalf("Failed to create native storage: %v", err)
				}

				nativeSrv = server.NewServer(server.ServerOptions{
					Storage: nativeStorage,
				})
				nativeProxy = NewRecordingProxy(nativeSrv)

				// Upload with native client
				nativeClient := client.NewClient(client.ClientOptions{
					BaseURL:   nativeHttpSrv.URL,
					Namespace: "default",
				})

				nativeFile := filepath.Join(tempDir, "native-upload.bin")
				if err := os.WriteFile(nativeFile, tt.data, 0644); err != nil {
					t.Fatalf("Failed to write native upload file: %v", err)
				}

				f, err := os.Open(nativeFile)
				if err != nil {
					t.Fatalf("Failed to open native upload file: %v", err)
				}
				defer func() {
					if err := f.Close(); err != nil {
						t.Fatalf("Failed to close native upload file: %v", err)
					}
				}()

				uploadSession := upload.NewSession(upload.SessionOptions{
					Client: nativeClient,
				})
				fileHashes, err := uploadSession.UploadFiles(context.Background(), f)
				if err != nil {
					t.Fatalf("Failed to upload file with native client: %v", err)
				}

				nativeRequests := nativeProxy.GetRequests()

				// Compare requests
				compareUploadRequests(t, xetgoRequests, nativeRequests, uploadResults[0].Hash, fileHashes[0].String())
			})

			t.Run("download_conformance", func(t *testing.T) {
				// Setup shared server
				storageDir := t.TempDir()
				var storage server.Storage
				var srv *server.Server
				var proxy *RecordingProxy
				var httpSrv *httptest.Server

				httpSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if proxy != nil {
						proxy.ServeHTTP(w, r)
					} else {
						http.Error(w, "server not initialized", http.StatusInternalServerError)
					}
				}))
				defer httpSrv.Close()

				var err error
				storage, err = server.NewFileStorage(server.FileStorageOptions{
					BasePath: storageDir,
					BaseURL:  httpSrv.URL,
				})
				if err != nil {
					t.Fatalf("Failed to create storage: %v", err)
				}

				srv = server.NewServer(server.ServerOptions{
					Storage: storage,
				})
				proxy = NewRecordingProxy(srv)

				// First upload file using native client
				nativeClient := client.NewClient(client.ClientOptions{
					BaseURL:   httpSrv.URL,
					Namespace: "default",
				})

				tempDir := t.TempDir()
				uploadFile := filepath.Join(tempDir, "upload.bin")
				if err := os.WriteFile(uploadFile, tt.data, 0644); err != nil {
					t.Fatalf("Failed to write upload file: %v", err)
				}

				f, err := os.Open(uploadFile)
				if err != nil {
					t.Fatalf("Failed to open upload file: %v", err)
				}

				uploadSession := upload.NewSession(upload.SessionOptions{
					Client: nativeClient,
				})
				fileHashes, err := uploadSession.UploadFiles(context.Background(), f)
				f.Close()
				if err != nil {
					t.Fatalf("Failed to upload file: %v", err)
				}

				fileHash := fileHashes[0]

				// Download with xet-go
				proxy.ClearRequests()
				xetgoDownloadFile := filepath.Join(tempDir, "xetgo-download.bin")
				downloadReq := []xetgo.DownloadRequest{
					{
						DestinationPath: xetgoDownloadFile,
						Hash:            fileHash.String(),
						FileSize:        int64(len(tt.data)),
					},
				}

				_, err = xetgo.DownloadFiles(downloadReq, httpSrv.URL, nil)
				if err != nil {
					t.Fatalf("Failed to download file with xet-go: %v", err)
				}

				xetgoRequests := proxy.GetRequests()

				// Download with native client
				proxy.ClearRequests()
				downloadSession := download.NewSession(download.SessionOptions{
					Client: nativeClient,
				})
				reader, err := downloadSession.DownloadFile(context.Background(), fileHash)
				if err != nil {
					t.Fatalf("Failed to download file with native client: %v", err)
				}

				nativeDownloadedData, err := io.ReadAll(reader)
				if err != nil {
					t.Fatalf("Failed to read downloaded data: %v", err)
				}

				nativeRequests := proxy.GetRequests()

				// Verify downloaded content matches
				xetgoDownloadedData, err := os.ReadFile(xetgoDownloadFile)
				if err != nil {
					t.Fatalf("Failed to read xet-go downloaded file: %v", err)
				}

				if !bytes.Equal(xetgoDownloadedData, tt.data) {
					t.Errorf("xet-go downloaded data mismatch: got %d bytes, want %d bytes", len(xetgoDownloadedData), len(tt.data))
				}

				if !bytes.Equal(nativeDownloadedData, tt.data) {
					t.Errorf("native downloaded data mismatch: got %d bytes, want %d bytes", len(nativeDownloadedData), len(tt.data))
				}

				// Compare download requests
				compareDownloadRequests(t, xetgoRequests, nativeRequests, fileHash.String())
			})
		})
	}
}

// compareUploadRequests compares HTTP requests from xet-go and native clients for uploads
func compareUploadRequests(t *testing.T, xetgoReqs, nativeReqs []RequestRecord, xetgoHash, nativeHash string) {
	t.Helper()

	// STRICT: Verify that file hashes match
	if xetgoHash != nativeHash {
		t.Errorf("File hash mismatch: xet-go=%s native=%s", xetgoHash, nativeHash)
		return
	}

	// Group requests by type
	xetgoByType := groupRequestsByType(xetgoReqs)
	nativeByType := groupRequestsByType(nativeReqs)

	// Get request types
	xetgoTypes := getSortedKeys(xetgoByType)
	nativeTypes := getSortedKeys(nativeByType)

	// Filter core requests (exclude dedup queries which are optional)
	coreXetgoTypes := filterCoreRequests(xetgoTypes)
	coreNativeTypes := filterCoreRequests(nativeTypes)

	// STRICT: Both clients must make core uploads (xorb + shard)
	if !equalStringSlices(coreXetgoTypes, coreNativeTypes) {
		t.Errorf("Core request type mismatch:\n  xet-go:  %v\n  native:  %v",
			coreXetgoTypes, coreNativeTypes)
	}

	// STRICT: Both must upload at least one xorb
	xetgoXorbCount := len(xetgoByType["POST:/v1/xorbs/default/{hash}"])
	nativeXorbCount := len(nativeByType["POST:/v1/xorbs/default/{hash}"])
	if xetgoXorbCount == 0 {
		t.Errorf("xet-go did not upload any xorbs")
	}
	if nativeXorbCount == 0 {
		t.Errorf("native client did not upload any xorbs")
	}

	// STRICT: Both must upload exactly one shard
	xetgoShardCount := len(xetgoByType["POST:/shards"]) + len(xetgoByType["POST:/v1/shards"])
	nativeShardCount := len(nativeByType["POST:/shards"]) + len(nativeByType["POST:/v1/shards"])
	if xetgoShardCount != 1 {
		t.Errorf("xet-go uploaded %d shards, expected exactly 1", xetgoShardCount)
	}
	if nativeShardCount != 1 {
		t.Errorf("native client uploaded %d shards, expected exactly 1", nativeShardCount)
	}

	// Symmetric validation: check for native-only request types
	for reqType := range nativeByType {
		if !strings.HasPrefix(reqType, "GET:/v1/chunks/") {
			if _, ok := xetgoByType[reqType]; !ok {
				t.Errorf("Request type %s present in native but not in xet-go", reqType)
			}
		}
	}

	// Compare xorb uploads - validate chunk content
	if strings.HasPrefix("POST:/v1/xorbs/", "POST:/v1/xorbs/") {
		xetgoXorbReqs := xetgoByType["POST:/v1/xorbs/default/{hash}"]
		nativeXorbReqs := nativeByType["POST:/v1/xorbs/default/{hash}"]
		compareXorbRequests(t, xetgoXorbReqs, nativeXorbReqs)
	}
}

// compareDownloadRequests compares HTTP requests from xet-go and native clients for downloads
func compareDownloadRequests(t *testing.T, xetgoReqs, nativeReqs []RequestRecord, fileHash string) {
	t.Helper()

	// Group requests by type
	xetgoByType := groupRequestsByType(xetgoReqs)
	nativeByType := groupRequestsByType(nativeReqs)

	// Get request types
	xetgoTypes := getSortedKeys(xetgoByType)
	nativeTypes := getSortedKeys(nativeByType)

	// STRICT: Both must query reconstruction (v1 or v2)
	xetgoReconCount := len(xetgoByType["GET:/v1/reconstructions/{hash}"]) + len(xetgoByType["GET:/v2/reconstructions/{hash}"])
	nativeReconCount := len(nativeByType["GET:/v1/reconstructions/{hash}"]) + len(nativeByType["GET:/v2/reconstructions/{hash}"])
	if xetgoReconCount == 0 {
		t.Errorf("xet-go did not query reconstruction")
	}
	if nativeReconCount == 0 {
		t.Errorf("native client did not query reconstruction")
	}

	// STRICT: Both must download xorb data
	xetgoXorbDownloadCount := len(xetgoByType["GET:/v1/xorbs/default/{hash}/data"])
	nativeXorbDownloadCount := len(nativeByType["GET:/v1/xorbs/default/{hash}/data"])
	if xetgoXorbDownloadCount == 0 {
		t.Errorf("xet-go did not download any xorb data")
	}
	if nativeXorbDownloadCount == 0 {
		t.Errorf("native client did not download any xorb data")
	}

	// Symmetric validation - allowing for v1/v2 API version differences
	for reqType := range nativeByType {
		// Check if this request type exists in xetgo, allowing v1/v2 API version differences
		if !hasEquivalentRequest(reqType, xetgoByType) {
			t.Errorf("Request type %s present in native but no equivalent in xet-go", reqType)
		}
	}
	for reqType := range xetgoByType {
		// Check if this request type exists in native, allowing v1/v2 API version differences
		if !hasEquivalentRequest(reqType, nativeByType) {
			t.Errorf("Request type %s present in xet-go but no equivalent in native", reqType)
		}
	}

	t.Logf("✓ Download conformance check passed for file %s", fileHash)
	t.Logf("  xet-go request types: %v", xetgoTypes)
	t.Logf("  native request types: %v", nativeTypes)
}

// hasEquivalentRequest checks if a request type or its v1/v2 equivalent exists in the map
func hasEquivalentRequest(reqType string, requests map[string][]RequestRecord) bool {
	if _, ok := requests[reqType]; ok {
		return true
	}

	// Check for v1/v2 API version differences
	if strings.Contains(reqType, "/v1/") {
		v2Type := strings.Replace(reqType, "/v1/", "/v2/", 1)
		if _, ok := requests[v2Type]; ok {
			return true
		}
	} else if strings.Contains(reqType, "/v2/") {
		v1Type := strings.Replace(reqType, "/v2/", "/v1/", 1)
		if _, ok := requests[v1Type]; ok {
			return true
		}
	}

	return false
}

// filterCoreRequests filters out deduplication queries to focus on core upload operations
func filterCoreRequests(types []string) []string {
	var result []string
	for _, t := range types {
		if !strings.HasPrefix(t, "GET:/v1/chunks/") {
			result = append(result, t)
		}
	}
	return result
}

// groupRequestsByType groups requests by method and path pattern
func groupRequestsByType(reqs []RequestRecord) map[string][]RequestRecord {
	result := make(map[string][]RequestRecord)
	for _, req := range reqs {
		// Normalize path for grouping
		reqType := fmt.Sprintf("%s:%s", req.Method, normalizePath(req.Path))
		result[reqType] = append(result[reqType], req)
	}
	return result
}

// normalizePath normalizes request paths by replacing hash values with placeholders
func normalizePath(path string) string {
	// Replace hash values in paths with {hash}
	parts := strings.Split(path, "/")
	for i, part := range parts {
		// If part looks like a hash (64 hex chars), replace with placeholder
		if len(part) == 64 && isHexString(part) {
			parts[i] = "{hash}"
		}
	}
	return strings.Join(parts, "/")
}

// isHexString checks if a string is a valid hex string
func isHexString(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

// getSortedKeys returns sorted keys from a map
func getSortedKeys(m map[string][]RequestRecord) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// equalStringSlices checks if two string slices are equal
func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// compareXorbRequests compares xorb upload requests by deserializing and comparing chunk content
func compareXorbRequests(t *testing.T, xetgoReqs, nativeReqs []RequestRecord) {
	t.Helper()

	// Collect all chunk hashes from both clients by deserializing xorbs
	xetgoChunkHashes := make(map[xet.Hash]bool)
	nativeChunkHashes := make(map[xet.Hash]bool)

	for _, req := range xetgoReqs {
		// Try to deserialize the xorb
		xorbObj, err := xorb.Deserialize(req.Body)
		if err != nil {
			// Try chunks-only format
			xorbObj, err = xorb.DeserializeChunksOnly(req.Body)
			if err != nil {
				t.Errorf("Failed to deserialize xet-go xorb: %v", err)
				continue
			}
		}

		// Collect chunk hashes
		for _, chunkHash := range xorbObj.ChunkHashes {
			xetgoChunkHashes[chunkHash] = true
		}
	}

	for _, req := range nativeReqs {
		// Try to deserialize the xorb
		xorbObj, err := xorb.Deserialize(req.Body)
		if err != nil {
			// Try chunks-only format
			xorbObj, err = xorb.DeserializeChunksOnly(req.Body)
			if err != nil {
				t.Errorf("Failed to deserialize native xorb: %v", err)
				continue
			}
		}

		// Collect chunk hashes
		for _, chunkHash := range xorbObj.ChunkHashes {
			nativeChunkHashes[chunkHash] = true
		}
	}

	// STRICT: Both clients must upload the same set of chunk hashes
	// (same chunking algorithm should produce same chunks)
	xetgoOnly := []xet.Hash{}
	for hash := range xetgoChunkHashes {
		if !nativeChunkHashes[hash] {
			xetgoOnly = append(xetgoOnly, hash)
		}
	}

	nativeOnly := []xet.Hash{}
	for hash := range nativeChunkHashes {
		if !xetgoChunkHashes[hash] {
			nativeOnly = append(nativeOnly, hash)
		}
	}

	if len(xetgoOnly) > 0 {
		t.Errorf("xet-go uploaded %d chunks that native did not: %v", len(xetgoOnly), xetgoOnly)
	}
	if len(nativeOnly) > 0 {
		t.Errorf("native uploaded %d chunks that xet-go did not: %v", len(nativeOnly), nativeOnly)
	}

	// Log success
	if len(xetgoOnly) == 0 && len(nativeOnly) == 0 {
		t.Logf("✓ Both clients uploaded identical chunk sets (%d chunks)", len(xetgoChunkHashes))
	}
}

var seed = rand.NewSource(0)

// makeBinaryData creates a deterministic byte sequence of the given size
func makeBinaryData(size int) []byte {
	result := make([]byte, size)
	for i := range result {
		result[i] = byte(seed.Int63() % 256)
	}
	return result
}
