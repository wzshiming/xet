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
	"github.com/wzshiming/xet/pkg/server"
	"github.com/wzshiming/xet/pkg/shard"
	"github.com/wzshiming/xet/pkg/xorb"
)

// RequestRecord captures details of an HTTP request
type RequestRecord struct {
	Method     string
	Path       string
	Headers    http.Header
	Body       []byte
	ClientType string // "xet-go" or "native"
	RequestID  string // Unique identifier for matching corresponding requests
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
		{
			name: "10MB",
			data: makeBinaryData(10 * 1024 * 1024),
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

				uploadSession := client.NewUploadSession(client.UploadSessionOptions{
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

				uploadSession := client.NewUploadSession(client.UploadSessionOptions{
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
				downloadSession := client.NewDownloadSession(client.DownloadSessionOptions{
					Client: nativeClient,
				})
				reader, _, err := downloadSession.DownloadFile(context.Background(), fileHash)
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
	xetgoXorbReqs := xetgoByType["POST:/v1/xorbs/default/{hash}"]
	nativeXorbReqs := nativeByType["POST:/v1/xorbs/default/{hash}"]
	compareXorbRequests(t, xetgoXorbReqs, nativeXorbReqs)

	// STRICT: Compare shard content
	xetgoShardReqs := append(xetgoByType["POST:/shards"], xetgoByType["POST:/v1/shards"]...)
	nativeShardReqs := append(nativeByType["POST:/shards"], nativeByType["POST:/v1/shards"]...)
	compareShardRequests(t, xetgoShardReqs, nativeShardReqs)
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

	// STRICT: Both must download the same number of xorbs
	if xetgoXorbDownloadCount != nativeXorbDownloadCount {
		t.Errorf("Xorb download count mismatch: xet-go=%d native=%d",
			xetgoXorbDownloadCount, nativeXorbDownloadCount)
	}

	// STRICT: Compare reconstruction query paths (must use same file hash)
	compareReconstructionPaths(t, xetgoReqs, nativeReqs, fileHash)

	// STRICT: Compare xorb download Range headers
	xetgoXorbReqs := xetgoByType["GET:/v1/xorbs/default/{hash}/data"]
	nativeXorbReqs := nativeByType["GET:/v1/xorbs/default/{hash}/data"]
	compareXorbDownloadRanges(t, xetgoXorbReqs, nativeXorbReqs)

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

	// STRICT: Both clients must upload the same number of xorbs
	if len(xetgoReqs) != len(nativeReqs) {
		t.Errorf("Xorb upload count mismatch: xet-go=%d native=%d", len(xetgoReqs), len(nativeReqs))
	}

	// Collect all chunk hashes from both clients by deserializing xorbs
	xetgoChunkHashes := make(map[xet.Hash]bool)
	nativeChunkHashes := make(map[xet.Hash]bool)

	// Also collect ordered chunk sequences per xorb for detailed comparison
	type xorbInfo struct {
		hash        string
		chunkHashes []xet.Hash
		chunkSizes  []int // uncompressed chunk sizes
	}
	var xetgoXorbs []xorbInfo
	var nativeXorbs []xorbInfo

	for _, req := range xetgoReqs {
		// Try to deserialize the xorb
		xorbObj, err := xorb.DeserializeBytes(req.Body, false)
		if err != nil {
			// Try chunks-only format
			xorbObj, err = xorb.DeserializeBytes(req.Body, true)
			if err != nil {
				t.Errorf("Failed to deserialize xet-go xorb: %v", err)
				continue
			}
		}

		info := xorbInfo{hash: req.Path}
		for i, chunkHash := range xorbObj.ChunkHashes {
			xetgoChunkHashes[chunkHash] = true
			info.chunkHashes = append(info.chunkHashes, chunkHash)
			if i < len(xorbObj.Chunks) {
				info.chunkSizes = append(info.chunkSizes, len(xorbObj.Chunks[i].UncompressedData))
			}
		}
		xetgoXorbs = append(xetgoXorbs, info)
	}

	for _, req := range nativeReqs {
		// Try to deserialize the xorb
		xorbObj, err := xorb.DeserializeBytes(req.Body, false)
		if err != nil {
			// Try chunks-only format
			xorbObj, err = xorb.DeserializeBytes(req.Body, true)
			if err != nil {
				t.Errorf("Failed to deserialize native xorb: %v", err)
				continue
			}
		}

		info := xorbInfo{hash: req.Path}
		for i, chunkHash := range xorbObj.ChunkHashes {
			nativeChunkHashes[chunkHash] = true
			info.chunkHashes = append(info.chunkHashes, chunkHash)
			if i < len(xorbObj.Chunks) {
				info.chunkSizes = append(info.chunkSizes, len(xorbObj.Chunks[i].UncompressedData))
			}
		}
		nativeXorbs = append(nativeXorbs, info)
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

	// STRICT: Compare chunk ordering within corresponding xorbs
	if len(xetgoXorbs) == len(nativeXorbs) {
		for i := range xetgoXorbs {
			if len(xetgoXorbs[i].chunkHashes) != len(nativeXorbs[i].chunkHashes) {
				t.Errorf("Xorb %d chunk count mismatch: xet-go=%d native=%d",
					i, len(xetgoXorbs[i].chunkHashes), len(nativeXorbs[i].chunkHashes))
				continue
			}
			for j := range xetgoXorbs[i].chunkHashes {
				if xetgoXorbs[i].chunkHashes[j] != nativeXorbs[i].chunkHashes[j] {
					t.Errorf("Xorb %d chunk %d hash mismatch: xet-go=%s native=%s",
						i, j, xetgoXorbs[i].chunkHashes[j], nativeXorbs[i].chunkHashes[j])
				}
			}
			// STRICT: Compare chunk sizes
			if len(xetgoXorbs[i].chunkSizes) != len(nativeXorbs[i].chunkSizes) {
				t.Errorf("Xorb %d chunk size count mismatch: xet-go=%d native=%d",
					i, len(xetgoXorbs[i].chunkSizes), len(nativeXorbs[i].chunkSizes))
			} else {
				for j := range xetgoXorbs[i].chunkSizes {
					if xetgoXorbs[i].chunkSizes[j] != nativeXorbs[i].chunkSizes[j] {
						t.Errorf("Xorb %d chunk %d size mismatch: xet-go=%d native=%d",
							i, j, xetgoXorbs[i].chunkSizes[j], nativeXorbs[i].chunkSizes[j])
					}
				}
			}
		}
	}

	// Log success
	if len(xetgoOnly) == 0 && len(nativeOnly) == 0 {
		t.Logf("✓ Both clients uploaded identical chunk sets (%d chunks)", len(xetgoChunkHashes))
	}
}

// compareShardRequests compares shard upload bodies between clients
func compareShardRequests(t *testing.T, xetgoReqs, nativeReqs []RequestRecord) {
	t.Helper()

	if len(xetgoReqs) == 0 || len(nativeReqs) == 0 {
		return // already reported by count checks
	}

	// Deserialize both shards
	xetgoShard, err := shard.Decode(bytes.NewReader(xetgoReqs[0].Body))
	if err != nil {
		t.Errorf("Failed to deserialize xet-go shard: %v", err)
		return
	}

	nativeShard, err := shard.Decode(bytes.NewReader(nativeReqs[0].Body))
	if err != nil {
		t.Errorf("Failed to deserialize native shard: %v", err)
		return
	}

	// STRICT: Both shards must have the same number of files
	if len(xetgoShard.Files) != len(nativeShard.Files) {
		t.Errorf("Shard file count mismatch: xet-go=%d native=%d",
			len(xetgoShard.Files), len(nativeShard.Files))
		return
	}

	// STRICT: Compare file blocks
	for i := range xetgoShard.Files {
		xetgoFile := xetgoShard.Files[i]
		nativeFile := nativeShard.Files[i]

		// STRICT: File hashes must match
		if xetgoFile.FileHash != nativeFile.FileHash {
			t.Errorf("Shard file %d hash mismatch: xet-go=%s native=%s",
				i, xetgoFile.FileHash, nativeFile.FileHash)
			continue
		}

		// STRICT: Same number of reconstruction entries
		if len(xetgoFile.Entries) != len(nativeFile.Entries) {
			t.Errorf("Shard file %d entry count mismatch: xet-go=%d native=%d",
				i, len(xetgoFile.Entries), len(nativeFile.Entries))
			continue
		}

		// STRICT: Compare reconstruction entries
		for j := range xetgoFile.Entries {
			xetgoEntry := xetgoFile.Entries[j]
			nativeEntry := nativeFile.Entries[j]

			if xetgoEntry.CASHash != nativeEntry.CASHash {
				t.Errorf("Shard file %d entry %d CASHash mismatch: xet-go=%s native=%s",
					i, j, xetgoEntry.CASHash, nativeEntry.CASHash)
			}
			if xetgoEntry.ChunkIndexStart != nativeEntry.ChunkIndexStart {
				t.Errorf("Shard file %d entry %d ChunkIndexStart mismatch: xet-go=%d native=%d",
					i, j, xetgoEntry.ChunkIndexStart, nativeEntry.ChunkIndexStart)
			}
			if xetgoEntry.ChunkIndexEnd != nativeEntry.ChunkIndexEnd {
				t.Errorf("Shard file %d entry %d ChunkIndexEnd mismatch: xet-go=%d native=%d",
					i, j, xetgoEntry.ChunkIndexEnd, nativeEntry.ChunkIndexEnd)
			}
			if xetgoEntry.UnpackedSegBytes != nativeEntry.UnpackedSegBytes {
				t.Errorf("Shard file %d entry %d UnpackedSegBytes mismatch: xet-go=%d native=%d",
					i, j, xetgoEntry.UnpackedSegBytes, nativeEntry.UnpackedSegBytes)
			}
		}
	}

	// STRICT: Both shards must have the same number of CAS blocks
	if len(xetgoShard.CASInfos) != len(nativeShard.CASInfos) {
		t.Errorf("Shard CAS block count mismatch: xet-go=%d native=%d",
			len(xetgoShard.CASInfos), len(nativeShard.CASInfos))
		return
	}

	// STRICT: Compare CAS blocks
	for i := range xetgoShard.CASInfos {
		xetgoCAS := xetgoShard.CASInfos[i]
		nativeCAS := nativeShard.CASInfos[i]

		if xetgoCAS.CASHash != nativeCAS.CASHash {
			t.Errorf("Shard CAS %d hash mismatch: xet-go=%s native=%s",
				i, xetgoCAS.CASHash, nativeCAS.CASHash)
			continue
		}

		if xetgoCAS.NumBytesInCAS != nativeCAS.NumBytesInCAS {
			t.Errorf("Shard CAS %d NumBytesInCAS mismatch: xet-go=%d native=%d",
				i, xetgoCAS.NumBytesInCAS, nativeCAS.NumBytesInCAS)
		}

		if len(xetgoCAS.Chunks) != len(nativeCAS.Chunks) {
			t.Errorf("Shard CAS %d chunk count mismatch: xet-go=%d native=%d",
				i, len(xetgoCAS.Chunks), len(nativeCAS.Chunks))
			continue
		}

		// STRICT: Compare individual chunk entries in CAS
		for j := range xetgoCAS.Chunks {
			xetgoChunk := xetgoCAS.Chunks[j]
			nativeChunk := nativeCAS.Chunks[j]

			if xetgoChunk.ChunkHash != nativeChunk.ChunkHash {
				t.Errorf("Shard CAS %d chunk %d hash mismatch: xet-go=%s native=%s",
					i, j, xetgoChunk.ChunkHash, nativeChunk.ChunkHash)
			}
			if xetgoChunk.UnpackedSegBytes != nativeChunk.UnpackedSegBytes {
				t.Errorf("Shard CAS %d chunk %d UnpackedSegBytes mismatch: xet-go=%d native=%d",
					i, j, xetgoChunk.UnpackedSegBytes, nativeChunk.UnpackedSegBytes)
			}
			if xetgoChunk.ByteRangeStart != nativeChunk.ByteRangeStart {
				t.Errorf("Shard CAS %d chunk %d ByteRangeStart mismatch: xet-go=%d native=%d",
					i, j, xetgoChunk.ByteRangeStart, nativeChunk.ByteRangeStart)
			}
		}
	}

	t.Logf("✓ Shard content matches between clients (%d files, %d CAS blocks)",
		len(xetgoShard.Files), len(xetgoShard.CASInfos))
}

// compareReconstructionPaths verifies both clients query reconstruction for the same file hash
func compareReconstructionPaths(t *testing.T, xetgoReqs, nativeReqs []RequestRecord, expectedFileHash string) {
	t.Helper()

	for _, req := range xetgoReqs {
		if strings.Contains(req.Path, "/reconstructions/") {
			parts := strings.Split(req.Path, "/")
			for _, part := range parts {
				if len(part) == 64 && isHexString(part) {
					if part != expectedFileHash {
						t.Errorf("xet-go reconstruction query uses wrong file hash: got %s want %s", part, expectedFileHash)
					}
				}
			}
		}
	}

	for _, req := range nativeReqs {
		if strings.Contains(req.Path, "/reconstructions/") {
			parts := strings.Split(req.Path, "/")
			for _, part := range parts {
				if len(part) == 64 && isHexString(part) {
					if part != expectedFileHash {
						t.Errorf("native reconstruction query uses wrong file hash: got %s want %s", part, expectedFileHash)
					}
				}
			}
		}
	}
}

// compareXorbDownloadRanges compares Range headers on xorb download requests between clients
func compareXorbDownloadRanges(t *testing.T, xetgoReqs, nativeReqs []RequestRecord) {
	t.Helper()

	// Group download requests by xorb path (actual path, not normalized)
	xetgoRangesByPath := make(map[string][]string)
	nativeRangesByPath := make(map[string][]string)

	for _, req := range xetgoReqs {
		rangeHeader := req.Headers.Get("Range")
		xetgoRangesByPath[req.Path] = append(xetgoRangesByPath[req.Path], rangeHeader)
	}

	for _, req := range nativeReqs {
		rangeHeader := req.Headers.Get("Range")
		nativeRangesByPath[req.Path] = append(nativeRangesByPath[req.Path], rangeHeader)
	}

	// STRICT: For each xorb path, compare the Range headers
	for path, xetgoRanges := range xetgoRangesByPath {
		nativeRanges, ok := nativeRangesByPath[path]
		if !ok {
			t.Errorf("xet-go downloaded xorb %s but native did not", path)
			continue
		}

		if len(xetgoRanges) != len(nativeRanges) {
			t.Errorf("Xorb %s download count mismatch: xet-go=%d native=%d",
				path, len(xetgoRanges), len(nativeRanges))
			continue
		}

		// Sort ranges for stable comparison
		sort.Strings(xetgoRanges)
		sort.Strings(nativeRanges)

		for i := range xetgoRanges {
			if xetgoRanges[i] != nativeRanges[i] {
				t.Errorf("Xorb %s download %d Range header mismatch: xet-go=%q native=%q",
					path, i, xetgoRanges[i], nativeRanges[i])
			}
		}
	}

	// Check for native-only xorb downloads
	for path := range nativeRangesByPath {
		if _, ok := xetgoRangesByPath[path]; !ok {
			t.Errorf("native downloaded xorb %s but xet-go did not", path)
		}
	}
}

var seed = rand.NewSource(1)

// makeBinaryData creates a deterministic byte sequence of the given size.
func makeBinaryData(size int) []byte {
	result := make([]byte, size)
	for i := range result {
		result[i] = byte(seed.Int63() % 256)
	}
	return result
}
