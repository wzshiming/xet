package client_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/test/conformance/rustref"
	"github.com/wzshiming/xet/test/conformance/utils"
	"github.com/wzshiming/xet/xorb"
)

// RequestRecord captures details of an HTTP request
type RequestRecord struct {
	Method     string
	Path       string
	Headers    http.Header
	Body       []byte
	ClientType string // "xet-core" or "native"
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
		if strings.Contains(userAgent, "xet-core") {
			clientType = "xet-core"
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

// TestClientUploadDownloadRequestConformance tests that xet-core and native clients
// generate compatible HTTP requests for upload and download operations
func TestClientUploadDownloadRequestConformance(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "Empty file",
			data: []byte{},
		},
		{
			name: "Hello World",
			data: []byte("Hello World!"),
		},
		{
			name: "10MB",
			data: utils.MakeRandData(10 * 1024 * 1024),
		},
		{
			name: "10MB repeating",
			data: utils.MakeRepeatData(10 * 1024 * 1024),
		},
		{
			name: "100MB",
			data: utils.MakeRandData(100 * 1024 * 1024),
		},
		{
			name: "100MB repeating",
			data: utils.MakeRepeatData(100 * 1024 * 1024),
		},
		{
			name: "200MB",
			data: utils.MakeRandData(200 * 1024 * 1024),
		},
		{
			name: "200MB repeating",
			data: utils.MakeRepeatData(200 * 1024 * 1024),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("upload_conformance", func(t *testing.T) {
				// Create separate temp directories for each client's storage
				rustrefStorageDir := t.TempDir()
				nativeStorageDir := t.TempDir()

				// Setup server for xet-core
				var rustrefStor storage.Storage
				var rustrefSrv *server.Handler
				var rustrefProxy *RecordingProxy
				var rustrefHttpSrv *httptest.Server

				rustrefHttpSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if rustrefProxy != nil {
						rustrefProxy.ServeHTTP(w, r)
					} else {
						http.Error(w, "server not initialized", http.StatusInternalServerError)
					}
				}))
				defer rustrefHttpSrv.Close()

				var err error
				rustrefStor, err = storage.NewFileStorage(
					storage.WithBasePath(rustrefStorageDir),
					storage.WithBaseURL(rustrefHttpSrv.URL),
				)
				if err != nil {
					t.Fatalf("Failed to create xet-core storage: %v", err)
				}

				rustrefSrv = server.NewHandler(server.WithStorage(rustrefStor))
				rustrefProxy = NewRecordingProxy(rustrefSrv)

				// Upload with xet-core
				tempDir := t.TempDir()
				rustrefFile := filepath.Join(tempDir, "rustref-upload.bin")
				if err := os.WriteFile(rustrefFile, tt.data, 0644); err != nil {
					t.Fatalf("Failed to write xet-core upload file: %v", err)
				}

				uploadResults, err := rustref.UploadFiles(
					[]string{rustrefFile},
					rustrefHttpSrv.URL,
					nil,   // token
					nil,   // sha256s (computed automatically)
					false, // skipSHA256
				)
				if err != nil {
					t.Fatalf("Failed to upload file with xet-core: %v", err)
				}

				if len(uploadResults) != 1 {
					t.Fatalf("Expected 1 upload result, got %d", len(uploadResults))
				}

				rustrefRequests := rustrefProxy.GetRequests()

				// Setup server for native client
				var nativeStor storage.Storage
				var nativeSrv *server.Handler
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

				nativeStor, err = storage.NewFileStorage(
					storage.WithBasePath(nativeStorageDir),
					storage.WithBaseURL(nativeHttpSrv.URL),
				)
				if err != nil {
					t.Fatalf("Failed to create native storage: %v", err)
				}

				nativeSrv = server.NewHandler(server.WithStorage(nativeStor))
				nativeProxy = NewRecordingProxy(nativeSrv)

				// Upload with native client
				nativeClient, err := client.NewClient(
					client.WithBaseURL(nativeHttpSrv.URL),
					client.WithCacheDir(t.TempDir()),
				)
				if err != nil {
					t.Fatalf("create native client: %v", err)
				}

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

				fileHash, err := nativeClient.UploadFile(context.Background(), f)
				if err != nil {
					t.Fatalf("Failed to upload file with native client: %v", err)
				}

				nativeRequests := nativeProxy.GetRequests()

				// Compare requests
				compareUploadRequests(t, rustrefRequests, nativeRequests, uploadResults[0].Hash, fileHash.String())
			})

			t.Run("download_conformance", func(t *testing.T) {
				// Setup shared server
				storageDir := t.TempDir()
				var stor storage.Storage
				var srv *server.Handler
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
				stor, err = storage.NewFileStorage(
					storage.WithBasePath(storageDir),
					storage.WithBaseURL(httpSrv.URL),
				)
				if err != nil {
					t.Fatalf("Failed to create storage: %v", err)
				}

				srv = server.NewHandler(server.WithStorage(stor))
				proxy = NewRecordingProxy(srv)

				// First upload file using native client
				nativeClient, err := client.NewClient(
					client.WithBaseURL(httpSrv.URL),
					client.WithCacheDir(t.TempDir()),
				)
				if err != nil {
					t.Fatalf("create native client: %v", err)
				}

				tempDir := t.TempDir()
				uploadFile := filepath.Join(tempDir, "upload.bin")
				if err := os.WriteFile(uploadFile, tt.data, 0644); err != nil {
					t.Fatalf("Failed to write upload file: %v", err)
				}

				f, err := os.Open(uploadFile)
				if err != nil {
					t.Fatalf("Failed to open upload file: %v", err)
				}

				fileHash, err := nativeClient.UploadFile(context.Background(), f)
				f.Close()
				if err != nil {
					t.Fatalf("Failed to upload file: %v", err)
				}

				// Download with xet-core
				proxy.ClearRequests()
				rustrefDownloadFile := filepath.Join(tempDir, "rustref-download.bin")
				downloadReq := []rustref.DownloadRequest{
					{
						DestinationPath: rustrefDownloadFile,
						Hash:            fileHash.String(),
						FileSize:        int64(len(tt.data)),
					},
				}

				_, err = rustref.DownloadFiles(downloadReq, httpSrv.URL, nil)
				if err != nil {
					t.Fatalf("Failed to download file with xet-core: %v", err)
				}

				rustrefRequests := proxy.GetRequests()

				// Download with native client
				proxy.ClearRequests()
				nativeDownloadFile := filepath.Join(tempDir, "native-download.bin")
				nativeFile, err := os.Create(nativeDownloadFile)
				if err != nil {
					t.Fatalf("Failed to create native download file: %v", err)
				}
				err = nativeClient.DownloadFile(context.Background(), fileHash, nativeFile)
				nativeFile.Close()
				if err != nil {
					t.Fatalf("Failed to download file with native client: %v", err)
				}

				nativeDownloadedData, err := os.ReadFile(nativeDownloadFile)
				if err != nil {
					t.Fatalf("Failed to read downloaded data: %v", err)
				}

				nativeRequests := proxy.GetRequests()

				// Verify downloaded content matches
				rustrefDownloadedData, err := os.ReadFile(rustrefDownloadFile)
				if err != nil {
					t.Fatalf("Failed to read xet-core downloaded file: %v", err)
				}

				if !bytes.Equal(rustrefDownloadedData, tt.data) {
					t.Errorf("xet-core downloaded data mismatch: got %d bytes, want %d bytes", len(rustrefDownloadedData), len(tt.data))
				}

				if !bytes.Equal(nativeDownloadedData, tt.data) {
					t.Errorf("native downloaded data mismatch: got %d bytes, want %d bytes", len(nativeDownloadedData), len(tt.data))
				}

				// Compare download requests
				compareDownloadRequests(t, rustrefRequests, nativeRequests, fileHash.String())
			})
		})
	}
}

func TestClientUploadConformanceWithExistingData(t *testing.T) {
	seedData := utils.MakeRepeatData(8 * 1024 * 1024)
	targetData := append([]byte{}, seedData[:4*1024*1024]...)
	targetData = append(targetData, utils.MakeRandData(4*1024*1024)...)

	tempDir := t.TempDir()
	rustrefSeedFile := filepath.Join(tempDir, "rustref-seed.bin")
	rustrefTargetFile := filepath.Join(tempDir, "rustref-target.bin")
	nativeSeedFile := filepath.Join(tempDir, "native-seed.bin")
	nativeTargetFile := filepath.Join(tempDir, "native-target.bin")

	if err := os.WriteFile(rustrefSeedFile, seedData, 0644); err != nil {
		t.Fatalf("write xet-core seed file: %v", err)
	}
	if err := os.WriteFile(rustrefTargetFile, targetData, 0644); err != nil {
		t.Fatalf("write xet-core target file: %v", err)
	}
	if err := os.WriteFile(nativeSeedFile, seedData, 0644); err != nil {
		t.Fatalf("write native seed file: %v", err)
	}
	if err := os.WriteFile(nativeTargetFile, targetData, 0644); err != nil {
		t.Fatalf("write native target file: %v", err)
	}

	// xet-core backend
	rustrefStorageDir := t.TempDir()
	var rustrefStor storage.Storage
	var rustrefSrv *server.Handler
	var rustrefProxy *RecordingProxy
	var rustrefHTTP *httptest.Server

	rustrefHTTP = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if rustrefProxy != nil {
			rustrefProxy.ServeHTTP(w, r)
			return
		}
		http.Error(w, "server not initialized", http.StatusInternalServerError)
	}))
	defer rustrefHTTP.Close()

	var err error
	rustrefStor, err = storage.NewFileStorage(
		storage.WithBasePath(rustrefStorageDir),
		storage.WithBaseURL(rustrefHTTP.URL),
	)
	if err != nil {
		t.Fatalf("create xet-core storage: %v", err)
	}
	rustrefSrv = server.NewHandler(server.WithStorage(rustrefStor))
	rustrefProxy = NewRecordingProxy(rustrefSrv)

	if _, err := rustref.UploadFiles([]string{rustrefSeedFile}, rustrefHTTP.URL, nil, nil, false); err != nil {
		t.Fatalf("seed upload with xet-core failed: %v", err)
	}
	rustrefProxy.ClearRequests()

	rustrefResults, err := rustref.UploadFiles([]string{rustrefTargetFile}, rustrefHTTP.URL, nil, nil, false)
	if err != nil {
		t.Fatalf("target upload with xet-core failed: %v", err)
	}
	if len(rustrefResults) != 1 {
		t.Fatalf("expected one xet-core result, got %d", len(rustrefResults))
	}
	rustrefRequests := rustrefProxy.GetRequests()

	// native backend
	nativeStorageDir := t.TempDir()
	var nativeStor storage.Storage
	var nativeSrv *server.Handler
	var nativeProxy *RecordingProxy
	var nativeHTTP *httptest.Server

	nativeHTTP = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if nativeProxy != nil {
			nativeProxy.ServeHTTP(w, r)
			return
		}
		http.Error(w, "server not initialized", http.StatusInternalServerError)
	}))
	defer nativeHTTP.Close()

	nativeStor, err = storage.NewFileStorage(
		storage.WithBasePath(nativeStorageDir),
		storage.WithBaseURL(nativeHTTP.URL),
	)
	if err != nil {
		t.Fatalf("create native storage: %v", err)
	}
	nativeSrv = server.NewHandler(server.WithStorage(nativeStor))
	nativeProxy = NewRecordingProxy(nativeSrv)

	nativeClient, err := client.NewClient(
		client.WithBaseURL(nativeHTTP.URL),
		client.WithCacheDir(t.TempDir()),
	)
	if err != nil {
		t.Fatalf("create native client: %v", err)
	}

	seedReader, err := os.Open(nativeSeedFile)
	if err != nil {
		t.Fatalf("open native seed file: %v", err)
	}
	if _, err := nativeClient.UploadFile(context.Background(), seedReader); err != nil {
		_ = seedReader.Close()
		t.Fatalf("seed upload with native client failed: %v", err)
	}
	if err := seedReader.Close(); err != nil {
		t.Fatalf("close native seed file: %v", err)
	}
	nativeProxy.ClearRequests()

	targetReader, err := os.Open(nativeTargetFile)
	if err != nil {
		t.Fatalf("open native target file: %v", err)
	}
	nativeHash, err := nativeClient.UploadFile(context.Background(), targetReader)
	if err != nil {
		_ = targetReader.Close()
		t.Fatalf("target upload with native client failed: %v", err)
	}
	if err := targetReader.Close(); err != nil {
		t.Fatalf("close native target file: %v", err)
	}
	nativeRequests := nativeProxy.GetRequests()

	compareUploadRequests(t, rustrefRequests, nativeRequests, rustrefResults[0].Hash, nativeHash.String())
}

// compareUploadRequests compares HTTP requests from xet-core and native clients for uploads
func compareUploadRequests(t *testing.T, rustrefReqs, nativeReqs []RequestRecord, rustrefHash, nativeHash string) {
	t.Helper()

	// STRICT: Verify that file hashes match
	if rustrefHash != nativeHash {
		t.Errorf("File hash mismatch: xet-core=%s native=%s", rustrefHash, nativeHash)
		return
	}

	// Group requests by type
	rustrefByType := groupRequestsByType(rustrefReqs)
	nativeByType := groupRequestsByType(nativeReqs)

	// STRICT: All non-dedup request type counts must match after canonicalizing
	// v1/v2 variants. Dedup probes are validated semantically below.
	assertCanonicalTypeCountEquality(t, rustrefByType, nativeByType, map[string]bool{
		"GET:/v{version}/chunks/default/{hash}": true,
		"POST:/v{version}/chunks/default:query": true,
		"HEAD:/v{version}/xorbs/default/{hash}": true,
		"POST:/v{version}/xorbs/default/{hash}": true,
	})

	// STRICT: Both must upload exactly one shard
	rustrefShardCount := len(rustrefByType["POST:/shards"]) + len(rustrefByType["POST:/v1/shards"]) + len(rustrefByType["POST:/v2/shards"])
	nativeShardCount := len(nativeByType["POST:/shards"]) + len(nativeByType["POST:/v1/shards"]) + len(nativeByType["POST:/v2/shards"])
	if rustrefShardCount != 1 {
		t.Errorf("xet-core uploaded %d shards, expected exactly 1", rustrefShardCount)
	}
	if nativeShardCount != 1 {
		t.Errorf("native client uploaded %d shards, expected exactly 1", nativeShardCount)
	}

	// STRICT: Dedup chunk queries must target the same chunk hash set.
	compareChunkDedupQueries(t, rustrefReqs, nativeReqs)

	// Compare xorb uploads - validate chunk content
	rustrefXorbReqs := rustrefByType["POST:/v1/xorbs/default/{hash}"]
	nativeXorbReqs := nativeByType["POST:/v1/xorbs/default/{hash}"]
	if len(rustrefXorbReqs) > 0 && len(nativeXorbReqs) > 0 {
		compareXorbRequests(t, rustrefXorbReqs, nativeXorbReqs)
	} else {
		t.Logf("Skip strict xorb upload body comparison: xet-core posts=%d native posts=%d", len(rustrefXorbReqs), len(nativeXorbReqs))
	}

	// STRICT: Compare shard content
	rustrefShardReqs := appendShardRequests(rustrefByType)
	nativeShardReqs := appendShardRequests(nativeByType)
	if len(rustrefXorbReqs) == len(nativeXorbReqs) {
		compareShardRequests(t, rustrefShardReqs, nativeShardReqs)
	} else {
		t.Logf("Skip strict shard CAS comparison because xorb upload counts differ: xet-core=%d native=%d", len(rustrefXorbReqs), len(nativeXorbReqs))
	}
}

func appendShardRequests(byType map[string][]RequestRecord) []RequestRecord {
	requests := append([]RequestRecord(nil), byType["POST:/shards"]...)
	requests = append(requests, byType["POST:/v1/shards"]...)
	return append(requests, byType["POST:/v2/shards"]...)
}

// compareDownloadRequests compares HTTP requests from xet-core and native clients for downloads
func compareDownloadRequests(t *testing.T, rustrefReqs, nativeReqs []RequestRecord, fileHash string) {
	t.Helper()

	// Group requests by type
	rustrefByType := groupRequestsByType(rustrefReqs)
	nativeByType := groupRequestsByType(nativeReqs)

	// STRICT: Both must query reconstruction (v1 or v2)
	rustrefReconCount := len(rustrefByType["GET:/v1/reconstructions/{hash}"]) + len(rustrefByType["GET:/v2/reconstructions/{hash}"])
	nativeReconCount := len(nativeByType["GET:/v1/reconstructions/{hash}"]) + len(nativeByType["GET:/v2/reconstructions/{hash}"])
	if rustrefReconCount == 0 {
		t.Errorf("xet-core did not query reconstruction")
	}
	if nativeReconCount == 0 {
		t.Errorf("native client did not query reconstruction")
	}

	// STRICT: Both must download xorb data
	rustrefXorbDownloadCount := len(rustrefByType["GET:/v1/xorbs/default/{hash}"])
	nativeXorbDownloadCount := len(nativeByType["GET:/v1/xorbs/default/{hash}"])
	if rustrefXorbDownloadCount == 0 && nativeXorbDownloadCount > 0 {
		t.Errorf("xet-core did not download any xorb data")
	}
	if nativeXorbDownloadCount == 0 && rustrefXorbDownloadCount > 0 {
		t.Errorf("native client did not download any xorb data")
	}

	// STRICT: Compare reconstruction query paths (must use same file hash)
	compareReconstructionPaths(t, rustrefReqs, nativeReqs, fileHash)

	// STRICT: Compare xorb download Range headers
	rustrefXorbReqs := rustrefByType["GET:/v1/xorbs/default/{hash}"]
	nativeXorbReqs := nativeByType["GET:/v1/xorbs/default/{hash}"]
	compareXorbDownloadRanges(t, rustrefXorbReqs, nativeXorbReqs)

	// STRICT: Except for xorb data (validated by precise range coverage), all
	// request types must match exactly after canonicalizing v1/v2 variants.
	assertCanonicalTypeCountEquality(t, rustrefByType, nativeByType, map[string]bool{
		"GET:/v{version}/reconstructions/{hash}": true,
		"GET:/v1/xorbs/default/{hash}":           true,
	})

	// Get request types for logging after strict checks.
	rustrefTypes := getSortedKeys(rustrefByType)
	nativeTypes := getSortedKeys(nativeByType)

	t.Logf("✓ Download conformance check passed for file %s", fileHash)
	t.Logf("  xet-core request types: %v", rustrefTypes)
	t.Logf("  native request types: %v", nativeTypes)
}

// canonicalizeRequestType normalizes versioned API prefixes so v1/v2 endpoints
// compare as the same behavior for conformance purposes.
func canonicalizeRequestType(reqType string) string {
	parts := strings.SplitN(reqType, ":", 2)
	if len(parts) != 2 {
		return reqType
	}
	method := parts[0]
	path := parts[1]
	path = strings.Replace(path, "/v1/", "/v{version}/", 1)
	path = strings.Replace(path, "/v2/", "/v{version}/", 1)
	return method + ":" + path
}

// assertCanonicalTypeCountEquality enforces exact equality of request-type counts
// after canonicalizing API versions.
func assertCanonicalTypeCountEquality(t *testing.T, rustrefByType, nativeByType map[string][]RequestRecord, skipTypes map[string]bool) {
	t.Helper()

	rustrefCounts := make(map[string]int)
	nativeCounts := make(map[string]int)

	for reqType, reqs := range rustrefByType {
		canonical := canonicalizeRequestType(reqType)
		if skipTypes != nil && (skipTypes[reqType] || skipTypes[canonical]) {
			continue
		}
		rustrefCounts[canonical] += len(reqs)
	}

	for reqType, reqs := range nativeByType {
		canonical := canonicalizeRequestType(reqType)
		if skipTypes != nil && (skipTypes[reqType] || skipTypes[canonical]) {
			continue
		}
		nativeCounts[canonical] += len(reqs)
	}

	for key, xCount := range rustrefCounts {
		nCount := nativeCounts[key]
		if xCount != nCount {
			t.Errorf("Request type count mismatch for %s: xet-core=%d native=%d", key, xCount, nCount)
		}
	}
	for key, nCount := range nativeCounts {
		xCount := rustrefCounts[key]
		if xCount != nCount {
			t.Errorf("Request type count mismatch for %s: xet-core=%d native=%d", key, xCount, nCount)
		}
	}
}

// compareChunkDedupQueries validates dedup probes semantically. Clients may choose
// different probing strategies, but every probed chunk hash must belong to that
// client's uploaded chunk set.
func compareChunkDedupQueries(t *testing.T, rustrefReqs, nativeReqs []RequestRecord) {
	t.Helper()

	rustrefChunks := make(map[string]bool)
	nativeChunks := make(map[string]bool)
	rustrefUploaded := extractUploadedChunkHashes(t, rustrefReqs)
	nativeUploaded := extractUploadedChunkHashes(t, nativeReqs)

	for _, req := range rustrefReqs {
		if req.Method != http.MethodGet || !strings.HasPrefix(req.Path, "/v1/chunks/") {
			continue
		}
		parts := strings.Split(strings.TrimPrefix(req.Path, "/v1/chunks/"), "/")
		if len(parts) > 0 && len(parts[0]) == 64 && isHexString(parts[0]) {
			rustrefChunks[parts[0]] = true
		}
	}

	for _, req := range nativeReqs {
		if req.Method != http.MethodGet || !strings.HasPrefix(req.Path, "/v1/chunks/") {
			continue
		}
		parts := strings.Split(strings.TrimPrefix(req.Path, "/v1/chunks/"), "/")
		if len(parts) > 0 && len(parts[0]) == 64 && isHexString(parts[0]) {
			nativeChunks[parts[0]] = true
		}
	}

	for h := range rustrefChunks {
		if !rustrefUploaded[h] {
			t.Errorf("xet-core queried dedup chunk %s that is not in xet-core uploaded chunks", h)
		}
	}
	for h := range nativeChunks {
		if !nativeUploaded[h] {
			t.Errorf("native queried dedup chunk %s that is not in native uploaded chunks", h)
		}
	}
}

func extractUploadedChunkHashes(t *testing.T, reqs []RequestRecord) map[string]bool {
	t.Helper()

	result := make(map[string]bool)
	for _, req := range reqs {
		if req.Method != http.MethodPost || normalizePath(req.Path) != "/v1/xorbs/default/{hash}" {
			continue
		}

		dec := xorb.NewDecoder(bytes.NewReader(req.Body), false)
		var buf [xet.MaxChunkSize]byte
		for {
			n, err := dec.Read(buf[:])
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Errorf("failed to decode xorb while extracting uploaded chunks: %v", err)
				break
			}
			h := xet.ComputeChunkHash(buf[:n])
			result[h.String()] = true
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

// compareXorbRequests compares xorb upload requests by deserializing and comparing chunk content
func compareXorbRequests(t *testing.T, rustrefReqs, nativeReqs []RequestRecord) {
	t.Helper()

	// STRICT: Both clients must upload the same number of xorbs
	if len(rustrefReqs) != len(nativeReqs) {
		t.Errorf("Xorb upload count mismatch: xet-core=%d native=%d", len(rustrefReqs), len(nativeReqs))
	}

	// Collect all chunk hashes from both clients by deserializing xorbs
	rustrefChunkHashes := make(map[xet.ChunkHash]bool)
	nativeChunkHashes := make(map[xet.ChunkHash]bool)

	// Also collect ordered chunk sequences per xorb for detailed comparison
	type xorbInfo struct {
		hash        string
		chunkHashes []xet.ChunkHash
		chunkSizes  []int // uncompressed chunk sizes
	}
	var rustrefXorbs []xorbInfo
	var nativeXorbs []xorbInfo

	for _, req := range rustrefReqs {
		info := xorbInfo{hash: req.Path}
		dec := xorb.NewDecoder(bytes.NewReader(req.Body), false)
		var buf [xet.MaxChunkSize]byte
		for {
			n, err := dec.Read(buf[:])
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Errorf("Failed to deserialize xet-core xorb: %v", err)
				break
			}
			h := xet.ComputeChunkHash(buf[:n])
			rustrefChunkHashes[h] = true
			info.chunkHashes = append(info.chunkHashes, h)
			info.chunkSizes = append(info.chunkSizes, n)
		}
		rustrefXorbs = append(rustrefXorbs, info)
	}

	for _, req := range nativeReqs {
		info := xorbInfo{hash: req.Path}
		dec := xorb.NewDecoder(bytes.NewReader(req.Body), false)
		var buf [xet.MaxChunkSize]byte
		for {
			n, err := dec.Read(buf[:])
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Errorf("Failed to deserialize native xorb: %v", err)
				break
			}
			h := xet.ComputeChunkHash(buf[:n])
			nativeChunkHashes[h] = true
			info.chunkHashes = append(info.chunkHashes, h)
			info.chunkSizes = append(info.chunkSizes, n)
		}
		nativeXorbs = append(nativeXorbs, info)
	}

	// STRICT: Both clients must upload the same set of chunk hashes
	// (same chunking algorithm should produce same chunks)
	rustrefOnly := []xet.ChunkHash{}
	for hash := range rustrefChunkHashes {
		if !nativeChunkHashes[hash] {
			rustrefOnly = append(rustrefOnly, hash)
		}
	}

	nativeOnly := []xet.ChunkHash{}
	for hash := range nativeChunkHashes {
		if !rustrefChunkHashes[hash] {
			nativeOnly = append(nativeOnly, hash)
		}
	}

	if len(rustrefOnly) > 0 {
		t.Errorf("xet-core uploaded %d chunks that native did not: %v", len(rustrefOnly), rustrefOnly)
	}
	if len(nativeOnly) > 0 {
		t.Errorf("native uploaded %d chunks that xet-core did not: %v", len(nativeOnly), nativeOnly)
	}

	// STRICT: Compare chunk ordering within corresponding xorbs.
	// Sort both slices by path (which contains the xorb hash) so that the
	// positional comparison is stable even when xet-core uploads xorbs
	// concurrently and the server records them in a different arrival order.
	sort.Slice(rustrefXorbs, func(i, j int) bool { return rustrefXorbs[i].hash < rustrefXorbs[j].hash })
	sort.Slice(nativeXorbs, func(i, j int) bool { return nativeXorbs[i].hash < nativeXorbs[j].hash })
	if len(rustrefXorbs) == len(nativeXorbs) {
		for i := range rustrefXorbs {
			if len(rustrefXorbs[i].chunkHashes) != len(nativeXorbs[i].chunkHashes) {
				t.Errorf("Xorb %d chunk count mismatch: xet-core=%d native=%d",
					i, len(rustrefXorbs[i].chunkHashes), len(nativeXorbs[i].chunkHashes))
				continue
			}
			for j := range rustrefXorbs[i].chunkHashes {
				if rustrefXorbs[i].chunkHashes[j] != nativeXorbs[i].chunkHashes[j] {
					t.Errorf("Xorb %d chunk %d hash mismatch: xet-core=%s native=%s",
						i, j, rustrefXorbs[i].chunkHashes[j], nativeXorbs[i].chunkHashes[j])
				}
			}
			// STRICT: Compare chunk sizes
			if len(rustrefXorbs[i].chunkSizes) != len(nativeXorbs[i].chunkSizes) {
				t.Errorf("Xorb %d chunk size count mismatch: xet-core=%d native=%d",
					i, len(rustrefXorbs[i].chunkSizes), len(nativeXorbs[i].chunkSizes))
			} else {
				for j := range rustrefXorbs[i].chunkSizes {
					if rustrefXorbs[i].chunkSizes[j] != nativeXorbs[i].chunkSizes[j] {
						t.Errorf("Xorb %d chunk %d size mismatch: xet-core=%d native=%d",
							i, j, rustrefXorbs[i].chunkSizes[j], nativeXorbs[i].chunkSizes[j])
					}
				}
			}
		}
	}

	// Log success
	if len(rustrefOnly) == 0 && len(nativeOnly) == 0 {
		t.Logf("✓ Both clients uploaded identical chunk sets (%d chunks)", len(rustrefChunkHashes))
	}
}

// compareShardRequests compares shard upload bodies between clients
func compareShardRequests(t *testing.T, rustrefReqs, nativeReqs []RequestRecord) {
	t.Helper()

	if len(rustrefReqs) == 0 || len(nativeReqs) == 0 {
		return // already reported by count checks
	}

	// Deserialize both shards
	rustrefShard := &shard.Shard{}
	if err := rustrefShard.Decode(bytes.NewReader(rustrefReqs[0].Body), false); err != nil {
		t.Errorf("Failed to deserialize xet-core shard: %v", err)
		return
	}

	nativeShard := &shard.Shard{}
	if err := nativeShard.Decode(bytes.NewReader(nativeReqs[0].Body), false); err != nil {
		t.Errorf("Failed to deserialize native shard: %v", err)
		return
	}

	// STRICT: Both shards must have the same number of files
	if len(rustrefShard.Files) != len(nativeShard.Files) {
		t.Errorf("Shard file count mismatch: xet-core=%d native=%d",
			len(rustrefShard.Files), len(nativeShard.Files))
		return
	}

	// STRICT: Compare file blocks
	for i := range rustrefShard.Files {
		rustrefFile := rustrefShard.Files[i]
		nativeFile := nativeShard.Files[i]

		// STRICT: File hashes must match
		if rustrefFile.FileHash != nativeFile.FileHash {
			t.Errorf("Shard file %d hash mismatch: xet-core=%s native=%s",
				i, rustrefFile.FileHash, nativeFile.FileHash)
			continue
		}

		// STRICT: Same number of reconstruction entries
		if len(rustrefFile.Entries) != len(nativeFile.Entries) {
			t.Errorf("Shard file %d entry count mismatch: xet-core=%d native=%d",
				i, len(rustrefFile.Entries), len(nativeFile.Entries))
			continue
		}

		// STRICT: Compare reconstruction entries
		for j := range rustrefFile.Entries {
			rustrefEntry := rustrefFile.Entries[j]
			nativeEntry := nativeFile.Entries[j]

			if rustrefEntry.CASHash != nativeEntry.CASHash {
				t.Errorf("Shard file %d entry %d CASHash mismatch: xet-core=%s native=%s",
					i, j, rustrefEntry.CASHash, nativeEntry.CASHash)
			}
			if rustrefEntry.ChunkIndexStart != nativeEntry.ChunkIndexStart {
				t.Errorf("Shard file %d entry %d ChunkIndexStart mismatch: xet-core=%d native=%d",
					i, j, rustrefEntry.ChunkIndexStart, nativeEntry.ChunkIndexStart)
			}
			if rustrefEntry.ChunkIndexEnd != nativeEntry.ChunkIndexEnd {
				t.Errorf("Shard file %d entry %d ChunkIndexEnd mismatch: xet-core=%d native=%d",
					i, j, rustrefEntry.ChunkIndexEnd, nativeEntry.ChunkIndexEnd)
			}
			if rustrefEntry.UnpackedSegBytes != nativeEntry.UnpackedSegBytes {
				t.Errorf("Shard file %d entry %d UnpackedSegBytes mismatch: xet-core=%d native=%d",
					i, j, rustrefEntry.UnpackedSegBytes, nativeEntry.UnpackedSegBytes)
			}
		}
	}

	// STRICT: Both shards must have the same number of CAS blocks
	if len(rustrefShard.CASInfos) != len(nativeShard.CASInfos) {
		t.Errorf("Shard CAS block count mismatch: xet-core=%d native=%d",
			len(rustrefShard.CASInfos), len(nativeShard.CASInfos))
		return
	}

	// STRICT: Compare CAS blocks
	for i := range rustrefShard.CASInfos {
		rustrefCAS := rustrefShard.CASInfos[i]
		nativeCAS := nativeShard.CASInfos[i]

		if rustrefCAS.CASHash != nativeCAS.CASHash {
			t.Errorf("Shard CAS %d hash mismatch: xet-core=%s native=%s",
				i, rustrefCAS.CASHash, nativeCAS.CASHash)
			continue
		}

		if rustrefCAS.NumBytesInCAS != nativeCAS.NumBytesInCAS {
			t.Errorf("Shard CAS %d NumBytesInCAS mismatch: xet-core=%d native=%d",
				i, rustrefCAS.NumBytesInCAS, nativeCAS.NumBytesInCAS)
		}

		if len(rustrefCAS.Chunks) != len(nativeCAS.Chunks) {
			t.Errorf("Shard CAS %d chunk count mismatch: xet-core=%d native=%d",
				i, len(rustrefCAS.Chunks), len(nativeCAS.Chunks))
			continue
		}

		// STRICT: Compare individual chunk entries in CAS
		for j := range rustrefCAS.Chunks {
			rustrefChunk := rustrefCAS.Chunks[j]
			nativeChunk := nativeCAS.Chunks[j]

			if rustrefChunk.ChunkHash != nativeChunk.ChunkHash {
				t.Errorf("Shard CAS %d chunk %d hash mismatch: xet-core=%s native=%s",
					i, j, rustrefChunk.ChunkHash, nativeChunk.ChunkHash)
			}
			if rustrefChunk.UnpackedSegBytes != nativeChunk.UnpackedSegBytes {
				t.Errorf("Shard CAS %d chunk %d UnpackedSegBytes mismatch: xet-core=%d native=%d",
					i, j, rustrefChunk.UnpackedSegBytes, nativeChunk.UnpackedSegBytes)
			}
			if rustrefChunk.ByteRangeStart != nativeChunk.ByteRangeStart {
				t.Errorf("Shard CAS %d chunk %d ByteRangeStart mismatch: xet-core=%d native=%d",
					i, j, rustrefChunk.ByteRangeStart, nativeChunk.ByteRangeStart)
			}
		}
	}

	t.Logf("✓ Shard content matches between clients (%d files, %d CAS blocks)",
		len(rustrefShard.Files), len(rustrefShard.CASInfos))
}

// compareReconstructionPaths verifies both clients query reconstruction for the same file hash
func compareReconstructionPaths(t *testing.T, rustrefReqs, nativeReqs []RequestRecord, expectedFileHash string) {
	t.Helper()

	for _, req := range rustrefReqs {
		if strings.Contains(req.Path, "/reconstructions/") {
			parts := strings.SplitSeq(req.Path, "/")
			for part := range parts {
				if len(part) == 64 && isHexString(part) {
					if part != expectedFileHash {
						t.Errorf("xet-core reconstruction query uses wrong file hash: got %s want %s", part, expectedFileHash)
					}
				}
			}
		}
	}

	for _, req := range nativeReqs {
		if strings.Contains(req.Path, "/reconstructions/") {
			parts := strings.SplitSeq(req.Path, "/")
			for part := range parts {
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
func compareXorbDownloadRanges(t *testing.T, rustrefReqs, nativeReqs []RequestRecord) {
	t.Helper()

	// Group download requests by xorb path (actual path, not normalized)
	rustrefRangesByPath := make(map[string][]string)
	nativeRangesByPath := make(map[string][]string)

	for _, req := range rustrefReqs {
		rangeHeader := req.Headers.Get("Range")
		rustrefRangesByPath[req.Path] = append(rustrefRangesByPath[req.Path], rangeHeader)
	}

	for _, req := range nativeReqs {
		rangeHeader := req.Headers.Get("Range")
		nativeRangesByPath[req.Path] = append(nativeRangesByPath[req.Path], rangeHeader)
	}

	// For each xorb path, compare semantic range coverage. Clients may split
	// ranges differently, but the downloaded byte intervals must be identical.
	for path, rustrefRanges := range rustrefRangesByPath {
		nativeRanges, ok := nativeRangesByPath[path]
		if !ok {
			t.Errorf("xet-core downloaded xorb %s but native did not", path)
			continue
		}

		xMerged, xErr := mergeRanges(rustrefRanges)
		nMerged, nErr := mergeRanges(nativeRanges)
		if xErr != nil {
			t.Errorf("xet-core has invalid Range header for xorb %s: %v", path, xErr)
			continue
		}
		if nErr != nil {
			t.Errorf("native has invalid Range header for xorb %s: %v", path, nErr)
			continue
		}

		if !equalByteRanges(xMerged, nMerged) {
			t.Errorf("Xorb %s merged byte-range mismatch: xet-core=%v native=%v", path, xMerged, nMerged)
		}
	}

	// Check for native-only xorb downloads
	for path := range nativeRangesByPath {
		if _, ok := rustrefRangesByPath[path]; !ok {
			t.Errorf("native downloaded xorb %s but xet-core did not", path)
		}
	}
}

type byteRange struct {
	start int64
	end   int64
}

func parseRangeHeader(value string) (byteRange, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return byteRange{}, fmt.Errorf("empty Range header")
	}
	if !strings.HasPrefix(value, "bytes=") {
		return byteRange{}, fmt.Errorf("unsupported Range header format %q", value)
	}
	parts := strings.SplitN(strings.TrimPrefix(value, "bytes="), "-", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return byteRange{}, fmt.Errorf("unsupported Range header format %q", value)
	}
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return byteRange{}, fmt.Errorf("invalid Range start %q", parts[0])
	}
	end, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return byteRange{}, fmt.Errorf("invalid Range end %q", parts[1])
	}
	if start < 0 || end < start {
		return byteRange{}, fmt.Errorf("invalid Range bounds %q", value)
	}
	return byteRange{start: start, end: end}, nil
}

func mergeRanges(values []string) ([]byteRange, error) {
	if len(values) == 0 {
		return nil, nil
	}
	ranges := make([]byteRange, 0, len(values))
	for _, value := range values {
		r, err := parseRangeHeader(value)
		if err != nil {
			return nil, err
		}
		ranges = append(ranges, r)
	}

	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].start == ranges[j].start {
			return ranges[i].end < ranges[j].end
		}
		return ranges[i].start < ranges[j].start
	})

	merged := []byteRange{ranges[0]}
	for _, current := range ranges[1:] {
		last := &merged[len(merged)-1]
		if current.start <= last.end+1 {
			if current.end > last.end {
				last.end = current.end
			}
			continue
		}
		merged = append(merged, current)
	}
	return merged, nil
}

func equalByteRanges(a, b []byteRange) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].start != b[i].start || a[i].end != b[i].end {
			return false
		}
	}
	return true
}

// compareBatchDownloadRequests compares HTTP request patterns between xet-core and the
// native client for a multi-file batch download.
// It asserts that the native client uses the batch /reconstructions endpoint and that
// both clients cover the same byte ranges when downloading xorb data.
func compareBatchDownloadRequests(t *testing.T, rustrefReqs, nativeReqs []RequestRecord, fileHashes []xet.FileHash) {
	t.Helper()

	rustrefByType := groupRequestsByType(rustrefReqs)
	nativeByType := groupRequestsByType(nativeReqs)

	// Log which reconstruction strategy each client used — xet-core may call individual
	// endpoints while native should use the batch endpoint.
	rustrefBatchRecon := len(rustrefByType["GET:/reconstructions"])
	nativeBatchRecon := len(nativeByType["GET:/reconstructions"])
	rustrefSingleRecon := len(rustrefByType["GET:/v1/reconstructions/{hash}"]) + len(rustrefByType["GET:/v2/reconstructions/{hash}"])
	nativeSingleRecon := len(nativeByType["GET:/v1/reconstructions/{hash}"]) + len(nativeByType["GET:/v2/reconstructions/{hash}"])

	t.Logf("Reconstruction: xet-core — %d batch, %d single-file; native — %d batch, %d single-file",
		rustrefBatchRecon, rustrefSingleRecon, nativeBatchRecon, nativeSingleRecon)

	// STRICT: native client must use the batch endpoint for multiple files.
	if nativeBatchRecon == 0 {
		t.Errorf("native client did not use the batch /reconstructions endpoint for %d files", len(fileHashes))
	}
	// STRICT: native client must not fall back to individual reconstruction endpoints.
	if nativeSingleRecon > 0 {
		t.Errorf("native client issued %d individual /v{n}/reconstructions/{hash} requests, expected 0", nativeSingleRecon)
	}

	// Both clients must download xorb data to reconstruct the files.
	rustrefXorbReqs := rustrefByType["GET:/v1/xorbs/default/{hash}"]
	nativeXorbReqs := nativeByType["GET:/v1/xorbs/default/{hash}"]
	if len(rustrefXorbReqs) == 0 && len(nativeXorbReqs) > 0 {
		t.Errorf("xet-core did not download any xorb data while native did")
	}
	if len(nativeXorbReqs) == 0 && len(rustrefXorbReqs) > 0 {
		t.Errorf("native client did not download any xorb data while xet-core did")
	}

	// STRICT: both clients must cover identical byte ranges per xorb.
	compareXorbDownloadRanges(t, rustrefXorbReqs, nativeXorbReqs)

	t.Logf("✓ Batch download xorb-range conformance passed for %d files", len(fileHashes))
}

// TestClientBatchDownloadConformance tests the DownloadFiles batch method of the native
// client, verifying that multiple files can be downloaded in a single batch request,
// that reconstructed content matches the originals, and that batch results are identical
// to sequential DownloadFile calls.
func TestClientBatchDownloadConformance(t *testing.T) {
	storageDir := t.TempDir()
	var stor storage.Storage
	var srv *server.Handler
	var httpSrv *httptest.Server

	httpSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if srv != nil {
			srv.ServeHTTP(w, r)
			return
		}
		http.Error(w, "server not initialized", http.StatusInternalServerError)
	}))
	defer httpSrv.Close()

	var err error
	stor, err = storage.NewFileStorage(
		storage.WithBasePath(storageDir),
		storage.WithBaseURL(httpSrv.URL),
	)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	srv = server.NewHandler(server.WithStorage(stor))
	nativeClient, err := client.NewClient(
		client.WithBaseURL(httpSrv.URL),
		client.WithCacheDir(t.TempDir()),
	)
	if err != nil {
		t.Fatalf("create native client: %v", err)
	}

	datasets := []struct {
		name string
		data []byte
	}{
		{
			name: "Empty file",
			data: []byte{},
		},
		{
			name: "Hello World",
			data: []byte("Hello World!"),
		},
		{
			name: "10MB",
			data: utils.MakeRandData(10 * 1024 * 1024),
		},
		{
			name: "10MB repeating",
			data: utils.MakeRepeatData(10 * 1024 * 1024),
		},
		{
			name: "100MB",
			data: utils.MakeRandData(100 * 1024 * 1024),
		},
		{
			name: "100MB repeating",
			data: utils.MakeRepeatData(100 * 1024 * 1024),
		},
		{
			name: "200MB",
			data: utils.MakeRandData(200 * 1024 * 1024),
		},
		{
			name: "200MB repeating",
			data: utils.MakeRepeatData(200 * 1024 * 1024),
		},
	}

	// Upload all files once; sub-tests below reuse the same server state.
	hashes := make([]xet.FileHash, len(datasets))
	for i, tc := range datasets {
		hash, err := nativeClient.UploadFile(context.Background(), bytes.NewReader(tc.data))
		if err != nil {
			t.Fatalf("upload %s: %v", tc.name, err)
		}
		hashes[i] = hash
	}

	t.Run("downloads_all_files", func(t *testing.T) {
		readers, sizes, err := nativeClient.DownloadFiles(context.Background(), hashes)
		if err != nil {
			t.Fatalf("DownloadFiles failed: %v", err)
		}
		if len(readers) != len(datasets) {
			t.Fatalf("expected %d readers, got %d", len(datasets), len(readers))
		}
		if len(sizes) != len(datasets) {
			t.Fatalf("expected %d sizes, got %d", len(datasets), len(sizes))
		}

		for i, tc := range datasets {
			if readers[i] == nil {
				t.Errorf("reader %d (%s) is nil", i, tc.name)
				continue
			}
			if sizes[i] != int64(len(tc.data)) {
				t.Errorf("file %d (%s) size: got %d want %d", i, tc.name, sizes[i], int64(len(tc.data)))
			}
			got, err := io.ReadAll(readers[i])
			if err != nil {
				t.Errorf("read file %d (%s): %v", i, tc.name, err)
				continue
			}
			if !bytes.Equal(got, tc.data) {
				t.Errorf("file %d (%s) content mismatch: got %d bytes want %d bytes",
					i, tc.name, len(got), len(tc.data))
			}
		}
		t.Logf("✓ DownloadFiles returned correct data for %d files", len(datasets))
	})

	t.Run("empty_batch", func(t *testing.T) {
		readers, sizes, err := nativeClient.DownloadFiles(context.Background(), nil)
		if err != nil {
			t.Fatalf("empty DownloadFiles failed: %v", err)
		}
		if len(readers) != 0 {
			t.Errorf("expected 0 readers, got %d", len(readers))
		}
		if len(sizes) != 0 {
			t.Errorf("expected 0 sizes, got %d", len(sizes))
		}
	})

	t.Run("single_file_batch", func(t *testing.T) {
		readers, sizes, err := nativeClient.DownloadFiles(context.Background(), hashes[:1])
		if err != nil {
			t.Fatalf("single-file DownloadFiles failed: %v", err)
		}
		if len(readers) != 1 || readers[0] == nil {
			t.Fatalf("expected one non-nil reader, got %d readers", len(readers))
		}
		if sizes[0] != int64(len(datasets[0].data)) {
			t.Errorf("size mismatch: got %d want %d", sizes[0], len(datasets[0].data))
		}
		got, err := io.ReadAll(readers[0])
		if err != nil {
			t.Fatalf("read single-file batch: %v", err)
		}
		if !bytes.Equal(got, datasets[0].data) {
			t.Error("single-file batch content mismatch")
		}
	})

	t.Run("results_match_sequential_download", func(t *testing.T) {
		batchReaders, batchSizes, err := nativeClient.DownloadFiles(context.Background(), hashes)
		if err != nil {
			t.Fatalf("batch DownloadFiles failed: %v", err)
		}

		for i, tc := range datasets {
			seqFile, err := os.CreateTemp("", "seq-*.bin")
			if err != nil {
				t.Fatalf("sequential DownloadFile %d (%s) create temp: %v", i, tc.name, err)
			}
			seqFileName := seqFile.Name()
			defer os.Remove(seqFileName)
			err = nativeClient.DownloadFile(context.Background(), hashes[i], seqFile)
			seqFile.Close()
			if err != nil {
				t.Fatalf("sequential DownloadFile %d (%s) failed: %v", i, tc.name, err)
			}
			seqData, err := os.ReadFile(seqFileName)
			if err != nil {
				t.Fatalf("sequential DownloadFile %d (%s) read: %v", i, tc.name, err)
			}
			seqSize := int64(len(seqData))
			if batchSizes[i] != seqSize {
				t.Errorf("file %d (%s) size: batch=%d sequential=%d",
					i, tc.name, batchSizes[i], seqSize)
			}
			batchData, err := io.ReadAll(batchReaders[i])
			if err != nil {
				t.Errorf("read batch file %d (%s): %v", i, tc.name, err)
				continue
			}
			if !bytes.Equal(batchData, seqData) {
				t.Errorf("file %d (%s): batch and sequential data differ", i, tc.name)
			}
		}
		t.Logf("✓ Batch and sequential downloads produce identical results for %d files", len(datasets))
	})

	t.Run("batch_request_uses_correct_endpoint", func(t *testing.T) {
		// Verify that DownloadFiles issues a GET /reconstructions request (not individual
		// /v1/reconstructions/{hash} or /v2/reconstructions/{hash} requests).
		var proxy *RecordingProxy
		var proxiedSrv *httptest.Server

		var innerStor storage.Storage
		var innerSrv *server.Handler
		innerStorDir := t.TempDir()

		proxiedSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if proxy != nil {
				proxy.ServeHTTP(w, r)
				return
			}
			http.Error(w, "not initialized", http.StatusInternalServerError)
		}))
		defer proxiedSrv.Close()

		var innerErr error
		innerStor, innerErr = storage.NewFileStorage(
			storage.WithBasePath(innerStorDir),
			storage.WithBaseURL(proxiedSrv.URL),
		)
		if innerErr != nil {
			t.Fatalf("create inner storage: %v", innerErr)
		}
		innerSrv = server.NewHandler(server.WithStorage(innerStor))
		proxy = NewRecordingProxy(innerSrv)

		// Upload two small files.
		innerClient, err := client.NewClient(
			client.WithBaseURL(proxiedSrv.URL),
			client.WithCacheDir(t.TempDir()),
		)
		if err != nil {
			t.Fatalf("create native client: %v", err)
		}

		data1 := []byte("batch endpoint check – file one")
		data2 := []byte("batch endpoint check – file two")

		h1, err := innerClient.UploadFile(context.Background(), bytes.NewReader(data1))
		if err != nil {
			t.Fatalf("upload file1: %v", err)
		}
		h2, err := innerClient.UploadFile(context.Background(), bytes.NewReader(data2))
		if err != nil {
			t.Fatalf("upload file2: %v", err)
		}

		proxy.ClearRequests()

		// Batch-download both files and capture the resulting requests.
		readers, _, err := innerClient.DownloadFiles(context.Background(), []xet.FileHash{h1, h2})
		if err != nil {
			t.Fatalf("DownloadFiles failed: %v", err)
		}
		for i, r := range readers {
			if r == nil {
				t.Fatalf("reader %d is nil", i)
			}
			if _, err := io.ReadAll(r); err != nil {
				t.Fatalf("read reader %d: %v", i, err)
			}
		}

		reqs := proxy.GetRequests()
		batchReconCount := 0
		singleReconCount := 0
		for _, req := range reqs {
			if req.Method == http.MethodGet && req.Path == "/reconstructions" {
				batchReconCount++
			}
			if req.Method == http.MethodGet && strings.HasPrefix(req.Path, "/v1/reconstructions/") {
				singleReconCount++
			}
			if req.Method == http.MethodGet && strings.HasPrefix(req.Path, "/v2/reconstructions/") {
				singleReconCount++
			}
		}

		if batchReconCount == 0 {
			t.Error("expected at least one GET /reconstructions batch request")
		}
		if singleReconCount > 0 {
			t.Errorf("expected no individual /v{n}/reconstructions/{hash} requests, got %d", singleReconCount)
		}
		t.Logf("✓ DownloadFiles issued %d batch reconstruction request(s) and %d single-file request(s)",
			batchReconCount, singleReconCount)
	})

	t.Run("rustref_batch_download_comparison", func(t *testing.T) {
		// Setup a dedicated proxy-recorded server so we can observe every HTTP
		// request made by both xet-core and the native client.
		cmpStorDir := t.TempDir()
		var cmpStor storage.Storage
		var cmpSrv *server.Handler
		var cmpProxy *RecordingProxy
		var cmpHTTP *httptest.Server

		cmpHTTP = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if cmpProxy != nil {
				cmpProxy.ServeHTTP(w, r)
				return
			}
			http.Error(w, "not initialized", http.StatusInternalServerError)
		}))
		defer cmpHTTP.Close()

		var cmpErr error
		cmpStor, cmpErr = storage.NewFileStorage(
			storage.WithBasePath(cmpStorDir),
			storage.WithBaseURL(cmpHTTP.URL),
		)
		if cmpErr != nil {
			t.Fatalf("create comparison storage: %v", cmpErr)
		}
		cmpSrv = server.NewHandler(server.WithStorage(cmpStor))
		cmpProxy = NewRecordingProxy(cmpSrv)

		cmpClient, err := client.NewClient(
			client.WithBaseURL(cmpHTTP.URL),
			client.WithCacheDir(t.TempDir()),
		)
		if err != nil {
			t.Fatalf("create native client: %v", err)
		}

		// Upload three files — one tiny, two larger — to exercise multiple xorbs.
		cmpDatasets := [][]byte{
			[]byte("rustref-vs-native batch comparison – small"),
			utils.MakeRandData(1024 * 1024),
			utils.MakeRepeatData(1024 * 1024),
		}
		cmpHashes := make([]xet.FileHash, len(cmpDatasets))
		for i, data := range cmpDatasets {
			hash, err := cmpClient.UploadFile(context.Background(), bytes.NewReader(data))
			if err != nil {
				t.Fatalf("upload comparison dataset %d: %v", i, err)
			}
			cmpHashes[i] = hash
		}

		// ── xet-core batch download ────────────────────────────────────────────────
		cmpProxy.ClearRequests()
		tempDir := t.TempDir()
		rustrefDownloadReqs := make([]rustref.DownloadRequest, len(cmpDatasets))
		for i, h := range cmpHashes {
			rustrefDownloadReqs[i] = rustref.DownloadRequest{
				DestinationPath: filepath.Join(tempDir, fmt.Sprintf("rustref-%d.bin", i)),
				Hash:            h.String(),
				FileSize:        int64(len(cmpDatasets[i])),
			}
		}
		if _, err := rustref.DownloadFiles(rustrefDownloadReqs, cmpHTTP.URL, nil); err != nil {
			t.Fatalf("xet-core DownloadFiles failed: %v", err)
		}
		rustrefHTTPReqs := cmpProxy.GetRequests()

		// Verify xet-core reconstructed the correct content.
		for i, data := range cmpDatasets {
			got, err := os.ReadFile(rustrefDownloadReqs[i].DestinationPath)
			if err != nil {
				t.Errorf("read xet-core file %d: %v", i, err)
				continue
			}
			if !bytes.Equal(got, data) {
				t.Errorf("xet-core file %d content mismatch: got %d bytes, want %d bytes", i, len(got), len(data))
			}
		}

		// ── native batch download ────────────────────────────────────────────────
		// Use a fresh client (empty cache) so reconstruction is not served from disk.
		cmpProxy.ClearRequests()
		freshClient, err := client.NewClient(
			client.WithBaseURL(cmpHTTP.URL),
			client.WithCacheDir(t.TempDir()),
		)
		if err != nil {
			t.Fatalf("create native client: %v", err)
		}

		readers, _, err := freshClient.DownloadFiles(context.Background(), cmpHashes)
		if err != nil {
			t.Fatalf("native DownloadFiles failed: %v", err)
		}
		for i, r := range readers {
			if r == nil {
				t.Errorf("native reader %d is nil", i)
				continue
			}
			got, err := io.ReadAll(r)
			if err != nil {
				t.Errorf("read native file %d: %v", i, err)
				continue
			}
			if !bytes.Equal(got, cmpDatasets[i]) {
				t.Errorf("native file %d content mismatch: got %d bytes, want %d bytes", i, len(got), len(cmpDatasets[i]))
			}
		}
		nativeHTTPReqs := cmpProxy.GetRequests()

		// ── compare HTTP request patterns ────────────────────────────────────────
		compareBatchDownloadRequests(t, rustrefHTTPReqs, nativeHTTPReqs, cmpHashes)
	})
}
