package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/download"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/upload"
)

// Handler represents an XET CAS server
type Handler struct {
	storage storage.Storage
	root    *mux.Router
	next    http.Handler
	authFn  AuthFunc
}

// AuthFunc is a function that validates authentication tokens
// It returns true if the token is valid
type AuthFunc func(token string) bool

// Option defines a functional option for configuring the Handler.
type Option func(*Handler)

// WithAuthFunc sets the authentication function for the server. If not set, the server will allow all requests.
func WithAuthFunc(authFn AuthFunc) Option {
	return func(h *Handler) {
		h.authFn = authFn
	}
}

// WithNext sets the next http.Handler to call if a request does not match any of the server's routes.
func WithNext(next http.Handler) Option {
	return func(h *Handler) {
		h.next = next
	}
}

// WithStorage sets the storage backend for the server. This is required for the server to function.
func WithStorage(storage storage.Storage) Option {
	return func(h *Handler) {
		h.storage = storage
	}
}

// NewHandler creates a new XET CAS server
func NewHandler(opts ...Option) *Handler {
	s := &Handler{
		storage: nil,
		root:    mux.NewRouter(),
		authFn:  nil,
	}

	for _, opt := range opts {
		opt(s)
	}

	s.registerRoutes()
	return s
}

// registerRoutes sets up all HTTP routes.
func (s *Handler) registerRoutes() {
	// Defined in specification but not used by xet-core, so we can leave these commented out for now.
	// s.root.HandleFunc("/api/v1/reconstructions/{file_hash}", s.handleGetReconstruction).Methods(http.MethodGet)
	// s.root.HandleFunc("/api/v1/xorbs/{namespace}/{xorb_hash}", s.handleUploadXorb).Methods(http.MethodPost)
	// s.root.HandleFunc("/api/v1/chunks/{namespace}/{chunk_hash}", s.handleQueryChunk).Methods(http.MethodGet)
	// s.root.HandleFunc("/api/v1/shards", s.handleUploadShard).Methods(http.MethodPost)

	// Used by xet-core but not defined in specification.
	s.root.HandleFunc("/v2/reconstructions/{file_hash}", s.handleGetReconstructionV2).Methods(http.MethodGet)
	s.root.HandleFunc("/v1/reconstructions/{file_hash}", s.handleGetReconstruction).Methods(http.MethodGet)
	s.root.HandleFunc("/reconstructions", s.handleBatchGetReconstruction).Methods(http.MethodGet)
	s.root.HandleFunc("/v1/xorbs/{namespace}/{xorb_hash}", s.handleUploadXorb).Methods(http.MethodPost)
	s.root.HandleFunc("/v1/xorbs/{namespace}/{xorb_hash}", s.handleHasXorb).Methods(http.MethodHead)
	s.root.HandleFunc("/v1/xorbs/{namespace}/{xorb_hash}", s.handleDownloadXorb).Methods(http.MethodGet)
	s.root.HandleFunc("/v1/chunks/{namespace}/{chunk_hash}", s.handleQueryChunk).Methods(http.MethodGet)
	s.root.HandleFunc("/v1/chunks/{namespace}:query", s.handleQueryChunksBatch).Methods(http.MethodPost)
	s.root.HandleFunc("/v2/shards", s.handleUploadShardV2).Methods(http.MethodPost)
	s.root.HandleFunc("/v1/shards", s.handleUploadShard).Methods(http.MethodPost)
	s.root.HandleFunc("/shards", s.handleUploadShard).Methods(http.MethodPost)
	s.root.HandleFunc("/xet-bridge/{sha256}", s.handleXetBridge).Methods(http.MethodGet, http.MethodHead)

	s.root.NotFoundHandler = s.next
}

// handleXetBridge reconstructs a complete file addressed by its SHA-256 digest.
func (s *Handler) handleXetBridge(w http.ResponseWriter, r *http.Request) {
	sh256Hash := mux.Vars(r)["sha256"]
	digestBytes, err := hex.DecodeString(sh256Hash)
	if err != nil || len(digestBytes) != sha256.Size {
		http.Error(w, "Invalid SHA-256 digest", http.StatusBadRequest)
		return
	}
	var digest [sha256.Size]byte
	copy(digest[:], digestBytes)

	content, err := s.storage.GetReconstructedFile(r.Context(), "default", digest)
	if err != nil {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	defer content.Close()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, sh256Hash))
	http.ServeContent(w, r, sh256Hash, time.Time{}, content)
}

// handleHasXorb handles HEAD /v1/xorbs/{namespace}/{xorb_hash}
func (s *Handler) handleHasXorb(w http.ResponseWriter, r *http.Request) {
	if !s.authenticate(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	vars := mux.Vars(r)
	namespace := vars["namespace"]
	xorbHashStr := vars["xorb_hash"]

	xorbHash, err := xet.ParseXorbHash(xorbHashStr)
	if err != nil {
		http.Error(w, "Invalid xorb hash", http.StatusBadRequest)
		return
	}

	exists, err := s.storage.HasXorb(r.Context(), namespace, xorbHash)
	if err != nil {
		http.Error(w, "Failed to check xorb", http.StatusInternalServerError)
		return
	}

	if !exists {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}

type batchChunkDedupQueryRequest struct {
	ChunkHashes []string `json:"chunk_hashes"`
}

type batchChunkDedupQueryResponse struct {
	Results []batchChunkDedupResult `json:"results"`
}

type batchChunkDedupResult struct {
	ChunkHash  string `json:"chunk_hash"`
	Found      bool   `json:"found"`
	XorbHash   string `json:"xorb_hash,omitempty"`
	ChunkIndex uint32 `json:"chunk_index,omitempty"`
}

// ServeHTTP implements http.Handler
func (s *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.root.ServeHTTP(w, r)
}

// authenticate checks if a request is authenticated
func (s *Handler) authenticate(r *http.Request) bool {
	if s.authFn == nil {
		return true // No authentication required
	}

	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return false
	}

	// Extract Bearer token
	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 || parts[0] != "Bearer" {
		return false
	}

	return s.authFn(parts[1])
}

// handleGetReconstruction handles GET /v1/reconstructions/{file_hash}
func (s *Handler) handleGetReconstruction(w http.ResponseWriter, r *http.Request) {
	// Extract file hash from path using mux
	vars := mux.Vars(r)
	fileHashStr := vars["file_hash"]

	fileHash, err := xet.ParseFileHash(fileHashStr)
	if err != nil {
		http.Error(w, "Invalid file hash", http.StatusBadRequest)
		return
	}

	// Get shard for this file
	shard, err := s.storage.GetShard(r.Context(), fileHash)
	if err != nil {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	// Build reconstruction response
	response, err := download.BuildReconstructionResponseV1(r.Context(), requestStorage(s.storage, r), "default", shard, fileHash, r.Header.Get("Range"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleBatchGetReconstruction handles GET /reconstructions?file_id=<hex>&file_id=<hex>&...
func (s *Handler) handleBatchGetReconstruction(w http.ResponseWriter, r *http.Request) {
	if !s.authenticate(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	fileIDStrs := r.URL.Query()["file_id"]
	if len(fileIDStrs) == 0 {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(&download.BatchReconstructionResponse{
			Files:     make(map[string][]download.Term),
			FetchInfo: make(map[string][]download.FetchInfoEntry),
		})
		return
	}

	fileHashes := make([]xet.FileHash, 0, len(fileIDStrs))
	for _, idStr := range fileIDStrs {
		h, err := xet.ParseFileHash(idStr)
		if err != nil {
			http.Error(w, fmt.Sprintf("invalid file_id %q: %v", idStr, err), http.StatusBadRequest)
			return
		}
		fileHashes = append(fileHashes, h)
	}

	batch := &download.BatchReconstructionResponse{
		Files:     make(map[string][]download.Term, len(fileHashes)),
		FetchInfo: make(map[string][]download.FetchInfoEntry),
	}

	for _, fileHash := range fileHashes {
		sh, err := s.storage.GetShard(r.Context(), fileHash)
		if err != nil {
			// Skip files not found; caller can check which hashes are absent.
			continue
		}
		single, err := download.BuildReconstructionResponseV1(r.Context(), requestStorage(s.storage, r), "default", sh, fileHash, "")
		if err != nil {
			continue
		}
		batch.Files[fileHash.String()] = single.Terms
		for xorbHash, entries := range single.FetchInfo {
			if _, exists := batch.FetchInfo[xorbHash]; !exists {
				batch.FetchInfo[xorbHash] = entries
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(batch)
}

// handleGetReconstructionV2 handles GET /v2/reconstructions/{file_hash}
func (s *Handler) handleGetReconstructionV2(w http.ResponseWriter, r *http.Request) {
	// Extract file hash from path using mux
	vars := mux.Vars(r)
	fileHashStr := vars["file_hash"]

	fileHash, err := xet.ParseFileHash(fileHashStr)
	if err != nil {
		http.Error(w, "Invalid file hash", http.StatusBadRequest)
		return
	}

	// Get shard for this file
	shard, err := s.storage.GetShard(r.Context(), fileHash)
	if err != nil {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	// Build V2 reconstruction response
	response, err := download.BuildReconstructionResponseV2(r.Context(), requestStorage(s.storage, r), "default", shard, fileHash, r.Header.Get("Range"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleUploadXorb handles POST /v1/xorbs/{namespace}/{xorb_hash}
func (s *Handler) handleUploadXorb(w http.ResponseWriter, r *http.Request) {
	// Authenticate
	if !s.authenticate(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if r.ContentLength <= 0 {
		http.Error(w, "Content-Length header required", http.StatusLengthRequired)
		return
	}

	// Extract parameters from path using mux
	vars := mux.Vars(r)
	namespace := vars["namespace"]
	xorbHashStr := vars["xorb_hash"]

	// Parse xorb hash
	xorbHash, err := xet.ParseXorbHash(xorbHashStr)
	if err != nil {
		http.Error(w, "Invalid xorb hash", http.StatusBadRequest)
		return
	}

	var body io.Reader = r.Body

	body = io.LimitReader(body, r.ContentLength)

	// Store xorb directly. PutXorb will normalize to full format with footer.
	wasInserted, err := s.storage.PutXorb(r.Context(), namespace, xorbHash, body)
	if err != nil {
		http.Error(w, "Failed to store xorb", http.StatusInternalServerError)
		return
	}

	// Return response
	response := upload.XorbUploadResponse{
		WasInserted: wasInserted,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleDownloadXorb handles GET /v1/xorbs/{namespace}/{xorb_hash}
func (s *Handler) handleDownloadXorb(w http.ResponseWriter, r *http.Request) {
	// Extract parameters from path using mux
	vars := mux.Vars(r)
	namespace := vars["namespace"]
	xorbHashStr := vars["xorb_hash"]

	// Parse xorb hash
	xorbHash, err := xet.ParseXorbHash(xorbHashStr)
	if err != nil {
		http.Error(w, "Invalid xorb hash", http.StatusBadRequest)
		return
	}

	// Get xorb object
	xorbReader, err := s.storage.GetXorbReadSeekCloser(r.Context(), namespace, xorbHash)
	if err != nil {
		http.Error(w, "Xorb not found", http.StatusNotFound)
		return
	}
	defer xorbReader.Close()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, xorbHash.String()))
	http.ServeContent(w, r, xorbHashStr, time.Time{}, xorbReader)
}

// handleUploadShard handles the legacy and v1 shard upload endpoints.
func (s *Handler) handleUploadShard(w http.ResponseWriter, r *http.Request) {
	// Authenticate
	if !s.authenticate(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if r.ContentLength <= 0 {
		http.Error(w, "Content-Length header required", http.StatusLengthRequired)
		return
	}

	wasInserted, status, err := s.storeUploadedShard(r)
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}

	// Return response
	result := 0
	if wasInserted {
		result = 1
	}

	response := upload.ShardUploadResponse{
		Result: result,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Handler) storeUploadedShard(r *http.Request) (bool, int, error) {
	body := io.LimitReader(r.Body, r.ContentLength)

	shardObj := shard.NewShard()
	if err := shardObj.Decode(body, false); err != nil {
		return false, http.StatusBadRequest, fmt.Errorf("invalid shard format: %w", err)
	}
	if err := shardObj.Validate(); err != nil {
		return false, http.StatusBadRequest, fmt.Errorf("invalid shard: %w", err)
	}
	for _, casBlock := range shardObj.CASInfos {
		exists, err := s.storage.HasXorb(r.Context(), "default", casBlock.CASHash)
		if err != nil || !exists {
			return false, http.StatusBadRequest, fmt.Errorf("invalid shard: referenced xorb not uploaded")
		}
	}

	wasInserted, err := s.storage.PutShard(r.Context(), shardObj)
	if err != nil {
		return false, http.StatusInternalServerError, fmt.Errorf("failed to store shard")
	}
	return wasInserted, http.StatusOK, nil
}

// handleQueryChunk handles GET /v1/chunks/{namespace}/{chunk_hash}
func (s *Handler) handleQueryChunk(w http.ResponseWriter, r *http.Request) {
	// Extract parameters from path using mux
	vars := mux.Vars(r)
	namespace := vars["namespace"]
	chunkHashStr := vars["chunk_hash"]

	// Parse chunk hash
	chunkHash, err := xet.ParseChunkHash(chunkHashStr)
	if err != nil {
		http.Error(w, "Invalid chunk hash", http.StatusBadRequest)
		return
	}

	// Query for chunk
	shardObj, err := s.storage.GetShardByChunkHash(r.Context(), namespace, chunkHash)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// Global dedup responses are persisted shard objects and include the footer
	// required by current xet-core clients.
	reader, err := shardObj.Encode(true)
	if err != nil {
		http.Error(w, "Failed to serialize shard", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	_, err = io.Copy(w, reader)
	if err != nil {
		// Error writing response, but headers already sent
		fmt.Fprintf(os.Stderr, "Error writing shard response: %v\n", err)
	}
}

// handleQueryChunksBatch handles POST /v1/chunks/{namespace}:query.
func (s *Handler) handleQueryChunksBatch(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	namespace := vars["namespace"]

	var reqBody batchChunkDedupQueryRequest
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	results := make([]batchChunkDedupResult, 0, len(reqBody.ChunkHashes))
	for _, chunkHashStr := range reqBody.ChunkHashes {
		res := batchChunkDedupResult{ChunkHash: chunkHashStr, Found: false}

		chunkHash, err := xet.ParseChunkHash(chunkHashStr)
		if err != nil {
			results = append(results, res)
			continue
		}

		shardObj, err := s.storage.GetShardByChunkHash(r.Context(), namespace, chunkHash)
		if err != nil || shardObj == nil {
			results = append(results, res)
			continue
		}

		xorbHash, chunkIndex, ok := findChunkLocationInShard(shardObj, chunkHash)
		if !ok {
			results = append(results, res)
			continue
		}

		res.Found = true
		res.XorbHash = xorbHash.String()
		res.ChunkIndex = chunkIndex
		results = append(results, res)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(batchChunkDedupQueryResponse{Results: results})
}

func findChunkLocationInShard(shardObj *shard.Shard, chunkHash xet.ChunkHash) (xet.XorbHash, uint32, bool) {
	for _, casBlock := range shardObj.CASInfos {
		for i, casChunk := range casBlock.Chunks {
			if casChunk.ChunkHash == chunkHash {
				return casBlock.CASHash, uint32(i), true
			}
		}
	}

	return xet.XorbHash{}, 0, false
}

type absoluteURLStorage struct {
	storage.Storage
	baseURL string
}

func requestStorage(stor storage.Storage, r *http.Request) download.StorageAdapter {
	return absoluteURLStorage{Storage: stor, baseURL: requestBaseURL(r)}
}

func (s absoluteURLStorage) GetXorbURL(namespace string, xorbHash xet.XorbHash) string {
	raw := s.Storage.GetXorbURL(namespace, xorbHash)
	u, err := url.Parse(raw)
	if err != nil || u.IsAbs() {
		return raw
	}
	base, err := url.Parse(s.baseURL + "/")
	if err != nil {
		return raw
	}
	return base.ResolveReference(u).String()
}

func requestBaseURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if forwarded := strings.TrimSpace(strings.Split(r.Header.Get("X-Forwarded-Proto"), ",")[0]); forwarded != "" {
		scheme = forwarded
	}
	return scheme + "://" + r.Host
}
