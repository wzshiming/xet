package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/download"
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
	// Defined in specification
	s.root.HandleFunc("/api/v1/reconstructions/{file_hash}", s.handleGetReconstruction).Methods(http.MethodGet)
	s.root.HandleFunc("/api/v1/xorbs/{namespace}/{xorb_hash}", s.handleUploadXorb).Methods(http.MethodPost)
	s.root.HandleFunc("/api/v1/chunks/{namespace}/{chunk_hash}", s.handleQueryChunk).Methods(http.MethodGet)
	s.root.HandleFunc("/api/v1/chunks/{namespace}:query", s.handleQueryChunksBatch).Methods(http.MethodPost)
	s.root.HandleFunc("/api/v1/shards", s.handleUploadShard).Methods(http.MethodPost)

	// Used by xet-core but not defined in spec
	s.root.HandleFunc("/v2/reconstructions/{file_hash}", s.handleGetReconstructionV2).Methods(http.MethodGet)
	s.root.HandleFunc("/v1/reconstructions/{file_hash}", s.handleGetReconstruction).Methods(http.MethodGet)
	s.root.HandleFunc("/v1/xorbs/{namespace}/{xorb_hash}", s.handleUploadXorb).Methods(http.MethodPost)
	s.root.HandleFunc("/v1/chunks/{namespace}/{chunk_hash}", s.handleQueryChunk).Methods(http.MethodGet)
	s.root.HandleFunc("/v1/chunks/{namespace}:query", s.handleQueryChunksBatch).Methods(http.MethodPost)

	// /v1/shards is defined in the spec as the upload endpoint for shards,
	// but xet-core actually uploads shards to /shards, so we support both.
	s.root.HandleFunc("/v1/shards", s.handleUploadShard).Methods(http.MethodPost)
	s.root.HandleFunc("/shards", s.handleUploadShard).Methods(http.MethodPost)

	// Download endpoint for xorb data, used by xet-core and the Go client.
	// Not defined in the spec, but we can support it without much effort since it's just serving raw stored xorb bytes.
	s.root.HandleFunc("/v1/xorbs/{namespace}/{xorb_hash}/data", s.handleDownloadXorb).Methods(http.MethodGet)

	s.root.NotFoundHandler = s.next
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

	fileHash, err := xet.ParseHash(fileHashStr)
	if err != nil {
		http.Error(w, "Invalid file hash", http.StatusBadRequest)
		return
	}

	// Get shard for this file
	shard, err := s.storage.GetShardByFileHash(r.Context(), fileHash)
	if err != nil {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	// Build reconstruction response
	response, err := download.BuildReconstructionResponseV1(r.Context(), s.storage, "default", shard, fileHash, r.Header.Get("Range"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleGetReconstructionV2 handles GET /v2/reconstructions/{file_hash}
func (s *Handler) handleGetReconstructionV2(w http.ResponseWriter, r *http.Request) {
	// Extract file hash from path using mux
	vars := mux.Vars(r)
	fileHashStr := vars["file_hash"]

	fileHash, err := xet.ParseHash(fileHashStr)
	if err != nil {
		http.Error(w, "Invalid file hash", http.StatusBadRequest)
		return
	}

	// Get shard for this file
	shard, err := s.storage.GetShardByFileHash(r.Context(), fileHash)
	if err != nil {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	// Build V2 reconstruction response
	response, err := download.BuildReconstructionResponseV2(r.Context(), s.storage, "default", shard, fileHash, r.Header.Get("Range"))
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

	// Extract parameters from path using mux
	vars := mux.Vars(r)
	namespace := vars["namespace"]
	xorbHashStr := vars["xorb_hash"]

	// Parse xorb hash
	xorbHash, err := xet.ParseHash(xorbHashStr)
	if err != nil {
		http.Error(w, "Invalid xorb hash", http.StatusBadRequest)
		return
	}

	deserializedXorb, err := upload.DecodeXorb(r.Body, xorbHash)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Store xorb directly. PutXorb will normalize to full format with footer.
	wasInserted, err := s.storage.PutXorb(r.Context(), namespace, deserializedXorb)
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

// handleDownloadXorb handles GET /v1/xorbs/{namespace}/{xorb_hash}/data
func (s *Handler) handleDownloadXorb(w http.ResponseWriter, r *http.Request) {
	// Extract parameters from path using mux
	vars := mux.Vars(r)
	namespace := vars["namespace"]
	xorbHashStr := vars["xorb_hash"]

	// Parse xorb hash
	xorbHash, err := xet.ParseHash(xorbHashStr)
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
	http.ServeContent(w, r, xorbHashStr, time.Time{}, xorbReader)
}

// handleUploadShard handles POST /shards
func (s *Handler) handleUploadShard(w http.ResponseWriter, r *http.Request) {
	// Authenticate
	if !s.authenticate(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Deserialize shard
	shard, err := upload.DecodeShard(r.Body)
	if err != nil {
		http.Error(w, "Invalid shard format", http.StatusBadRequest)
		return
	}

	// Store shard
	wasInserted, err := s.storage.PutShard(r.Context(), shard)
	if err != nil {
		http.Error(w, "Failed to store shard", http.StatusInternalServerError)
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

// handleQueryChunk handles GET /v1/chunks/{namespace}/{chunk_hash}
func (s *Handler) handleQueryChunk(w http.ResponseWriter, r *http.Request) {
	// Extract parameters from path using mux
	vars := mux.Vars(r)
	namespace := vars["namespace"]
	chunkHashStr := vars["chunk_hash"]

	// Parse chunk hash
	chunkHash, err := xet.ParseHash(chunkHashStr)
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

	// Serialize shard without footer (for API responses) and stream directly
	reader, err := upload.EncodeChunkQueryResponse(shardObj)
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

		chunkHash, err := xet.ParseHash(chunkHashStr)
		if err != nil {
			results = append(results, res)
			continue
		}

		shardObj, err := s.storage.GetShardByChunkHash(r.Context(), namespace, chunkHash)
		if err != nil || shardObj == nil || len(shardObj.CASInfos) == 0 || len(shardObj.CASInfos[0].Chunks) == 0 {
			results = append(results, res)
			continue
		}

		casBlock := shardObj.CASInfos[0]
		res.Found = true
		res.XorbHash = casBlock.CASHash.String()
		res.ChunkIndex = 0
		results = append(results, res)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(batchChunkDedupQueryResponse{Results: results})
}
