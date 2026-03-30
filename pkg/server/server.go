package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/pkg/client"
	"github.com/wzshiming/xet/pkg/download"
	"github.com/wzshiming/xet/pkg/shard"
	"github.com/wzshiming/xet/pkg/xorb"
)

// Server represents an XET CAS server
type Server struct {
	storage Storage
	router  *mux.Router
	authFn  AuthFunc
}

// AuthFunc is a function that validates authentication tokens
// It returns true if the token is valid
type AuthFunc func(token string) bool

// ServerOptions configures the server
type ServerOptions struct {
	Storage Storage
	AuthFn  AuthFunc // Optional authentication function
}

// NewServer creates a new XET CAS server
func NewServer(opts ServerOptions) *Server {
	s := &Server{
		storage: opts.Storage,
		router:  mux.NewRouter(),
		authFn:  opts.AuthFn,
	}

	s.registerRoutes()
	return s
}

// registerRoutes sets up all HTTP routes.
func (s *Server) registerRoutes() {
	// Defined in specification
	s.router.HandleFunc("/api/v1/reconstructions/{file_hash}", s.handleGetReconstruction).Methods(http.MethodGet)
	s.router.HandleFunc("/api/v1/xorbs/{namespace}/{xorb_hash}", s.handleUploadXorb).Methods(http.MethodPost)
	s.router.HandleFunc("/api/v1/chunks/{namespace}/{chunk_hash}", s.handleQueryChunk).Methods(http.MethodGet)
	s.router.HandleFunc("/api/v1/shards", s.handleUploadShard).Methods(http.MethodPost)

	// Used by xet-core but not defined in spec
	s.router.HandleFunc("/v2/reconstructions/{file_hash}", s.handleGetReconstructionV2).Methods(http.MethodGet)
	s.router.HandleFunc("/v1/reconstructions/{file_hash}", s.handleGetReconstruction).Methods(http.MethodGet)
	s.router.HandleFunc("/v1/xorbs/{namespace}/{xorb_hash}", s.handleUploadXorb).Methods(http.MethodPost)
	s.router.HandleFunc("/v1/chunks/{namespace}/{chunk_hash}", s.handleQueryChunk).Methods(http.MethodGet)

	// /v1/shards is defined in the spec as the upload endpoint for shards,
	// but xet-core actually uploads shards to /shards, so we support both.
	s.router.HandleFunc("/v1/shards", s.handleUploadShard).Methods(http.MethodPost)
	s.router.HandleFunc("/shards", s.handleUploadShard).Methods(http.MethodPost)

	// Download endpoint for xorb data, used by xet-core and the Go client.
	// Not defined in the spec, but we can support it without much effort since it's just serving raw stored xorb bytes.
	s.router.HandleFunc("/v1/xorbs/{namespace}/{xorb_hash}/data", s.handleDownloadXorb).Methods(http.MethodGet)
}

// ServeHTTP implements http.Handler
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Log request
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		fmt.Printf("[%s] %s %s - %v\n", r.Method, r.URL.Path, r.RemoteAddr, duration)
	}()

	s.router.ServeHTTP(w, r)
}

// authenticate checks if a request is authenticated
func (s *Server) authenticate(r *http.Request) bool {
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
func (s *Server) handleGetReconstruction(w http.ResponseWriter, r *http.Request) {
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
func (s *Server) handleGetReconstructionV2(w http.ResponseWriter, r *http.Request) {
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
func (s *Server) handleUploadXorb(w http.ResponseWriter, r *http.Request) {
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

	deserializedXorb, err := xorb.Decode(r.Body, true)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid xorb format: %v", err), http.StatusBadRequest)
		return
	}

	// Verify hash matches URL parameter
	if deserializedXorb.Hash != xorbHash {
		http.Error(w, fmt.Sprintf("Hash mismatch: xorb has %s, URL has %s", deserializedXorb.Hash.String(), xorbHash.String()), http.StatusBadRequest)
		return
	}

	// Store xorb directly. StoreXorb will normalize to full format with footer.
	wasInserted, err := s.storage.StoreXorb(r.Context(), namespace, deserializedXorb)
	if err != nil {
		http.Error(w, "Failed to store xorb", http.StatusInternalServerError)
		return
	}

	// Return response
	response := client.XorbUploadResponse{
		WasInserted: wasInserted,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleDownloadXorb handles GET /v1/xorbs/{namespace}/{xorb_hash}/data
func (s *Server) handleDownloadXorb(w http.ResponseWriter, r *http.Request) {
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
func (s *Server) handleUploadShard(w http.ResponseWriter, r *http.Request) {
	// Authenticate
	if !s.authenticate(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Deserialize shard
	shard, err := shard.Decode(r.Body)
	if err != nil {
		http.Error(w, "Invalid shard format", http.StatusBadRequest)
		return
	}

	// Store shard
	wasInserted, err := s.storage.StoreShard(r.Context(), shard)
	if err != nil {
		http.Error(w, "Failed to store shard", http.StatusInternalServerError)
		return
	}

	// Return response
	result := 0
	if wasInserted {
		result = 1
	}

	response := client.ShardUploadResponse{
		Result: result,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleQueryChunk handles GET /v1/chunks/{namespace}/{chunk_hash}
func (s *Server) handleQueryChunk(w http.ResponseWriter, r *http.Request) {
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
	reader, err := shard.Encode(shardObj, false)
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

// ListenAndServe starts the server
func (s *Server) ListenAndServe(addr string) error {
	fmt.Printf("Starting XET CAS server on %s\n", addr)
	return http.ListenAndServe(addr, s)
}

// ListenAndServeTLS starts the server with TLS
func (s *Server) ListenAndServeTLS(addr, certFile, keyFile string) error {
	fmt.Printf("Starting XET CAS server on %s (TLS)\n", addr)
	return http.ListenAndServeTLS(addr, certFile, keyFile, s)
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown(ctx context.Context) error {
	// Note: This would need to be implemented with http.Server
	// For now, this is a placeholder
	return nil
}
