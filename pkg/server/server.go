package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/wzshiming/xet/pkg/client"
	"github.com/wzshiming/xet/pkg/shard"
	"github.com/wzshiming/xet/pkg/xet"
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

// registerRoutes sets up all HTTP routes
func (s *Server) registerRoutes() {
	s.router.HandleFunc("/api/v1/reconstructions/{file_hash}", s.handleGetReconstruction).Methods(http.MethodGet)
	s.router.HandleFunc("/api/v1/xorbs/{namespace}/{xorb_hash}", s.handleUploadXorb).Methods(http.MethodPost)
	s.router.HandleFunc("/api/v1/xorbs/{namespace}/{xorb_hash}/data", s.handleDownloadXorb).Methods(http.MethodGet)
	s.router.HandleFunc("/api/v1/shards", s.handleUploadShard).Methods(http.MethodPost)
	s.router.HandleFunc("/api/v1/chunks/{namespace}/{chunk_hash}", s.handleQueryChunk).Methods(http.MethodGet)
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

// handleGetReconstruction handles GET /api/v1/reconstructions/{file_hash}
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
	response, err := s.buildReconstructionResponse(shard, fileHash, r.Header.Get("Range"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// buildReconstructionResponse builds a reconstruction response from a shard
func (s *Server) buildReconstructionResponse(sh *shard.Shard, fileHash xet.Hash, rangeHeader string) (*client.ReconstructionResponse, error) {
	// Find the file block for this file hash
	var fileBlock *shard.FileBlock
	for i := range sh.Files {
		if sh.Files[i].FileHash == fileHash {
			fileBlock = &sh.Files[i]
			break
		}
	}

	if fileBlock == nil {
		return nil, fmt.Errorf("file not found in shard")
	}

	// Parse range header if present
	var requestedStart, requestedEnd int64
	hasRange := false
	if rangeHeader != "" {
		hasRange = true
		rangeHeader = strings.TrimPrefix(rangeHeader, "bytes=")
		parts := strings.Split(rangeHeader, "-")
		if len(parts) == 2 {
			fmt.Sscanf(parts[0], "%d", &requestedStart)
			fmt.Sscanf(parts[1], "%d", &requestedEnd)
		}
	}

	response := &client.ReconstructionResponse{
		OffsetIntoFirstRange: 0,
		Terms:                []client.Term{},
		FetchInfo:            make(map[string][]client.FetchInfoEntry),
	}

	// Calculate cumulative byte positions for each term
	var currentByteOffset int64

	// Build terms from file data sequence entries
	for _, entry := range fileBlock.Entries {
		termStart := currentByteOffset
		termEnd := currentByteOffset + int64(entry.UnpackedSegBytes)

		// Skip terms that are completely outside the requested range
		if hasRange {
			if termEnd <= requestedStart {
				currentByteOffset = termEnd
				continue
			}
			if termStart > requestedEnd {
				break
			}
		}

		// Find the CAS block
		var casBlock *shard.CASBlock
		for i := range sh.CASInfos {
			if sh.CASInfos[i].CASHash == entry.CASHash {
				casBlock = &sh.CASInfos[i]
				break
			}
		}

		if casBlock == nil {
			currentByteOffset = termEnd
			continue
		}

		// Calculate offset into first term if this is the first included term
		if len(response.Terms) == 0 && hasRange && termStart < requestedStart {
			response.OffsetIntoFirstRange = requestedStart - termStart
		}

		term := client.Term{
			Hash:           entry.CASHash.String(),
			UnpackedLength: uint64(entry.UnpackedSegBytes),
			Range: client.ChunkRange{
				Start: entry.ChunkIndexStart,
				End:   entry.ChunkIndexEnd,
			},
		}
		response.Terms = append(response.Terms, term)

		// Build fetch info - calculate byte range for the chunks
		var startByte, endByte int64
		for i := uint32(0); i < entry.ChunkIndexEnd; i++ {
			if int(i) < len(casBlock.Chunks) {
				// ByteRangeStart gives us the cumulative offset
				if i == entry.ChunkIndexStart {
					startByte = int64(casBlock.Chunks[i].ByteRangeStart)
				}
				if i == entry.ChunkIndexEnd-1 {
					endByte = int64(casBlock.Chunks[i].ByteRangeStart + casBlock.Chunks[i].UnpackedSegBytes - 1)
				}
			}
		}

		xorbURL := s.storage.GetXorbURL("default", entry.CASHash)

		fetchEntry := client.FetchInfoEntry{
			Range: client.ChunkRange{
				Start: entry.ChunkIndexStart,
				End:   entry.ChunkIndexEnd,
			},
			URL: xorbURL,
			URLRange: client.ByteRange{
				Start: startByte,
				End:   endByte,
			},
		}

		xorbHashStr := entry.CASHash.String()
		response.FetchInfo[xorbHashStr] = append(response.FetchInfo[xorbHashStr], fetchEntry)

		currentByteOffset = termEnd
	}

	return response, nil
}

// handleUploadXorb handles POST /api/v1/xorbs/{namespace}/{xorb_hash}
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

	// Read xorb data
	xorbData, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	// Verify xorb format and hash
	deserializedXorb, err := xorb.Deserialize(xorbData)
	if err != nil {
		http.Error(w, "Invalid xorb format", http.StatusBadRequest)
		return
	}

	if deserializedXorb.Hash != xorbHash {
		http.Error(w, "Hash mismatch", http.StatusBadRequest)
		return
	}

	// Store xorb
	wasInserted, err := s.storage.StoreXorb(r.Context(), namespace, xorbHash, xorbData)
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

// handleDownloadXorb handles GET /api/v1/xorbs/{namespace}/{xorb_hash}/data
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

	// Get xorb data
	xorbData, err := s.storage.GetXorb(r.Context(), namespace, xorbHash)
	if err != nil {
		http.Error(w, "Xorb not found", http.StatusNotFound)
		return
	}

	// Handle range requests
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		s.handleRangeRequest(w, r, xorbData, rangeHeader)
		return
	}

	// Return full xorb
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(xorbData)))
	w.Write(xorbData)
}

// handleRangeRequest handles HTTP range requests
func (s *Server) handleRangeRequest(w http.ResponseWriter, r *http.Request, data []byte, rangeHeader string) {
	// Parse range header (simple implementation for bytes=start-end)
	rangeHeader = strings.TrimPrefix(rangeHeader, "bytes=")
	parts := strings.Split(rangeHeader, "-")

	if len(parts) != 2 {
		http.Error(w, "Invalid range", http.StatusRequestedRangeNotSatisfiable)
		return
	}

	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		http.Error(w, "Invalid range", http.StatusRequestedRangeNotSatisfiable)
		return
	}

	end, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		http.Error(w, "Invalid range", http.StatusRequestedRangeNotSatisfiable)
		return
	}

	if start < 0 || end >= int64(len(data)) || start > end {
		http.Error(w, "Range not satisfiable", http.StatusRequestedRangeNotSatisfiable)
		return
	}

	rangeData := data[start : end+1]

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
	w.Header().Set("Content-Length", strconv.Itoa(len(rangeData)))
	w.WriteHeader(http.StatusPartialContent)
	w.Write(rangeData)
}

// handleUploadShard handles POST /api/v1/shards
func (s *Server) handleUploadShard(w http.ResponseWriter, r *http.Request) {
	// Authenticate
	if !s.authenticate(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Read shard data
	shardData, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	// Deserialize shard
	shard, err := shard.Deserialize(shardData)
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

// handleQueryChunk handles GET /api/v1/chunks/{namespace}/{chunk_hash}
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
	shard, err := s.storage.GetShardByChunkHash(r.Context(), namespace, chunkHash)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// Serialize shard without footer (for API responses)
	shardData, err := shard.Serialize()
	if err != nil {
		http.Error(w, "Failed to serialize shard", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(shardData)
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
