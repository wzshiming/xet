package server

import (
	"bytes"
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
	response, err := s.buildReconstructionResponse(r.Context(), "default", shard, fileHash, r.Header.Get("Range"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// buildReconstructionResponse builds a reconstruction response from a shard.
//
// URL ranges within the fetch_info entries point into the compressed-data
// stream of the stored xorb (i.e. the raw compressed bytes for each chunk,
// concatenated without the 8-byte per-chunk headers).  This matches the
// convention used by xet-core: ByteRangeStart is an offset into the
// header-stripped stream, and range requests to the xorb download endpoint
// are served from the same stripped stream (see handleDownloadXorb).
func (s *Server) buildReconstructionResponse(ctx context.Context, namespace string, sh *shard.Shard, fileHash xet.Hash, rangeHeader string) (*client.ReconstructionResponse, error) {
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

		// Build fetch info.
		//
		// URL ranges are byte offsets within the compressed-data stream of the
		// stored xorb (headers stripped).  Load the stored xorb and compute
		// the accurate ranges from the actual compressed chunk sizes.
		startByte, endByte := s.compressedDataRange(ctx, namespace, entry.CASHash, entry.ChunkIndexStart, entry.ChunkIndexEnd)

		xorbURL := s.storage.GetXorbURL(namespace, entry.CASHash)

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

// compressedDataRange returns the [start, end] byte range (inclusive) within
// the stored xorb binary for the given chunk range [chunkStart, chunkEnd).
// The returned range includes the 8-byte chunk header for each chunk, so that
// xet-core can parse the header (version, compressed/uncompressed size,
// compression type) when it downloads that byte range.
func (s *Server) compressedDataRange(ctx context.Context, namespace string, xorbHash xet.Hash, chunkStart, chunkEnd uint32) (startByte, endByte int64) {
	xorbObj, err := s.storage.GetXorb(ctx, namespace, xorbHash)
	if err != nil {
		return 0, 0
	}

	// Serialize to bytes to compute chunk offsets in the stored format.
	// We need the actual byte layout to calculate ranges for HTTP range requests.
	xorbData, err := xorb.SerializeBytes(xorbObj, false)
	if err != nil {
		return 0, 0
	}

	// Parse chunks to find byte offsets in the stored xorb.
	// The stored format for xet-core uploads is [header0(8)][data0][header1(8)][data1]...
	// where header = version(1) + compressedSize(3 LE) + comprType(1) + uncompressedSize(3 LE).
	type chunkSpan struct{ start, end int64 }
	var spans []chunkSpan
	offset := int64(0)
	data := xorbData

	for int(offset) < len(data) {
		if int(offset)+8 > len(data) {
			break
		}
		// Stop at XETBLOB footer (Go-client full format)
		if int(offset)+7 <= len(data) && string(data[offset:offset+7]) == xorb.XorbIdentifier {
			break
		}

		headerStart := offset

		// Read compressed size (3-byte LE, bytes 1-3 of the 8-byte header)
		compressedSize := int64(data[offset+1]) | int64(data[offset+2])<<8 | int64(data[offset+3])<<16
		offset += 8 // skip header

		if int(offset)+int(compressedSize) > len(data) {
			break
		}

		offset += compressedSize

		// The chunk span includes the header and the compressed payload.
		spans = append(spans, chunkSpan{start: headerStart, end: offset - 1})
	}

	if int(chunkStart) >= len(spans) || int(chunkEnd) > len(spans) || chunkStart >= chunkEnd {
		return 0, 0
	}

	return spans[chunkStart].start, spans[chunkEnd-1].end
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
	response, err := s.buildReconstructionResponseV2(r.Context(), "default", shard, fileHash, r.Header.Get("Range"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// buildReconstructionResponseV2 builds a V2 reconstruction response from a shard.
// The V2 format groups fetch ranges by xorb and combines consecutive chunk ranges
// into multi-range fetch entries for more efficient downloading.
func (s *Server) buildReconstructionResponseV2(ctx context.Context, namespace string, sh *shard.Shard, fileHash xet.Hash, rangeHeader string) (*client.ReconstructionResponseV2, error) {
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

	response := &client.ReconstructionResponseV2{
		OffsetIntoFirstRange: 0,
		Terms:                []client.Term{},
		Xorbs:                make(map[string][]client.XorbMultiRangeFetch),
	}

	// Calculate cumulative byte positions for each term
	var currentByteOffset int64

	// Build terms and group fetch info by xorb
	type fetchInfo struct {
		chunkStart uint32
		chunkEnd   uint32
		startByte  int64
		endByte    int64
	}
	xorbFetchRanges := make(map[string][]fetchInfo)

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

		// Calculate byte ranges for this term
		startByte, endByte := s.compressedDataRange(ctx, namespace, entry.CASHash, entry.ChunkIndexStart, entry.ChunkIndexEnd)

		// Collect fetch info grouped by xorb
		xorbHashStr := entry.CASHash.String()
		xorbFetchRanges[xorbHashStr] = append(xorbFetchRanges[xorbHashStr], fetchInfo{
			chunkStart: entry.ChunkIndexStart,
			chunkEnd:   entry.ChunkIndexEnd,
			startByte:  startByte,
			endByte:    endByte,
		})

		currentByteOffset = termEnd
	}

	// Convert grouped fetch info into V2 format
	// For now, we create one XorbMultiRangeFetch per xorb with all ranges
	// A more sophisticated implementation could group consecutive/nearby ranges
	for xorbHashStr, ranges := range xorbFetchRanges {
		xorbHash, _ := xet.ParseHash(xorbHashStr)
		xorbURL := s.storage.GetXorbURL(namespace, xorbHash)

		var descriptors []client.XorbRangeDescriptor
		for _, r := range ranges {
			descriptors = append(descriptors, client.XorbRangeDescriptor{
				Chunks: client.ChunkRange{
					Start: r.chunkStart,
					End:   r.chunkEnd,
				},
				Bytes: client.ByteRange{
					Start: r.startByte,
					End:   r.endByte,
				},
			})
		}

		multiRangeFetch := client.XorbMultiRangeFetch{
			URL:    xorbURL,
			Ranges: descriptors,
		}

		response.Xorbs[xorbHashStr] = []client.XorbMultiRangeFetch{multiRangeFetch}
	}

	return response, nil
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

	deserializedXorb, err := xorb.Deserialize(r.Body, true)
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

	// Read shard data
	shardData, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	// Deserialize shard
	shard, err := shard.Decode(bytes.NewReader(shardData))
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
