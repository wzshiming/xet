package download

import (
	"context"
	"fmt"

	"github.com/wzshiming/xet/pkg/client"
	"github.com/wzshiming/xet/pkg/xet"
	"github.com/wzshiming/xet/pkg/xorb"
)

// Session represents a download session
type Session struct {
	client     *client.Client
	chunkCache map[xet.Hash][]byte
}

// SessionOptions configures a download session
type SessionOptions struct {
	Client         *client.Client
	EnableCaching  bool
}

// NewSession creates a new download session
func NewSession(opts SessionOptions) *Session {
	var cache map[xet.Hash][]byte
	if opts.EnableCaching {
		cache = make(map[xet.Hash][]byte)
	}

	return &Session{
		client:     opts.Client,
		chunkCache: cache,
	}
}

// DownloadFile downloads and reconstructs a file from its hash
func (s *Session) DownloadFile(ctx context.Context, fileHash xet.Hash) ([]byte, error) {
	return s.DownloadFileRange(ctx, fileHash, 0, -1)
}

// DownloadFileRange downloads a byte range from a file
func (s *Session) DownloadFileRange(ctx context.Context, fileHash xet.Hash, start, length int64) ([]byte, error) {
	// Step 1: Query reconstruction
	var reconstruction *client.ReconstructionResponse
	var err error

	if length > 0 {
		end := start + length - 1
		reconstruction, err = s.client.GetReconstructionRange(ctx, fileHash, start, end)
	} else {
		reconstruction, err = s.client.GetReconstruction(ctx, fileHash)
	}

	if err != nil {
		return nil, fmt.Errorf("query reconstruction: %w", err)
	}

	// Step 2: Download and process terms
	var result []byte
	skipBytes := reconstruction.OffsetIntoFirstRange

	for termIdx, term := range reconstruction.Terms {
		// Parse xorb hash
		xorbHashStr := term.Hash
		xorbHash, err := xet.ParseHash(xorbHashStr)
		if err != nil {
			return nil, fmt.Errorf("parse xorb hash: %w", err)
		}

		// Get fetch info for this xorb
		fetchInfoList, ok := reconstruction.FetchInfo[xorbHashStr]
		if !ok || len(fetchInfoList) == 0 {
			return nil, fmt.Errorf("no fetch info for xorb %s", xorbHashStr)
		}

		// Download xorb data
		fetchInfo := fetchInfoList[0] // Usually only one entry per xorb
		xorbData, err := s.client.DownloadXorbData(ctx, fetchInfo.URL, &fetchInfo.URLRange)
		if err != nil {
			return nil, fmt.Errorf("download xorb data: %w", err)
		}

		// Deserialize xorb
		xorbObj, err := xorb.Deserialize(xorbData)
		if err != nil {
			return nil, fmt.Errorf("deserialize xorb: %w", err)
		}

		// Verify xorb hash
		if xorbObj.Hash != xorbHash {
			return nil, fmt.Errorf("xorb hash mismatch: expected %s, got %s", xorbHash.String(), xorbObj.Hash.String())
		}

		// Extract chunks in the range
		chunkStart := term.Range.Start
		chunkEnd := term.Range.End

		if chunkEnd > uint32(len(xorbObj.Chunks)) {
			return nil, fmt.Errorf("chunk range out of bounds: [%d, %d) vs %d chunks", chunkStart, chunkEnd, len(xorbObj.Chunks))
		}

		// Concatenate chunks
		for i := chunkStart; i < chunkEnd; i++ {
			chunk := xorbObj.Chunks[i]

			// Cache if enabled
			if s.chunkCache != nil {
				s.chunkCache[chunk.Hash] = chunk.UncompressedData
			}

			// Apply skip for first term
			if termIdx == 0 && skipBytes > 0 {
				if skipBytes >= int64(len(chunk.UncompressedData)) {
					skipBytes -= int64(len(chunk.UncompressedData))
					continue
				}
				result = append(result, chunk.UncompressedData[skipBytes:]...)
				skipBytes = 0
			} else {
				result = append(result, chunk.UncompressedData...)
			}
		}
	}

	// Step 3: Truncate to requested length if needed
	if length > 0 && int64(len(result)) > length {
		result = result[:length]
	}

	return result, nil
}

// GetCachedChunk retrieves a cached chunk if available
func (s *Session) GetCachedChunk(chunkHash xet.Hash) ([]byte, bool) {
	if s.chunkCache == nil {
		return nil, false
	}
	data, ok := s.chunkCache[chunkHash]
	return data, ok
}

// ClearCache clears the chunk cache
func (s *Session) ClearCache() {
	if s.chunkCache != nil {
		s.chunkCache = make(map[xet.Hash][]byte)
	}
}
