package download

import (
	"context"
	"fmt"
	"io"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/pkg/client"
	"github.com/wzshiming/xet/pkg/xorb"
)

// Session represents a download session
type Session struct {
	client     *client.Client
	chunkCache map[xet.Hash][]byte
}

// SessionOptions configures a download session
type SessionOptions struct {
	Client        *client.Client
	EnableCaching bool
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
func (s *Session) DownloadFile(ctx context.Context, fileHash xet.Hash) (io.Reader, error) {
	// Step 1: Query reconstruction
	reconstruction, err := s.client.GetReconstruction(ctx, fileHash)
	if err != nil {
		return nil, fmt.Errorf("query reconstruction: %w", err)
	}

	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()

		err := s.wrtieTo(ctx, pw, reconstruction)
		if err != nil {
			pw.CloseWithError(err)
		}
	}()

	return pr, nil
}

// DownloadFileRange downloads and reconstructs a byte range of a file from its hash
func (s *Session) DownloadFileRange(ctx context.Context, fileHash xet.Hash, start, end int64) (io.Reader, error) {
	// Step 1: Query reconstruction with range
	reconstruction, err := s.client.GetReconstructionRange(ctx, fileHash, start, end)
	if err != nil {
		return nil, fmt.Errorf("query reconstruction range: %w", err)
	}

	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()

		err := s.wrtieTo(ctx, pw, reconstruction)
		if err != nil {
			pw.CloseWithError(err)
		}
	}()

	return pr, nil
}

func (s *Session) wrtieTo(ctx context.Context, w io.Writer, reconstruction *client.ReconstructionResponse) error {
	// Step 2: Download and process terms

	skipBytes := reconstruction.OffsetIntoFirstRange

	for termIdx, term := range reconstruction.Terms {
		// Parse xorb hash
		xorbHashStr := term.Hash
		xorbHash, err := xet.ParseHash(xorbHashStr)
		if err != nil {
			return fmt.Errorf("parse xorb hash: %w", err)
		}

		// Get fetch info for this xorb
		fetchInfoList, ok := reconstruction.FetchInfo[xorbHashStr]
		if !ok || len(fetchInfoList) == 0 {
			return fmt.Errorf("no fetch info for xorb %s", xorbHashStr)
		}

		// Download xorb data
		fetchInfo := fetchInfoList[0] // Usually only one entry per xorb

		// Determine if we should use URLRange for efficient partial download
		// URLRange points to the exact byte range in the compressed xorb data
		var byteRange *client.ByteRange
		useChunksOnly := false

		// Use URLRange if it's non-zero (indicating a partial range request)
		if fetchInfo.URLRange.Start != 0 || fetchInfo.URLRange.End != 0 {
			byteRange = &fetchInfo.URLRange
			useChunksOnly = true
		}

		xorbData, err := s.client.DownloadXorbData(ctx, fetchInfo.URL, byteRange)
		if err != nil {
			return fmt.Errorf("download xorb data: %w", err)
		}

		// Deserialize xorb
		var xorbObj *xorb.Xorb
		if useChunksOnly {
			// When downloading a range, we get only the chunk data without footer
			xorbObj, err = xorb.DeserializeChunksOnly(xorbData)
		} else {
			// Full xorb with footer
			xorbObj, err = xorb.Deserialize(xorbData)
		}
		if err != nil {
			return fmt.Errorf("deserialize xorb: %w", err)
		}

		// Verify xorb hash only when we have the full xorb
		if !useChunksOnly && xorbObj.Hash != xorbHash {
			return fmt.Errorf("xorb hash mismatch: expected %s, got %s", xorbHash.String(), xorbObj.Hash.String())
		}

		// Extract chunks in the range
		chunkStart := term.Range.Start
		chunkEnd := term.Range.End

		if chunkEnd > uint32(len(xorbObj.Chunks)) {
			return fmt.Errorf("chunk range out of bounds: [%d, %d) vs %d chunks", chunkStart, chunkEnd, len(xorbObj.Chunks))
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
				_, err := w.Write(chunk.UncompressedData[skipBytes:])
				if err != nil {
					return fmt.Errorf("write to output: %w", err)
				}
				skipBytes = 0
			} else {
				_, err := w.Write(chunk.UncompressedData)
				if err != nil {
					return fmt.Errorf("write to output: %w", err)
				}
			}
		}
	}
	return nil
}
