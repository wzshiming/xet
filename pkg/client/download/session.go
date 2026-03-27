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
			return fmt.Errorf("no fetch info for xorb %s", xorbHash)
		}

		chunkStart := term.Range.Start
		chunkEnd := term.Range.End

		// Download only the needed byte range for this term
		fetchInfo, err := findFetchInfo(fetchInfoList, chunkStart, chunkEnd)
		if err != nil {
			return fmt.Errorf("select fetch info for xorb %s: %w", xorbHash, err)
		}

		xorbData, err := s.client.DownloadXorbData(ctx, fetchInfo.URL, &fetchInfo.URLRange)
		if err != nil {
			return fmt.Errorf("download xorb %s data: %w", xorbHash, err)
		}

		// Deserialize chunk data from the ranged response (no footer expected)
		xorbObj, err := xorb.DeserializeChunksOnly(xorbData)
		if err != nil {
			return fmt.Errorf("deserialize xorb %s range: %w", xorbHash, err)
		}

		// Ensure the ranged payload covers the expected chunk span
		chunkOffset := fetchInfo.Range.Start
		expectedChunks := int(fetchInfo.Range.End - fetchInfo.Range.Start)
		if len(xorbObj.Chunks) < expectedChunks {
			return fmt.Errorf("xorb %s range contained %d chunks, expected at least %d", xorbHash, len(xorbObj.Chunks), expectedChunks)
		}

		if int(chunkEnd-chunkOffset) > len(xorbObj.Chunks) {
			return fmt.Errorf("xorb %s range missing chunks for [%d, %d), have %d starting at %d", xorbHash, chunkStart, chunkEnd, len(xorbObj.Chunks), chunkOffset)
		}

		// Concatenate chunks
		for i := chunkStart; i < chunkEnd; i++ {
			relativeIdx := int(i - chunkOffset)
			if relativeIdx < 0 || relativeIdx >= len(xorbObj.Chunks) {
				return fmt.Errorf("chunk index %d out of range for xorb %s fetched data [%d, %d)", i, xorbHash, fetchInfo.Range.Start, fetchInfo.Range.End)
			}

			chunk := xorbObj.Chunks[relativeIdx]

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

func findFetchInfo(entries []client.FetchInfoEntry, chunkStart, chunkEnd uint32) (*client.FetchInfoEntry, error) {
	for i := range entries {
		entry := &entries[i]
		if entry.Range.Start <= chunkStart && entry.Range.End >= chunkEnd {
			return entry, nil
		}
	}
	return nil, fmt.Errorf("no fetch info covers chunk range [%d, %d)", chunkStart, chunkEnd)
}
