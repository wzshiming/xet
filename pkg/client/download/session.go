package download

import (
	"context"
	"fmt"
	"io"
	"math"

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
	reader, _, err := s.DownloadFileWithLength(ctx, fileHash)
	return reader, err
}

// DownloadFileWithLength downloads a file and returns a reader and the expected length.
// The length is computed from the reconstruction response, so it is available
// before any data is streamed to the caller.
func (s *Session) DownloadFileWithLength(ctx context.Context, fileHash xet.Hash) (io.Reader, int64, error) {
	// Step 1: Query reconstruction
	reconstruction, err := s.client.GetReconstruction(ctx, fileHash)
	if err != nil {
		return nil, 0, fmt.Errorf("query reconstruction: %w", err)
	}

	expectedLength := expectedLength(reconstruction)

	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()

		err := s.wrtieTo(ctx, pw, reconstruction)
		if err != nil {
			pw.CloseWithError(err)
		}
	}()

	return pr, expectedLength, nil
}

// DownloadFileRange downloads and reconstructs a byte range of a file from its hash
func (s *Session) DownloadFileRange(ctx context.Context, fileHash xet.Hash, start, end int64) (io.Reader, error) {
	reader, _, err := s.DownloadFileRangeWithLength(ctx, fileHash, start, end)
	return reader, err
}

// DownloadFileRangeWithLength downloads a byte range of a file and returns a reader and the expected length.
// The length is computed from the reconstruction response for the requested range.
func (s *Session) DownloadFileRangeWithLength(ctx context.Context, fileHash xet.Hash, start, end int64) (io.Reader, int64, error) {
	// Step 1: Query reconstruction with range
	reconstruction, err := s.client.GetReconstructionRange(ctx, fileHash, start, end)
	if err != nil {
		return nil, 0, fmt.Errorf("query reconstruction range: %w", err)
	}

	expectedLength := expectedLength(reconstruction)

	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()

		err := s.wrtieTo(ctx, pw, reconstruction)
		if err != nil {
			pw.CloseWithError(err)
		}
	}()

	return pr, expectedLength, nil
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

// DownloadFileV2 downloads and reconstructs a file from its hash using the V2 API
func (s *Session) DownloadFileV2(ctx context.Context, fileHash xet.Hash) (io.Reader, error) {
	reader, _, err := s.DownloadFileWithLengthV2(ctx, fileHash)
	return reader, err
}

// DownloadFileWithLengthV2 downloads a file using the V2 reconstruction API and returns
// a reader and the expected length.
func (s *Session) DownloadFileWithLengthV2(ctx context.Context, fileHash xet.Hash) (io.Reader, int64, error) {
	reconstruction, err := s.client.GetReconstructionV2(ctx, fileHash)
	if err != nil {
		return nil, 0, fmt.Errorf("query reconstruction v2: %w", err)
	}

	expectedLength := expectedLengthV2(reconstruction)

	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()

		err := s.writeToV2(ctx, pw, reconstruction)
		if err != nil {
			pw.CloseWithError(err)
		}
	}()

	return pr, expectedLength, nil
}

// DownloadFileRangeV2 downloads a byte range of a file using the V2 reconstruction API
func (s *Session) DownloadFileRangeV2(ctx context.Context, fileHash xet.Hash, start, end int64) (io.Reader, error) {
	reader, _, err := s.DownloadFileRangeWithLengthV2(ctx, fileHash, start, end)
	return reader, err
}

// DownloadFileRangeWithLengthV2 downloads a byte range of a file using the V2 reconstruction
// API and returns a reader and the expected length.
func (s *Session) DownloadFileRangeWithLengthV2(ctx context.Context, fileHash xet.Hash, start, end int64) (io.Reader, int64, error) {
	reconstruction, err := s.client.GetReconstructionRangeV2(ctx, fileHash, start, end)
	if err != nil {
		return nil, 0, fmt.Errorf("query reconstruction range v2: %w", err)
	}

	expectedLength := expectedLengthV2(reconstruction)

	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()

		err := s.writeToV2(ctx, pw, reconstruction)
		if err != nil {
			pw.CloseWithError(err)
		}
	}()

	return pr, expectedLength, nil
}

func (s *Session) writeToV2(ctx context.Context, w io.Writer, reconstruction *client.ReconstructionResponseV2) error {
	skipBytes := reconstruction.OffsetIntoFirstRange

	for termIdx, term := range reconstruction.Terms {
		xorbHashStr := term.Hash
		xorbHash, err := xet.ParseHash(xorbHashStr)
		if err != nil {
			return fmt.Errorf("parse xorb hash: %w", err)
		}

		// Get fetch info for this xorb
		fetchList, ok := reconstruction.Xorbs[xorbHashStr]
		if !ok || len(fetchList) == 0 {
			return fmt.Errorf("no fetch info for xorb %s", xorbHashStr)
		}

		fetchEntry := fetchList[0]

		// Find the range descriptor that covers this term's chunk range
		var matchedRange *client.XorbRangeDescriptor
		for i := range fetchEntry.Ranges {
			if fetchEntry.Ranges[i].Chunks.Start == term.Range.Start &&
				fetchEntry.Ranges[i].Chunks.End == term.Range.End {
				matchedRange = &fetchEntry.Ranges[i]
				break
			}
		}

		// Determine whether to issue a ranged download
		var byteRange *client.ByteRange
		useChunksOnly := false

		if matchedRange != nil && (matchedRange.Bytes.Start != 0 || matchedRange.Bytes.End != 0) {
			byteRange = &matchedRange.Bytes
			useChunksOnly = true
		}

		xorbData, err := s.client.DownloadXorbData(ctx, fetchEntry.URL, byteRange)
		if err != nil {
			return fmt.Errorf("download xorb data: %w", err)
		}

		var xorbObj *xorb.Xorb
		if useChunksOnly {
			xorbObj, err = xorb.DeserializeChunksOnly(xorbData)
		} else {
			xorbObj, err = xorb.Deserialize(xorbData)
		}
		if err != nil {
			return fmt.Errorf("deserialize xorb: %w", err)
		}

		if !useChunksOnly && xorbObj.Hash != xorbHash {
			return fmt.Errorf("xorb hash mismatch: expected %s, got %s", xorbHash.String(), xorbObj.Hash.String())
		}

		// When downloading a partial byte range the returned chunks are
		// re-indexed from 0, so map the term's absolute range to local indices.
		var localStart, localEnd uint32
		if useChunksOnly {
			localStart = 0
			localEnd = term.Range.End - term.Range.Start
		} else {
			localStart = term.Range.Start
			localEnd = term.Range.End
		}

		if localEnd > uint32(len(xorbObj.Chunks)) {
			return fmt.Errorf("chunk range out of bounds: [%d, %d) vs %d chunks", localStart, localEnd, len(xorbObj.Chunks))
		}

		for i := localStart; i < localEnd; i++ {
			chunk := xorbObj.Chunks[i]

			if s.chunkCache != nil {
				s.chunkCache[chunk.Hash] = chunk.UncompressedData
			}

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

func expectedLengthV2(reconstruction *client.ReconstructionResponseV2) int64 {
	var total uint64
	for _, term := range reconstruction.Terms {
		total += term.UnpackedLength
	}

	if reconstruction.OffsetIntoFirstRange <= 0 {
		if total > math.MaxInt64 {
			return math.MaxInt64
		}
		return int64(total)
	}

	if uint64(reconstruction.OffsetIntoFirstRange) >= total {
		return 0
	}

	remaining := total - uint64(reconstruction.OffsetIntoFirstRange)
	if remaining > math.MaxInt64 {
		return math.MaxInt64
	}

	return int64(remaining)
}

func expectedLength(reconstruction *client.ReconstructionResponse) int64 {
	var total uint64
	for _, term := range reconstruction.Terms {
		total += term.UnpackedLength
	}

	if reconstruction.OffsetIntoFirstRange <= 0 {
		if total > math.MaxInt64 {
			return math.MaxInt64
		}
		return int64(total)
	}

	if uint64(reconstruction.OffsetIntoFirstRange) >= total {
		return 0
	}

	remaining := total - uint64(reconstruction.OffsetIntoFirstRange)
	if remaining > math.MaxInt64 {
		return math.MaxInt64
	}

	return int64(remaining)
}
