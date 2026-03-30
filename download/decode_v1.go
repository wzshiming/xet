package download

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"

	"github.com/wzshiming/xet/xorb"
)

// ReaderV1 implements io.Reader for V1 reconstruction
type ReaderV1 struct {
	client         ClientAdapter
	ctx            context.Context
	reconstruction *ReconstructionResponse
	skipBytes      int64

	// State for reading
	termIdx     int
	chunkIdx    uint32
	chunkOffset int
	currentTerm *Term
	currentXorb *xorb.Xorb
	localStart  uint32 // Local chunk index start (relative to downloaded xorb)
	localEnd    uint32 // Local chunk index end (relative to downloaded xorb)
	err         error
}

// NewReaderV1 creates a new V1 reconstruction reader
func NewReaderV1(ctx context.Context, client ClientAdapter, reconstruction *ReconstructionResponse) io.Reader {
	return &ReaderV1{
		client:         client,
		ctx:            ctx,
		reconstruction: reconstruction,
		skipBytes:      reconstruction.OffsetIntoFirstRange,
	}
}

func (r *ReaderV1) Read(p []byte) (n int, err error) {
	if r.err != nil {
		return 0, r.err
	}

	for n < len(p) {
		// Check if we're done with all terms
		if r.termIdx >= len(r.reconstruction.Terms) {
			return n, io.EOF
		}

		// Load next term if needed
		if r.currentTerm == nil {
			if err := r.loadTerm(); err != nil {
				r.err = err
				if n > 0 {
					return n, nil
				}
				return 0, err
			}
		}

		// Check if we're done with current term's chunks
		if r.chunkIdx >= r.localEnd {
			r.currentTerm = nil
			r.currentXorb = nil
			r.termIdx++
			continue
		}

		// Read from current chunk
		chunk := r.currentXorb.Chunks[r.chunkIdx]

		// Apply skip for first chunk of first term
		data := chunk.UncompressedData
		if r.termIdx == 0 && r.chunkIdx == r.localStart && r.skipBytes > 0 {
			if r.skipBytes >= int64(len(data)) {
				r.skipBytes -= int64(len(data))
				r.chunkIdx++
				r.chunkOffset = 0
				continue
			}
			data = data[r.skipBytes:]
			r.skipBytes = 0
		}

		// Copy data from current position
		if r.chunkOffset < len(data) {
			copied := copy(p[n:], data[r.chunkOffset:])
			n += copied
			r.chunkOffset += copied

			// If we've consumed this chunk, move to next
			if r.chunkOffset >= len(data) {
				r.chunkIdx++
				r.chunkOffset = 0
			}
		} else {
			// Move to next chunk
			r.chunkIdx++
			r.chunkOffset = 0
		}
	}

	return n, nil
}

func (r *ReaderV1) loadTerm() error {
	term := &r.reconstruction.Terms[r.termIdx]
	r.currentTerm = term

	// Get fetch info for this xorb
	fetchInfoList, ok := r.reconstruction.FetchInfo[term.Hash]
	if !ok || len(fetchInfoList) == 0 {
		return fmt.Errorf("no fetch info for xorb %s", term.Hash)
	}

	// Find the fetch info entry that covers this term's chunk range
	var fetchInfo *FetchInfoEntry
	for i := range fetchInfoList {
		if fetchInfoList[i].Range.Start == term.Range.Start && fetchInfoList[i].Range.End == term.Range.End {
			fetchInfo = &fetchInfoList[i]
			break
		}
	}

	if fetchInfo == nil {
		return fmt.Errorf("no matching fetch info for term chunk range [%d, %d)", term.Range.Start, term.Range.End)
	}

	// Determine if we should use URLRange for efficient partial download
	byteRange := &fetchInfo.URLRange

	// We need to pass the request opts to the client, but the interface expects interface{}
	// This will be handled by the actual client implementation
	header := http.Header{
		"Range": []string{fmt.Sprintf("bytes=%d-%d", byteRange.Start, byteRange.End)},
	}

	xorbObj, err := r.client.DownloadXorb(r.ctx, fetchInfo.URL, header)
	if err != nil {
		return fmt.Errorf("download xorb: %w", err)
	}

	// Convert absolute chunk indices to local indices within the downloaded xorb
	// The fetchInfo.Range describes the absolute chunk range that was downloaded
	// The downloaded xorb starts at chunk 0, so we need to subtract the offset
	localStart := term.Range.Start - fetchInfo.Range.Start
	localEnd := term.Range.End - fetchInfo.Range.Start

	// Validate chunk range
	if localEnd > uint32(len(xorbObj.Chunks)) {
		return fmt.Errorf("chunk range out of bounds: [%d, %d) vs %d chunks", localStart, localEnd, len(xorbObj.Chunks))
	}

	r.currentXorb = xorbObj
	r.localStart = localStart
	r.localEnd = localEnd
	r.chunkIdx = localStart
	r.chunkOffset = 0

	return nil
}

// ExpectedLengthV1 calculates the expected file length from V1 reconstruction
func ExpectedLengthV1(reconstruction *ReconstructionResponse) int64 {
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
