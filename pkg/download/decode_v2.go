package download

import (
	"context"
	"fmt"
	"io"
	"math"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/pkg/xorb"
)

// ReaderV2 implements io.Reader for V2 reconstruction
type ReaderV2 struct {
	client         ClientAdapter
	chunkCache     map[xet.Hash][]byte
	ctx            context.Context
	reconstruction *ReconstructionResponseV2
	skipBytes      int64

	// State for reading
	termIdx     int
	chunkIdx    uint32
	chunkOffset int
	currentTerm *Term
	currentXorb *xorb.Xorb
	localStart  uint32
	localEnd    uint32
	err         error
}

// NewReaderV2 creates a new V2 reconstruction reader
func NewReaderV2(ctx context.Context, client ClientAdapter, reconstruction *ReconstructionResponseV2, chunkCache map[xet.Hash][]byte) io.Reader {
	return &ReaderV2{
		client:         client,
		chunkCache:     chunkCache,
		ctx:            ctx,
		reconstruction: reconstruction,
		skipBytes:      reconstruction.OffsetIntoFirstRange,
	}
}

func (r *ReaderV2) Read(p []byte) (n int, err error) {
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

func (r *ReaderV2) loadTerm() error {
	term := &r.reconstruction.Terms[r.termIdx]
	r.currentTerm = term

	// Parse xorb hash
	xorbHash, err := xet.ParseHash(term.Hash)
	if err != nil {
		return fmt.Errorf("parse xorb hash: %w", err)
	}

	// Get fetch info for this xorb
	fetchList, ok := r.reconstruction.Xorbs[term.Hash]
	if !ok || len(fetchList) == 0 {
		return fmt.Errorf("no fetch info for xorb %s", term.Hash)
	}

	fetchEntry := fetchList[0]

	// Find the range descriptor that covers this term's chunk range
	var matchedRange *XorbRangeDescriptor
	for i := range fetchEntry.Ranges {
		if fetchEntry.Ranges[i].Chunks.Start == term.Range.Start &&
			fetchEntry.Ranges[i].Chunks.End == term.Range.End {
			matchedRange = &fetchEntry.Ranges[i]
			break
		}
	}

	// Determine whether to issue a ranged download
	var byteRange *ByteRange
	useChunksOnly := false

	if matchedRange != nil && (matchedRange.Bytes.Start != 0 || matchedRange.Bytes.End != 0) {
		byteRange = &matchedRange.Bytes
		useChunksOnly = true
	}

	var reqOpts []interface{}
	if byteRange != nil {
		reqOpts = append(reqOpts, byteRange)
	}

	xorbObj, err := r.client.DownloadXorb(r.ctx, fetchEntry.URL, reqOpts...)
	if err != nil {
		return fmt.Errorf("download xorb: %w", err)
	}

	// Verify xorb hash only when we have the full xorb
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

	// Validate chunk range
	if localEnd > uint32(len(xorbObj.Chunks)) {
		return fmt.Errorf("chunk range out of bounds: [%d, %d) vs %d chunks", localStart, localEnd, len(xorbObj.Chunks))
	}

	// Cache chunks if enabled
	if r.chunkCache != nil {
		for i := localStart; i < localEnd; i++ {
			chunk := xorbObj.Chunks[i]
			r.chunkCache[chunk.Hash] = chunk.UncompressedData
		}
	}

	r.currentXorb = xorbObj
	r.localStart = localStart
	r.localEnd = localEnd
	r.chunkIdx = localStart
	r.chunkOffset = 0

	return nil
}

// ExpectedLengthV2 calculates the expected file length from V2 reconstruction
func ExpectedLengthV2(reconstruction *ReconstructionResponseV2) int64 {
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
