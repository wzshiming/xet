package reconstruction

import (
	"context"
	"fmt"
	"io"
	"math"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/pkg/xorb"
)

// XorbFetcher is an interface for downloading xorb objects with range requests
type XorbFetcher interface {
	FetchXorb(ctx context.Context, url string, rangeStart, rangeEnd int64) (*xorb.Xorb, error)
}

// ChunkCache is an interface for caching chunks
type ChunkCache interface {
	Get(hash xet.Hash) ([]byte, bool)
	Set(hash xet.Hash, data []byte)
}

// ReconstructionResponse represents the V1 response from the file reconstruction API
type ReconstructionResponse struct {
	OffsetIntoFirstRange int64                       `json:"offset_into_first_range"`
	Terms                []Term                      `json:"terms"`
	FetchInfo            map[string][]FetchInfoEntry `json:"fetch_info"`
}

// ReconstructionResponseV2 represents the V2 response from the file reconstruction API
type ReconstructionResponseV2 struct {
	OffsetIntoFirstRange int64                            `json:"offset_into_first_range"`
	Terms                []Term                           `json:"terms"`
	Xorbs                map[string][]XorbMultiRangeFetch `json:"xorbs"`
}

// Term represents a single term in the file reconstruction
type Term struct {
	Hash           string     `json:"hash"`
	UnpackedLength uint64     `json:"unpacked_length"`
	Range          ChunkRange `json:"range"`
}

// ChunkRange represents a chunk index range [start, end)
type ChunkRange struct {
	Start uint32 `json:"start"`
	End   uint32 `json:"end"` // Exclusive
}

// FetchInfoEntry represents fetch information for downloading xorb data
type FetchInfoEntry struct {
	Range    ChunkRange `json:"range"`
	URL      string     `json:"url"`
	URLRange ByteRange  `json:"url_range"`
}

// ByteRange represents a byte range [start, end] (inclusive)
type ByteRange struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"` // Inclusive
}

// XorbMultiRangeFetch represents a signed multi-range fetch entry covering multiple byte ranges for a xorb
type XorbMultiRangeFetch struct {
	URL    string                `json:"url"`
	Ranges []XorbRangeDescriptor `json:"ranges"`
}

// XorbRangeDescriptor describes a chunk/byte range within a xorb
type XorbRangeDescriptor struct {
	Chunks ChunkRange `json:"chunks"`
	Bytes  ByteRange  `json:"bytes"`
}

// ReaderV1 creates an io.Reader that reconstructs a file using V1 reconstruction format
func ReaderV1(ctx context.Context, reconstruction *ReconstructionResponse, fetcher XorbFetcher, cache ChunkCache) (io.Reader, int64) {
	expectedLength := ExpectedLength(reconstruction)

	reader := &reconstructionReaderV1{
		ctx:            ctx,
		reconstruction: reconstruction,
		fetcher:        fetcher,
		cache:          cache,
		skipBytes:      reconstruction.OffsetIntoFirstRange,
	}

	return reader, expectedLength
}

// reconstructionReaderV1 implements io.Reader for V1 reconstruction
type reconstructionReaderV1 struct {
	ctx            context.Context
	reconstruction *ReconstructionResponse
	fetcher        XorbFetcher
	cache          ChunkCache
	skipBytes      int64

	// State for reading
	termIdx     int
	chunkIdx    uint32
	chunkOffset int
	currentTerm *Term
	currentXorb *xorb.Xorb
	err         error
}

func (r *reconstructionReaderV1) Read(p []byte) (n int, err error) {
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
		if r.chunkIdx >= r.currentTerm.Range.End {
			r.currentTerm = nil
			r.currentXorb = nil
			r.termIdx++
			continue
		}

		// Read from current chunk
		chunk := r.currentXorb.Chunks[r.chunkIdx]

		// Apply skip for first chunk of first term
		data := chunk.UncompressedData
		if r.termIdx == 0 && r.chunkIdx == r.currentTerm.Range.Start && r.skipBytes > 0 {
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

func (r *reconstructionReaderV1) loadTerm() error {
	term := &r.reconstruction.Terms[r.termIdx]
	r.currentTerm = term

	// Parse xorb hash
	xorbHash, err := xet.ParseHash(term.Hash)
	if err != nil {
		return fmt.Errorf("parse xorb hash: %w", err)
	}

	// Get fetch info for this xorb
	fetchInfoList, ok := r.reconstruction.FetchInfo[term.Hash]
	if !ok || len(fetchInfoList) == 0 {
		return fmt.Errorf("no fetch info for xorb %s", term.Hash)
	}

	fetchInfo := fetchInfoList[0]

	// Determine if we should use URLRange for efficient partial download
	useChunksOnly := false
	rangeStart := int64(0)
	rangeEnd := int64(0)

	if fetchInfo.URLRange.Start != 0 || fetchInfo.URLRange.End != 0 {
		rangeStart = fetchInfo.URLRange.Start
		rangeEnd = fetchInfo.URLRange.End
		useChunksOnly = true
	}

	xorbObj, err := r.fetcher.FetchXorb(r.ctx, fetchInfo.URL, rangeStart, rangeEnd)
	if err != nil {
		return fmt.Errorf("download xorb: %w", err)
	}

	// Verify xorb hash only when we have the full xorb
	if !useChunksOnly && xorbObj.Hash != xorbHash {
		return fmt.Errorf("xorb hash mismatch: expected %s, got %s", xorbHash.String(), xorbObj.Hash.String())
	}

	// Validate chunk range
	if term.Range.End > uint32(len(xorbObj.Chunks)) {
		return fmt.Errorf("chunk range out of bounds: [%d, %d) vs %d chunks", term.Range.Start, term.Range.End, len(xorbObj.Chunks))
	}

	// Cache chunks if enabled
	if r.cache != nil {
		for i := term.Range.Start; i < term.Range.End; i++ {
			chunk := xorbObj.Chunks[i]
			r.cache.Set(chunk.Hash, chunk.UncompressedData)
		}
	}

	r.currentXorb = xorbObj
	r.chunkIdx = term.Range.Start
	r.chunkOffset = 0

	return nil
}

// ReaderV2 creates an io.Reader that reconstructs a file using V2 reconstruction format
func ReaderV2(ctx context.Context, reconstruction *ReconstructionResponseV2, fetcher XorbFetcher, cache ChunkCache) (io.Reader, int64) {
	expectedLength := ExpectedLengthV2(reconstruction)

	reader := &reconstructionReaderV2{
		ctx:            ctx,
		reconstruction: reconstruction,
		fetcher:        fetcher,
		cache:          cache,
		skipBytes:      reconstruction.OffsetIntoFirstRange,
	}

	return reader, expectedLength
}

// reconstructionReaderV2 implements io.Reader for V2 reconstruction
type reconstructionReaderV2 struct {
	ctx            context.Context
	reconstruction *ReconstructionResponseV2
	fetcher        XorbFetcher
	cache          ChunkCache
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

func (r *reconstructionReaderV2) Read(p []byte) (n int, err error) {
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

func (r *reconstructionReaderV2) loadTerm() error {
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
	useChunksOnly := false
	rangeStart := int64(0)
	rangeEnd := int64(0)

	if matchedRange != nil && (matchedRange.Bytes.Start != 0 || matchedRange.Bytes.End != 0) {
		rangeStart = matchedRange.Bytes.Start
		rangeEnd = matchedRange.Bytes.End
		useChunksOnly = true
	}

	xorbObj, err := r.fetcher.FetchXorb(r.ctx, fetchEntry.URL, rangeStart, rangeEnd)
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
	if r.cache != nil {
		for i := localStart; i < localEnd; i++ {
			chunk := xorbObj.Chunks[i]
			r.cache.Set(chunk.Hash, chunk.UncompressedData)
		}
	}

	r.currentXorb = xorbObj
	r.localStart = localStart
	r.localEnd = localEnd
	r.chunkIdx = localStart
	r.chunkOffset = 0

	return nil
}

// ExpectedLength calculates the expected length of reconstructed data from V1 response
func ExpectedLength(reconstruction *ReconstructionResponse) int64 {
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

// ExpectedLengthV2 calculates the expected length of reconstructed data from V2 response
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
