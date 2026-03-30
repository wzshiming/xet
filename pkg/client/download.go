package client

import (
	"context"
	"fmt"
	"io"
	"math"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/pkg/xorb"
)

// DownloadSession represents a download session
type DownloadSession struct {
	client     *Client
	chunkCache map[xet.Hash][]byte
}

// DownloadSessionOptions configures a download session
type DownloadSessionOptions struct {
	Client        *Client
	EnableCaching bool
}

// NewDownloadSession creates a new download session
func NewDownloadSession(opts DownloadSessionOptions) *DownloadSession {
	var cache map[xet.Hash][]byte
	if opts.EnableCaching {
		cache = make(map[xet.Hash][]byte)
	}

	return &DownloadSession{
		client:     opts.Client,
		chunkCache: cache,
	}
}

// DownloadFile downloads and reconstructs a file from its hash, automatically falling back to V1 if V2 is not supported
func (s *DownloadSession) DownloadFile(ctx context.Context, fileHash xet.Hash, opts ...ReqOpt) (io.Reader, int64, error) {
	r, size, err := s.DownloadFileV2(ctx, fileHash, opts...)
	if err != nil {
		if err == errNotFound {
			return s.DownloadFileV1(ctx, fileHash, opts...)
		}
		return nil, 0, err
	}
	return r, size, nil
}

// DownloadFileV1 downloads and reconstructs a file from its hash
func (s *DownloadSession) DownloadFileV1(ctx context.Context, fileHash xet.Hash, opts ...ReqOpt) (io.Reader, int64, error) {
	// Step 1: Query reconstruction
	reconstruction, err := s.client.GetReconstructionV1(ctx, fileHash, opts...)
	if err != nil {
		return nil, 0, fmt.Errorf("query reconstruction: %w", err)
	}

	expectedLength := expectedLength(reconstruction)

	// Create a reader that reconstructs the file on-demand
	reader := &reconstructionReaderV1{
		session:        s,
		ctx:            ctx,
		reconstruction: reconstruction,
		skipBytes:      reconstruction.OffsetIntoFirstRange,
	}

	return reader, expectedLength, nil
}

// reconstructionReaderV1 implements io.Reader for V1 reconstruction
type reconstructionReaderV1 struct {
	session        *DownloadSession
	ctx            context.Context
	reconstruction *ReconstructionResponse
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
	var byteRange *ByteRange
	useChunksOnly := false

	if fetchInfo.URLRange.Start != 0 || fetchInfo.URLRange.End != 0 {
		byteRange = &fetchInfo.URLRange
		useChunksOnly = true
	}

	reqOpts := []ReqOpt{}
	if byteRange != nil {
		reqOpts = append(reqOpts, WithRange(byteRange.Start, byteRange.End))
	}

	xorbData, err := r.session.client.DownloadXorbData(r.ctx, fetchInfo.URL, reqOpts...)
	if err != nil {
		return fmt.Errorf("download xorb data: %w", err)
	}

	// Deserialize xorb
	var xorbObj *xorb.Xorb
	if useChunksOnly {
		xorbObj, err = xorb.DeserializeBytes(xorbData, true)
	} else {
		xorbObj, err = xorb.DeserializeBytes(xorbData, false)
	}
	if err != nil {
		return fmt.Errorf("deserialize xorb: %w", err)
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
	if r.session.chunkCache != nil {
		for i := term.Range.Start; i < term.Range.End; i++ {
			chunk := xorbObj.Chunks[i]
			r.session.chunkCache[chunk.Hash] = chunk.UncompressedData
		}
	}

	r.currentXorb = xorbObj
	r.chunkIdx = term.Range.Start
	r.chunkOffset = 0

	return nil
}

// DownloadFileV2 downloads and reconstructs a file from its hash using the V2 API
func (s *DownloadSession) DownloadFileV2(ctx context.Context, fileHash xet.Hash, opts ...ReqOpt) (io.Reader, int64, error) {
	reconstruction, err := s.client.GetReconstructionV2(ctx, fileHash, opts...)
	if err != nil {
		return nil, 0, fmt.Errorf("query reconstruction v2: %w", err)
	}

	expectedLength := expectedLengthV2(reconstruction)

	// Create a reader that reconstructs the file on-demand
	reader := &reconstructionReaderV2{
		session:        s,
		ctx:            ctx,
		reconstruction: reconstruction,
		skipBytes:      reconstruction.OffsetIntoFirstRange,
	}

	return reader, expectedLength, nil
}

// reconstructionReaderV2 implements io.Reader for V2 reconstruction
type reconstructionReaderV2 struct {
	session        *DownloadSession
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
	var byteRange *ByteRange
	useChunksOnly := false

	if matchedRange != nil && (matchedRange.Bytes.Start != 0 || matchedRange.Bytes.End != 0) {
		byteRange = &matchedRange.Bytes
		useChunksOnly = true
	}

	reqOpts := []ReqOpt{}
	if byteRange != nil {
		reqOpts = append(reqOpts, WithRange(byteRange.Start, byteRange.End))
	}

	xorbData, err := r.session.client.DownloadXorbData(r.ctx, fetchEntry.URL, reqOpts...)
	if err != nil {
		return fmt.Errorf("download xorb data: %w", err)
	}

	// Deserialize xorb
	var xorbObj *xorb.Xorb
	if useChunksOnly {
		xorbObj, err = xorb.DeserializeBytes(xorbData, true)
	} else {
		xorbObj, err = xorb.DeserializeBytes(xorbData, false)
	}
	if err != nil {
		return fmt.Errorf("deserialize xorb: %w", err)
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
	if r.session.chunkCache != nil {
		for i := localStart; i < localEnd; i++ {
			chunk := xorbObj.Chunks[i]
			r.session.chunkCache[chunk.Hash] = chunk.UncompressedData
		}
	}

	r.currentXorb = xorbObj
	r.localStart = localStart
	r.localEnd = localEnd
	r.chunkIdx = localStart
	r.chunkOffset = 0

	return nil
}

func expectedLengthV2(reconstruction *ReconstructionResponseV2) int64 {
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

func expectedLength(reconstruction *ReconstructionResponse) int64 {
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
