package download

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"

	"github.com/wzshiming/xet/xorb"
)

// ReaderV2 implements io.Reader for V2 reconstruction
type ReaderV2 struct {
	client         ClientAdapter
	ctx            context.Context
	reconstruction *ReconstructionResponseV2
	skipBytes      int64
	termFetches    []selectedFetchV2
	prefetcher     *xorbPrefetcher
	initErr        error

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

type selectedFetchV2 struct {
	key        string
	chunkStart uint32
	chunkEnd   uint32
}

// NewReaderV2 creates a new V2 reconstruction reader
func NewReaderV2(ctx context.Context, client ClientAdapter, reconstruction *ReconstructionResponseV2, opts ...func(*options)) io.Reader {
	options := &options{}
	for _, opt := range opts {
		opt(options)
	}

	termFetches, tasks, err := planReaderV2(reconstruction)
	return &ReaderV2{
		client:         client,
		ctx:            ctx,
		reconstruction: reconstruction,
		skipBytes:      reconstruction.OffsetIntoFirstRange,
		termFetches:    termFetches,
		prefetcher:     newXorbPrefetcher(ctx, client, tasks, options.concurrencyValue()),
		initErr:        err,
	}
}

func (r *ReaderV2) Read(p []byte) (n int, err error) {
	if r.initErr != nil {
		return 0, r.initErr
	}

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

	selected := r.termFetches[r.termIdx]
	xorbObj, err := r.prefetcher.Wait(selected.key)
	if err != nil {
		return fmt.Errorf("download xorb: %w", err)
	}

	localStart := term.Range.Start - selected.chunkStart
	localEnd := term.Range.End - selected.chunkStart

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

func planReaderV2(reconstruction *ReconstructionResponseV2) ([]selectedFetchV2, []xorbFetchTask, error) {
	selected := make([]selectedFetchV2, len(reconstruction.Terms))
	tasks := make([]xorbFetchTask, 0, len(reconstruction.Terms))
	for i := range reconstruction.Terms {
		term := &reconstruction.Terms[i]
		fetch, rg, err := selectFetchInfoV2(reconstruction, term)
		if err != nil {
			return nil, nil, err
		}

		key := fmt.Sprintf("%s|%s|%d|%d|%d|%d", term.Hash, fetch.URL, rg.Chunks.Start, rg.Chunks.End, rg.Bytes.Start, rg.Bytes.End)
		selected[i] = selectedFetchV2{
			key:        key,
			chunkStart: rg.Chunks.Start,
			chunkEnd:   rg.Chunks.End,
		}
		tasks = append(tasks, xorbFetchTask{
			key: key,
			url: fetch.URL,
			header: http.Header{
				"Range": []string{fmt.Sprintf("bytes=%d-%d", rg.Bytes.Start, rg.Bytes.End)},
			},
		})
	}
	return selected, tasks, nil
}

func selectFetchInfoV2(reconstruction *ReconstructionResponseV2, term *Term) (*XorbMultiRangeFetch, *XorbRangeDescriptor, error) {
	fetchList, ok := reconstruction.Xorbs[term.Hash]
	if !ok || len(fetchList) == 0 {
		return nil, nil, fmt.Errorf("no fetch info for xorb %s", term.Hash)
	}

	var selectedFetch *XorbMultiRangeFetch
	var selectedRange *XorbRangeDescriptor
	for i := range fetchList {
		for j := range fetchList[i].Ranges {
			rg := &fetchList[i].Ranges[j]
			if rg.Chunks.Start <= term.Range.Start && rg.Chunks.End >= term.Range.End {
				if selectedRange == nil || (rg.Bytes.End-rg.Bytes.Start) < (selectedRange.Bytes.End-selectedRange.Bytes.Start) {
					selectedFetch = &fetchList[i]
					selectedRange = rg
				}
			}
		}
	}

	if selectedFetch == nil || selectedRange == nil {
		return nil, nil, fmt.Errorf("no matching range descriptor for term chunk range [%d, %d)", term.Range.Start, term.Range.End)
	}

	return selectedFetch, selectedRange, nil
}

// ExpectedTransferBytesV2 computes the total compressed bytes that will be transferred
// over the network when reconstructing a V2 file. Deduplicated ranges are counted once.
func ExpectedTransferBytesV2(reconstruction *ReconstructionResponseV2) int64 {
	_, tasks, err := planReaderV2(reconstruction)
	if err != nil {
		return 0
	}
	seen := make(map[string]struct{}, len(tasks))
	var total int64
	for _, task := range tasks {
		if _, ok := seen[task.key]; ok {
			continue
		}
		seen[task.key] = struct{}{}
		rng := task.header.Get("Range")
		var start, end int64
		if n, _ := fmt.Sscanf(rng, "bytes=%d-%d", &start, &end); n == 2 && end >= start {
			total += end - start + 1
		}
	}
	return total
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
