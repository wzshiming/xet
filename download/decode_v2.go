package download

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"math"

	"github.com/wzshiming/xet/internal/pool"
)

// ReaderV2 implements io.Reader for V2 reconstruction
type ReaderV2 struct {
	client         ClientAdapter
	ctx            context.Context
	reconstruction *ReconstructionResponseV2
	skipBytes      int64
	termFetches    []selectedFetch
	prefetcher     *prefetcher

	// State for reading
	termIdx      int
	chunkIdx     uint32
	chunkOffset  int
	currentTerm  *Term
	currentCache *chunkCache
	localStart   uint32
	localEnd     uint32
	err          error
}

// NewReaderV2 creates a new V2 reconstruction reader.
func NewReaderV2(ctx context.Context, client ClientAdapter, reconstruction *ReconstructionResponseV2, opts ...Option) (io.ReadCloser, error) {
	options := &options{}
	for _, opt := range opts {
		opt(options)
	}

	cache := options.cache
	if cache == nil {
		return nil, fmt.Errorf("no cache manager provided")
	}
	// Adopt pre-existing entries and clean up orphaned partial files before
	// the download starts; only the manager's first prepare walks the cache
	// directory.
	cache.prepare()

	termFetches, tasks, err := planReaderV2(reconstruction)
	if err != nil {
		return nil, fmt.Errorf("plan reader: %w", err)
	}

	prefetcher, err := newPrefetcher(ctx, client, termFetches, tasks, cache, options)
	if err != nil {
		return nil, fmt.Errorf("initialize prefetcher: %w", err)
	}

	return &ReaderV2{
		client:         client,
		ctx:            ctx,
		reconstruction: reconstruction,
		skipBytes:      reconstruction.OffsetIntoFirstRange,
		termFetches:    termFetches,
		prefetcher:     prefetcher,
	}, nil
}

func (r *ReaderV2) Read(p []byte) (n int, err error) {
	if r.err != nil {
		return 0, r.err
	}

	buf := pool.GetChunkBuf()
	defer pool.PutChunkBuf(buf)

	for n < len(p) {
		// Check if we're done with all terms
		if r.termIdx >= len(r.reconstruction.Terms) {
			r.cleanup()
			return n, io.EOF
		}

		// Load next term if needed
		if r.currentTerm == nil {
			if err := r.loadTerm(); err != nil {
				r.err = err
				r.cleanup()
				if n > 0 {
					return n, nil
				}
				return 0, err
			}
		}

		// Check if we're done with current term's chunks
		if r.chunkIdx >= r.localEnd {
			r.finishCurrentTerm()
			continue
		}

		// Get chunk on demand by index
		size, chunkErr := r.currentCache.Chunk(r.chunkIdx, buf[:])
		if chunkErr != nil {
			r.err = chunkErr
			r.cleanup()
			if n > 0 {
				return n, nil
			}
			return 0, chunkErr
		}

		data := buf[:size]

		// Apply skip for the first chunk of the first term
		if r.termIdx == 0 && r.chunkIdx == r.localStart && r.skipBytes > 0 {
			if r.skipBytes >= size {
				r.skipBytes -= size
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

			if r.chunkOffset >= len(data) {
				r.chunkIdx++
				r.chunkOffset = 0
			}
		} else {
			r.chunkIdx++
			r.chunkOffset = 0
		}
	}

	return n, nil
}

// cleanup closes the prefetcher, which owns and releases all caches.
func (r *ReaderV2) cleanup() {
	if r.prefetcher != nil {
		r.prefetcher.Close()
	}
	r.prefetcher = nil
	r.currentCache = nil
	r.currentTerm = nil
}

// Close releases the prefetcher and all cache references held by the reader.
// It is safe to call multiple times and after Read has returned io.EOF.
func (r *ReaderV2) Close() error {
	r.cleanup()
	if r.err == nil {
		r.err = fs.ErrClosed
	}
	return nil
}

// finishCurrentTerm advances to the next reconstruction term.
func (r *ReaderV2) finishCurrentTerm() {
	if r.currentTerm == nil {
		return
	}

	r.currentTerm = nil
	r.termIdx++
}

func (r *ReaderV2) loadTerm() error {
	term := &r.reconstruction.Terms[r.termIdx]
	r.currentTerm = term

	selected := r.termFetches[r.termIdx]
	cache, err := r.prefetcher.Get(selected.key)
	if err != nil {
		return fmt.Errorf("download xorb: %w", err)
	}

	localStart := term.Range.Start - selected.chunkStart
	localEnd := term.Range.End - selected.chunkStart

	r.currentCache = cache
	r.localStart = localStart
	r.localEnd = localEnd
	r.chunkIdx = localStart
	r.chunkOffset = 0

	return nil
}

func planReaderV2(reconstruction *ReconstructionResponseV2) ([]selectedFetch, []fetchTask, error) {
	selected := make([]selectedFetch, len(reconstruction.Terms))
	tasks := make([]fetchTask, 0, len(reconstruction.Terms))
	for i := range reconstruction.Terms {
		term := &reconstruction.Terms[i]
		fetch, rg, err := selectFetchInfoV2(reconstruction, term)
		if err != nil {
			return nil, nil, err
		}

		key := fetchKey{
			Hash:  term.Hash,
			Start: rg.Bytes.Start,
			End:   rg.Bytes.End,
		}

		selected[i] = selectedFetch{
			key:        key,
			chunkStart: rg.Chunks.Start,
			chunkEnd:   rg.Chunks.End,
		}
		tasks = append(tasks, fetchTask{
			key:        key,
			url:        fetch.URL,
			chunkStart: rg.Chunks.Start,
			chunkEnd:   rg.Chunks.End,
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
	seen := make(map[fetchKey]struct{}, len(tasks))
	var total int64
	for _, task := range tasks {
		if _, ok := seen[task.key]; ok {
			continue
		}
		seen[task.key] = struct{}{}

		start := task.key.Start
		end := task.key.End

		if end >= start {
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
