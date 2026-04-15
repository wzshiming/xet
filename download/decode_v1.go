package download

import (
	"context"
	"fmt"
	"io"
	"math"
)

// ReaderV1 implements io.Reader for V1 reconstruction
type ReaderV1 struct {
	client         ClientAdapter
	ctx            context.Context
	reconstruction *ReconstructionResponseV1
	skipBytes      int64
	termFetches    []selectedFetch
	remainingUses  map[fetchKey]int
	prefetcher     *prefetcher

	// State for reading
	termIdx      int
	chunkIdx     uint32
	chunkOffset  int
	currentTerm  *Term
	currentCache *chunkCache
	localStart   uint32 // Local chunk index start (relative to downloaded xorb)
	localEnd     uint32 // Local chunk index end (relative to downloaded xorb)
	err          error
}

// NewReaderV1 creates a new V1 reconstruction reader
func NewReaderV1(ctx context.Context, client ClientAdapter, reconstruction *ReconstructionResponseV1, opts ...Option) (io.Reader, error) {
	options := &options{}
	for _, opt := range opts {
		opt(options)
	}

	termFetches, tasks, err := planReaderV1(reconstruction)
	if err != nil {
		return nil, fmt.Errorf("plan reader: %w", err)
	}

	prefetcher, err := newPrefetcher(ctx, client, termFetches, tasks, options)
	if err != nil {
		return nil, fmt.Errorf("initialize prefetcher: %w", err)
	}

	return &ReaderV1{
		client:         client,
		ctx:            ctx,
		reconstruction: reconstruction,
		skipBytes:      reconstruction.OffsetIntoFirstRange,
		termFetches:    termFetches,
		remainingUses:  countFetchUses(termFetches),
		prefetcher:     prefetcher,
	}, nil
}

func (r *ReaderV1) Read(p []byte) (n int, err error) {
	if r.err != nil {
		return 0, r.err
	}

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
		data, chunkErr := r.currentCache.Chunk(r.chunkIdx)
		if chunkErr != nil {
			r.err = chunkErr
			r.cleanup()
			if n > 0 {
				return n, nil
			}
			return 0, chunkErr
		}

		// Apply skip for the first chunk of the first term
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

// cleanup closes the prefetcher (which releases all caches and the shared temp file).
func (r *ReaderV1) cleanup() {
	if r.prefetcher != nil {
		r.prefetcher.Close()
	}
	r.currentCache = nil
	r.currentTerm = nil
}

// finishCurrentTerm decrements the use count for the current fetchKey and
// closes the cache when no more terms reference it.
func (r *ReaderV1) finishCurrentTerm() {
	if r.currentTerm == nil {
		return
	}

	key := r.termFetches[r.termIdx].key
	remaining := r.remainingUses[key] - 1
	if remaining <= 0 {
		delete(r.remainingUses, key)
		if r.currentCache != nil {
			r.currentCache.Close()
			r.currentCache = nil
		}
	} else {
		r.remainingUses[key] = remaining
	}

	r.currentTerm = nil
	r.termIdx++
}

func (r *ReaderV1) loadTerm() error {
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

func planReaderV1(reconstruction *ReconstructionResponseV1) ([]selectedFetch, []fetchTask, error) {
	selected := make([]selectedFetch, len(reconstruction.Terms))
	tasks := make([]fetchTask, 0, len(reconstruction.Terms))
	for i := range reconstruction.Terms {
		term := &reconstruction.Terms[i]
		fetchInfo, err := selectFetchInfoV1(reconstruction, term)
		if err != nil {
			return nil, nil, err
		}

		key := fetchKey{
			Hash:  term.Hash,
			Start: fetchInfo.URLRange.Start,
			End:   fetchInfo.URLRange.End,
		}
		selected[i] = selectedFetch{
			key:        key,
			chunkStart: fetchInfo.Range.Start,
			chunkEnd:   fetchInfo.Range.End,
		}
		tasks = append(tasks, fetchTask{
			key: key,
			url: fetchInfo.URL,
		})
	}
	return selected, tasks, nil
}

func selectFetchInfoV1(reconstruction *ReconstructionResponseV1, term *Term) (*FetchInfoEntry, error) {
	fetchInfoList, ok := reconstruction.FetchInfo[term.Hash]
	if !ok || len(fetchInfoList) == 0 {
		return nil, fmt.Errorf("no fetch info for xorb %s", term.Hash)
	}

	var fetchInfo *FetchInfoEntry
	for i := range fetchInfoList {
		if fetchInfoList[i].Range.Start <= term.Range.Start && fetchInfoList[i].Range.End >= term.Range.End {
			if fetchInfo == nil || (fetchInfoList[i].URLRange.End-fetchInfoList[i].URLRange.Start) < (fetchInfo.URLRange.End-fetchInfo.URLRange.Start) {
				fetchInfo = &fetchInfoList[i]
			}
		}
	}

	if fetchInfo == nil {
		return nil, fmt.Errorf("no matching fetch info for term chunk range [%d, %d)", term.Range.Start, term.Range.End)
	}

	return fetchInfo, nil
}

// ExpectedTransferBytesV1 computes the total compressed bytes that will be transferred
// over the network when reconstructing a V1 file. Deduplicated ranges are counted once.
func ExpectedTransferBytesV1(reconstruction *ReconstructionResponseV1) int64 {
	_, tasks, err := planReaderV1(reconstruction)
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

// ExpectedLengthV1 calculates the expected file length from V1 reconstruction
func ExpectedLengthV1(reconstruction *ReconstructionResponseV1) int64 {
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
