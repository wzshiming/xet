package download

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"math"

	"github.com/wzshiming/xet/internal/pool"
)

// ReaderV1 implements io.Reader and io.WriterTo for V1 reconstruction
type ReaderV1 struct {
	client         ClientAdapter
	ctx            context.Context
	reconstruction *ReconstructionResponseV1
	skipBytes      int64
	termFetches    []selectedFetch
	prefetcher     *prefetcher
	verifier       *fileVerifier

	// State for reading
	termIdx      int
	chunkIdx     uint32         // next chunk to read from currentCache
	buf          *pool.ChunkBuf // pooled; holds the last chunk read
	chunk        []byte         // unread rest of the chunk in buf
	reread       chunkSource    // set while buf holds a whole chunk a later term reads again
	rereadIdx    uint32
	currentTerm  *Term
	currentCache chunkSource
	localStart   uint32 // Local chunk index start (relative to downloaded xorb)
	localEnd     uint32 // Local chunk index end (relative to downloaded xorb)
	err          error
}

// NewReaderV1 creates a new V1 reconstruction reader.
func NewReaderV1(ctx context.Context, client ClientAdapter, reconstruction *ReconstructionResponseV1, opts ...Option) (io.ReadCloser, error) {
	options := &options{memoryLimit: DefaultMemoryLimit}
	for _, opt := range opts {
		opt(options)
	}

	verifier, err := newFileVerifier(options, reconstruction.OffsetIntoFirstRange)
	if err != nil {
		return nil, err
	}
	if options.cache != nil {
		// Adopt pre-existing entries and clean up orphaned partial files before
		// the download starts; only the manager's first prepare walks the cache
		// directory.
		options.cache.prepare()
	}

	termFetches, tasks, err := planReaderV1(reconstruction)
	if err != nil {
		return nil, fmt.Errorf("plan reader: %w", err)
	}

	prefetcher, err := newPrefetcher(ctx, client, termFetches, tasks, options.cache, options)
	if err != nil {
		return nil, fmt.Errorf("initialize prefetcher: %w", err)
	}

	return &ReaderV1{
		client:         client,
		ctx:            ctx,
		reconstruction: reconstruction,
		skipBytes:      reconstruction.OffsetIntoFirstRange,
		termFetches:    termFetches,
		prefetcher:     prefetcher,
		verifier:       verifier,
	}, nil
}

func (r *ReaderV1) Read(p []byte) (n int, err error) {
	if r.err != nil {
		return 0, r.err
	}

	for n < len(p) {
		if err := r.fill(); err != nil {
			if n > 0 && err != io.EOF {
				return n, nil
			}
			return n, err
		}
		r.reread = nil
		copied := copy(p[n:], r.chunk)
		r.chunk = r.chunk[copied:]
		n += copied
	}

	return n, nil
}

// WriteTo implements io.WriterTo, writing each remaining chunk with one Write.
func (r *ReaderV1) WriteTo(w io.Writer) (n int64, err error) {
	if r.err != nil {
		return 0, r.err
	}

	for {
		if err := r.fill(); err != nil {
			if err == io.EOF {
				return n, nil
			}
			return n, err
		}
		written, err := writeChunk(w, r.chunk)
		r.chunk = r.chunk[written:]
		n += int64(written)
		if err != nil {
			r.reread = nil
			return n, err
		}
		if r.reread != nil {
			moveWritten(r.reread, r.rereadIdx, w, int64(written))
			r.reread = nil
		}
	}
}

// fill reads the next chunk into buf once chunk is drained; errors other than io.EOF stick.
func (r *ReaderV1) fill() error {
	for len(r.chunk) == 0 {
		// Check if we're done with all terms
		if r.termIdx >= len(r.reconstruction.Terms) {
			if r.verifier != nil {
				if err := r.verifier.verify(); err != nil {
					r.err = err
					r.cleanup()
					return err
				}
			}
			r.cleanup()
			return io.EOF
		}

		// Load next term if needed
		if r.currentTerm == nil {
			if err := r.loadTerm(); err != nil {
				r.err = err
				r.cleanup()
				return err
			}
		}

		// Check if we're done with current term's chunks
		if r.chunkIdx >= r.localEnd {
			r.finishCurrentTerm()
			continue
		}

		// Get chunk on demand by index
		if r.buf == nil {
			r.buf = pool.GetChunkBuf()
		}
		ref, err := r.currentCache.readChunk(r.chunkIdx, r.buf[:])
		if err != nil {
			r.err = err
			r.cleanup()
			return err
		}
		r.reread = nil
		if ref.again && !(r.termIdx == 0 && r.skipBytes > 0) {
			r.reread, r.rereadIdx = r.currentCache, r.chunkIdx
		}
		r.chunkIdx++
		size := int64(ref.size)
		data := r.buf[:size]

		if r.verifier != nil {
			r.verifier.add(ref, data)
		}

		// Apply OffsetIntoFirstRange skip; it may span multiple leading chunks
		if r.termIdx == 0 && r.skipBytes > 0 {
			skip := min(r.skipBytes, size)
			r.skipBytes -= skip
			data = data[skip:]
		}
		r.chunk = data
	}
	return nil
}

// cleanup closes the prefetcher, which owns and releases all caches, and returns buf to the pool.
func (r *ReaderV1) cleanup() {
	if r.prefetcher != nil {
		r.prefetcher.Close()
	}
	if r.buf != nil {
		pool.PutChunkBuf(r.buf)
	}
	r.prefetcher = nil
	r.currentCache = nil
	r.reread = nil
	r.currentTerm = nil
	r.buf = nil
	r.chunk = nil
}

// Close releases the prefetcher and all cache references held by the reader.
// It is safe to call multiple times and after Read has returned io.EOF.
func (r *ReaderV1) Close() error {
	r.cleanup()
	if r.err == nil {
		r.err = fs.ErrClosed
	}
	return nil
}

// finishCurrentTerm advances to the next reconstruction term.
func (r *ReaderV1) finishCurrentTerm() {
	if r.currentTerm == nil {
		return
	}

	r.prefetcher.finishTerm(r.termFetches[r.termIdx].key, r.termIdx)
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
		if err := checkFetchRange(term, fetchInfo.Range, fetchInfo.URLRange); err != nil {
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
			term:       term.Range,
		}
		tasks = append(tasks, fetchTask{
			key:        key,
			url:        fetchInfo.URL,
			chunkStart: fetchInfo.Range.Start,
			chunkEnd:   fetchInfo.Range.End,
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
