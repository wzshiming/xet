package download

import (
	"context"
	"fmt"
	"io"
	"math"
)

// ReaderV2 implements io.Reader and io.WriterTo for V2 reconstruction.
type ReaderV2 struct {
	*termReader
}

// NewReaderV2 creates a new V2 reconstruction reader. It fetches nothing and
// takes no memory budget before its first Read or WriteTo; Close stops it.
func NewReaderV2(ctx context.Context, client ClientAdapter, reconstruction *ReconstructionResponseV2, opts ...Option) (io.ReadCloser, error) {
	tasks, err := planReaderV2(reconstruction)
	if err != nil {
		return nil, fmt.Errorf("plan reader: %w", err)
	}
	r, err := newTermReader(ctx, client, reconstruction.Terms, reconstruction.OffsetIntoFirstRange, tasks, opts...)
	if err != nil {
		return nil, err
	}
	return &ReaderV2{r}, nil
}

func planReaderV2(reconstruction *ReconstructionResponseV2) ([]fetchTask, error) {
	tasks := make([]fetchTask, 0, len(reconstruction.Terms))
	for i := range reconstruction.Terms {
		term := &reconstruction.Terms[i]
		fetch, rg, err := selectFetchInfoV2(reconstruction, term)
		if err != nil {
			return nil, err
		}
		if err := checkFetchRange(term, rg.Chunks, rg.Bytes); err != nil {
			return nil, err
		}

		key := fetchKey{
			Hash:  term.Hash,
			Start: rg.Bytes.Start,
			End:   rg.Bytes.End,
		}

		tasks = append(tasks, fetchTask{
			key:        key,
			url:        fetch.URL,
			chunkStart: rg.Chunks.Start,
			chunkEnd:   rg.Chunks.End,
		})
	}
	return tasks, nil
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
// over the network when reconstructing a V2 file. Deduplicated ranges are counted once,
// and a range a reader fetches again beyond its window (see WithConcurrency) is not
// counted again.
func ExpectedTransferBytesV2(reconstruction *ReconstructionResponseV2) int64 {
	tasks, err := planReaderV2(reconstruction)
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
