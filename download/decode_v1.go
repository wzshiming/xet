package download

import (
	"context"
	"fmt"
	"io"
	"math"
)

// ReaderV1 implements io.Reader and io.WriterTo for V1 reconstruction.
type ReaderV1 struct {
	*termReader
}

// NewReaderV1 creates a new V1 reconstruction reader. It starts fetching at
// once; Close stops it.
func NewReaderV1(ctx context.Context, client ClientAdapter, reconstruction *ReconstructionResponseV1, opts ...Option) (io.ReadCloser, error) {
	tasks, err := planReaderV1(reconstruction)
	if err != nil {
		return nil, fmt.Errorf("plan reader: %w", err)
	}
	r, err := newTermReader(ctx, client, reconstruction.Terms, reconstruction.OffsetIntoFirstRange, tasks, opts...)
	if err != nil {
		return nil, err
	}
	return &ReaderV1{r}, nil
}

func planReaderV1(reconstruction *ReconstructionResponseV1) ([]fetchTask, error) {
	tasks := make([]fetchTask, 0, len(reconstruction.Terms))
	for i := range reconstruction.Terms {
		term := &reconstruction.Terms[i]
		fetchInfo, err := selectFetchInfoV1(reconstruction, term)
		if err != nil {
			return nil, err
		}
		if err := checkFetchRange(term, fetchInfo.Range, fetchInfo.URLRange); err != nil {
			return nil, err
		}

		key := fetchKey{
			Hash:  term.Hash,
			Start: fetchInfo.URLRange.Start,
			End:   fetchInfo.URLRange.End,
		}
		tasks = append(tasks, fetchTask{
			key:        key,
			url:        fetchInfo.URL,
			chunkStart: fetchInfo.Range.Start,
			chunkEnd:   fetchInfo.Range.End,
		})
	}
	return tasks, nil
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
	tasks, err := planReaderV1(reconstruction)
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
