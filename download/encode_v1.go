package download

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
)

// Invalid ranges are ignored; unsatisfiable ranges have start > end.
func parseByteRange(header string, size int64) (start, end int64, ok bool) {
	spec, isBytes := strings.CutPrefix(header, "bytes=")
	first, last, hasDash := strings.Cut(spec, "-")
	if !isBytes || !hasDash {
		return 0, 0, false
	}
	if first == "" {
		suffix, err := strconv.ParseUint(last, 10, 63)
		if err != nil {
			return 0, 0, false
		}
		return max(size-int64(suffix), 0), size - 1, true
	}
	firstPos, err := strconv.ParseUint(first, 10, 63)
	if err != nil {
		return 0, 0, false
	}
	start, end = int64(firstPos), size-1
	if last != "" {
		lastPos, err := strconv.ParseUint(last, 10, 63)
		if err != nil || int64(lastPos) < start {
			return 0, 0, false
		}
		end = min(int64(lastPos), end)
	}
	return start, end, true
}

// BuildReconstructionResponseV1 builds a reconstruction response from a shard.
//
// URL ranges within the fetch_info entries point into the compressed-data
// stream of the stored xorb (i.e. the raw compressed bytes for each chunk,
// concatenated without the 8-byte per-chunk headers).  This matches the
// convention used by xet-core: ByteRangeStart is an offset into the
// header-stripped stream, and range requests to the xorb download endpoint
// are served from the same stripped stream (see handleDownloadXorb).
func BuildReconstructionResponseV1(ctx context.Context, storage StorageAdapter, namespace string, sh *shard.Shard, fileHash xet.FileHash, rangeHeader string) (*ReconstructionResponseV1, error) {
	// Find the file block for this file hash
	var fileBlock *shard.FileBlock
	for i := range sh.Files {
		if sh.Files[i].FileHash == fileHash {
			fileBlock = &sh.Files[i]
			break
		}
	}

	if fileBlock == nil {
		return nil, fmt.Errorf("file not found in shard")
	}

	var fileSize int64
	for _, entry := range fileBlock.Entries {
		fileSize += int64(entry.UnpackedSegBytes)
	}
	requestedStart, requestedEnd, hasRange := parseByteRange(rangeHeader, fileSize)

	response := &ReconstructionResponseV1{
		OffsetIntoFirstRange: 0,
		Terms:                []Term{},
		FetchInfo:            make(map[string][]FetchInfoEntry),
	}

	// Calculate cumulative byte positions for each term
	var currentByteOffset int64

	// Build terms from file data sequence entries
	for _, entry := range fileBlock.Entries {
		termStart := currentByteOffset
		termEnd := currentByteOffset + int64(entry.UnpackedSegBytes)

		// Skip terms that are completely outside the requested range
		if hasRange {
			if termEnd <= requestedStart {
				currentByteOffset = termEnd
				continue
			}
			if termStart > requestedEnd {
				break
			}
		}

		// Calculate offset into first term if this is the first included term
		if len(response.Terms) == 0 && hasRange && termStart < requestedStart {
			response.OffsetIntoFirstRange = requestedStart - termStart
		}

		term := Term{
			Hash:           entry.CASHash.String(),
			UnpackedLength: uint64(entry.UnpackedSegBytes),
			Range: ChunkRange{
				Start: entry.ChunkIndexStart,
				End:   entry.ChunkIndexEnd,
			},
		}
		response.Terms = append(response.Terms, term)

		// Build fetch info.
		//
		// URL ranges are byte offsets within the compressed-data stream of the
		// stored xorb (headers stripped).  Load the stored xorb and compute
		// the accurate ranges from the actual compressed chunk sizes.
		startByte, endByte, err := storage.GetXorbDataRange(ctx, namespace, entry.CASHash, entry.ChunkIndexStart, entry.ChunkIndexEnd)
		if err != nil {
			return nil, fmt.Errorf("failed to get xorb data range: %w", err)
		}

		xorbURL, err := storage.GetXorbURL(ctx, namespace, entry.CASHash)
		if err != nil {
			return nil, fmt.Errorf("failed to get xorb URL: %w", err)
		}

		fetchEntry := FetchInfoEntry{
			Range: ChunkRange{
				Start: entry.ChunkIndexStart,
				End:   entry.ChunkIndexEnd,
			},
			URL: xorbURL,
			URLRange: ByteRange{
				Start: startByte,
				End:   endByte,
			},
		}

		xorbHashStr := entry.CASHash.String()
		response.FetchInfo[xorbHashStr] = append(response.FetchInfo[xorbHashStr], fetchEntry)

		currentByteOffset = termEnd
	}

	return response, nil
}
