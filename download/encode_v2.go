package download

import (
	"context"
	"fmt"
	"strings"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
)

// BuildReconstructionResponseV2 builds a V2 reconstruction response from a shard.
// The V2 format groups fetch ranges by xorb and combines consecutive chunk ranges
// into multi-range fetch entries for more efficient downloading.
func BuildReconstructionResponseV2(ctx context.Context, storage StorageAdapter, namespace string, sh *shard.Shard, fileHash xet.Hash, rangeHeader string) (*ReconstructionResponseV2, error) {
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

	// Parse range header if present
	var requestedStart, requestedEnd int64
	hasRange := false
	if rangeHeader != "" {
		hasRange = true
		rangeHeader = strings.TrimPrefix(rangeHeader, "bytes=")
		parts := strings.Split(rangeHeader, "-")
		if len(parts) == 2 {
			fmt.Sscanf(parts[0], "%d", &requestedStart)
			fmt.Sscanf(parts[1], "%d", &requestedEnd)
		}
	}

	response := &ReconstructionResponseV2{
		OffsetIntoFirstRange: 0,
		Terms:                []Term{},
		Xorbs:                make(map[string][]XorbMultiRangeFetch),
	}

	// Calculate cumulative byte positions for each term
	var currentByteOffset int64

	// Build terms and group fetch info by xorb
	type fetchInfo struct {
		chunkStart uint32
		chunkEnd   uint32
		startByte  int64
		endByte    int64
	}
	xorbFetchRanges := make(map[string][]fetchInfo)

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

		// Calculate byte ranges for this term
		startByte, endByte := storage.GetXorbDataRange(ctx, namespace, entry.CASHash, entry.ChunkIndexStart, entry.ChunkIndexEnd)

		// Collect fetch info grouped by xorb
		xorbHashStr := entry.CASHash.String()
		xorbFetchRanges[xorbHashStr] = append(xorbFetchRanges[xorbHashStr], fetchInfo{
			chunkStart: entry.ChunkIndexStart,
			chunkEnd:   entry.ChunkIndexEnd,
			startByte:  startByte,
			endByte:    endByte,
		})

		currentByteOffset = termEnd
	}

	// Convert grouped fetch info into V2 format
	// For now, we create one XorbMultiRangeFetch per xorb with all ranges
	// A more sophisticated implementation could group consecutive/nearby ranges
	for xorbHashStr, ranges := range xorbFetchRanges {
		xorbHash, _ := xet.ParseHash(xorbHashStr)
		xorbURL := storage.GetXorbURL(namespace, xorbHash)

		var descriptors []XorbRangeDescriptor
		for _, r := range ranges {
			descriptors = append(descriptors, XorbRangeDescriptor{
				Chunks: ChunkRange{
					Start: r.chunkStart,
					End:   r.chunkEnd,
				},
				Bytes: ByteRange{
					Start: r.startByte,
					End:   r.endByte,
				},
			})
		}

		multiRangeFetch := XorbMultiRangeFetch{
			URL:    xorbURL,
			Ranges: descriptors,
		}

		response.Xorbs[xorbHashStr] = []XorbMultiRangeFetch{multiRangeFetch}
	}

	return response, nil
}
