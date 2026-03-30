package download

import (
	"context"
	"fmt"
	"strings"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
)

// BuildReconstructionResponseV1 builds a reconstruction response from a shard.
//
// URL ranges within the fetch_info entries point into the compressed-data
// stream of the stored xorb (i.e. the raw compressed bytes for each chunk,
// concatenated without the 8-byte per-chunk headers).  This matches the
// convention used by xet-core: ByteRangeStart is an offset into the
// header-stripped stream, and range requests to the xorb download endpoint
// are served from the same stripped stream (see handleDownloadXorb).
func BuildReconstructionResponseV1(ctx context.Context, storage StorageAdapter, namespace string, sh *shard.Shard, fileHash xet.Hash, rangeHeader string) (*ReconstructionResponse, error) {
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

	response := &ReconstructionResponse{
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

		// Find the CAS block
		var casBlock *shard.CASBlock
		for i := range sh.CASInfos {
			if sh.CASInfos[i].CASHash == entry.CASHash {
				casBlock = &sh.CASInfos[i]
				break
			}
		}

		if casBlock == nil {
			currentByteOffset = termEnd
			continue
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
		startByte, endByte := storage.GetXorbDataRange(ctx, namespace, entry.CASHash, entry.ChunkIndexStart, entry.ChunkIndexEnd)

		xorbURL := storage.GetXorbURL(namespace, entry.CASHash)

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
