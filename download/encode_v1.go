package download

import (
	"context"
	"fmt"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/xorb"
)

// BuildReconstructionResponseV1 builds a reconstruction response from a shard.
//
// URL ranges within the fetch_info entries point into the compressed-data
// stream of the stored xorb (i.e. the raw compressed bytes for each chunk,
// concatenated without the 8-byte per-chunk headers).  This matches the
// convention used by xet-core: ByteRangeStart is an offset into the
// header-stripped stream, and range requests to the xorb download endpoint
// are served from the same stripped stream (see handleDownloadXorb).
//
// The terms are those holding bytes of rng; ParseByteRange("", size) selects the whole file.
func BuildReconstructionResponseV1(ctx context.Context, storage StorageAdapter, sh *shard.Shard, fileHash xet.FileHash, rng ByteRange) (*ReconstructionResponseV1, error) {
	fileBlock := sh.File(fileHash)
	if fileBlock == nil {
		return nil, fmt.Errorf("file not found in shard")
	}

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
		if termEnd <= rng.Start {
			currentByteOffset = termEnd
			continue
		}
		if termStart > rng.End {
			break
		}

		// Calculate offset into first term if this is the first included term
		if len(response.Terms) == 0 && termStart < rng.Start {
			response.OffsetIntoFirstRange = rng.Start - termStart
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
		offsets, err := storage.GetXorbChunkOffsets(ctx, entry.CASHash)
		if err != nil {
			return nil, fmt.Errorf("failed to get xorb chunk offsets: %w", err)
		}
		startByte, endByte, err := xorb.ChunkDataRangeFromOffsets(offsets, entry.ChunkIndexStart, entry.ChunkIndexEnd)
		if err != nil {
			return nil, fmt.Errorf("failed to get xorb data range: %w", err)
		}

		xorbURL, err := storage.GetXorbURL(ctx, entry.CASHash)
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
