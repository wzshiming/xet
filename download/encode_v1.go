package download

import (
	"context"
	"fmt"
	"slices"

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
// The first and the last are trimmed to the chunks holding bytes of rng when the shard
// has a CAS block of their xorb, so OffsetIntoFirstRange stays within the first chunk.
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
	for _, seg := range fileBlock.Entries {
		termStart := currentByteOffset
		termEnd := currentByteOffset + int64(seg.UnpackedSegBytes)

		// Skip terms that are completely outside the requested range
		if termEnd <= rng.Start {
			currentByteOffset = termEnd
			continue
		}
		if termStart > rng.End {
			break
		}

		entry, skipped := trimSegment(sh, seg, termStart, rng)
		termStart += skipped

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

		// hf_xet keys a xorb's blocks by start chunk: of the ranges sharing one, keep the longest.
		xorbHashStr := entry.CASHash.String()
		entries := response.FetchInfo[xorbHashStr]
		if k := slices.IndexFunc(entries, func(e FetchInfoEntry) bool { return e.Range.Start == fetchEntry.Range.Start }); k < 0 {
			response.FetchInfo[xorbHashStr] = append(entries, fetchEntry)
		} else if fetchEntry.Range.End > entries[k].Range.End {
			entries[k] = fetchEntry
		}

		currentByteOffset = termEnd
	}

	return response, nil
}
