package reconstruction

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/pkg/shard"
	"github.com/wzshiming/xet/pkg/xorb"
)

// StorageAdapter provides access to storage operations needed for reconstruction encoding
type StorageAdapter interface {
	GetXorbURL(namespace string, xorbHash xet.Hash) string
	GetXorbReadSeekCloser(ctx context.Context, namespace string, xorbHash xet.Hash) (io.ReadSeekCloser, error)
}

// BuildReconstructionResponse builds a reconstruction response from a shard.
//
// URL ranges within the fetch_info entries point into the compressed-data
// stream of the stored xorb (i.e. the raw compressed bytes for each chunk,
// concatenated without the 8-byte per-chunk headers).  This matches the
// convention used by xet-core: ByteRangeStart is an offset into the
// header-stripped stream, and range requests to the xorb download endpoint
// are served from the same stripped stream (see handleDownloadXorb).
func BuildReconstructionResponse(ctx context.Context, storage StorageAdapter, namespace string, sh *shard.Shard, fileHash xet.Hash, rangeHeader string) (*ReconstructionResponse, error) {
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
		startByte, endByte := compressedDataRange(ctx, storage, namespace, entry.CASHash, entry.ChunkIndexStart, entry.ChunkIndexEnd)

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

// compressedDataRange returns the [start, end] byte range (inclusive) within
// the stored xorb binary for the given chunk range [chunkStart, chunkEnd).
// The returned range includes the 8-byte chunk header for each chunk, so that
// xet-core can parse the header (version, compressed/uncompressed size,
// compression type) when it downloads that byte range.
func compressedDataRange(ctx context.Context, storage StorageAdapter, namespace string, xorbHash xet.Hash, chunkStart, chunkEnd uint32) (startByte, endByte int64) {
	xorbReader, err := storage.GetXorbReadSeekCloser(ctx, namespace, xorbHash)
	if err != nil {
		return 0, 0
	}

	// Serialize to bytes to compute chunk offsets in the stored format.
	// We need the actual byte layout to calculate ranges for HTTP range requests.
	xorbData, err := io.ReadAll(xorbReader)
	if err != nil {
		return 0, 0
	}

	// Parse chunks to find byte offsets in the stored xorb.
	// The stored format for xet-core uploads is [header0(8)][data0][header1(8)][data1]...
	// where header = version(1) + compressedSize(3 LE) + comprType(1) + uncompressedSize(3 LE).
	type chunkSpan struct{ start, end int64 }
	var spans []chunkSpan
	offset := int64(0)
	data := xorbData

	for int(offset) < len(data) {
		if int(offset)+8 > len(data) {
			break
		}
		// Stop at XETBLOB footer (Go-client full format)
		if int(offset)+7 <= len(data) && string(data[offset:offset+7]) == xorb.XorbIdentifier {
			break
		}

		headerStart := offset

		// Read compressed size (3-byte LE, bytes 1-3 of the 8-byte header)
		compressedSize := int64(data[offset+1]) | int64(data[offset+2])<<8 | int64(data[offset+3])<<16
		offset += 8 // skip header

		if int(offset)+int(compressedSize) > len(data) {
			break
		}

		offset += compressedSize

		// The chunk span includes the header and the compressed payload.
		spans = append(spans, chunkSpan{start: headerStart, end: offset - 1})
	}

	if int(chunkStart) >= len(spans) || int(chunkEnd) > len(spans) || chunkStart >= chunkEnd {
		return 0, 0
	}

	return spans[chunkStart].start, spans[chunkEnd-1].end
}

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

		// Calculate byte ranges for this term
		startByte, endByte := compressedDataRange(ctx, storage, namespace, entry.CASHash, entry.ChunkIndexStart, entry.ChunkIndexEnd)

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
