package download

import (
	"context"
	"io"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/pkg/xorb"
)

// StorageAdapter provides access to storage operations needed for reconstruction encoding
type StorageAdapter interface {
	GetXorbURL(namespace string, xorbHash xet.Hash) string
	GetXorbReadSeekCloser(ctx context.Context, namespace string, xorbHash xet.Hash) (io.ReadSeekCloser, error)
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
