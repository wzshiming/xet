package xorb

import "github.com/wzshiming/xet"

// GroupChunkIndicesBySize performs size-based grouping of chunks.
// It groups chunk indices by their uncompressed sizes, targeting the specified
// raw payload limit and the protocol's maximum chunk count.
// A new group is started before adding a chunk that would cause the total
// to exceed targetSize.
//
// This function implements the core grouping logic extracted from upload.go.
// It's independent of specific chunk types to allow reuse across different packages.
//
// Parameters:
//   - chunkSizes: array of chunk sizes in order
//   - targetSize: target maximum size for each group
//
// Returns a GroupingIndices containing groups of chunk indices.
func GroupChunkIndicesBySize(chunkSizes []uint32, targetSize uint64) [][]int {
	var groups [][]int
	var currentGroup []int
	var currentSize uint64

	for i, size := range chunkSizes {
		chunkSize := uint64(size)

		// MAX_XORB_SIZE applies to the sum of uncompressed chunk bytes. A xorb
		// may contain exactly targetSize bytes, but never more than the size or
		// chunk-count limit.
		if len(currentGroup) > 0 && (currentSize+chunkSize > targetSize || len(currentGroup) >= xet.MaxChunksPerXorb) {
			groups = append(groups, currentGroup)
			currentGroup = nil
			currentSize = 0
		}

		currentGroup = append(currentGroup, i)
		currentSize += chunkSize
	}

	// Add remaining group
	if len(currentGroup) > 0 {
		groups = append(groups, currentGroup)
	}

	return groups
}
