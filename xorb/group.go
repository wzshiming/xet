package xorb

// GroupChunkIndicesBySize performs size-based grouping of chunks.
// It groups chunk indices into groups targeting the specified size limit.
// A new group is started before adding a chunk that would cause the total
// to reach or exceed targetSize.
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

		// Finalize the current group before adding a chunk that would reach or
		// exceed the target size, matching the xet-go reference implementation.
		if len(currentGroup) > 0 && currentSize+chunkSize >= targetSize {
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
