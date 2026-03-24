// Package gearhash implements the Gearhash-based content-defined chunking
// algorithm as specified in the XET protocol (Section 5).
package gearhash

const (
	// TargetChunkSize is the target chunk size (64 KiB).
	TargetChunkSize = 65536
	// MinChunkSize is the minimum chunk size (8 KiB).
	MinChunkSize = 8192
	// MaxChunkSize is the maximum chunk size (128 KiB).
	MaxChunkSize = 131072
	// Mask is the 16-bit mask used for boundary detection.
	Mask = 0xFFFF000000000000
)

// ChunkFile splits data into variable-sized chunks using the Gearhash algorithm.
// It returns the list of chunks as byte slices.
func ChunkFile(data []byte) [][]byte {
	var (
		h           uint64
		startOffset int
		chunks      [][]byte
		n           = len(data)
	)

	for i := 0; i < n; i++ {
		b := data[i]
		h = (h << 1) + table[b]

		chunkSize := i - startOffset + 1

		if chunkSize < MinChunkSize {
			continue
		}

		if chunkSize >= MaxChunkSize {
			chunks = append(chunks, data[startOffset:i+1])
			startOffset = i + 1
			h = 0
			continue
		}

		if (h & Mask) == 0 {
			chunks = append(chunks, data[startOffset:i+1])
			startOffset = i + 1
			h = 0
		}
	}

	if startOffset < n {
		chunks = append(chunks, data[startOffset:n])
	}

	return chunks
}

// ChunkBoundaries returns the end offsets (exclusive) of each chunk boundary.
func ChunkBoundaries(data []byte) []int {
	var (
		h           uint64
		startOffset int
		boundaries  []int
		n           = len(data)
	)

	for i := 0; i < n; i++ {
		b := data[i]
		h = (h << 1) + table[b]

		chunkSize := i - startOffset + 1

		if chunkSize < MinChunkSize {
			continue
		}

		if chunkSize >= MaxChunkSize {
			boundaries = append(boundaries, i+1)
			startOffset = i + 1
			h = 0
			continue
		}

		if (h & Mask) == 0 {
			boundaries = append(boundaries, i+1)
			startOffset = i + 1
			h = 0
		}
	}

	if startOffset < n {
		boundaries = append(boundaries, n)
	}

	return boundaries
}
