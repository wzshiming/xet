package reconstruction

import (
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/pkg/shard"
)

// ChunkInfo contains information about a chunk within a file
type ChunkInfo struct {
	Hash      xet.Hash
	Data      []byte
	XorbHash  xet.Hash
	ChunkIdx  uint32
	FileIndex int
}

// BuildFileBlock constructs a shard.FileBlock for a file from its chunks
// It groups consecutive chunks from the same xorb into terms and computes verification hashes
func BuildFileBlock(fileHash xet.Hash, chunks []ChunkInfo) shard.FileBlock {
	fileBlock := shard.FileBlock{
		FileHash:     fileHash,
		Flags:        shard.FileWithVerification,
		Entries:      make([]shard.FileDataSequenceEntry, 0),
		Verification: make([]xet.Hash, 0),
	}

	if len(chunks) == 0 {
		return fileBlock
	}

	// Group consecutive chunks by xorb to create terms
	type termInfo struct {
		xorbHash   xet.Hash
		startIndex uint32
		endIndex   uint32
		bytes      uint32
		chunkStart int // start in chunks slice
		chunkEnd   int // end in chunks slice
	}

	var terms []termInfo
	var currentXorbHash xet.Hash
	var currentStart uint32
	var currentBytes uint32
	var currentChunkStart int

	for i, chunk := range chunks {
		xorbHash := chunk.XorbHash
		chunkSize := uint32(len(chunk.Data))
		chunkIndexInXorb := chunk.ChunkIdx

		if i == 0 || xorbHash != currentXorbHash {
			// Start new term
			if i > 0 {
				terms = append(terms, termInfo{
					xorbHash:   currentXorbHash,
					startIndex: currentStart,
					endIndex:   currentStart + uint32(i-currentChunkStart),
					bytes:      currentBytes,
					chunkStart: currentChunkStart,
					chunkEnd:   i,
				})
			}
			currentXorbHash = xorbHash
			currentStart = chunkIndexInXorb
			currentBytes = chunkSize
			currentChunkStart = i
		} else {
			// Same xorb - chunks should be consecutive
			// Verify the chunk index is consecutive
			expectedIndex := currentStart + uint32(i-currentChunkStart)
			if chunkIndexInXorb != expectedIndex {
				// Non-consecutive chunks in same xorb
				// Start a new term
				terms = append(terms, termInfo{
					xorbHash:   currentXorbHash,
					startIndex: currentStart,
					endIndex:   currentStart + uint32(i-currentChunkStart),
					bytes:      currentBytes,
					chunkStart: currentChunkStart,
					chunkEnd:   i,
				})
				currentXorbHash = xorbHash
				currentStart = chunkIndexInXorb
				currentBytes = chunkSize
				currentChunkStart = i
			} else {
				currentBytes += chunkSize
			}
		}
	}

	// Add final term
	terms = append(terms, termInfo{
		xorbHash:   currentXorbHash,
		startIndex: currentStart,
		endIndex:   currentStart + uint32(len(chunks)-currentChunkStart),
		bytes:      currentBytes,
		chunkStart: currentChunkStart,
		chunkEnd:   len(chunks),
	})

	// Build entries and verification hashes for each term
	for _, term := range terms {
		fileBlock.Entries = append(fileBlock.Entries, shard.FileDataSequenceEntry{
			CASHash:          term.xorbHash,
			CASFlags:         0,
			UnpackedSegBytes: term.bytes,
			ChunkIndexStart:  term.startIndex,
			ChunkIndexEnd:    term.endIndex,
		})

		// Compute verification hash for this term's chunk range
		termChunkHashes := make([]xet.Hash, term.chunkEnd-term.chunkStart)
		for i := term.chunkStart; i < term.chunkEnd; i++ {
			termChunkHashes[i-term.chunkStart] = chunks[i].Hash
		}
		verificationHash := xet.ComputeVerificationHash(termChunkHashes)
		fileBlock.Verification = append(fileBlock.Verification, verificationHash)
	}

	return fileBlock
}

// BuildCASBlocks constructs shard.CASBlock entries for all xorbs referenced by chunks
// It collects chunk information for each xorb, computing byte offsets
func BuildCASBlocks(chunks []ChunkInfo) []shard.CASBlock {
	// Track unique xorbs
	xorbMap := make(map[xet.Hash]*shard.CASBlock)
	xorbChunksMap := make(map[xet.Hash][]shard.CASChunkSequenceEntry)
	xorbBytesMap := make(map[xet.Hash]uint32)
	xorbSeenChunks := make(map[xet.Hash]map[xet.Hash]bool)

	for _, chunk := range chunks {
		xorbHash := chunk.XorbHash
		chunkSize := uint32(len(chunk.Data))

		if _, exists := xorbMap[xorbHash]; !exists {
			xorbMap[xorbHash] = &shard.CASBlock{
				CASHash: xorbHash,
				Chunks:  make([]shard.CASChunkSequenceEntry, 0),
			}
			xorbChunksMap[xorbHash] = make([]shard.CASChunkSequenceEntry, 0)
			xorbBytesMap[xorbHash] = 0
			xorbSeenChunks[xorbHash] = make(map[xet.Hash]bool)
		}

		// Only add each chunk once per xorb
		if !xorbSeenChunks[xorbHash][chunk.Hash] {
			xorbSeenChunks[xorbHash][chunk.Hash] = true
			entry := shard.CASChunkSequenceEntry{
				ChunkHash:        chunk.Hash,
				ByteRangeStart:   xorbBytesMap[xorbHash],
				UnpackedSegBytes: chunkSize,
			}
			xorbChunksMap[xorbHash] = append(xorbChunksMap[xorbHash], entry)
			xorbBytesMap[xorbHash] += chunkSize
		}
	}

	// Build final CAS blocks
	var casBlocks []shard.CASBlock
	for xorbHash, casBlock := range xorbMap {
		casBlock.Chunks = xorbChunksMap[xorbHash]
		casBlock.NumBytesInCAS = xorbBytesMap[xorbHash]
		casBlocks = append(casBlocks, *casBlock)
	}

	return casBlocks
}
