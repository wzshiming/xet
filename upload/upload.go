package upload

import (
	"context"
	"fmt"
	"io"
	"sort"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/xorb"
)

// DeduplicationResult represents the result of deduplication for a chunk.
type DeduplicationResult struct {
	ChunkHash  xet.Hash
	IsNew      bool
	XorbHash   xet.Hash
	ChunkIndex uint32
}

// chunkInfo contains information about a chunk within a file.
type chunkInfo struct {
	FileIndex int
	Data      []byte
	Hash      xet.Hash
	Offset    uint64
	Dedup     *DeduplicationResult
}

// xorbGroup represents a group of chunks to be packed into a single xorb.
type xorbGroup struct {
	Chunks      [][]byte
	ChunkHashes []xet.Hash
	StartIndex  int
}

// UploadFiles chunks, deduplicates, and uploads multiple files using the
// provided client adapter. It returns the computed file hashes.
func UploadFiles(ctx context.Context, client ClientAdapter, readers ...io.Reader) ([]xet.Hash, error) {
	localChunkCache := make(map[xet.Hash]*DeduplicationResult)

	// Step 1: Chunk all files and deduplicate
	var allChunks []chunkInfo
	var fileHashes []xet.Hash
	fileChunkRanges := make(map[int][]int) // fileIndex -> chunk indices

	for index, reader := range readers {
		// Compute chunk hashes
		chunkHashes := []xet.Hash{}

		// Compute chunk sizes
		chunkSizes := []uint64{}

		err := xet.ChunkData(reader, func(offset int64, chunk xet.ChunkBytes) error {
			chunkHash := chunk.Hash()

			chunkHashes = append(chunkHashes, chunkHash)
			chunkSizes = append(chunkSizes, chunk.Size())

			// Deduplicate
			dedupResult := deduplicateChunk(ctx, client, localChunkCache, chunkHash)

			chunkIdx := len(allChunks)
			allChunks = append(allChunks, chunkInfo{
				FileIndex: index,
				Data:      chunk.Bytes(),
				Hash:      chunkHash,
				Offset:    uint64(offset),
				Dedup:     dedupResult,
			})

			fileChunkRanges[index] = append(fileChunkRanges[index], chunkIdx)
			return nil
		})

		if err != nil {
			return nil, fmt.Errorf("chunk data for file %d: %w", index, err)
		}

		fileHash := xet.ComputeFileHash(chunkHashes, chunkSizes)

		fileHashes = append(fileHashes, fileHash)
	}

	// Step 2: Group new chunks into xorbs
	var newChunks []chunkInfo
	seenNewChunk := make(map[xet.Hash]bool)
	for _, chunk := range allChunks {
		if chunk.Dedup.IsNew && !seenNewChunk[chunk.Hash] {
			newChunks = append(newChunks, chunk)
			seenNewChunk[chunk.Hash] = true
		}
	}

	xorbs := groupChunksIntoXorbs(newChunks, xet.MaxXorbSerializedSize)

	// Step 3: Upload xorbs
	if err := uploadXorbs(ctx, client, localChunkCache, xorbs); err != nil {
		return nil, fmt.Errorf("upload xorbs: %w", err)
	}

	// Step 4: Build and upload shard
	if err := buildAndUploadShard(ctx, client, fileHashes, allChunks, fileChunkRanges); err != nil {
		return nil, fmt.Errorf("build and upload shard: %w", err)
	}

	return fileHashes, nil
}

// deduplicateChunk checks if a chunk already exists.
func deduplicateChunk(ctx context.Context, client ClientAdapter, cache map[xet.Hash]*DeduplicationResult, chunkHash xet.Hash) *DeduplicationResult {
	// Check local session cache first
	if result, ok := cache[chunkHash]; ok {
		return result
	}

	// Check global deduplication if enabled

	shardData, err := client.QueryChunkDeduplication(ctx, chunkHash)
	if err == nil && shardData != nil {
		// Found in global dedup - extract xorb hash and chunk index
		if len(shardData.CASInfos) > 0 {
			casBlock := shardData.CASInfos[0]
			if len(casBlock.Chunks) > 0 {
				result := &DeduplicationResult{
					ChunkHash:  chunkHash,
					IsNew:      false,
					XorbHash:   casBlock.CASHash,
					ChunkIndex: 0, // First chunk in the returned info
				}
				cache[chunkHash] = result
				return result
			}
		}
	}

	// Not found - mark as new
	result := &DeduplicationResult{
		ChunkHash: chunkHash,
		IsNew:     true,
	}
	cache[chunkHash] = result
	return result
}

// groupChunksIntoXorbs groups chunks into xorbs targeting the specified size.
// A new group is started before adding a chunk that would cause the total to
// reach or exceed targetXorbSize, matching the xet-go reference implementation.
func groupChunksIntoXorbs(chunks []chunkInfo, targetXorbSize uint64) []*xorbGroup {
	var groups []*xorbGroup
	var currentGroup *xorbGroup
	var currentSize uint64

	for i, chunk := range chunks {
		chunkSize := uint64(len(chunk.Data))

		// Finalize the current group before adding a chunk that would reach or
		// exceed the target size.
		if currentGroup != nil && len(currentGroup.Chunks) > 0 && currentSize+chunkSize >= targetXorbSize {
			groups = append(groups, currentGroup)
			currentGroup = nil
			currentSize = 0
		}

		if currentGroup == nil {
			currentGroup = &xorbGroup{
				Chunks:      make([][]byte, 0),
				ChunkHashes: make([]xet.Hash, 0),
				StartIndex:  i,
			}
		}

		currentGroup.Chunks = append(currentGroup.Chunks, chunk.Data)
		currentGroup.ChunkHashes = append(currentGroup.ChunkHashes, chunk.Hash)
		currentSize += chunkSize
	}

	// Add remaining group
	if currentGroup != nil && len(currentGroup.Chunks) > 0 {
		groups = append(groups, currentGroup)
	}

	return groups
}

// uploadXorbs serializes and uploads all xorbs.
func uploadXorbs(ctx context.Context, client ClientAdapter, cache map[xet.Hash]*DeduplicationResult, groups []*xorbGroup) error {
	for _, group := range groups {
		// Create xorb
		xorbObj := xorb.NewXorb()
		for _, chunkData := range group.Chunks {
			if err := xorbObj.AddChunk(chunkData); err != nil {
				return fmt.Errorf("add chunk to xorb: %w", err)
			}
		}

		// Upload
		_, err := client.UploadXorb(ctx, xorbObj)
		if err != nil {
			return fmt.Errorf("upload xorb %s: %w", xorbObj.Hash.String(), err)
		}

		// Update local cache with xorb information
		for i, chunkHash := range group.ChunkHashes {
			if result, ok := cache[chunkHash]; ok && result.IsNew {
				result.IsNew = false
				result.XorbHash = xorbObj.Hash
				result.ChunkIndex = uint32(i)
			}
		}
	}

	return nil
}

// buildAndUploadShard constructs and uploads the shard.
func buildAndUploadShard(ctx context.Context, client ClientAdapter, fileHashes []xet.Hash, allChunks []chunkInfo, fileChunkRanges map[int][]int) error {
	// Create shard with default header
	sh := shard.NewShard()

	// Track unique xorbs
	xorbMap := make(map[xet.Hash]*shard.CASBlock)

	// Build file blocks
	for fileIdx, fileHash := range fileHashes {
		chunkIndices := fileChunkRanges[fileIdx]

		// Collect all chunk hashes for this file
		fileChunkHashes := make([]xet.Hash, len(chunkIndices))
		for i, chunkIdx := range chunkIndices {
			fileChunkHashes[i] = allChunks[chunkIdx].Hash
		}

		fileBlock := shard.FileBlock{
			FileHash:     fileHash,
			Flags:        shard.FileWithVerification,
			Entries:      make([]shard.FileDataSequenceEntry, 0),
			Verification: make([]xet.Hash, 0),
		}

		// Group consecutive chunks by xorb to create terms
		type termInfo struct {
			xorbHash   xet.Hash
			startIndex uint32
			endIndex   uint32
			bytes      uint32
			chunkStart int // start in fileChunkHashes
			chunkEnd   int // end in fileChunkHashes
		}

		var terms []termInfo
		var currentXorbHash xet.Hash
		var currentStart uint32
		var currentBytes uint32
		var currentChunkStart int

		for i, chunkIdx := range chunkIndices {
			chunk := allChunks[chunkIdx]
			xorbHash := chunk.Dedup.XorbHash
			chunkSize := uint32(len(chunk.Data))
			chunkIndexInXorb := chunk.Dedup.ChunkIndex

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
					// Non-consecutive chunks in same xorb - this shouldn't happen
					// unless there's deduplication within the file
					// For now, start a new term
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

			// Track this xorb
			if _, exists := xorbMap[xorbHash]; !exists {
				xorbMap[xorbHash] = &shard.CASBlock{
					CASHash: xorbHash,
					Chunks:  make([]shard.CASChunkSequenceEntry, 0),
				}
			}
		}

		// Add final term
		if len(chunkIndices) > 0 {
			terms = append(terms, termInfo{
				xorbHash:   currentXorbHash,
				startIndex: currentStart,
				endIndex:   currentStart + uint32(len(chunkIndices)-currentChunkStart),
				bytes:      currentBytes,
				chunkStart: currentChunkStart,
				chunkEnd:   len(chunkIndices),
			})
		}

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
			termChunkHashes := fileChunkHashes[term.chunkStart:term.chunkEnd]
			verificationHash := xet.ComputeVerificationHash(termChunkHashes)
			fileBlock.Verification = append(fileBlock.Verification, verificationHash)
		}

		sh.Files = append(sh.Files, fileBlock)
	}

	// Build CAS blocks by collecting chunk information from all chunks
	xorbChunksMap := make(map[xet.Hash][]shard.CASChunkSequenceEntry)
	xorbBytesMap := make(map[xet.Hash]uint32)              // total uncompressed bytes per xorb
	xorbSeenChunks := make(map[xet.Hash]map[xet.Hash]bool) // track added chunks per xorb

	for _, chunk := range allChunks {
		xorbHash := chunk.Dedup.XorbHash
		chunkSize := uint32(len(chunk.Data))

		if _, exists := xorbSeenChunks[xorbHash]; !exists {
			xorbChunksMap[xorbHash] = make([]shard.CASChunkSequenceEntry, 0)
			xorbBytesMap[xorbHash] = 0
			xorbSeenChunks[xorbHash] = make(map[xet.Hash]bool)
		}

		// Only add each chunk once
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

	for _, casBlock := range xorbMap {
		if chunks, ok := xorbChunksMap[casBlock.CASHash]; ok {
			casBlock.Chunks = chunks
			casBlock.NumBytesInCAS = xorbBytesMap[casBlock.CASHash]
		}
		sh.CASInfos = append(sh.CASInfos, *casBlock)
	}

	// Sort CAS blocks by hash for a deterministic shard layout that matches
	// the reference implementation (xet-go).
	sort.Slice(sh.CASInfos, func(i, j int) bool {
		return sh.CASInfos[i].CASHash.String() < sh.CASInfos[j].CASHash.String()
	})

	_, err := client.UploadShard(ctx, sh)
	if err != nil {
		return fmt.Errorf("upload shard: %w", err)
	}

	return nil
}
