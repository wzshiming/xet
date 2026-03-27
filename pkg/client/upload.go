package client

import (
	"context"
	"fmt"
	"io"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/pkg/shard"
	"github.com/wzshiming/xet/pkg/xorb"
)

// UploadSession represents an upload session
type UploadSession struct {
	client            *Client
	localChunkCache   map[xet.Hash]*DeduplicationResult
	targetXorbSize    uint64
	enableGlobalDedup bool
}

// DeduplicationResult represents the result of deduplication for a chunk
type DeduplicationResult struct {
	ChunkHash  xet.Hash
	IsNew      bool
	XorbHash   xet.Hash
	ChunkIndex uint32
}

// UploadSessionOptions configures an upload session
type UploadSessionOptions struct {
	Client            *Client
	EnableGlobalDedup bool
}

// NewUploadSession creates a new upload session
func NewUploadSession(opts UploadSessionOptions) *UploadSession {
	return &UploadSession{
		client:            opts.Client,
		localChunkCache:   make(map[xet.Hash]*DeduplicationResult),
		targetXorbSize:    xet.MaxXorbSerializedSize,
		enableGlobalDedup: opts.EnableGlobalDedup,
	}
}

// chunkInfo contains information about a chunk within a file
type chunkInfo struct {
	FileIndex int
	Data      []byte
	Hash      xet.Hash
	Offset    uint64
	Dedup     *DeduplicationResult
}

// UploadFiles uploads multiple files and returns their hashes
func (s *UploadSession) UploadFiles(ctx context.Context, readers ...io.Reader) ([]xet.Hash, error) {
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
			dedupResult := s.deduplicateChunk(ctx, chunkHash)

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
	for _, chunk := range allChunks {
		if chunk.Dedup.IsNew {
			newChunks = append(newChunks, chunk)
		}
	}

	xorbs := s.groupChunksIntoXorbs(newChunks)

	// Step 3: Upload xorbs
	if err := s.uploadXorbs(ctx, xorbs); err != nil {
		return nil, fmt.Errorf("upload xorbs: %w", err)
	}

	// Step 4: Build and upload shard
	if err := s.buildAndUploadShard(ctx, fileHashes, allChunks, fileChunkRanges); err != nil {
		return nil, fmt.Errorf("build and upload shard: %w", err)
	}

	return fileHashes, nil
}

// deduplicateChunk checks if a chunk already exists
func (s *UploadSession) deduplicateChunk(ctx context.Context, chunkHash xet.Hash) *DeduplicationResult {
	// Check local session cache first
	if result, ok := s.localChunkCache[chunkHash]; ok {
		return result
	}

	// Check global deduplication if enabled
	if s.enableGlobalDedup && s.client != nil {
		shardData, err := s.client.QueryChunkDeduplication(ctx, chunkHash)
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
					s.localChunkCache[chunkHash] = result
					return result
				}
			}
		}
	}

	// Not found - mark as new
	result := &DeduplicationResult{
		ChunkHash: chunkHash,
		IsNew:     true,
	}
	s.localChunkCache[chunkHash] = result
	return result
}

// xorbGroup represents a group of chunks to be packed into a single xorb
type xorbGroup struct {
	Chunks      [][]byte
	ChunkHashes []xet.Hash
	Xorb        *xorb.Xorb
	Serialized  []byte
	StartIndex  int
}

// groupChunksIntoXorbs groups chunks into xorbs targeting the specified size
func (s *UploadSession) groupChunksIntoXorbs(chunks []chunkInfo) []*xorbGroup {
	var groups []*xorbGroup
	var currentGroup *xorbGroup
	var currentSize uint64
	startIndex := 0

	for i, chunk := range chunks {
		if currentGroup == nil {
			currentGroup = &xorbGroup{
				Chunks:      make([][]byte, 0),
				ChunkHashes: make([]xet.Hash, 0),
				StartIndex:  startIndex,
			}
		}

		currentGroup.Chunks = append(currentGroup.Chunks, chunk.Data)
		currentGroup.ChunkHashes = append(currentGroup.ChunkHashes, chunk.Hash)
		currentSize += uint64(len(chunk.Data))

		// Check if we should start a new group
		if currentSize >= s.targetXorbSize {
			groups = append(groups, currentGroup)
			currentGroup = nil
			currentSize = 0
			startIndex = i + 1
		}
	}

	// Add remaining group
	if currentGroup != nil && len(currentGroup.Chunks) > 0 {
		groups = append(groups, currentGroup)
	}

	return groups
}

// uploadXorbs serializes and uploads all xorbs
func (s *UploadSession) uploadXorbs(ctx context.Context, groups []*xorbGroup) error {
	for _, group := range groups {
		// Create xorb
		xorbObj := xorb.NewXorb()
		for _, chunkData := range group.Chunks {
			if err := xorbObj.AddChunk(chunkData); err != nil {
				return fmt.Errorf("add chunk to xorb: %w", err)
			}
		}

		// Serialize
		serialized, err := xorbObj.Serialize()
		if err != nil {
			return fmt.Errorf("serialize xorb: %w", err)
		}

		group.Xorb = xorbObj
		group.Serialized = serialized

		// Upload
		if s.client != nil {
			_, err = s.client.UploadXorb(ctx, xorbObj.Hash, serialized)
			if err != nil {
				return fmt.Errorf("upload xorb %s: %w", xorbObj.Hash.String(), err)
			}
		}

		// Update local cache with xorb information
		for i, chunkHash := range group.ChunkHashes {
			if result, ok := s.localChunkCache[chunkHash]; ok && result.IsNew {
				result.IsNew = false
				result.XorbHash = xorbObj.Hash
				result.ChunkIndex = uint32(i)
			}
		}
	}

	return nil
}

// buildAndUploadShard constructs and uploads the shard
func (s *UploadSession) buildAndUploadShard(ctx context.Context, fileHashes []xet.Hash, allChunks []chunkInfo, fileChunkRanges map[int][]int) error {
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
	xorbBytesMap := make(map[xet.Hash]uint32) // total uncompressed bytes per xorb

	for _, chunk := range allChunks {
		xorbHash := chunk.Dedup.XorbHash
		chunkSize := uint32(len(chunk.Data))

		if _, exists := xorbChunksMap[xorbHash]; !exists {
			xorbChunksMap[xorbHash] = make([]shard.CASChunkSequenceEntry, 0)
			xorbBytesMap[xorbHash] = 0
		}

		// Only add each chunk once (by checking if chunk index is already added)
		alreadyAdded := false
		for _, existing := range xorbChunksMap[xorbHash] {
			if existing.ChunkHash == chunk.Hash {
				alreadyAdded = true
				break
			}
		}

		if !alreadyAdded {
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

	// Serialize and upload
	serialized, err := sh.Serialize()
	if err != nil {
		return fmt.Errorf("serialize shard: %w", err)
	}

	_, err = s.client.UploadShard(ctx, serialized)
	if err != nil {
		return fmt.Errorf("upload shard: %w", err)
	}

	return nil
}
