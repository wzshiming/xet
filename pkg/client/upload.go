package client

import (
	"context"
	"fmt"
	"io"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/pkg/reconstruction"
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

		// Upload
		_, err := s.client.UploadXorb(ctx, xorbObj)
		if err != nil {
			return fmt.Errorf("upload xorb %s: %w", xorbObj.Hash.String(), err)
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

	// Build file blocks for each file
	for fileIdx, fileHash := range fileHashes {
		chunkIndices := fileChunkRanges[fileIdx]

		// Convert chunkInfo to reconstruction.ChunkInfo
		reconChunks := make([]reconstruction.ChunkInfo, len(chunkIndices))
		for i, chunkIdx := range chunkIndices {
			chunk := allChunks[chunkIdx]
			reconChunks[i] = reconstruction.ChunkInfo{
				Hash:      chunk.Hash,
				Data:      chunk.Data,
				XorbHash:  chunk.Dedup.XorbHash,
				ChunkIdx:  chunk.Dedup.ChunkIndex,
				FileIndex: chunk.FileIndex,
			}
		}

		// Build file block using reconstruction package
		fileBlock := reconstruction.BuildFileBlock(fileHash, reconChunks)
		sh.Files = append(sh.Files, fileBlock)
	}

	// Convert all chunks to reconstruction.ChunkInfo for CAS blocks
	reconAllChunks := make([]reconstruction.ChunkInfo, len(allChunks))
	for i, chunk := range allChunks {
		reconAllChunks[i] = reconstruction.ChunkInfo{
			Hash:      chunk.Hash,
			Data:      chunk.Data,
			XorbHash:  chunk.Dedup.XorbHash,
			ChunkIdx:  chunk.Dedup.ChunkIndex,
			FileIndex: chunk.FileIndex,
		}
	}

	// Build CAS blocks using reconstruction package
	casBlocks := reconstruction.BuildCASBlocks(reconAllChunks)
	sh.CASInfos = casBlocks

	_, err := s.client.UploadShard(ctx, sh)
	if err != nil {
		return fmt.Errorf("upload shard: %w", err)
	}

	return nil
}
