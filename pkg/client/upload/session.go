package upload

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/pkg/client"
	"github.com/wzshiming/xet/pkg/shard"
	"github.com/wzshiming/xet/pkg/xorb"
)

// Session represents an upload session
type Session struct {
	client            *client.Client
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

// SessionOptions configures an upload session
type SessionOptions struct {
	Client            *client.Client
	TargetXorbSize    uint64
	EnableGlobalDedup bool
}

// NewSession creates a new upload session
func NewSession(opts SessionOptions) *Session {
	if opts.TargetXorbSize == 0 {
		opts.TargetXorbSize = xet.TargetChunkSize
	}

	return &Session{
		client:            opts.Client,
		localChunkCache:   make(map[xet.Hash]*DeduplicationResult),
		targetXorbSize:    opts.TargetXorbSize,
		enableGlobalDedup: opts.EnableGlobalDedup,
	}
}

// FileUploadInfo contains information about a file to upload
type FileUploadInfo struct {
	Path     string
	Data     []byte
	FileHash xet.Hash
	SHA256   [32]byte
}

// ChunkInfo contains information about a chunk within a file
type ChunkInfo struct {
	FileIndex int
	Data      []byte
	Hash      xet.Hash
	Offset    uint64
	Dedup     *DeduplicationResult
}

// UploadFiles uploads one or more files to the CAS server
func (s *Session) UploadFiles(ctx context.Context, files []FileUploadInfo) error {
	// Step 1: Chunk all files and deduplicate
	var allChunks []ChunkInfo
	fileChunkRanges := make(map[int][]int) // fileIndex -> chunk indices

	for fileIdx, file := range files {
		err := xet.ChunkData(bytes.NewReader(file.Data), func(offset int64, chunk []byte) error {
			chunkHash := xet.ComputeChunkHash(chunk)

			// Deduplicate
			dedupResult := s.deduplicateChunk(ctx, chunkHash)

			newChunk := make([]byte, len(chunk))
			copy(newChunk, chunk)
			chunkIdx := len(allChunks)
			allChunks = append(allChunks, ChunkInfo{
				FileIndex: fileIdx,
				Data:      newChunk,
				Hash:      chunkHash,
				Offset:    uint64(offset),
				Dedup:     dedupResult,
			})

			fileChunkRanges[fileIdx] = append(fileChunkRanges[fileIdx], chunkIdx)
			return nil
		})
		if err != nil {
			return fmt.Errorf("chunk file %s: %w", file.Path, err)
		}
	}

	// Step 2: Group new chunks into xorbs
	var newChunks []ChunkInfo
	for _, chunk := range allChunks {
		if chunk.Dedup.IsNew {
			newChunks = append(newChunks, chunk)
		}
	}

	xorbs := s.groupChunksIntoXorbs(newChunks)

	// Step 3: Upload xorbs
	if err := s.uploadXorbs(ctx, xorbs); err != nil {
		return fmt.Errorf("upload xorbs: %w", err)
	}

	// Step 4: Build and upload shard
	if err := s.buildAndUploadShard(ctx, files, allChunks, fileChunkRanges); err != nil {
		return fmt.Errorf("build and upload shard: %w", err)
	}

	return nil
}

// deduplicateChunk checks if a chunk already exists
func (s *Session) deduplicateChunk(ctx context.Context, chunkHash xet.Hash) *DeduplicationResult {
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

// XorbGroup represents a group of chunks to be packed into a single xorb
type XorbGroup struct {
	Chunks      [][]byte
	ChunkHashes []xet.Hash
	Xorb        *xorb.Xorb
	Serialized  []byte
	StartIndex  int
}

// groupChunksIntoXorbs groups chunks into xorbs targeting the specified size
func (s *Session) groupChunksIntoXorbs(chunks []ChunkInfo) []*XorbGroup {
	var groups []*XorbGroup
	var currentGroup *XorbGroup
	var currentSize uint64
	startIndex := 0

	for i, chunk := range chunks {
		if currentGroup == nil {
			currentGroup = &XorbGroup{
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
func (s *Session) uploadXorbs(ctx context.Context, groups []*XorbGroup) error {
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
func (s *Session) buildAndUploadShard(ctx context.Context, files []FileUploadInfo, allChunks []ChunkInfo, fileChunkRanges map[int][]int) error {
	// Create shard with default header
	sh := shard.NewShard()

	// Track unique xorbs
	xorbMap := make(map[xet.Hash]*shard.CASBlock)

	// Build file blocks
	for fileIdx, file := range files {
		chunkIndices := fileChunkRanges[fileIdx]

		// Collect all chunk hashes for this file
		fileChunkHashes := make([]xet.Hash, len(chunkIndices))
		for i, chunkIdx := range chunkIndices {
			fileChunkHashes[i] = allChunks[chunkIdx].Hash
		}

		fileBlock := shard.FileBlock{
			FileHash:     file.FileHash,
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
				currentStart = chunk.Dedup.ChunkIndex
				currentBytes = chunkSize
				currentChunkStart = i
			} else {
				currentBytes += chunkSize
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

	// Build CAS blocks
	for _, casBlock := range xorbMap {
		sh.CASInfos = append(sh.CASInfos, *casBlock)
	}

	// Serialize and upload
	serialized, err := sh.Serialize()
	if err != nil {
		return fmt.Errorf("serialize shard: %w", err)
	}

	if s.client != nil {
		_, err = s.client.UploadShard(ctx, serialized)
		if err != nil {
			return fmt.Errorf("upload shard: %w", err)
		}
	}

	return nil
}

// ComputeFileInfo computes hash information for a file
func ComputeFileInfo(data []byte) (FileUploadInfo, error) {
	// Compute chunk hashes
	chunkHashes := []xet.Hash{}

	// Compute chunk sizes
	chunkSizes := []uint64{}

	err := xet.ChunkData(bytes.NewReader(data), func(offset int64, chunk []byte) error {
		chunkHashes = append(chunkHashes, xet.ComputeChunkHash(chunk))
		chunkSizes = append(chunkSizes, uint64(len(chunk)))
		return nil
	})
	if err != nil {
		return FileUploadInfo{}, fmt.Errorf("chunk data: %w", err)
	}

	fileHash := xet.ComputeFileHash(chunkHashes, chunkSizes)

	// Compute SHA-256
	sha256Hash := sha256.Sum256(data)

	return FileUploadInfo{
		Data:     data,
		FileHash: fileHash,
		SHA256:   sha256Hash,
	}, nil
}
