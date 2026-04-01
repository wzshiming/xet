package upload

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"sort"
	"sync"

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

type options struct {
	concurrency  int
	enableSHA256 bool
}

// WithConcurrency configures how many upload tasks run concurrently.
func WithConcurrency(concurrency int) func(*options) {
	return func(o *options) {
		o.concurrency = concurrency
	}
}

// WithEnableSHA256 configures whether to compute and include SHA-256 hashes in the shard metadata.
func WithEnableSHA256(enabled bool) func(*options) {
	return func(o *options) {
		o.enableSHA256 = enabled
	}
}

// UploadFile chunks, deduplicates, and uploads a single file using the provided client adapter.
func UploadFile(ctx context.Context, client ClientAdapter, reader io.Reader, opts ...func(*options)) (xet.Hash, error) {
	hashes, err := UploadFiles(ctx, client, []io.Reader{reader}, opts...)
	if err != nil {
		return xet.Hash{}, err
	}
	return hashes[0], nil
}

// UploadFiles chunks, deduplicates, and uploads multiple files using the
// provided client adapter. It returns the computed file hashes.
func UploadFiles(ctx context.Context, client ClientAdapter, readers []io.Reader, opts ...func(*options)) ([]xet.Hash, error) {
	options := &options{}
	for _, opt := range opts {
		opt(options)
	}

	concurrency := max(1, options.concurrency)

	localChunkCache := make(map[xet.Hash]*DeduplicationResult)

	// Step 1: Chunk all files
	var allChunks []chunkInfo
	var fileHashes []xet.Hash
	var fileSHA256s [][32]byte
	fileChunkRanges := make(map[int][]int) // fileIndex -> chunk indices

	for index, reader := range readers {
		var sha256Hasher hash.Hash
		if options.enableSHA256 {
			sha256Hasher = sha256.New()
			reader = io.TeeReader(reader, sha256Hasher)
		}

		// Compute chunk hashes
		chunkHashes := []xet.Hash{}

		// Compute chunk sizes
		chunkSizes := []uint64{}

		err := xet.ChunkData(reader, func(offset int64, chunk xet.ChunkBytes) error {
			chunkHash := chunk.Hash()

			chunkHashes = append(chunkHashes, chunkHash)
			chunkSizes = append(chunkSizes, chunk.Size())

			chunkIdx := len(allChunks)
			allChunks = append(allChunks, chunkInfo{
				FileIndex: index,
				Data:      chunk.Bytes(),
				Hash:      chunkHash,
				Offset:    uint64(offset),
			})

			fileChunkRanges[index] = append(fileChunkRanges[index], chunkIdx)
			return nil
		})

		if err != nil {
			return nil, fmt.Errorf("chunk data for file %d: %w", index, err)
		}

		fileHash := xet.ComputeFileHash(chunkHashes, chunkSizes)

		fileHashes = append(fileHashes, fileHash)

		if options.enableSHA256 {
			var fileSHA256 [32]byte
			copy(fileSHA256[:], sha256Hasher.Sum(nil))
			fileSHA256s = append(fileSHA256s, fileSHA256)
		}

	}

	// Step 2: Deduplicate unique chunks
	uniqueChunkHashes := uniqueChunkHashes(allChunks)
	deduplicateChunks(ctx, client, localChunkCache, uniqueChunkHashes, concurrency)
	for i := range allChunks {
		allChunks[i].Dedup = localChunkCache[allChunks[i].Hash]
	}

	// Step 3: Group new chunks into xorbs
	var newChunks []chunkInfo
	seenNewChunk := make(map[xet.Hash]bool)
	for _, chunk := range allChunks {
		if chunk.Dedup.IsNew && !seenNewChunk[chunk.Hash] {
			newChunks = append(newChunks, chunk)
			seenNewChunk[chunk.Hash] = true
		}
	}

	xorbs := groupChunksIntoXorbs(newChunks, xet.MaxXorbSerializedSize)

	// Step 4: Upload xorbs
	if err := uploadXorbs(ctx, client, localChunkCache, xorbs, concurrency); err != nil {
		return nil, fmt.Errorf("upload xorbs: %w", err)
	}

	// Step 5: Build and upload shard
	if err := buildAndUploadShard(ctx, client, fileHashes, fileSHA256s, allChunks, fileChunkRanges); err != nil {
		return nil, fmt.Errorf("build and upload shard: %w", err)
	}

	return fileHashes, nil
}

func deduplicateChunks(ctx context.Context, client ClientAdapter, cache map[xet.Hash]*DeduplicationResult, chunkHashes []xet.Hash, concurrency int) {
	if len(chunkHashes) == 0 {
		return
	}

	for _, chunkHash := range chunkHashes {
		cache[chunkHash] = &DeduplicationResult{ChunkHash: chunkHash, IsNew: true}
	}

	probeChunkHashes := selectChunkHashesForGlobalDedup(chunkHashes)
	if len(probeChunkHashes) == 0 {
		return
	}

	if batchClient, ok := client.(BatchDeduplicationClientAdapter); ok {
		if deduplicateChunksBatch(ctx, batchClient, cache, probeChunkHashes, concurrency) {
			return
		}
	}

	deduplicateChunksSingle(ctx, client, cache, probeChunkHashes, concurrency)
}

func selectChunkHashesForGlobalDedup(chunkHashes []xet.Hash) []xet.Hash {
	if len(chunkHashes) == 0 {
		return nil
	}

	const minSpacingBetweenGlobalDedupQueries = 256
	probes := make([]xet.Hash, 0, len(chunkHashes)/minSpacingBetweenGlobalDedupQueries+1)
	lastProbeIndex := -minSpacingBetweenGlobalDedupQueries
	for i, chunkHash := range chunkHashes {
		if i == 0 {
			probes = append(probes, chunkHash)
			lastProbeIndex = i
			continue
		}

		if i-lastProbeIndex < minSpacingBetweenGlobalDedupQueries {
			continue
		}

		if isChunkHashGlobalDedupEligible(chunkHash) {
			probes = append(probes, chunkHash)
			lastProbeIndex = i
		}
	}
	return probes
}

func isChunkHashGlobalDedupEligible(chunkHash xet.Hash) bool {
	const dedupModulus uint64 = 1024
	value := binary.LittleEndian.Uint64(chunkHash[24:32])
	return value%dedupModulus == 0
}

func deduplicateChunksBatch(ctx context.Context, client BatchDeduplicationClientAdapter, cache map[xet.Hash]*DeduplicationResult, chunkHashes []xet.Hash, concurrency int) bool {
	if len(chunkHashes) == 0 {
		return true
	}
	if concurrency <= 0 {
		concurrency = 1
	}

	const batchSize = 256
	batchCount := (len(chunkHashes) + batchSize - 1) / batchSize
	if concurrency > batchCount {
		concurrency = batchCount
	}
	if concurrency <= 0 {
		concurrency = 1
	}

	queue := make(chan []xet.Hash, batchCount)
	for start := 0; start < len(chunkHashes); start += batchSize {
		end := min(start+batchSize, len(chunkHashes))
		batch := make([]xet.Hash, end-start)
		copy(batch, chunkHashes[start:end])
		queue <- batch
	}
	close(queue)

	var mu sync.Mutex
	var hadError bool
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for range concurrency {
		go func() {
			defer wg.Done()
			for batch := range queue {
				results, err := client.QueryChunksDeduplication(ctx, batch)
				if err != nil {
					mu.Lock()
					hadError = true
					mu.Unlock()
					return
				}

				mu.Lock()
				for _, chunkHash := range batch {
					result, ok := results[chunkHash]
					if !ok || result == nil {
						result = &DeduplicationResult{ChunkHash: chunkHash, IsNew: true}
					} else if !result.IsNew && result.XorbHash == (xet.Hash{}) {
						// A dedup hit without a CAS target is unusable; treat as new.
						result = &DeduplicationResult{ChunkHash: chunkHash, IsNew: true}
					}
					cache[chunkHash] = result
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	return !hadError
}

func deduplicateChunksSingle(ctx context.Context, client ClientAdapter, cache map[xet.Hash]*DeduplicationResult, chunkHashes []xet.Hash, concurrency int) {
	if len(chunkHashes) == 0 {
		return
	}
	if concurrency <= 0 {
		concurrency = 1
	}
	if concurrency > len(chunkHashes) {
		concurrency = len(chunkHashes)
	}

	queue := make(chan xet.Hash, len(chunkHashes))
	for _, chunkHash := range chunkHashes {
		queue <- chunkHash
	}
	close(queue)

	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for range concurrency {
		go func() {
			defer wg.Done()
			for chunkHash := range queue {
				result := queryDeduplicationChunk(ctx, client, chunkHash)
				if !result.IsNew && result.XorbHash == (xet.Hash{}) {
					// Defensive fallback for malformed dedup responses.
					result = &DeduplicationResult{ChunkHash: chunkHash, IsNew: true}
				}
				mu.Lock()
				cache[chunkHash] = result
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
}

// queryDeduplicationChunk checks if a chunk already exists globally.
func queryDeduplicationChunk(ctx context.Context, client ClientAdapter, chunkHash xet.Hash) *DeduplicationResult {
	shardData, err := client.QueryChunkDeduplication(ctx, chunkHash)
	if err == nil && shardData != nil {
		xorbHash, chunkIndex, ok := findChunkLocationInShard(shardData, chunkHash)
		if ok {
			result := &DeduplicationResult{
				ChunkHash:  chunkHash,
				IsNew:      false,
				XorbHash:   xorbHash,
				ChunkIndex: chunkIndex,
			}
			return result
		}
	}

	// Not found - mark as new
	result := &DeduplicationResult{
		ChunkHash: chunkHash,
		IsNew:     true,
	}
	return result
}

func findChunkLocationInShard(shardData *shard.Shard, chunkHash xet.Hash) (xet.Hash, uint32, bool) {
	for _, casBlock := range shardData.CASInfos {
		for i, casChunk := range casBlock.Chunks {
			if casChunk.ChunkHash == chunkHash {
				return casBlock.CASHash, uint32(i), true
			}
		}
	}

	return xet.Hash{}, 0, false
}

func uniqueChunkHashes(chunks []chunkInfo) []xet.Hash {
	seen := make(map[xet.Hash]bool, len(chunks))
	unique := make([]xet.Hash, 0, len(chunks))
	for _, chunk := range chunks {
		if seen[chunk.Hash] {
			continue
		}
		seen[chunk.Hash] = true
		unique = append(unique, chunk.Hash)
	}
	return unique
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
func uploadXorbs(ctx context.Context, client ClientAdapter, cache map[xet.Hash]*DeduplicationResult, groups []*xorbGroup, concurrency int) error {
	if len(groups) == 0 {
		return nil
	}
	if concurrency <= 0 {
		concurrency = 1
	}
	if concurrency > len(groups) {
		concurrency = len(groups)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	queue := make(chan *xorbGroup, len(groups))
	for _, group := range groups {
		queue <- group
	}
	close(queue)

	var firstErr error
	var errOnce sync.Once
	var cacheMu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for range concurrency {
		go func() {
			defer wg.Done()
			for group := range queue {
				if ctx.Err() != nil {
					return
				}

				xorbObj := xorb.NewXorb()
				for _, chunkData := range group.Chunks {
					if err := xorbObj.AddChunk(chunkData); err != nil {
						errOnce.Do(func() {
							firstErr = fmt.Errorf("add chunk to xorb: %w", err)
							cancel()
						})
						return
					}
				}

				if _, err := client.UploadXorb(ctx, xorbObj); err != nil {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("upload xorb %s: %w", xorbObj.Hash.String(), err)
						cancel()
					})
					return
				}

				cacheMu.Lock()
				for i, chunkHash := range group.ChunkHashes {
					if result, ok := cache[chunkHash]; ok && result.IsNew {
						result.XorbHash = xorbObj.Hash
						result.ChunkIndex = uint32(i)
					}
				}
				cacheMu.Unlock()
			}
		}()
	}

	wg.Wait()
	return firstErr
}

// buildAndUploadShard constructs and uploads the shard.
func buildAndUploadShard(ctx context.Context, client ClientAdapter, fileHashes []xet.Hash, fileSHA256s [][32]byte, allChunks []chunkInfo, fileChunkRanges map[int][]int) error {
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
		if len(fileSHA256s) == len(fileHashes) &&
			len(chunkIndices) != 0 &&
			fileSHA256s[fileIdx] != ([32]byte{}) {
			fileBlock.MetadataExt = &shard.FileMetadataExt{
				SHA256Hash: xet.EncodeSHA256ForMetadata(fileSHA256s[fileIdx]),
			}
			fileBlock.Flags |= shard.FileWithMetadataExt
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
			if chunk.Dedup.IsNew {
				if _, exists := xorbMap[xorbHash]; !exists {
					xorbMap[xorbHash] = &shard.CASBlock{
						CASHash: xorbHash,
						Chunks:  make([]shard.CASChunkSequenceEntry, 0),
					}
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
		if !chunk.Dedup.IsNew {
			continue
		}

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
