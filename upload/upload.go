package upload

import (
	"context"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
	"maps"
	"os"
	"sync"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/internal/pool"
	"github.com/wzshiming/xet/progress"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/xorb"
)

type Chunk struct {
	Reader *syncReadSeeker
	Offset int64
	Size   uint32
}

// syncReadSeeker wraps io.ReadSeeker with a mutex so concurrent goroutines can
// each perform an atomic seek+read without interleaving.
type syncReadSeeker struct {
	mu sync.Mutex
	r  io.ReadSeeker
}

func newSyncReadSeeker(r io.ReadSeeker) *syncReadSeeker {
	return &syncReadSeeker{r: r}
}

// readAt atomically seeks to offset and reads exactly len(buf) bytes.
func (s *syncReadSeeker) readAt(buf []byte, offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.r.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("seek chunk: %w", err)
	}
	if _, err := io.ReadFull(s.r, buf); err != nil {
		return fmt.Errorf("read chunk: %w", err)
	}
	return nil
}

// chunkInfo contains information about a chunk within a file.
type chunkInfo struct {
	Hash  xet.ChunkHash
	Chunk Chunk
}

// xorbGroup represents a group of chunks to be packed into a single xorb.
type xorbGroup struct {
	Chunks      []Chunk
	ChunkHashes []xet.ChunkHash
	StartIndex  int
}

type options struct {
	cache        *CacheManager
	concurrency  int
	enableSHA256 bool
	progressFunc progress.ProgressFunc
}

// Option is a functional option for UploadFile and UploadFiles.
type Option func(*options)

// WithConcurrency configures how many upload tasks run concurrently.
func WithConcurrency(concurrency int) Option {
	return func(o *options) {
		o.concurrency = concurrency
	}
}

// WithEnableSHA256 configures whether to compute and include SHA-256 hashes
// in the shard metadata.
func WithEnableSHA256(enabled bool) Option {
	return func(o *options) {
		o.enableSHA256 = enabled
	}
}

// WithProgressFunc sets a callback to receive upload progress updates.
// Progress is reported at xorb-task granularity and committed only after a
// xorb upload succeeds, so retries never inflate the current value.
func WithProgressFunc(progressFunc progress.ProgressFunc) Option {
	return func(o *options) {
		o.progressFunc = progressFunc
	}
}

// WithCacheManager sets the manager for upload staging files and cached
// chunk locations.
func WithCacheManager(cache *CacheManager) Option {
	return func(o *options) {
		o.cache = cache
	}
}

// UploadFile chunks, deduplicates, and uploads a single file using the
// provided client adapter.
func UploadFile(ctx context.Context, client ClientAdapter, readSeeker io.ReadSeeker, opts ...Option) (xet.FileHash, error) {
	hashes, err := UploadFiles(ctx, client, []io.ReadSeeker{readSeeker}, opts...)
	if err != nil {
		return xet.FileHash{}, err
	}
	return hashes[0], nil
}

// UploadFiles chunks, deduplicates, and uploads multiple files using the
// provided client adapter. It returns the computed file hashes.
func UploadFiles(ctx context.Context, client ClientAdapter, readSeekers []io.ReadSeeker, opts ...Option) ([]xet.FileHash, error) {
	options := &options{}
	for _, opt := range opts {
		opt(options)
	}
	if options.cache == nil {
		options.cache = NewCacheManager("", DefaultCacheSize)
	}
	options.cache.prepare()

	concurrency := max(1, options.concurrency)

	// Step 1: Chunk all files
	var allChunks []chunkInfo
	fileHashes := make([]xet.FileHash, len(readSeekers))
	fileInfos := make([]shard.FileInfo, len(readSeekers))
	fileChunkHashes := make([][]xet.ChunkHash, len(readSeekers))
	chunkIndex := make(map[xet.ChunkHash]int)

	for index, readSeeker := range readSeekers {
		sr := newSyncReadSeeker(readSeeker)

		// ChunkData offsets are relative to the reader's current position;
		// record it so readAt can seek with absolute offsets.
		base, err := readSeeker.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, fmt.Errorf("get start offset for file %d: %w", index, err)
		}

		var sha256Hasher hash.Hash
		var reader io.Reader = readSeeker
		if options.enableSHA256 {
			sha256Hasher = sha256.New()
			reader = io.TeeReader(reader, sha256Hasher)
		}

		var chunkHashes []xet.ChunkHash
		var chunkSizes []uint64

		err = xet.ChunkData(reader, func(offset int64, chunk []byte) error {
			chunkHash := xet.ComputeChunkHash(chunk)

			chunkHashes = append(chunkHashes, chunkHash)
			chunkSizes = append(chunkSizes, uint64(len(chunk)))

			idx, exists := chunkIndex[chunkHash]
			if !exists {
				idx = len(allChunks)
				allChunks = append(allChunks, chunkInfo{
					Hash: chunkHash,
					Chunk: Chunk{
						Reader: sr,
						Offset: base + offset,
						Size:   uint32(len(chunk)),
					},
				})

				chunkIndex[chunkHash] = idx
			}

			fileInfos[index].ChunkIndices = append(fileInfos[index].ChunkIndices, idx)
			return nil
		})

		if err != nil {
			return nil, fmt.Errorf("chunk data for file %d: %w", index, err)
		}

		fileChunkHashes[index] = append(fileChunkHashes[index], chunkHashes...)

		fileInfos[index].Hash = xet.ComputeFileHash(chunkHashes, chunkSizes)
		fileHashes[index] = fileInfos[index].Hash

		// Zero-entry files keep the zero "not available" SHA256: servers
		// recompute the digest from file terms and reject a mismatch.
		if options.enableSHA256 && len(fileInfos[index].ChunkIndices) != 0 {
			copy(fileInfos[index].SHA256[:], sha256Hasher.Sum(nil))
		}
	}

	// Step 2: Deduplicate unique chunks
	uniqueHashes := make([]xet.ChunkHash, len(allChunks))
	for i, chunk := range allChunks {
		uniqueHashes[i] = chunk.Hash
	}
	globalDedupProbeChunkHashes := selectChunkHashesForGlobalDedupAcrossFiles(fileChunkHashes)
	located, err := deduplicateChunks(ctx, client, uniqueHashes, globalDedupProbeChunkHashes, concurrency)
	if err != nil {
		return nil, fmt.Errorf("deduplicate chunks: %w", err)
	}

	// Step 3: Group new chunks into xorbs
	var newChunks []chunkInfo
	for _, chunk := range allChunks {
		if _, found := located[chunk.Hash]; !found {
			newChunks = append(newChunks, chunk)
		}
	}

	// Get chunk sizes for grouping
	newChunkSizes := make([]uint32, len(newChunks))
	for i, chunk := range newChunks {
		newChunkSizes[i] = chunk.Chunk.Size
	}

	groupIndices := xorb.GroupChunkIndicesBySize(newChunkSizes, xet.MaxXorbSize)

	// Reconstruct xorbGroups from the grouping indices
	var xorbs []*xorbGroup
	for _, indices := range groupIndices {
		if len(indices) == 0 {
			continue
		}
		group := &xorbGroup{
			Chunks:      make([]Chunk, 0, len(indices)),
			ChunkHashes: make([]xet.ChunkHash, 0, len(indices)),
			StartIndex:  indices[0],
		}
		for _, idx := range indices {
			group.Chunks = append(group.Chunks, newChunks[idx].Chunk)
			group.ChunkHashes = append(group.ChunkHashes, newChunks[idx].Hash)
		}
		xorbs = append(xorbs, group)
	}

	// Step 4: Upload xorbs
	uploaded := make(map[xet.ChunkHash]shard.ChunkLocation)
	if err := uploadXorbs(ctx, client, uploaded, xorbs, concurrency, options.cache, options.progressFunc); err != nil {
		return nil, fmt.Errorf("upload xorbs: %w", err)
	}

	// Step 5: Build and upload shard
	chunkInfos := make([]shard.ChunkInfo, len(allChunks))
	for i, chunk := range allChunks {
		loc, isNew := uploaded[chunk.Hash]
		if !isNew {
			loc = located[chunk.Hash]
		}
		chunkInfos[i] = shard.ChunkInfo{
			Hash:       chunk.Hash,
			Size:       chunk.Chunk.Size,
			IsNew:      isNew,
			XorbHash:   loc.XorbHash,
			ChunkIndex: loc.ChunkIndex,
		}
	}

	_, err = client.UploadShard(ctx, shard.BuildShard(fileInfos, chunkInfos))
	if err != nil {
		return nil, fmt.Errorf("upload shard: %w", err)
	}

	return fileHashes, nil
}

func queryShards(ctx context.Context, client ClientAdapter, located map[xet.ChunkHash]shard.ChunkLocation, probes []xet.ChunkHash, candidates []xet.ChunkHash) error {
	m, err := client.QueryDedupShards(ctx, probes, candidates...)
	if err != nil {
		return fmt.Errorf("query dedup shards: %w", err)
	}

	maps.Copy(located, m)
	return nil
}

// deduplicateChunks returns where the CAS already stores chunkHashes; a hash
// absent from the result is new.
func deduplicateChunks(ctx context.Context, client ClientAdapter, chunkHashes []xet.ChunkHash, globalDedupProbeChunkHashes []xet.ChunkHash, concurrency int) (map[xet.ChunkHash]shard.ChunkLocation, error) {
	if len(chunkHashes) == 0 {
		return nil, nil
	}

	if len(globalDedupProbeChunkHashes) == 0 {
		return nil, fmt.Errorf("no eligible chunk hashes for global deduplication")
	}

	if concurrency <= 0 {
		concurrency = 1
	}

	located := make(map[xet.ChunkHash]shard.ChunkLocation, len(chunkHashes))

	// Pass every chunk hash as a keyed-shard candidate so entries in
	// HMAC-keyed shards can be matched back to raw hashes.
	if err := queryShards(ctx, client, located, globalDedupProbeChunkHashes, chunkHashes); err != nil {
		return nil, fmt.Errorf("query shards: %w", err)
	}

	return located, nil
}

func selectChunkHashesForGlobalDedupAcrossFiles(fileChunkHashes [][]xet.ChunkHash) []xet.ChunkHash {
	var totalChunks int
	for _, chunkHashes := range fileChunkHashes {
		totalChunks += len(chunkHashes)
	}

	probes := make([]xet.ChunkHash, 0, totalChunks)
	seen := make(map[xet.ChunkHash]struct{}, totalChunks)
	for _, chunkHashes := range fileChunkHashes {
		for _, chunkHash := range selectChunkHashesForGlobalDedup(chunkHashes) {
			if _, ok := seen[chunkHash]; ok {
				continue
			}
			seen[chunkHash] = struct{}{}
			probes = append(probes, chunkHash)
		}
	}

	return probes
}

func selectChunkHashesForGlobalDedup(chunkHashes []xet.ChunkHash) []xet.ChunkHash {
	if len(chunkHashes) == 0 {
		return nil
	}

	const minSpacingBetweenGlobalDedupQueries = 256
	probes := make([]xet.ChunkHash, 0, len(chunkHashes)/minSpacingBetweenGlobalDedupQueries+1)
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

		if shard.IsChunkGlobalDedupEligible(chunkHash, false, 0) {
			probes = append(probes, chunkHash)
			lastProbeIndex = i
		}
	}
	return probes
}

type preparedXorb struct {
	hash        xet.XorbHash
	path        string
	size        int64
	chunkHashes []xet.ChunkHash
}

// uploadXorbs serializes and uploads all xorbs, recording in uploaded where
// each chunk of groups ended up.
func uploadXorbs(ctx context.Context, client ClientAdapter, uploaded map[xet.ChunkHash]shard.ChunkLocation, groups []*xorbGroup, concurrency int, staging *CacheManager, progressFunc progress.ProgressFunc) error {
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

	prepareQueue := make(chan *xorbGroup, len(groups))
	for _, group := range groups {
		prepareQueue <- group
	}
	close(prepareQueue)

	var firstErr error
	var errOnce sync.Once
	var uploadedMu sync.Mutex
	prepared := make([]preparedXorb, 0, len(groups))
	var preparedMu sync.Mutex
	var staged []string
	var stagedMu sync.Mutex
	defer func() {
		stagedMu.Lock()
		defer stagedMu.Unlock()
		for _, path := range staged {
			_ = os.Remove(path)
		}
	}()
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for range concurrency {
		go func() {
			defer wg.Done()
			buf := pool.GetChunkBuf()
			defer pool.PutChunkBuf(buf)
			for group := range prepareQueue {
				if ctx.Err() != nil {
					return
				}

				tmpFile, err := staging.create()
				if err != nil {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("create temp file: %w", err)
						cancel()
					})
					return
				}
				tmpPath := tmpFile.Name()
				stagedMu.Lock()
				staged = append(staged, tmpPath)
				stagedMu.Unlock()

				encoder := xorb.NewEncoder(tmpFile, true)
				for _, chunk := range group.Chunks {
					if err := chunk.Reader.readAt(buf[:chunk.Size], chunk.Offset); err != nil {
						_ = tmpFile.Close()
						errOnce.Do(func() {
							firstErr = fmt.Errorf("read chunk data: %w", err)
							cancel()
						})
						return
					}
					if _, err := encoder.Write(buf[:chunk.Size]); err != nil {
						_ = tmpFile.Close()
						errOnce.Do(func() {
							firstErr = fmt.Errorf("encode chunk: %w", err)
							cancel()
						})
						return
					}
				}

				if err := encoder.Close(); err != nil {
					_ = tmpFile.Close()
					errOnce.Do(func() {
						firstErr = fmt.Errorf("finalize xorb: %w", err)
						cancel()
					})
					return
				}

				xorbHash := encoder.SummoryHash()
				stat, err := tmpFile.Stat()
				if err != nil {
					_ = tmpFile.Close()
					errOnce.Do(func() {
						firstErr = fmt.Errorf("stat xorb temp file: %w", err)
						cancel()
					})
					return
				}
				size := stat.Size()

				if err := tmpFile.Close(); err != nil {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("close xorb temp file: %w", err)
						cancel()
					})
					return
				}

				exists, err := client.HasXorb(ctx, xorbHash)
				if err != nil {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("check xorb %s exists: %w", xorbHash.String(), err)
						cancel()
					})
					return
				}

				if exists {
					_ = os.Remove(tmpPath)
					uploadedMu.Lock()
					for i, chunkHash := range group.ChunkHashes {
						uploaded[chunkHash] = shard.ChunkLocation{XorbHash: xorbHash, ChunkIndex: uint32(i)}
					}
					uploadedMu.Unlock()
					continue
				}

				preparedMu.Lock()
				prepared = append(prepared, preparedXorb{
					hash:        xorbHash,
					path:        tmpPath,
					size:        size,
					chunkHashes: append([]xet.ChunkHash(nil), group.ChunkHashes...),
				})
				preparedMu.Unlock()
			}
		}()
	}

	wg.Wait()
	if firstErr == nil {
		// Workers exit silently when the parent context is canceled.
		firstErr = ctx.Err()
	}
	if firstErr != nil {
		return firstErr
	}

	uploadQueue := make(chan preparedXorb, len(prepared))
	for _, item := range prepared {
		uploadQueue <- item
	}
	close(uploadQueue)

	if progressFunc != nil {
		for _, item := range prepared {
			progressFunc(item.hash.String(), 0, item.size)
		}
	}

	wg = sync.WaitGroup{}
	wg.Add(concurrency)
	for range concurrency {
		go func() {
			defer wg.Done()
			for item := range uploadQueue {
				if ctx.Err() != nil {
					return
				}

				f, err := os.Open(item.path)
				if err != nil {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("open xorb temp file: %w", err)
						cancel()
					})
					return
				}

				_, err = client.UploadXorb(ctx, item.hash, f)
				_ = f.Close()
				_ = os.Remove(item.path)
				if err != nil {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("upload xorb %s: %w", item.hash.String(), err)
						cancel()
					})
					return
				}

				uploadedMu.Lock()
				for i, chunkHash := range item.chunkHashes {
					uploaded[chunkHash] = shard.ChunkLocation{XorbHash: item.hash, ChunkIndex: uint32(i)}
				}
				uploadedMu.Unlock()

				if progressFunc != nil {
					progressFunc(item.hash.String(), item.size, item.size)
				}
			}
		}()
	}

	wg.Wait()
	if firstErr == nil {
		firstErr = ctx.Err()
	}
	return firstErr
}
