package upload

import (
	"context"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
	"maps"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/internal/pool"
	"github.com/wzshiming/xet/progress"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/xorb"
)

// DeduplicationResult represents the result of deduplication for a chunk.
type DeduplicationResult struct {
	ChunkHash  xet.ChunkHash
	IsNew      bool
	XorbHash   xet.XorbHash
	ChunkIndex uint32
}

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

// lazyChunkSeeker implements io.ReadSeeker over a contiguous slice of a
// syncReadSeeker. Each Read performs an atomic seek+read via readAt so that
// concurrent goroutines encoding different xorbs from the same underlying file
// do not race on the shared ReadSeeker position.
type lazyChunkSeeker struct {
	sr   *syncReadSeeker
	base int64 // absolute file offset where this chunk begins
	pos  int64 // current read position relative to the chunk start
}

func (l *lazyChunkSeeker) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		l.pos = offset
	case io.SeekCurrent:
		l.pos += offset
	default:
		return 0, fmt.Errorf("lazyChunkSeeker: unsupported whence %d", whence)
	}
	return l.pos, nil
}

func (l *lazyChunkSeeker) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if err := l.sr.readAt(p, l.base+l.pos); err != nil {
		return 0, err
	}
	l.pos += int64(len(p))
	return len(p), nil
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
	cacheDir     string
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

// WithEnableSHA256 configures whether to compute and include SHA-256 hashes in the shard metadata.
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

// WithCacheDir sets the directory to use for temporary cache files during upload.
func WithCacheDir(cacheDir string) Option {
	return func(o *options) {
		o.cacheDir = cacheDir
	}
}

// UploadFile chunks, deduplicates, and uploads a single file using the provided client adapter.
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

	concurrency := max(1, options.concurrency)

	// Step 1: Chunk all files
	var allChunks []chunkInfo
	fileHashes := make([]xet.FileHash, len(readSeekers))
	fileInfos := make([]shard.FileInfo, len(readSeekers))
	fileChunkHashes := make([][]xet.ChunkHash, len(readSeekers))
	chunkIndex := make(map[xet.ChunkHash]int)

	for index, readSeeker := range readSeekers {
		sr := newSyncReadSeeker(readSeeker)

		var sha256Hasher hash.Hash
		var reader io.Reader = readSeeker
		if options.enableSHA256 {
			sha256Hasher = sha256.New()
			reader = io.TeeReader(reader, sha256Hasher)
		}

		var chunkHashes []xet.ChunkHash
		var chunkSizes []uint64

		err := xet.ChunkData(reader, func(offset int64, chunk []byte) error {
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
						Offset: offset,
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
	localChunkCache, err := deduplicateChunks(ctx, client, uniqueHashes, globalDedupProbeChunkHashes, concurrency)
	if err != nil {
		return nil, fmt.Errorf("deduplicate chunks: %w", err)
	}

	// Step 3: Group new chunks into xorbs
	var newChunks []chunkInfo
	for _, chunk := range allChunks {
		if localChunkCache[chunk.Hash].IsNew {
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

	if options.cacheDir == "" {
		options.cacheDir = os.TempDir()
	}

	// Step 4: Upload xorbs
	if err := uploadXorbs(ctx, client, localChunkCache, xorbs, concurrency, options.cacheDir, options.progressFunc); err != nil {
		return nil, fmt.Errorf("upload xorbs: %w", err)
	}

	// Step 5: Build and upload shard
	chunkInfos := make([]shard.ChunkInfo, len(allChunks))
	for i, chunk := range allChunks {
		dedup := localChunkCache[chunk.Hash]
		chunkInfos[i] = shard.ChunkInfo{
			Hash:       chunk.Hash,
			Size:       chunk.Chunk.Size,
			IsNew:      dedup.IsNew,
			XorbHash:   dedup.XorbHash,
			ChunkIndex: dedup.ChunkIndex,
		}
	}

	_, err = client.UploadShard(ctx, shard.BuildShard(fileInfos, chunkInfos))
	if err != nil {
		return nil, fmt.Errorf("upload shard: %w", err)
	}

	return fileHashes, nil
}

func queryShards(ctx context.Context, client ClientAdapter, cache map[xet.ChunkHash]*DeduplicationResult, probes []xet.ChunkHash) error {
	m, err := client.QueryDedupShards(ctx, probes)
	if err != nil {
		return fmt.Errorf("query dedup shards: %w", err)
	}

	maps.Copy(cache, m)
	return nil
}

func deduplicateChunks(ctx context.Context, client ClientAdapter, chunkHashes []xet.ChunkHash, globalDedupProbeChunkHashes []xet.ChunkHash, concurrency int) (map[xet.ChunkHash]*DeduplicationResult, error) {
	if len(chunkHashes) == 0 {
		return nil, nil
	}

	if len(globalDedupProbeChunkHashes) == 0 {
		return nil, fmt.Errorf("no eligible chunk hashes for global deduplication")
	}

	if concurrency <= 0 {
		concurrency = 1
	}

	cache := make(map[xet.ChunkHash]*DeduplicationResult, len(chunkHashes))

	if err := queryShards(ctx, client, cache, globalDedupProbeChunkHashes); err != nil {
		return nil, fmt.Errorf("query shards: %w", err)
	}

	for _, chunkHash := range chunkHashes {
		if _, hit := cache[chunkHash]; hit {
			continue
		}
		cache[chunkHash] = &DeduplicationResult{
			ChunkHash: chunkHash,
			IsNew:     true,
		}
	}

	return cache, nil
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

// uploadXorbs serializes and uploads all xorbs.
type preparedXorb struct {
	hash        xet.XorbHash
	path        string
	size        int64
	chunkHashes []xet.ChunkHash
}

func uploadXorbs(ctx context.Context, client ClientAdapter, cache map[xet.ChunkHash]*DeduplicationResult, groups []*xorbGroup, concurrency int, cacheDir string, progressFunc progress.ProgressFunc) error {
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
	var cacheMu sync.Mutex
	prepared := make([]preparedXorb, 0, len(groups))
	var preparedMu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for range concurrency {
		go func() {
			defer wg.Done()
			for group := range prepareQueue {
				if ctx.Err() != nil {
					return
				}

				tmpFile, err := os.CreateTemp(cacheDir, "xet-upload-xorb-*")
				if err != nil {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("create temp file: %w", err)
						cancel()
					})
					return
				}
				tmpPath := tmpFile.Name()

				buf := pool.GetChunkBuf()
				defer pool.PutChunkBuf(buf)

				encoder := xorb.NewEncoder(tmpFile, true)
				for _, chunk := range group.Chunks {
					if err := chunk.Reader.readAt(buf[:chunk.Size], chunk.Offset); err != nil {
						errOnce.Do(func() {
							firstErr = fmt.Errorf("read chunk data: %w", err)
							cancel()
						})
						return
					}
					if _, err := encoder.Write(buf[:chunk.Size]); err != nil {
						errOnce.Do(func() {
							firstErr = fmt.Errorf("encode chunk: %w", err)
							cancel()
						})
						return
					}
				}

				if err := encoder.Close(); err != nil {
					tmpFile.Close()
					os.Remove(tmpPath)
					errOnce.Do(func() {
						firstErr = fmt.Errorf("finalize xorb: %w", err)
						cancel()
					})
					return
				}

				xorbHash := encoder.SummoryHash()
				stat, err := tmpFile.Stat()
				if err != nil {
					tmpFile.Close()
					os.Remove(tmpPath)
					errOnce.Do(func() {
						firstErr = fmt.Errorf("stat xorb temp file: %w", err)
						cancel()
					})
					return
				}
				size := stat.Size()

				if err := tmpFile.Close(); err != nil {
					os.Remove(tmpPath)
					errOnce.Do(func() {
						firstErr = fmt.Errorf("close xorb temp file: %w", err)
						cancel()
					})
					return
				}

				exists, err := client.HasXorb(ctx, xorbHash)
				if err != nil {
					os.Remove(tmpPath)
					errOnce.Do(func() {
						firstErr = fmt.Errorf("check xorb %s exists: %w", xorbHash.String(), err)
						cancel()
					})
					return
				}

				if exists {
					os.Remove(tmpPath)
					cacheMu.Lock()
					for i, chunkHash := range group.ChunkHashes {
						if result, ok := cache[chunkHash]; ok && result.IsNew {
							result.XorbHash = xorbHash
							result.ChunkIndex = uint32(i)
						}
					}
					cacheMu.Unlock()
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
	if firstErr != nil {
		for _, item := range prepared {
			_ = os.Remove(item.path)
		}
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

	var doneBytes atomic.Int64
	wg = sync.WaitGroup{}
	wg.Add(concurrency)
	for range concurrency {
		go func() {
			defer wg.Done()
			for item := range uploadQueue {
				if ctx.Err() != nil {
					return
				}

				f, err := os.Open(filepath.Clean(item.path))
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

				cacheMu.Lock()
				for i, chunkHash := range item.chunkHashes {
					if result, ok := cache[chunkHash]; ok && result.IsNew {
						result.XorbHash = item.hash
						result.ChunkIndex = uint32(i)
					}
				}
				cacheMu.Unlock()

				if progressFunc != nil {
					doneBytes.Add(item.size)
					progressFunc(item.hash.String(), item.size, item.size)
				}
			}
		}()
	}

	wg.Wait()
	if firstErr != nil {
		for _, item := range prepared {
			_ = os.Remove(item.path)
		}
		return firstErr
	}

	_ = doneBytes.Load()
	return nil
}
