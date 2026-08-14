package download

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/wzshiming/xet/internal/flock"
	"github.com/wzshiming/xet/internal/pool"
)

// chunkRef records the location of one decoded chunk within the backing file.
type chunkRef struct {
	offset  int64
	size    int32
	fileIdx int // index into chunkCache.files; 0 means chunkCache.file
}

type cacheRange struct {
	cacheDir             string
	hash                 string
	chunkStart, chunkEnd uint32
	bytesStart, bytesEnd int64
}

func newCacheRange(cacheDir, hash string, chunkStart, chunkEnd uint32, bytesStart, bytesEnd int64) (cacheRange, error) {
	if len(hash) < 2 || chunkEnd < chunkStart || bytesEnd < bytesStart {
		return cacheRange{}, fmt.Errorf("invalid cache range for hash %q", hash)
	}
	return cacheRange{
		cacheDir:   cacheDir,
		hash:       hash,
		chunkStart: chunkStart,
		chunkEnd:   chunkEnd,
		bytesStart: bytesStart,
		bytesEnd:   bytesEnd,
	}, nil
}

func (r cacheRange) dir() string {
	return filepath.Join(r.cacheDir, r.hash[:2], r.hash[2:])
}

func (r cacheRange) path() string {
	return filepath.Join(r.dir(), cacheFileName(r.chunkStart, r.chunkEnd, r.bytesStart, r.bytesEnd))
}

// chunkCache is the single cache for decoded xorb chunk ranges.
//
// Each entry is stored as a single file at:
//
//	cacheDir/<hash[:2]>/<hash[2:]>/<chunkStart>-<chunkEnd>_<bytesStart>-<bytesEnd>
//
// where bytesStart and bytesEnd identify the source xorb byte range.
//
// File format (all integers little-endian):
//
//	[numOffsets  uint32]           // = numChunks + 1
//	[offset[0]   uint32]           // always 0
//	[offset[1]   uint32]           // end of chunk 0
//	...
//	[offset[N]   uint32]           // = total data length
//	[data bytes ...]               // decoded chunks concatenated
//	[crc32       uint32]           // checksum of the header and data
//
// The crc32 (IEEE) input is the data region followed by the header, so the
// streaming writer hashes chunks as they arrive and folds in the offset table
// when it is patched at seal time. It is checked lazily, on the first open
// per manager.
//
// The entry file itself is the flock target, held by its writer from creation
// until the file is sealed. Completeness is arithmetic: the name fixes the
// header size, so a file is complete exactly when its size equals the header
// plus offset[N] plus the trailer. A file only grows from incomplete to
// complete and complete files are never rewritten, only unlinked, so readers
// adopt them without locking. Writers truncate leftovers of crashed downloads
// in place; that is safe because readers never adopt incomplete files.
type chunkCache struct {
	dec            io.Reader
	metas          []chunkRef
	writePos       int64 // next write offset in file
	done           bool
	readonly       bool // true for read-only cache from openCachedRange
	mut            sync.Mutex
	file           *os.File   // backing file
	files          []*os.File // additional files for multi-file read path (fileIdx > 0)
	lockFile       *os.File   // same handle as file while the write lock is held
	path           string     // entry file path
	hash           string     // xorb hash, set for writers to hint the merger
	expectedChunks uint32
	crc            uint32 // incremental crc32 of the data region while writing
	published      bool
	manager        *CacheManager
	refPaths       []string // cache files acquired from manager, released in Done
}

// cacheFileName returns the entry file name for a chunk range.
func cacheFileName(start, end uint32, bytesStart, bytesEnd int64) string {
	return fmt.Sprintf("%d-%d_%d-%d", start, end, bytesStart, bytesEnd)
}

func cacheFilePath(cacheDir, hash string, start, end uint32, bytesStart, bytesEnd int64) string {
	return filepath.Join(cacheDir, hash[:2], hash[2:], cacheFileName(start, end, bytesStart, bytesEnd))
}

// parseCacheFileName parses a filename of the form
// "<chunkStart>-<chunkEnd>_<bytesStart>-<bytesEnd>" and returns the components.
// Returns ok=false if parsing fails.
func parseCacheFileName(name string) (chunkStart, chunkEnd uint32, bytesStart, bytesEnd int64, ok bool) {
	parts := strings.Split(name, "_")
	if len(parts) != 2 {
		return
	}
	chunkParts := strings.SplitN(parts[0], "-", 2)
	if len(chunkParts) != 2 {
		return
	}
	cs, err := strconv.ParseUint(chunkParts[0], 10, 32)
	if err != nil {
		return
	}
	ce, err := strconv.ParseUint(chunkParts[1], 10, 32)
	if err != nil {
		return
	}
	bytesParts := strings.SplitN(parts[1], "-", 2)
	if len(bytesParts) != 2 {
		return
	}
	bs, err := strconv.ParseInt(bytesParts[0], 10, 64)
	if err != nil || bs < 0 {
		return
	}
	be, err := strconv.ParseInt(bytesParts[1], 10, 64)
	if err != nil || be < 0 {
		return
	}
	if ce < cs || be < bs {
		return
	}
	return uint32(cs), uint32(ce), bs, be, true
}

// newChunkCache creates a chunkCache that decodes chunks from dec and writes
// them to a cache file. Concurrent callers wait on the entry lock and reuse a
// completed file published by the first caller.
//
// The caller must call Done() to release the file lock.
func newChunkCache(dec io.Reader, m *CacheManager, hash string, chunkStart, chunkEnd uint32, bytesStart, bytesEnd int64) (*chunkCache, error) {
	lockFile, err := lockChunkCache(m.dir, hash, chunkStart, chunkEnd, bytesStart, bytesEnd)
	if err != nil {
		return nil, err
	}
	cache, err := openCachedRange(m, hash, chunkStart, chunkEnd)
	if err != nil || cache != nil {
		flock.Unlock(lockFile) //nolint:errcheck
		lockFile.Close()
		return cache, err
	}
	cache, err = newLockedChunkCache(dec, m, hash, chunkStart, chunkEnd, bytesStart, bytesEnd, lockFile)
	if err != nil {
		flock.Unlock(lockFile) //nolint:errcheck
		lockFile.Close()
	}
	return cache, err
}

func lockChunkCache(cacheDir, hash string, chunkStart, chunkEnd uint32, bytesStart, bytesEnd int64) (*os.File, error) {
	r, err := newCacheRange(cacheDir, hash, chunkStart, chunkEnd, bytesStart, bytesEnd)
	if err != nil {
		return nil, err
	}

	// Cache eviction may unlink the entry file (or its directory) while a
	// contender waits on the lock, so verify after locking that the handle
	// still backs the path and retry on a fresh file otherwise.
	const maxAttempts = 10
	for attempt := 0; ; attempt++ {
		if err := os.MkdirAll(r.dir(), 0o755); err != nil {
			return nil, fmt.Errorf("create cache dir: %w", err)
		}
		lockFile, err := os.OpenFile(r.path(), os.O_RDWR|os.O_CREATE, 0o644)
		if err != nil {
			// Retry only races with eviction: the directory vanishing, or a
			// delete-pending name on Windows (a permission error). Anything
			// else is a persistent failure.
			if (os.IsNotExist(err) || os.IsPermission(err)) && attempt < maxAttempts {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			return nil, fmt.Errorf("create cache file: %w", err)
		}
		if err := flock.Lock(lockFile); err != nil {
			lockFile.Close()
			return nil, fmt.Errorf("lock cache file: %w", err)
		}
		pathInfo, statErr := os.Stat(r.path())
		fileInfo, fstatErr := lockFile.Stat()
		if statErr == nil && fstatErr == nil && os.SameFile(pathInfo, fileInfo) {
			return lockFile, nil
		}
		flock.Unlock(lockFile) //nolint:errcheck
		lockFile.Close()
		if attempt >= maxAttempts {
			return nil, fmt.Errorf("lock cache file: entry file keeps being replaced")
		}
	}
}

func newLockedChunkCache(dec io.Reader, m *CacheManager, hash string, chunkStart, chunkEnd uint32, bytesStart, bytesEnd int64, lockFile *os.File) (*chunkCache, error) {
	r, err := newCacheRange(m.dir, hash, chunkStart, chunkEnd, bytesStart, bytesEnd)
	if err != nil {
		return nil, err
	}

	// The locked entry file is the backing data file; truncation discards any
	// leftover from a crashed download, which readers never adopt.
	f := lockFile
	if err := f.Truncate(0); err != nil {
		return nil, fmt.Errorf("truncate cache file: %w", err)
	}

	// Write a placeholder header. The actual numOffsets is known from the
	// chunk range, so we can write the correct header size upfront.
	numChunks := chunkEnd - chunkStart
	numOffsets := numChunks + 1
	headerSize := 4 + 4*int(numOffsets)
	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header[0:], numOffsets)
	// offsets[0] = 0, the rest will be filled in during LoadAll.
	if _, err := f.WriteAt(header, 0); err != nil {
		return nil, fmt.Errorf("write header: %w", err)
	}

	return &chunkCache{
		dec:            dec,
		file:           f,
		writePos:       int64(headerSize), // data starts after the header
		lockFile:       lockFile,
		path:           r.path(),
		hash:           hash,
		expectedChunks: numChunks,
		manager:        m,
	}, nil
}

func defaultCacheDir(cacheDir string) string {
	if cacheDir == "" {
		return filepath.Join(os.TempDir(), "xet-cache")
	}
	return cacheDir
}

// openCachedRange opens one or more cache files that together cover
// [chunkStart, chunkEnd). It scans the hash directory and assembles metas
// from cached files. Returns nil if the range cannot be fully covered.
func openCachedRange(m *CacheManager, hash string, chunkStart, chunkEnd uint32) (*chunkCache, error) {
	if len(hash) < 2 {
		return nil, nil
	}
	hashDir := filepath.Join(m.dir, hash[:2], hash[2:])
	files, err := os.ReadDir(hashDir)
	if err != nil {
		if os.IsNotExist(err) || os.IsPermission(err) {
			return nil, nil
		}
		return nil, err
	}

	type candidate struct {
		path       string
		start, end uint32
	}
	var candidates []candidate
	for _, de := range files {
		if de.IsDir() || strings.HasPrefix(de.Name(), ".") {
			continue
		}
		cs, ce, _, _, ok := parseCacheFileName(de.Name())
		if !ok {
			continue
		}
		if ce <= chunkStart || cs >= chunkEnd {
			continue
		}
		candidates = append(candidates, candidate{
			path:  filepath.Join(hashDir, de.Name()),
			start: cs,
			end:   ce,
		})
	}

	if len(candidates) == 0 {
		return nil, nil
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].start < candidates[j].start
	})

	type fileSegment struct {
		metas []chunkRef
		file  *os.File
	}
	var acquired []string
	var segments []fileSegment
	closeSegments := func() {
		for _, seg := range segments {
			seg.file.Close()
		}
		for _, p := range acquired {
			m.release(p)
		}
	}
	covered := chunkStart

	for _, c := range candidates {
		if covered >= chunkEnd {
			break
		}
		if c.start > covered {
			break
		}
		if c.end <= covered {
			continue
		}

		needStart := covered
		needEnd := min(c.end, chunkEnd)

		f, err := os.Open(c.path)
		if err != nil {
			// The entry may have been evicted between listing and opening;
			// skip it. On Windows an evicted name that is still open elsewhere
			// stays delete-pending and surfaces as a permission error.
			if os.IsNotExist(err) || os.IsPermission(err) {
				continue
			}
			closeSegments()
			return nil, err
		}

		fi, statErr := f.Stat()
		if statErr != nil {
			f.Close()
			continue
		}

		layout, err := readCacheFileLayout(f, c.start, c.end, fi.Size())
		if err != nil {
			// Still being written, or a crashed leftover; both are invisible
			// to readers. Cleanup happens under the entry flock in reconcile.
			f.Close()
			continue
		}

		// Entries are verified once per manager, on their first open.
		if !m.wasVerified(c.path) {
			if err := verifyCacheFileCRC(f, layout); err != nil {
				f.Close()
				closeSegments()
				// Remove the corrupted entry under its flock, like eviction.
				removeCacheEntry(c.path)
				return nil, nil
			}
			m.markVerified(c.path)
		}

		metas, err := readCacheFileMetas(f, layout, c.start, needStart, needEnd)
		if err != nil {
			f.Close()
			closeSegments()
			// Remove the corrupted entry under its flock, like eviction.
			removeCacheEntry(c.path)
			return nil, nil
		}
		m.acquire(c.path, fi.Size())
		acquired = append(acquired, c.path)
		segments = append(segments, fileSegment{metas: metas, file: f})
		covered = needEnd
	}

	if covered < chunkEnd {
		closeSegments()
		return nil, nil
	}

	var allMetas []chunkRef
	for i, seg := range segments {
		for _, ref := range seg.metas {
			allMetas = append(allMetas, chunkRef{
				offset:  ref.offset,
				size:    ref.size,
				fileIdx: i,
			})
		}
	}

	extraFiles := make([]*os.File, len(segments)-1)
	for i := 1; i < len(segments); i++ {
		extraFiles[i-1] = segments[i].file
	}
	if len(segments) > 1 {
		// Several files served one range; schedule their compaction.
		m.noteMergeCandidate(hash)
	}
	return &chunkCache{
		metas:    allMetas,
		readonly: true,
		file:     segments[0].file,
		files:    extraFiles,
		manager:  m,
		refPaths: acquired,
	}, nil
}

// cacheFileLayout describes where the offset table, data, and checksum
// trailer live in a sealed cache file.
type cacheFileLayout struct {
	numOffsets  uint32
	headerBytes int64 // offset table ends here; the data region starts here
	dataBytes   int64 // decoded data length; the trailer follows
	crc         uint32
}

// readCacheFileLayout validates the exact size arithmetic that defines a
// sealed entry: header, data, then a trailing crc32. Any other size means
// the file is still being written or is a crashed leftover.
func readCacheFileLayout(f *os.File, fileStart, fileEnd uint32, fileSize int64) (cacheFileLayout, error) {
	if fileEnd < fileStart {
		return cacheFileLayout{}, fmt.Errorf("invalid chunk range")
	}
	expected := fileEnd - fileStart + 1
	headerBytes := 4 + 4*int64(expected)
	if fileSize < headerBytes+4 {
		return cacheFileLayout{}, fmt.Errorf("cache file incomplete")
	}
	var head [4]byte
	if _, err := f.ReadAt(head[:], 0); err != nil {
		return cacheFileLayout{}, err
	}
	if binary.LittleEndian.Uint32(head[:]) != expected {
		return cacheFileLayout{}, fmt.Errorf("invalid cache offset count")
	}
	var lastBuf [4]byte
	if _, err := f.ReadAt(lastBuf[:], headerBytes-4); err != nil {
		return cacheFileLayout{}, err
	}
	layout := cacheFileLayout{
		numOffsets:  expected,
		headerBytes: headerBytes,
		dataBytes:   int64(binary.LittleEndian.Uint32(lastBuf[:])),
	}
	if fileSize != layout.headerBytes+layout.dataBytes+4 {
		return cacheFileLayout{}, fmt.Errorf("cache file incomplete")
	}
	var crcBuf [4]byte
	if _, err := f.ReadAt(crcBuf[:], fileSize-4); err != nil {
		return cacheFileLayout{}, err
	}
	layout.crc = binary.LittleEndian.Uint32(crcBuf[:])
	return layout, nil
}

// verifyCacheFileCRC re-reads the whole file and compares it against the
// checksum stored in the trailer. The CRC input is the data region followed
// by the header, the same order finalize feeds the hasher.
func verifyCacheFileCRC(f *os.File, layout cacheFileLayout) error {
	sum := uint32(0)
	buf := pool.GetChunkBuf()
	defer pool.PutChunkBuf(buf)
	end := layout.headerBytes + layout.dataBytes
	for off := layout.headerBytes; off < end; {
		n := min(int64(len(buf)), end-off)
		if _, err := f.ReadAt(buf[:n], off); err != nil {
			return err
		}
		sum = crc32.Update(sum, crc32.IEEETable, buf[:n])
		off += n
	}
	hdr := make([]byte, layout.headerBytes)
	if _, err := f.ReadAt(hdr, 0); err != nil {
		return err
	}
	sum = crc32.Update(sum, crc32.IEEETable, hdr)
	if sum != layout.crc {
		return fmt.Errorf("cache file checksum mismatch")
	}
	return nil
}

// readCacheFileMetas reads metas for [chunkStart, chunkEnd) from an already-open
// cache file. The caller is responsible for closing f in all cases.
func readCacheFileMetas(f *os.File, layout cacheFileLayout, fileStart, chunkStart, chunkEnd uint32) ([]chunkRef, error) {
	if chunkStart < fileStart || chunkEnd < chunkStart {
		return nil, fmt.Errorf("invalid chunk range")
	}
	idxStart := int(chunkStart - fileStart)
	idxEnd := int(chunkEnd - fileStart)
	if idxEnd >= int(layout.numOffsets) || idxStart > idxEnd {
		return nil, fmt.Errorf("invalid chunk range")
	}

	numChunks := int(chunkEnd - chunkStart)
	rawBuf := make([]byte, (numChunks+1)*4)
	if _, err := f.ReadAt(rawBuf, int64(4+idxStart*4)); err != nil {
		return nil, err
	}

	metas := make([]chunkRef, numChunks)
	for i := range numChunks {
		start := binary.LittleEndian.Uint32(rawBuf[i*4:])
		end := binary.LittleEndian.Uint32(rawBuf[(i+1)*4:])
		if end < start || int64(end) > layout.dataBytes {
			return nil, fmt.Errorf("invalid cache offsets")
		}
		metas[i] = chunkRef{
			offset: layout.headerBytes + int64(start),
			size:   int32(end - start),
		}
	}
	return metas, nil
}

// Chunk returns the decoded chunk at idx, decoding forward as needed.
// Already-decoded chunks are served from the backing file.
func (c *chunkCache) Chunk(idx uint32, buf []byte) (int64, error) {
	if err := c.loadTo(idx); err != nil {
		return 0, fmt.Errorf("load chunk %d: %w", idx, err)
	}

	c.mut.Lock()
	if int(idx) >= len(c.metas) {
		total := len(c.metas)
		c.mut.Unlock()
		return 0, fmt.Errorf("chunk %d out of range (total: %d)", idx, total)
	}
	m := c.metas[idx]
	var f *os.File
	if m.fileIdx == 0 {
		f = c.file
	} else if m.fileIdx-1 < len(c.files) {
		f = c.files[m.fileIdx-1]
	}
	c.mut.Unlock()

	if f == nil {
		return 0, fmt.Errorf("chunk %d: file closed", idx)
	}
	if int(m.size) > len(buf) {
		return 0, fmt.Errorf("buffer too small for chunk: need %d bytes", m.size)
	}
	n, err := f.ReadAt(buf[:m.size], m.offset)
	return int64(n), err
}

// load decodes the next chunk and writes it to the backing file.
func (c *chunkCache) load() (int, error) {
	c.mut.Lock()
	defer c.mut.Unlock()

	if c.done || c.readonly {
		return 0, io.EOF
	}

	tmp := pool.GetChunkBuf()
	defer pool.PutChunkBuf(tmp)

	n, err := c.dec.Read(tmp[:])
	if err != nil {
		if err == io.EOF {
			c.done = true
			return 0, io.EOF
		}
		return 0, err
	}

	if _, werr := c.file.WriteAt(tmp[:n], c.writePos); werr != nil {
		return 0, fmt.Errorf("write chunk to file: %w", werr)
	}
	c.crc = crc32.Update(c.crc, crc32.IEEETable, tmp[:n])
	c.metas = append(c.metas, chunkRef{offset: c.writePos, size: int32(n)})
	c.writePos += int64(n)
	return len(c.metas), nil
}

// loadTo decodes and caches chunks until index idx is available or the decoder
// is exhausted.
func (c *chunkCache) loadTo(idx uint32) error {
	for {
		c.mut.Lock()
		curr := len(c.metas)
		done := c.done
		c.mut.Unlock()

		if curr > int(idx) || done {
			return nil
		}

		_, err := c.load()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

// LoadTo decodes and caches chunks up to the specified index.
func (c *chunkCache) LoadTo(idx uint32) error {
	return c.loadTo(idx)
}

// LoadAll decodes and caches all remaining chunks, then patches the offset
// header and syncs the file to disk.
func (c *chunkCache) LoadAll() error {
	for {
		_, err := c.load()
		if err != nil {
			if err == io.EOF {
				return c.finalize()
			}
			return err
		}
	}
}

// finalize patches the offset table in the header, seals the file with the
// crc32 trailer, syncs it, and releases the write lock. The entry becomes
// visible to readers once its size matches the sealed layout. No-op for
// read-only caches.
func (c *chunkCache) finalize() error {
	c.mut.Lock()
	defer c.mut.Unlock()
	metas := c.metas
	file := c.file
	readonly := c.readonly

	if readonly {
		return nil
	}
	if file == nil {
		return fmt.Errorf("cache file is closed")
	}
	if uint32(len(metas)) != c.expectedChunks {
		return fmt.Errorf("decoded %d chunks, expected %d", len(metas), c.expectedChunks)
	}

	// Compute sequential offsets and write them into the pre-allocated header.
	numOffsets := uint32(len(metas) + 1)
	offsets := make([]uint32, numOffsets)
	for i, m := range metas {
		offsets[i+1] = offsets[i] + uint32(m.size)
	}

	header := make([]byte, 4+4*int(numOffsets))
	binary.LittleEndian.PutUint32(header[0:], numOffsets)
	for i, o := range offsets {
		binary.LittleEndian.PutUint32(header[4+4*i:], o)
	}
	if _, err := file.WriteAt(header, 0); err != nil {
		return fmt.Errorf("write cache header: %w", err)
	}
	// The data region was hashed chunk by chunk in load; fold in the patched
	// header and seal the file with the checksum trailer.
	var trailer [4]byte
	binary.LittleEndian.PutUint32(trailer[:], crc32.Update(c.crc, crc32.IEEETable, header))
	if _, err := file.WriteAt(trailer[:], c.writePos); err != nil {
		return fmt.Errorf("write cache trailer: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync cache file: %w", err)
	}
	c.published = true
	if c.manager != nil {
		// Track the sealed entry (still referenced by this open cache) before
		// releasing the lock so eviction never catches it unreferenced. The
		// freshly synced file matches the checksum it was sealed with, so
		// remember it as verified.
		c.manager.acquire(c.path, c.writePos+4)
		c.manager.markVerified(c.path)
		c.refPaths = append(c.refPaths, c.path)
	}
	if c.lockFile != nil {
		if err := flock.Unlock(c.lockFile); err != nil {
			return fmt.Errorf("unlock cache file: %w", err)
		}
		c.lockFile = nil
	}
	if c.manager != nil {
		// Evict any overage now that the cache grew.
		c.manager.evaluate()
		c.manager.noteMergeCandidate(c.hash)
	}
	return nil
}

// Done marks the decoder as exhausted and releases the backing file(s).
// The file lock is released automatically when the file is closed.
func (c *chunkCache) Done() {
	c.mut.Lock()
	c.done = true
	if c.file != nil && !c.readonly && !c.published {
		// Discard the unfinished entry while still holding its lock so
		// waiting contenders find an empty file and rewrite it.
		c.file.Truncate(0) //nolint:errcheck
	}
	if c.lockFile != nil {
		flock.Unlock(c.lockFile) //nolint:errcheck
		c.lockFile = nil
	}
	if c.file != nil {
		c.file.Close()
		c.file = nil
	}
	for _, f := range c.files {
		f.Close()
	}
	c.files = nil
	var mgr *CacheManager
	if c.manager != nil {
		for _, p := range c.refPaths {
			c.manager.release(p)
		}
		if len(c.refPaths) > 0 {
			mgr = c.manager
		}
		c.refPaths = nil
		c.manager = nil
	}
	c.mut.Unlock()
	if mgr != nil {
		// References just dropped, so entries kept alive during the download
		// become evictable now rather than at the next publish.
		mgr.evaluate()
	}
}
