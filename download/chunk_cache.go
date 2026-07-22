package download

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/wzshiming/xet/internal/flock"
	"github.com/wzshiming/xet/internal/pool"
)

// chunkRef records the location of one decoded chunk within the backing file.
type chunkRef struct {
	offset  int64
	size    int32
	fileIdx int // index into chunkCache.files; 0 means chunkCache.file
}

// chunkCache is the single cache for decoded xorb chunk ranges.
//
// Each entry is stored as a single file at:
//
//	cacheDir/<hash[:2]>/<hash[2:]>/<chunkStart>-<chunkEnd>_<bytesStart>-<bytesEnd>
//
// where bytesStart and bytesEnd indicate the range of bytes in the original
// file that this cache entry covers.
//
// File format (all integers little-endian):
//
//	[numOffsets  uint32]           // = numChunks + 1
//	[offset[0]   uint32]           // always 0
//	[offset[1]   uint32]           // end of chunk 0
//	...
//	[offset[N]   uint32]           // = total data length
//	[data bytes ...]               // decoded chunks concatenated
//
// A per-cache-entry file lock (flock) coordinates concurrent writers so
// that only one process/goroutine downloads while others wait and then
// reuse the completed cache file.
// When reading, the file size is verified against the expected header size;
// incomplete writes (smaller than expected) are treated as cache misses
// and the file is removed.
type chunkCache struct {
	dec      io.Reader
	metas    []chunkRef
	writePos int64 // next write offset in file
	done     bool
	readonly bool // true for read-only cache from openCachedRange
	mut      sync.Mutex
	file     *os.File   // backing file
	files    []*os.File // additional files for multi-file read path (fileIdx > 0)
	lockFile *os.File   // file lock handle, non-nil for write path
}

// cacheFileName returns the base filename for a cache entry.
func cacheFileName(start, end uint32, bytesStart, bytesEnd int64) string {
	return fmt.Sprintf("%d-%d_%d-%d", start, end, bytesStart, bytesEnd)
}

// cacheFilePath returns the full path for a cache entry.
func cacheFilePath(cacheDir, hash string, start, end uint32, bytesStart, bytesEnd int64) string {
	return filepath.Join(cacheDir, hash[:2], hash[2:], cacheFileName(start, end, bytesStart, bytesEnd))
}

// parseCacheFileName parses a filename of the form
// "<chunkStart>-<chunkEnd>_<bytesStart>-<bytesEnd>" and returns the components.
// Returns ok=false if parsing fails.
func parseCacheFileName(name string) (chunkStart, chunkEnd uint32, bytesStart, bytesEnd int64, ok bool) {
	parts := strings.SplitN(name, "_", 2)
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
	return uint32(cs), uint32(ce), bs, be, true
}

// newChunkCache creates a chunkCache that decodes chunks from dec and writes
// them to a cache file. A per-cache-entry file lock (flock) coordinates
// concurrent writers: the first caller acquires the lock and downloads;
// subsequent callers block on the lock, then find the completed cache file
// and use it.
//
// The caller must call Done() to release the file lock.
func newChunkCache(dec io.Reader, cacheDir, hash string, chunkStart, chunkEnd uint32, bytesStart, bytesEnd int64) (*chunkCache, error) {
	dir := filepath.Join(cacheDir, hash[:2], hash[2:])
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}

	finalPath := cacheFilePath(cacheDir, hash, chunkStart, chunkEnd, bytesStart, bytesEnd)

	// Open (or create) the cache file and acquire an exclusive file lock.
	// If another process/goroutine is already writing, the lock will block
	// until it finishes. After acquiring the lock, check whether the file
	// is complete by verifying its size.
	f, err := os.OpenFile(finalPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("create cache file: %w", err)
	}

	err = flock.Lock(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("lock cache file: %w", err)
	}

	// Check if the file is already complete (another writer finished while
	// we were waiting for the lock).
	expectedSize := expectedCacheFileSize(chunkStart, chunkEnd)
	if fi, _ := f.Stat(); fi != nil && fi.Size() >= expectedSize {
		flock.Unlock(f)
		f.Close()
		return openCachedRange(cacheDir, hash, chunkStart, chunkEnd)
	}

	// Truncate and start fresh — the previous writer (if any) crashed before
	// completing.
	f.Truncate(0) //nolint:errcheck

	// Write a placeholder header. The actual numOffsets is known from the
	// chunk range, so we can write the correct header size upfront.
	numChunks := chunkEnd - chunkStart
	numOffsets := numChunks + 1
	headerSize := 4 + 4*int(numOffsets)
	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header[0:], numOffsets)
	// offsets[0] = 0, the rest will be filled in during LoadAll.
	if _, err := f.Write(header); err != nil {
		f.Close()
		os.Remove(finalPath) //nolint:errcheck
		return nil, fmt.Errorf("write header: %w", err)
	}

	return &chunkCache{
		dec:      dec,
		file:     f,
		writePos: int64(headerSize), // data starts after the header
		lockFile: f,
	}, nil
}

// expectedCacheFileSize returns the expected file size (in bytes) for a
// complete cache file covering [chunkStart, chunkEnd). This is used to
// detect incomplete writes: if the actual file is smaller, the write
// was interrupted and the file should be discarded.
func expectedCacheFileSize(chunkStart, chunkEnd uint32) int64 {
	numChunks := chunkEnd - chunkStart
	numOffsets := numChunks + 1
	headerSize := int64(4 + 4*int(numOffsets))
	// The data size is unknown at this point, so we only check that the
	// header (at minimum) is fully written. A complete file will always
	// be larger than the header alone.
	return headerSize
}

// openCachedRange opens one or more cache files that together cover
// [chunkStart, chunkEnd). It scans the hash directory and assembles metas
// from cached files. Returns nil if the range cannot be fully covered.
func openCachedRange(cacheDir, hash string, chunkStart, chunkEnd uint32) (*chunkCache, error) {
	if len(hash) < 2 {
		return nil, nil
	}
	hashDir := filepath.Join(cacheDir, hash[:2], hash[2:])
	files, err := os.ReadDir(hashDir)
	if err != nil {
		if os.IsNotExist(err) {
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
	var segments []fileSegment
	covered := chunkStart

	for _, c := range candidates {
		if covered >= chunkEnd {
			break
		}
		if c.start > covered {
			break
		}

		needStart := covered
		needEnd := min(c.end, chunkEnd)

		f, err := os.Open(c.path)
		if err != nil {
			for _, seg := range segments {
				seg.file.Close()
			}
			return nil, err
		}

		// Verify the file is complete by checking its size against the
		// expected header size. Incomplete files (from a crashed writer)
		// are treated as cache misses and cleaned up.
		if fi, _ := f.Stat(); fi != nil && fi.Size() < expectedCacheFileSize(c.start, c.end) {
			f.Close()
			for _, seg := range segments {
				seg.file.Close()
			}
			os.Remove(c.path) //nolint:errcheck
			return nil, nil
		}

		metas, err := readCacheFileMetas(f, c.start, needStart, needEnd)
		if err != nil {
			f.Close()
			for _, seg := range segments {
				seg.file.Close()
			}
			// Remove corrupted cache file so it doesn't cause repeated errors.
			os.Remove(c.path) //nolint:errcheck
			return nil, nil
		}
		segments = append(segments, fileSegment{metas: metas, file: f})
		covered = needEnd
	}

	if covered < chunkEnd {
		for _, seg := range segments {
			seg.file.Close()
		}
		return nil, nil
	}

	var allMetas []chunkRef
	for i, seg := range segments {
		for _, m := range seg.metas {
			allMetas = append(allMetas, chunkRef{
				offset:  m.offset,
				size:    m.size,
				fileIdx: i,
			})
		}
	}

	extraFiles := make([]*os.File, len(segments)-1)
	for i := 1; i < len(segments); i++ {
		extraFiles[i-1] = segments[i].file
	}
	return &chunkCache{
		metas:    allMetas,
		done:     true,
		readonly: true,
		file:     segments[0].file,
		files:    extraFiles,
	}, nil
}

// readCacheFileMetas reads metas for [chunkStart, chunkEnd) from an already-open
// cache file. The caller is responsible for closing f in all cases.
func readCacheFileMetas(f *os.File, offset, chunkStart, chunkEnd uint32) ([]chunkRef, error) {
	var countBuf [4]byte
	if _, err := f.ReadAt(countBuf[:], 0); err != nil {
		return nil, err
	}
	numOffsets := binary.LittleEndian.Uint32(countBuf[:])

	if chunkStart < offset || chunkEnd < chunkStart {
		return nil, fmt.Errorf("invalid chunk range")
	}
	idxStart := int(chunkStart - offset)
	idxEnd := int(chunkEnd - offset)
	if idxEnd >= int(numOffsets) || idxStart > idxEnd {
		return nil, fmt.Errorf("invalid chunk range")
	}

	numChunks := int(chunkEnd - chunkStart)
	rawBuf := make([]byte, (numChunks+1)*4)
	if _, err := f.ReadAt(rawBuf, int64(4+idxStart*4)); err != nil {
		return nil, err
	}

	headerBytes := int64(4 + 4*int(numOffsets))
	metas := make([]chunkRef, numChunks)
	for i := range numChunks {
		start := binary.LittleEndian.Uint32(rawBuf[i*4:])
		end := binary.LittleEndian.Uint32(rawBuf[(i+1)*4:])
		metas[i] = chunkRef{
			offset: headerBytes + int64(start),
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
				c.finalize()
				return nil
			}
			return err
		}
	}
}

// finalize patches the offset table in the header with the actual chunk sizes,
// then syncs the file to disk. No-op for read-only caches.
func (c *chunkCache) finalize() {
	c.mut.Lock()
	metas := c.metas
	file := c.file
	readonly := c.readonly
	c.mut.Unlock()

	if file == nil || len(metas) == 0 || readonly {
		return
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
		return
	}
	file.Sync() //nolint:errcheck
}

// Done marks the decoder as exhausted and releases the backing file(s).
// The file lock is released automatically when the file is closed.
func (c *chunkCache) Done() {
	c.mut.Lock()
	c.done = true
	if c.file != nil {
		c.file.Close()
		c.file = nil
	}
	for _, f := range c.files {
		f.Close()
	}
	c.files = nil
	c.mut.Unlock()
}
