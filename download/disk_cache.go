package download

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/wzshiming/xet/internal/pool"
)

// DiskCache is a persistent, cross-session cache of decoded xorb chunk ranges.
//
// Each entry is stored as a single file at:
//
//	cacheDir/<hash[:2]>/<hash[2:]>/<chunkStart>-<chunkEnd>_<bytesStart>-<bytesEnd>
//
// where bytesStart and bytesEnd indicate the range of bytes in the original file that this cache entry covers.
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
// There is no in-memory index; on each get() the cache directory is scanned
// directly to find a matching (or superset) file.
type DiskCache struct {
	cacheDir    string
	mut         sync.Mutex
	dirtyHashes map[string]struct{} // hashes with newly written cache files
}

// NewDiskCache creates a diskCache rooted at cacheDir.
func NewDiskCache(cacheDir string) (*DiskCache, error) {
	if cacheDir == "" {
		cacheDir = filepath.Join(os.TempDir(), "xet-cache")
	}
	return &DiskCache{cacheDir: cacheDir}, nil
}

// diskCacheFileName returns the base filename for a cache entry.
func diskCacheFileName(start, end uint32, bytesStart, bytesEnd int64) string {
	return fmt.Sprintf("%d-%d_%d-%d", start, end, bytesStart, bytesEnd)
}

// entryPath returns the full path for a cache entry.
func entryPath(cacheDir, hash string, start, end uint32, bytesStart, bytesEnd int64) string {
	return filepath.Join(cacheDir, hash[:2], hash[2:], diskCacheFileName(start, end, bytesStart, bytesEnd))
}

// parseCacheFileName parses a filename of the form "<chunkStart>-<chunkEnd>_<bytesStart>-<bytesEnd>" and returns the components. Returns ok=false if parsing fails.
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

// get returns a chunkCache backed by disk-cache files for [chunkStart, chunkEnd).
// It scans the hash directory and assembles metas from one or more cached files
// that together cover the requested range (each file may cover a sub-range).
// Returns nil if the range cannot be fully covered.
func (dc *DiskCache) get(hash string, chunkStart, chunkEnd uint32) (*chunkCache, error) {
	if len(hash) < 2 {
		return nil, fmt.Errorf("invalid hash: %s", hash)
	}
	hashDir := filepath.Join(dc.cacheDir, hash[:2], hash[2:])
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
		if de.IsDir() || strings.HasPrefix(de.Name(), ".tmp-") {
			continue
		}
		cs, ce, _, _, ok := parseCacheFileName(de.Name())
		if !ok {
			continue
		}
		// Keep only files whose chunk range overlaps [chunkStart, chunkEnd).
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

	// Sort by start position.
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].start < candidates[j].start
	})

	// Build the metas slice by walking candidates left-to-right, filling [chunkStart, chunkEnd).
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
		// This candidate must cover the current frontier.
		if c.start > covered {
			break // gap — can't fully cover
		}

		// Clamp to the portion we still need.
		needStart := covered
		needEnd := min(c.end, chunkEnd)

		f, err := os.Open(c.path)
		if err != nil {
			for _, seg := range segments {
				seg.file.Close()
			}
			return nil, err
		}

		metas, err := readCacheFileMetas(f, c.start, needStart, needEnd)
		if err != nil {
			f.Close()
			for _, seg := range segments {
				seg.file.Close()
			}
			os.Remove(c.path) //nolint:errcheck
			return nil, fmt.Errorf("read cache file metas: %w", err)
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

	// Assemble metas pointing directly at the original files.
	// fileIdx==0 → segments[0].file (stored in chunkCache.file)
	// fileIdx==N → segments[N].file (stored in chunkCache.files[N-1])
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
		metas: allMetas,
		done:  true,
		file:  segments[0].file,
		files: extraFiles,
	}, nil
}

// readCacheFileMetas reads metas for [chunkStart, chunkEnd) from an already-open
// cache file. The caller is responsible for closing f in all cases.
func readCacheFileMetas(f *os.File, offset, chunkStart, chunkEnd uint32) ([]chunkRef, error) {
	var numOffsets uint32
	if err := binary.Read(f, binary.LittleEndian, &numOffsets); err != nil {
		return nil, err
	}
	offsets := make([]uint32, numOffsets)
	if err := binary.Read(f, binary.LittleEndian, &offsets); err != nil {
		return nil, err
	}

	idxStart := int(chunkStart - offset)
	idxEnd := int(chunkEnd - offset)
	if idxEnd >= int(numOffsets) || idxStart < 0 || idxStart > idxEnd {
		return nil, fmt.Errorf("invalid chunk range")
	}

	headerBytes := int64(4 + 4*int(numOffsets))
	numChunks := int(chunkEnd - chunkStart)
	metas := make([]chunkRef, numChunks)
	for i := range numChunks {
		metas[i] = chunkRef{
			offset: headerBytes + int64(offsets[idxStart+i]),
			size:   int32(offsets[idxStart+i+1] - offsets[idxStart+i]),
		}
	}
	return metas, nil
}

// put stores the decoded chunks for (hash, chunkStart, chunkEnd) to disk.
// It writes the offset header first, then streams each chunk directly from
// file without buffering all data in memory.
func (dc *DiskCache) put(hash string, chunkStart, chunkEnd uint32, bytesStart, bytesEnd int64, metas []chunkRef, file *os.File) {
	if len(hash) < 2 || len(metas) == 0 {
		return
	}

	numChunks := uint32(len(metas))
	numOffsets := numChunks + 1

	// Compute sequential offsets from meta sizes (no file read needed).
	offsets := make([]uint32, numOffsets)
	for i, m := range metas {
		offsets[i+1] = offsets[i] + uint32(m.size)
	}

	path := entryPath(dc.cacheDir, hash, chunkStart, chunkEnd, bytesStart, bytesEnd)
	if _, err := os.Stat(path); err == nil {
		return // already present
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return
	}
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return
	}
	tmpName := tmp.Name()

	// Write header: [numOffsets uint32][offset[0]..offset[N] uint32]
	headerSize := 4 + 4*int(numOffsets)
	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header[0:], numOffsets)
	for i, o := range offsets {
		binary.LittleEndian.PutUint32(header[4+4*i:], o)
	}
	if _, err := tmp.Write(header); err != nil {
		tmp.Close()
		os.Remove(tmpName) //nolint:errcheck
		return
	}

	// Stream each chunk directly from the backing file.
	chunkBuf := pool.GetChunkBuf()
	defer pool.PutChunkBuf(chunkBuf)
	for _, m := range metas {
		buf := chunkBuf[:m.size]
		if _, err := file.ReadAt(buf, m.offset); err != nil {
			tmp.Close()
			os.Remove(tmpName) //nolint:errcheck
			return
		}
		if _, err := tmp.Write(buf); err != nil {
			tmp.Close()
			os.Remove(tmpName) //nolint:errcheck
			return
		}
	}

	if err := tmp.Close(); err != nil {
		os.Remove(tmpName) //nolint:errcheck
		return
	}
	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName) //nolint:errcheck
		return
	}

	dc.mut.Lock()
	if dc.dirtyHashes == nil {
		dc.dirtyHashes = make(map[string]struct{})
	}
	dc.dirtyHashes[hash] = struct{}{}
	dc.mut.Unlock()
}

// Compact merges adjacent cached files for all hashes written since the last
// Compact call. Call this once all downloads for a session are complete.
func (dc *DiskCache) Compact() error {
	dc.mut.Lock()
	hashes := dc.dirtyHashes
	dc.dirtyHashes = nil
	dc.mut.Unlock()

	for hash := range hashes {
		if err := mergeAdjacentFiles(dc.cacheDir, hash); err != nil {
			return err
		}
	}
	return nil
}

// mergeAdjacentFiles scans all cached files for hash, sorts them, finds every
// contiguous-or-overlapping run, and merges each run into a single file in one
// pass. Overlapping regions are deduplicated via greedy left-to-right coverage
// (same strategy as get). All originals in a successfully merged run are removed.
func mergeAdjacentFiles(cacheDir, hash string) error {
	if len(hash) < 2 {
		return nil
	}
	hashDir := filepath.Join(cacheDir, hash[:2], hash[2:])

	files, err := os.ReadDir(hashDir)
	if err != nil {
		return nil // directory not found is not an error
	}

	type entry struct {
		path                 string
		chunkStart, chunkEnd uint32
		bytesStart, bytesEnd int64
	}
	var entries []entry
	for _, de := range files {
		if de.IsDir() || strings.HasPrefix(de.Name(), ".tmp-") {
			continue
		}
		cs, ce, bs, be, ok := parseCacheFileName(de.Name())
		if !ok {
			continue
		}
		entries = append(entries, entry{
			path:       filepath.Join(hashDir, de.Name()),
			chunkStart: cs,
			chunkEnd:   ce,
			bytesStart: bs,
			bytesEnd:   be,
		})
	}

	if len(entries) < 2 {
		return nil
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].chunkStart != entries[j].chunkStart {
			return entries[i].chunkStart < entries[j].chunkStart
		}
		return entries[i].chunkEnd < entries[j].chunkEnd
	})

	// Walk entries, collecting contiguous-or-overlapping runs and merging each.
	i := 0
	for i < len(entries) {
		// Extend run while next entry overlaps or is adjacent to the current frontier.
		runEnd := entries[i].chunkEnd
		j := i + 1
		for j < len(entries) && entries[j].chunkStart <= runEnd {
			if entries[j].chunkEnd > runEnd {
				runEnd = entries[j].chunkEnd
			}
			j++
		}

		if j-i < 2 {
			i = j
			continue
		}

		run := entries[i:j]
		runStart := run[0].chunkStart
		runBytesStart := run[0].bytesStart
		runBytesEnd := run[0].bytesEnd
		for _, re := range run[1:] {
			if re.bytesStart < runBytesStart {
				runBytesStart = re.bytesStart
			}
			if re.bytesEnd > runBytesEnd {
				runBytesEnd = re.bytesEnd
			}
		}

		// Greedy left-to-right coverage to deduplicate overlapping regions.
		type segData struct {
			metas      []chunkRef
			f          *os.File
			chunkStart uint32
			chunkEnd   uint32
		}
		var segs []segData
		covered := runStart
		ok := true
		for _, e := range run {
			if covered >= runEnd {
				break
			}
			if e.chunkStart > covered {
				// Gap — cannot fully cover; skip this run.
				ok = false
				break
			}
			needStart := covered
			needEnd := min(e.chunkEnd, runEnd)

			f, err := os.Open(e.path)
			if err != nil {
				for _, s := range segs {
					s.f.Close()
				}
				return err
			}

			metas, err := readCacheFileMetas(f, e.chunkStart, needStart, needEnd)
			if err != nil {
				f.Close()
				for _, s := range segs {
					s.f.Close()
				}
				os.Remove(e.path) //nolint:errcheck
				return fmt.Errorf("read cache file metas: %w", err)
			}
			if metas == nil {
				f.Close()
				ok = false
				break
			}
			segs = append(segs, segData{metas: metas, f: f, chunkStart: needStart, chunkEnd: needEnd})
			covered = needEnd
		}
		if !ok || covered < runEnd {
			for _, s := range segs {
				s.f.Close()
			}
			return fmt.Errorf("merge %s [%d,%d): gap or read error", hash, runStart, runEnd)
		}

		// Build the merged header+data buffer.
		totalChunks := 0
		for _, s := range segs {
			totalChunks += len(s.metas)
		}
		numOffsets := uint32(totalChunks + 1)
		offsets := make([]uint32, numOffsets)
		allMetas := make([]chunkRef, 0, totalChunks)
		for _, s := range segs {
			allMetas = append(allMetas, s.metas...)
		}
		for k, m := range allMetas {
			offsets[k+1] = offsets[k] + uint32(m.size)
		}
		totalData := int(offsets[numOffsets-1])
		headerSize := 4 + 4*int(numOffsets)
		buf := make([]byte, headerSize+totalData)
		binary.LittleEndian.PutUint32(buf[0:], numOffsets)
		for k, o := range offsets {
			binary.LittleEndian.PutUint32(buf[4+4*k:], o)
		}

		pos := headerSize
		writeOk := true
		for _, s := range segs {
			for _, m := range s.metas {
				if _, err := s.f.ReadAt(buf[pos:pos+int(m.size)], m.offset); err != nil {
					writeOk = false
					break
				}
				pos += int(m.size)
			}
			if !writeOk {
				break
			}
		}
		for _, s := range segs {
			s.f.Close()
		}

		if !writeOk {
			return fmt.Errorf("merge %s [%d,%d): read chunk data failed", hash, runStart, runEnd)
		}

		// Write the merged file atomically.
		mergedPath := entryPath(cacheDir, hash, runStart, runEnd, runBytesStart, runBytesEnd)
		dir := filepath.Dir(mergedPath)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("merge %s [%d,%d): mkdir: %w", hash, runStart, runEnd, err)
		}
		tmp, err := os.CreateTemp(dir, ".tmp-*")
		if err != nil {
			return fmt.Errorf("merge %s [%d,%d): create temp: %w", hash, runStart, runEnd, err)
		}
		tmpName := tmp.Name()
		if _, err := tmp.Write(buf); err != nil {
			tmp.Close()
			os.Remove(tmpName) //nolint:errcheck
			return fmt.Errorf("merge %s [%d,%d): write: %w", hash, runStart, runEnd, err)
		}
		if err := tmp.Close(); err != nil {
			os.Remove(tmpName) //nolint:errcheck
			return fmt.Errorf("merge %s [%d,%d): close: %w", hash, runStart, runEnd, err)
		}
		if err := os.Rename(tmpName, mergedPath); err != nil {
			os.Remove(tmpName) //nolint:errcheck
			return fmt.Errorf("merge %s [%d,%d): rename: %w", hash, runStart, runEnd, err)
		}

		for _, e := range run {
			if e.path != mergedPath {
				os.Remove(e.path) //nolint:errcheck
			}
		}
		i = j
	}
	return nil
}

// Evict removes the least-recently-used hash directories until the total cache
// size is at or below maxBytes. A hash directory's recency is the newest mtime
// among its cache files.
func (dc *DiskCache) Evict(maxBytes int64) error {
	if maxBytes < 0 {
		return nil
	}

	if maxBytes == 0 {
		// Clear everything.
		topDirs, err := os.ReadDir(dc.cacheDir)
		if err != nil {
			return fmt.Errorf("read cache dir: %w", err)
		}
		for _, top := range topDirs {
			if !top.IsDir() {
				continue
			}
			topPath := filepath.Join(dc.cacheDir, top.Name())
			subDirs, err := os.ReadDir(topPath)
			if err != nil {
				return fmt.Errorf("read cache subdir: %w", err)
			}
			for _, sub := range subDirs {
				if !sub.IsDir() {
					continue
				}
				hashDir := filepath.Join(topPath, sub.Name())
				os.RemoveAll(hashDir) //nolint:errcheck
			}
			os.Remove(topPath) //nolint:errcheck
		}
		return nil
	}

	type hashEntry struct {
		hashDir    string
		lastUpdate time.Time
		size       int64
	}

	// Walk the two-level prefix structure: cacheDir/<xx>/<rest>
	topDirs, err := os.ReadDir(dc.cacheDir)
	if err != nil {
		return fmt.Errorf("read cache dir: %w", err)
	}

	var entries []hashEntry
	var totalSize int64

	for _, top := range topDirs {
		if !top.IsDir() {
			continue
		}
		topPath := filepath.Join(dc.cacheDir, top.Name())
		subDirs, err := os.ReadDir(topPath)
		if err != nil {
			return fmt.Errorf("read cache subdir: %w", err)
		}
		for _, sub := range subDirs {
			if !sub.IsDir() {
				continue
			}
			hashDir := filepath.Join(topPath, sub.Name())
			files, err := os.ReadDir(hashDir)
			if err != nil {
				return fmt.Errorf("read hash dir: %w", err)
			}
			var dirSize int64
			var newest time.Time
			for _, f := range files {
				if f.IsDir() || strings.HasPrefix(f.Name(), ".tmp-") {
					continue
				}
				info, err := f.Info()
				if err != nil {
					return fmt.Errorf("stat cache file: %w", err)
				}
				dirSize += info.Size()
				if info.ModTime().After(newest) {
					newest = info.ModTime()
				}
			}
			if dirSize == 0 {
				continue
			}
			entries = append(entries, hashEntry{
				hashDir:    hashDir,
				lastUpdate: newest,
				size:       dirSize,
			})
			totalSize += dirSize
		}
	}

	if totalSize <= maxBytes {
		return nil
	}

	// Sort oldest-first.
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].lastUpdate.Before(entries[j].lastUpdate)
	})

	for _, e := range entries {
		if totalSize <= maxBytes {
			break
		}
		if err := os.RemoveAll(e.hashDir); err == nil {
			totalSize -= e.size
			// Remove empty prefix dir if possible.
			os.Remove(filepath.Dir(e.hashDir)) //nolint:errcheck
		}
	}
	return nil
}
