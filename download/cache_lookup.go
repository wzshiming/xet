package download

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/wzshiming/xet"
)

// cachedChunks is one sealed cache entry, pinned against eviction until release.
type cachedChunks struct {
	manager    *CacheManager
	path       string
	file       *os.File
	chunkStart int      // the entry's first chunk index in the xorb
	dataStart  int64    // file offset of the data region
	offsets    []uint32 // chunk chunkStart+i is data[offsets[i]:offsets[i+1]]
	released   sync.Once
}

// lookup returns a sealed entry of hash whose chunk span covers
// [chunkStart, chunkEnd), pinned against eviction until its release, or nil
// on a miss. An entry still being written is a miss, a corrupt one is removed
// and missed, and a nil manager always misses.
func (m *CacheManager) lookup(ctx context.Context, hash xet.XorbHash, chunkStart, chunkEnd int) (*cachedChunks, error) {
	if m == nil {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	hashDir := cacheHashDir(m.dir, hash.String())
	entries, err := os.ReadDir(hashDir)
	if err != nil {
		if os.IsNotExist(err) || os.IsPermission(err) {
			return nil, nil
		}
		return nil, err
	}
	for _, de := range entries {
		start, end, _, _, ok := parseCacheFileName(de.Name())
		if !ok || de.IsDir() || int(start) > chunkStart || int(end) < chunkEnd {
			continue
		}
		path := filepath.Join(hashDir, de.Name())
		f, err := os.Open(path)
		if err != nil {
			// Evicted since the listing; on Windows a delete-pending name is a permission error.
			if os.IsNotExist(err) || os.IsPermission(err) {
				continue
			}
			return nil, err
		}
		info, err := f.Stat()
		if err != nil {
			f.Close()
			continue
		}
		layout, err := readCacheFileLayout(f, start, end, info.Size())
		if err != nil {
			// Still being written, or a crashed leftover.
			f.Close()
			continue
		}
		// Entries are verified once per manager, on their first open.
		if !m.wasVerified(path) {
			if err := verifyCacheFileCRC(f, layout); err != nil {
				f.Close()
				// Remove the corrupted entry under its flock, like eviction.
				removeCacheEntry(path)
				return nil, nil
			}
			m.markVerified(path)
		}
		offsets, err := readCacheOffsets(f, layout)
		if err != nil {
			f.Close()
			removeCacheEntry(path)
			return nil, nil
		}
		m.acquire(path, info.Size())
		return &cachedChunks{
			manager:    m,
			path:       path,
			file:       f,
			chunkStart: int(start),
			dataStart:  layout.headerBytes,
			offsets:    offsets,
		}, nil
	}
	return nil, nil
}

// readCacheOffsets reads the offset table of a sealed entry.
func readCacheOffsets(f *os.File, layout cacheFileLayout) ([]uint32, error) {
	raw := make([]byte, 4*int(layout.numOffsets))
	if _, err := f.ReadAt(raw, 4); err != nil {
		return nil, err
	}
	offsets := make([]uint32, layout.numOffsets)
	for i := range offsets {
		offsets[i] = binary.LittleEndian.Uint32(raw[4*i:])
		// The last offset is the data length, so ascending offsets stay inside the data.
		if i > 0 && offsets[i] < offsets[i-1] {
			return nil, fmt.Errorf("invalid cache offsets")
		}
	}
	return offsets, nil
}

// size returns the decoded length of chunks [from, to), which must lie inside the entry.
func (c *cachedChunks) size(from, to int) int64 {
	if from < c.chunkStart || from > to || to-c.chunkStart >= len(c.offsets) {
		panic(fmt.Sprintf("chunk cache: span [%d,%d) outside entry [%d,%d)", from, to, c.chunkStart, c.chunkStart+len(c.offsets)-1))
	}
	return int64(c.offsets[to-c.chunkStart] - c.offsets[from-c.chunkStart])
}

// read fills dst[:c.size(from, to)] with the decoded chunks [from, to).
func (c *cachedChunks) read(dst []byte, from, to int) error {
	n := c.size(from, to)
	if int64(len(dst)) < n {
		return fmt.Errorf("chunk cache: %d byte buffer for %d bytes of chunks [%d,%d)", len(dst), n, from, to)
	}
	_, err := c.file.ReadAt(dst[:n], c.dataStart+int64(c.offsets[from-c.chunkStart]))
	return err
}

// release unpins and closes the entry; calls after the first do nothing.
func (c *cachedChunks) release() {
	c.released.Do(func() {
		c.file.Close()
		c.manager.release(c.path)
		// Unpinned, the entry may be what eviction needs now.
		c.manager.evaluate()
	})
}
