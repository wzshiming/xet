package download

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sync"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/internal/flock"
)

// cacheKey names one cache entry: <hash dir>/<ChunkStart>-<ChunkEnd>_<ByteStart>-<ByteEnd>.
type cacheKey struct {
	Hash                 xet.XorbHash
	ChunkStart, ChunkEnd int
	ByteStart, ByteEnd   int64
}

// cacheWriter writes cache entries in the background, best effort.
type cacheWriter struct {
	manager     *CacheManager
	maxInflight int
	beforeWrite func() // test hook, run in the write goroutine before the entry is written
	mu          sync.Mutex
	finished    sync.Cond // on mu; broadcast when pending drops to zero
	inflight    int       // writes counted against maxInflight
	pending     int       // writes whose done has not returned yet
}

// newWriter returns a writer with at most maxInflight writes in flight, or
// nil for a nil manager.
func (m *CacheManager) newWriter(maxInflight int) *cacheWriter {
	if m == nil {
		return nil
	}
	w := &cacheWriter{manager: m, maxInflight: maxInflight}
	w.finished.L = &w.mu
	return w
}

// put writes data as the entry key in the background and runs done once
// the write is over; with maxInflight writes already in flight it drops the
// write and runs done before returning. Chunk key.ChunkStart+i is
// data[offsets[i]:offsets[i+1]], and data must not change until done runs.
func (w *cacheWriter) put(key cacheKey, offsets []int64, data []byte, done func()) {
	if w == nil {
		done()
		return
	}
	checkEntryOffsets(key, offsets, len(data))
	w.mu.Lock()
	if w.inflight >= w.maxInflight {
		w.mu.Unlock()
		done()
		return
	}
	w.inflight++
	w.pending++
	w.mu.Unlock()
	go func() {
		if w.beforeWrite != nil {
			w.beforeWrite()
		}
		w.manager.storeEntry(key, offsets, data)
		// Free the slot first so done can put the next write.
		w.mu.Lock()
		w.inflight--
		w.mu.Unlock()
		done()
		w.mu.Lock()
		w.pending--
		if w.pending == 0 {
			w.finished.Broadcast()
		}
		w.mu.Unlock()
	}()
}

// wait blocks until every write this writer started has finished.
func (w *cacheWriter) wait() {
	if w == nil {
		return
	}
	w.mu.Lock()
	for w.pending > 0 {
		w.finished.Wait()
	}
	w.mu.Unlock()
}

// checkEntryOffsets panics unless offsets lay out size bytes as the chunks of key.
func checkEntryOffsets(key cacheKey, offsets []int64, size int) {
	ok := len(offsets) > 0 && len(offsets) == key.ChunkEnd-key.ChunkStart+1 &&
		offsets[0] == 0 && offsets[len(offsets)-1] == int64(size)
	for i := 1; ok && i < len(offsets); i++ {
		ok = offsets[i-1] <= offsets[i]
	}
	if !ok {
		panic(fmt.Sprintf("chunk cache: %d offsets do not lay out %d bytes as chunks [%d,%d)", len(offsets), size, key.ChunkStart, key.ChunkEnd))
	}
}

// storeEntry writes the entry key in the format chunkCache seals, unless
// another writer holds its lock or it is sealed already, and registers it as
// seal and then Done register an entry.
func (m *CacheManager) storeEntry(key cacheKey, offsets []int64, data []byte) {
	hash := key.Hash.String()
	start, end := uint32(key.ChunkStart), uint32(key.ChunkEnd)
	lockFile, err := lockChunkCache(lockNoWait, m.dir, hash, start, end, key.ByteStart, key.ByteEnd)
	if err != nil {
		return
	}
	path := cacheFilePath(m.dir, hash, start, end, key.ByteStart, key.ByteEnd)
	size, written := writeEntryFile(lockFile, start, end, offsets, data)
	if written {
		// Tracked before the lock goes, and trusted: the checksum is over what was just written.
		m.acquire(path, size)
		m.markVerified(path)
	}
	flock.Unlock(lockFile) //nolint:errcheck
	lockFile.Close()
	if !written {
		return
	}
	m.release(path)
	m.evaluate()
	m.noteMergeCandidate(hash)
}

// lockNoWait makes lockChunkCache try the entry lock once instead of waiting for it.
var lockNoWait = func() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}()

// writeEntryFile writes the entry into its locked file f unless f is sealed
// already, and returns the entry's size. The offset table, the data and the
// checksum trailer go in that order, so the size arithmetic shows the entry
// only once it is whole.
func writeEntryFile(f *os.File, start, end uint32, offsets []int64, data []byte) (int64, bool) {
	info, err := f.Stat()
	if err != nil {
		return 0, false
	}
	if _, err := readCacheFileLayout(f, start, end, info.Size()); err == nil {
		return 0, false
	}
	header := make([]byte, 4+4*len(offsets))
	binary.LittleEndian.PutUint32(header, uint32(len(offsets)))
	for i, off := range offsets {
		binary.LittleEndian.PutUint32(header[4+4*i:], uint32(off))
	}
	// The checksum covers the data and then the header, as seal computes it.
	var trailer [4]byte
	binary.LittleEndian.PutUint32(trailer[:], crc32.Update(crc32.ChecksumIEEE(data), crc32.IEEETable, header))
	// Truncation discards a crashed leftover, which readers never adopt.
	if f.Truncate(0) != nil {
		return 0, false
	}
	if _, err := f.WriteAt(header, 0); err != nil {
		return 0, false
	}
	if _, err := f.WriteAt(data, int64(len(header))); err != nil {
		return 0, false
	}
	size := int64(len(header) + len(data) + len(trailer))
	if _, err := f.WriteAt(trailer[:], size-int64(len(trailer))); err != nil {
		return 0, false
	}
	return size, true
}
