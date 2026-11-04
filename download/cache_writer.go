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
	beforeWrite func() // test hook, run in an entry's write goroutine before it writes anything
	afterWrite  func() // test hook, run in an entry's write goroutine after each write to its file
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
// the write is over; when begin declines the write it runs done before
// returning. Chunk key.ChunkStart+i is data[offsets[i]:offsets[i+1]], and
// data must not change until done runs.
func (w *cacheWriter) put(key cacheKey, offsets []int64, data []byte, done func()) {
	if w == nil {
		done()
		return
	}
	checkEntryOffsets(key, offsets, len(data))
	e := w.begin(key, done)
	if e == nil {
		done()
		return
	}
	e.advance(data)
	e.finish(offsets)
}

// entryWrite is one cache entry streamed to its locked file by a goroutine
// that owns the file, as the entry's data is handed over.
type entryWrite struct {
	writer *cacheWriter
	key    cacheKey
	path   string
	file   *os.File // locked until the write is over
	done   func()

	mu      sync.Mutex
	changed sync.Cond // on mu; signaled when data grows or the write is finished or aborted
	data    []byte    // the entry's data handed over so far
	offsets []int64   // set by finish
	aborted bool
}

// begin starts writing the entry key in the background from the data that
// advance hands over, and runs done once that write is over. It returns nil
// and writes nothing for a nil writer, with maxInflight writes in flight, or
// when another writer holds the entry or the entry is sealed already.
func (w *cacheWriter) begin(key cacheKey, done func()) *entryWrite {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	if w.inflight >= w.maxInflight {
		w.mu.Unlock()
		return nil
	}
	w.inflight++
	w.pending++
	w.mu.Unlock()
	file, path := w.manager.lockEntry(key)
	if file == nil {
		w.end(func() {})
		return nil
	}
	e := &entryWrite{writer: w, key: key, path: path, file: file, done: done}
	e.changed.L = &e.mu
	go e.run()
	return e
}

// advance hands over the entry's data decoded so far. A later call may pass
// a larger array holding the same bytes first; the bytes handed over must not
// change until done runs. It does nothing on a nil entry.
func (e *entryWrite) advance(data []byte) {
	if e == nil {
		return
	}
	e.mu.Lock()
	e.data = data
	e.changed.Signal()
	e.mu.Unlock()
}

// finish seals the entry once the data last handed over is written; chunk
// key.ChunkStart+i is data[offsets[i]:offsets[i+1]]. It does nothing on a nil
// entry.
func (e *entryWrite) finish(offsets []int64) {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	checkEntryOffsets(e.key, offsets, len(e.data))
	e.offsets = offsets
	e.changed.Signal()
}

// abort discards the entry unless it is finished. It does nothing on a nil
// entry.
func (e *entryWrite) abort() {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.offsets == nil {
		e.aborted = true
		e.changed.Signal()
	}
}

// run writes the entry and publishes it, or removes its file when the entry
// is aborted or a write fails.
func (e *entryWrite) run() {
	if e.writer.beforeWrite != nil {
		e.writer.beforeWrite()
	}
	if size, ok := e.write(); ok {
		e.writer.manager.publishEntry(e.key.Hash.String(), e.path, e.file, size)
	} else {
		discardEntryFile(e.path, e.file)
	}
	e.writer.end(e.done)
}

// write writes the data past the room for the offset table as it is handed
// over, hashing it on the way, and then seals the entry and returns its size;
// it returns false once the entry is aborted or a write fails.
func (e *entryWrite) write() (int64, bool) {
	// Truncation discards a crashed leftover, which readers never adopt.
	if e.file.Truncate(0) != nil {
		return 0, false
	}
	start := int64(4 + 4*(e.key.ChunkEnd-e.key.ChunkStart+1))
	var crc uint32
	written := 0
	for {
		e.mu.Lock()
		for len(e.data) == written && e.offsets == nil && !e.aborted {
			e.changed.Wait()
		}
		data, offsets, aborted := e.data, e.offsets, e.aborted
		e.mu.Unlock()
		switch {
		case aborted:
			return 0, false
		case len(data) > written:
			// A larger array holds the same bytes first, so only those past written are new.
			if !e.writeAt(data[written:], start+int64(written)) {
				return 0, false
			}
			crc = crc32.Update(crc, crc32.IEEETable, data[written:])
			written = len(data)
		default:
			return e.seal(offsets, crc)
		}
	}
}

// seal writes the offset table and then the checksum trailer, given crc,
// the checksum of the data written; the trailer makes the size arithmetic
// show the entry whole.
func (e *entryWrite) seal(offsets []int64, crc uint32) (int64, bool) {
	header := make([]byte, 4+4*len(offsets))
	binary.LittleEndian.PutUint32(header, uint32(len(offsets)))
	for i, off := range offsets {
		binary.LittleEndian.PutUint32(header[4+4*i:], uint32(off))
	}
	if !e.writeAt(header, 0) {
		return 0, false
	}
	// The checksum covers the data and then the header, as chunkCache.seal computes it.
	var trailer [4]byte
	binary.LittleEndian.PutUint32(trailer[:], crc32.Update(crc, crc32.IEEETable, header))
	size := int64(len(header)) + offsets[len(offsets)-1] + int64(len(trailer))
	if !e.writeAt(trailer[:], size-int64(len(trailer))) {
		return 0, false
	}
	return size, true
}

func (e *entryWrite) writeAt(b []byte, off int64) bool {
	_, err := e.file.WriteAt(b, off)
	if e.writer.afterWrite != nil {
		e.writer.afterWrite()
	}
	return err == nil
}

// end frees a write's slot, runs done and then counts the write as over.
func (w *cacheWriter) end(done func()) {
	// Free the slot first so done can begin the next write.
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

// lockEntry takes the lock of the entry key without waiting for it and
// returns the entry file and its path, or nil when another writer holds the
// lock or the entry is sealed already.
func (m *CacheManager) lockEntry(key cacheKey) (*os.File, string) {
	hash := key.Hash.String()
	start, end := uint32(key.ChunkStart), uint32(key.ChunkEnd)
	f, err := lockChunkCache(lockNoWait, m.dir, hash, start, end, key.ByteStart, key.ByteEnd)
	if err != nil {
		return nil, ""
	}
	if info, err := f.Stat(); err == nil {
		if _, err := readCacheFileLayout(f, start, end, info.Size()); err != nil {
			return f, cacheFilePath(m.dir, hash, start, end, key.ByteStart, key.ByteEnd)
		}
	}
	flock.Unlock(f) //nolint:errcheck
	f.Close()
	return nil, ""
}

// lockNoWait makes lockChunkCache try the entry lock once instead of waiting for it.
var lockNoWait = func() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}()

// publishEntry registers the sealed entry of hash at path, size bytes long,
// as seal and then Done register an entry, releasing the lock its file f
// holds.
func (m *CacheManager) publishEntry(hash, path string, f *os.File, size int64) {
	// Tracked before the lock goes, and trusted: the checksum is over what was just written.
	m.acquire(path, size)
	m.markVerified(path)
	flock.Unlock(f) //nolint:errcheck
	f.Close()
	m.release(path)
	m.evaluate()
	m.noteMergeCandidate(hash)
}

// discardEntryFile removes the unsealed entry at path while its file f
// holds the lock, which keeps eviction and other writers away, and then
// releases the lock.
func discardEntryFile(path string, f *os.File) {
	os.Remove(path) //nolint:errcheck
	removeEmptyCacheDirs(path)
	flock.Unlock(f) //nolint:errcheck
	f.Close()
}
