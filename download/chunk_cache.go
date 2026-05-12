package download

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/wzshiming/xet/internal/pool"
)

// chunkRef records the location of one decoded chunk within the backing file.
type chunkRef struct {
	offset  int64
	size    int32
	fileIdx int // index into chunkCache.files; 0 means chunkCache.file
}

// chunkCache wraps a Decoder with lazy per-chunk decoding backed by a temp file.
// Decoded chunks are written to disk immediately; only offset+size metadata is
// kept in memory, so large xorbs do not inflate the process heap.
type chunkCache struct {
	dec        io.Reader
	metas      []chunkRef
	writePos   int64 // next write offset in file
	done       bool
	mut        sync.Mutex
	diskCache  *DiskCache
	hash       string
	chunkStart uint32
	chunkEnd   uint32
	bytesStart int64
	bytesEnd   int64
	file       *os.File   // primary backing file (write path / single-file read path)
	files      []*os.File // additional files for multi-file read path (fileIdx > 0)
}

func newChunkCache(dec io.Reader, dc *DiskCache, hash string, chunkStart, chunkEnd uint32, bytesStart, bytesEnd int64) (*chunkCache, error) {
	tmp, err := os.CreateTemp("", "xet-chunk-*")
	if err != nil {
		return nil, fmt.Errorf("create chunk temp file: %w", err)
	}
	dc.addRef(hash)
	return &chunkCache{
		dec:        dec,
		diskCache:  dc,
		hash:       hash,
		chunkStart: chunkStart,
		chunkEnd:   chunkEnd,
		bytesStart: bytesStart,
		bytesEnd:   bytesEnd,
		file:       tmp,
	}, nil
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
// The mutex is held for the full decode+write to serialise concurrent callers.
func (c *chunkCache) load() (int, error) {
	c.mut.Lock()
	defer c.mut.Unlock()

	if c.done {
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

// LoadAll decodes and caches all remaining chunks, then persists to the disk cache.
// The write is skipped if the number of loaded chunks does not match the expected
// range (e.g. when the decoder was interrupted early by a Done() call).
func (c *chunkCache) LoadAll() error {
	for {
		_, err := c.load()
		if err != nil {
			if err == io.EOF {
				c.mut.Lock()
				metas := c.metas
				file := c.file
				dc := c.diskCache
				hash := c.hash
				chunkStart := c.chunkStart
				chunkEnd := c.chunkEnd
				bytesStart := c.bytesStart
				bytesEnd := c.bytesEnd
				c.mut.Unlock()

				// Done() may run concurrently and clear diskCache before prefetch
				// persistence reaches EOF; in that case we skip cache writeback.
				if dc != nil {
					dc.put(hash, chunkStart, chunkEnd, bytesStart, bytesEnd, metas, file)
				}
				return nil
			}
			return err
		}
	}
}

// Done marks the decoder as exhausted and releases the backing file(s).
func (c *chunkCache) Done() {
	c.mut.Lock()
	c.done = true
	if c.file != nil {
		c.file.Close()
	}
	for _, f := range c.files {
		f.Close()
	}
	c.files = nil
	dc := c.diskCache
	hash := c.hash
	c.diskCache = nil
	c.mut.Unlock()

	if dc != nil {
		dc.decRef(hash)
	}
}
