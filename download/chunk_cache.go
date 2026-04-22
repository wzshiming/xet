package download

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/wzshiming/xet/internal/pool"
)

// chunkRef records the position of one decoded chunk in the backing temp file.
type chunkRef struct {
	offset int64
	length int32
}

// chunkCache wraps a Decoder with lazy per-chunk decoding backed by a temp file.
// Decoded chunks are written to disk so that dedup'd xorb chunks (the same chunk
// position referenced by multiple reconstruction terms) can be re-read without a
// second download, while keeping memory usage low.
type chunkCache struct {
	dec   io.Reader
	store *chunkStore
	index []chunkRef
	done  bool // decoder exhausted or closed
	mut   sync.RWMutex
}

type chunkStore struct {
	file        *os.File
	writeOffset int64
	mut         sync.RWMutex
}

func newChunkStore() (*chunkStore, error) {
	f, err := os.CreateTemp("", "xorb-chunks-*")
	if err != nil {
		return nil, err
	}
	return &chunkStore{file: f}, nil
}

func (s *chunkStore) append(data []byte) (int64, error) {
	s.mut.Lock()
	defer s.mut.Unlock()
	if s.file == nil {
		return 0, os.ErrClosed
	}
	n, err := s.file.WriteAt(data, s.writeOffset)
	if err != nil {
		return 0, err
	}
	if n != len(data) {
		return 0, io.ErrShortWrite
	}
	writeOffset := s.writeOffset
	s.writeOffset += int64(n)
	return writeOffset, nil
}

func (s *chunkStore) readAt(buf []byte, offset int64) (int64, error) {
	s.mut.RLock()
	defer s.mut.RUnlock()
	if s.file == nil {
		return 0, os.ErrClosed
	}

	var n int
	for n != len(buf) {
		sn, err := s.file.ReadAt(buf[n:], offset+int64(n))
		n += sn
		if err != nil {
			return int64(n), err
		}
	}
	return int64(n), nil
}

func (s *chunkStore) Close() error {
	s.mut.Lock()
	defer s.mut.Unlock()
	if s.file == nil {
		return nil
	}
	name := s.file.Name()
	s.file.Close()
	os.Remove(name)
	s.file = nil
	return nil
}

func newChunkCache(dec io.Reader, store *chunkStore) (*chunkCache, error) {
	return &chunkCache{dec: dec, store: store}, nil
}

// Chunk returns the decoded chunk at idx, decoding forward as needed.
// Already-decoded chunks are served from the backing temp file.
func (c *chunkCache) Chunk(idx uint32, buf []byte) (int64, error) {
	err := c.LoadTo(idx)
	if err != nil {
		return 0, fmt.Errorf("load chunk %d: %w", idx, err)
	}

	n, err := c.chunk(idx, buf)
	if err != nil {
		return 0, fmt.Errorf("get chunk %d: %w", idx, err)
	}
	return n, nil
}

func (c *chunkCache) chunk(idx uint32, buf []byte) (int64, error) {
	c.mut.RLock()
	defer c.mut.RUnlock()

	ref := c.index[idx]
	if len(buf) < int(ref.length) {
		return 0, fmt.Errorf("buffer too small for chunk: need %d bytes", ref.length)
	}

	n, err := c.store.readAt(buf[:ref.length], ref.offset)
	if err != nil {
		return 0, fmt.Errorf("read at offset %d: %w", ref.offset, err)
	}
	if int64(n) != int64(ref.length) {
		return 0, fmt.Errorf("short read: expected %d bytes, got %d", ref.length, n)
	}
	return n, nil
}

func (c *chunkCache) load() (int, error) {
	if c.done {
		return 0, io.EOF
	}

	tmp := pool.GetChunkBuf()
	defer pool.PutChunkBuf(tmp)

	c.mut.Lock()
	defer c.mut.Unlock()

	n, err := c.dec.Read(tmp[:])
	if err != nil {
		if err == io.EOF {
			c.Done()
			return 0, io.EOF
		}
		return 0, err
	}

	offset, err := c.store.append(tmp[:n])
	if err != nil {
		return 0, fmt.Errorf("append chunk: %w", err)
	}

	c.index = append(c.index, chunkRef{offset: offset, length: int32(n)})
	return len(c.index), nil
}

// LoadTo decodes and caches chunks up to the specified index.
func (c *chunkCache) LoadTo(idx uint32) error {
	for {
		curr, err := c.load()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if curr > int(idx) {
			return nil
		}
	}
}

// LoadAll decodes and caches all chunks.
func (c *chunkCache) LoadAll() error {
	for {
		_, err := c.load()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

func (c *chunkCache) Done() {
	if !c.done {
		c.done = true
	}
}
