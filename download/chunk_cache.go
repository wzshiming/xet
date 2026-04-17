package download

import (
	"io"
	"os"
	"sync"

	"github.com/wzshiming/xet/internal/pool"
	"github.com/wzshiming/xet/xorb"
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
	dec   *xorb.Decoder
	store *chunkStore
	index []chunkRef
	done  bool // decoder exhausted or closed
	mut   sync.Mutex
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

func newChunkCache(dec *xorb.Decoder, store *chunkStore) (*chunkCache, error) {
	return &chunkCache{dec: dec, store: store}, nil
}

// Chunk returns the decoded chunk at idx, decoding forward as needed.
// Already-decoded chunks are served from the backing temp file.
func (c *chunkCache) Chunk(idx uint32) ([]byte, error) {
	c.mut.Lock()
	defer c.mut.Unlock()
	if int(idx) < len(c.index) {
		ref := c.index[idx]
		data := make([]byte, ref.length)
		n, err := c.store.readAt(data, ref.offset)
		if err != nil {
			return nil, err
		}
		if int64(n) != int64(ref.length) {
			return nil, io.ErrUnexpectedEOF
		}
		return data, nil
	}
	for uint32(len(c.index)) <= idx {
		err := c.load()
		if err != nil {
			return nil, err
		}
	}
	ref := c.index[idx]
	data := make([]byte, ref.length)
	n, err := c.store.readAt(data, ref.offset)
	if err != nil {
		return nil, err
	}
	if int64(n) != int64(ref.length) {
		return nil, io.ErrUnexpectedEOF
	}
	return data, nil
}

func (c *chunkCache) load() error {
	if c.done {
		return io.EOF
	}

	tmp := pool.GetChunkBuf()
	defer pool.PutChunkBuf(tmp)

	n, err := c.dec.Read(tmp[:])
	if err != nil {
		if err == io.EOF {
			c.Done()
			return io.EOF
		}
		return err
	}
	offset, err := c.store.append(tmp[:n])
	if err != nil {
		return err
	}
	c.index = append(c.index, chunkRef{offset: offset, length: int32(n)})
	return nil
}

// LoadAll decodes and caches all chunks.
func (c *chunkCache) LoadAll() error {
	for {
		c.mut.Lock()
		err := c.load()
		c.mut.Unlock()
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
		c.dec.Close()
	}
}
