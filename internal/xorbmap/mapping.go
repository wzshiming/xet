package xorbmap

import (
	"bytes"
	"io"
	"os"
	"sync/atomic"

	"github.com/edsrzf/mmap-go"
	"github.com/wzshiming/xet"
)

// Mapping is a read-only mapping of one xorb file shared by the cache and every holder; the last hold's Release unmaps it for good.
type Mapping struct {
	cache *Cache
	hash  xet.XorbHash
	data  mmap.MMap // nil for a 0-byte xorb or once unmapped
	refs  atomic.Int64
}

// acquire takes a hold, reporting false once the last one is gone.
func (m *Mapping) acquire() bool {
	for {
		n := m.refs.Load()
		if n == 0 {
			return false
		}
		if m.refs.CompareAndSwap(n, n+1) {
			return true
		}
	}
}

// Bytes returns the xorb's bytes, nil when it is empty; they are valid only while held.
func (m *Mapping) Bytes() []byte {
	return m.data
}

// Release drops a hold; dropping the last unmaps the xorb and calls the cache's release hook.
func (m *Mapping) Release() {
	if m.refs.Add(-1) != 0 {
		return
	}
	if m.data != nil {
		_ = m.data.Unmap()
		m.cache.mapped.Add(-1)
	}
	if m.cache.release != nil {
		m.cache.release(m.hash)
	}
}

// Reader takes over the caller's hold and reads Bytes like a bytes.Reader until Close releases it; Read and Seek must not race Close.
func (m *Mapping) Reader() io.ReadSeekCloser {
	r := &reader{br: bytes.NewReader(m.data)}
	r.held.Store(m)
	return r
}

// reader reads a held mapping and never touches it once closed.
type reader struct {
	held atomic.Pointer[Mapping] // nil once closed
	br   *bytes.Reader
}

func (r *reader) Read(p []byte) (int, error) {
	if r.held.Load() == nil {
		return 0, os.ErrClosed
	}
	return r.br.Read(p)
}

func (r *reader) Seek(offset int64, whence int) (int64, error) {
	if r.held.Load() == nil {
		return 0, os.ErrClosed
	}
	return r.br.Seek(offset, whence)
}

func (r *reader) Close() error {
	m := r.held.Swap(nil)
	if m == nil {
		return os.ErrClosed
	}
	m.Release()
	return nil
}
