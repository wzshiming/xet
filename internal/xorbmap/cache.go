// Package xorbmap caches read-only memory mappings of whole xorb files, reference-counted so an evicted mapping lives until its last reader closes.
package xorbmap

import (
	"fmt"
	"os"
	"sync/atomic"

	"github.com/edsrzf/mmap-go"
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/internal/lru"
	"golang.org/x/sync/singleflight"
)

// Cache is an LRU of xorb mappings that fills each hash at most once at a time, outside its lock.
type Cache struct {
	lru     *lru.Cache[xet.XorbHash, *Mapping]
	fills   singleflight.Group
	open    func(xet.XorbHash) (*os.File, error)
	release func(xet.XorbHash)
	mapped  atomic.Int64 // live OS mappings, cached or held only by readers
}

// New caches up to maxEntries mappings (unbounded below one) of the files open returns; release(h), if non-nil, runs once a mapping of h is gone or a fill fails after open succeeded, possibly under the lru mutex and overlapping a later fill of h, so it must not block and must not call Cache methods.
func New(maxEntries int, open func(xet.XorbHash) (*os.File, error), release func(xet.XorbHash)) *Cache {
	c := &Cache{
		lru:     lru.New[xet.XorbHash, *Mapping](maxEntries),
		open:    open,
		release: release,
	}
	// Eviction drops only the cache's hold, so it never waits for readers.
	c.lru.OnEvicted = func(_ xet.XorbHash, m *Mapping) {
		m.Release()
	}
	return c
}

// Acquire returns a held mapping of h, mapping the file open returns on a miss; the caller releases it or hands it to Reader.
func (c *Cache) Acquire(h xet.XorbHash) (*Mapping, error) {
	for {
		if m := c.hit(h); m != nil {
			return m, nil
		}
		leader := false
		v, err, _ := c.fills.Do(h.String(), func() (any, error) {
			leader = true
			return c.fill(h)
		})
		if err != nil {
			return nil, err
		}
		// The leader keeps the hold fill granted it; a waiter takes its own and retries if the mapping died first.
		if m := v.(*Mapping); leader || m.acquire() {
			return m, nil
		}
	}
}

// hit returns a held cached mapping of h, or nil when none is cached or the cached one just died.
func (c *Cache) hit(h xet.XorbHash) *Mapping {
	if m, ok := c.lru.Get(h); ok && m.acquire() {
		return m
	}
	return nil
}

// fill caches a new mapping of h holding one ref for the cache and one for the caller, unless a fill that landed meanwhile hits.
func (c *Cache) fill(h xet.XorbHash) (_ *Mapping, err error) {
	if m := c.hit(h); m != nil {
		return m, nil
	}
	f, err := c.open(h)
	if err != nil {
		return nil, fmt.Errorf("open xorb %s: %w", h, err)
	}
	// release frees what open set up; deferred first so it runs after f.Close.
	defer func() {
		if err != nil && c.release != nil {
			c.release(h)
		}
	}()
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat xorb %s: %w", h, err)
	}
	m := &Mapping{cache: c, hash: h}
	// A 0-byte file cannot be mapped.
	if info.Size() > 0 {
		data, err := mmap.Map(f, mmap.RDONLY, 0)
		if err != nil {
			return nil, fmt.Errorf("map xorb %s: %w", h, err)
		}
		m.data = data
		c.mapped.Add(1)
	}
	m.refs.Store(2)
	c.lru.Add(h, m)
	return m, nil
}

// Has reports whether h is cached right now, counting as a use; it never fills.
func (c *Cache) Has(h xet.XorbHash) bool {
	_, ok := c.lru.Get(h)
	return ok
}

// Remove drops the cache's hold on h's mapping; readers keep theirs.
func (c *Cache) Remove(h xet.XorbHash) {
	c.lru.Remove(h)
}

// Len returns the number of cached mappings.
func (c *Cache) Len() int {
	return c.lru.Len()
}

// Mapped returns the live OS mappings, cached or held only by readers; a 0-byte xorb maps nothing.
func (c *Cache) Mapped() int64 {
	return c.mapped.Load()
}
