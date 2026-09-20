package storage

import (
	"sync"

	"github.com/golang/groupcache/lru"
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
)

// Default index cache sizes, in entries.
const (
	DefaultCacheSize = 4096
)

// Cache is a bounded, goroutine-safe LRU cache.
type Cache[K comparable, V any] struct {
	mut sync.Mutex
	lru *lru.Cache // nil when caching is disabled
}

// Non-positive sizes disable caching.
func NewCache[K comparable, V any](size int) *Cache[K, V] {
	if size < 1 {
		return &Cache[K, V]{}
	}
	return &Cache[K, V]{lru: lru.New(size)}
}

// Get returns the cached value for key, marking it most recently used.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	var zero V
	if c.lru == nil {
		return zero, false
	}
	c.mut.Lock()
	defer c.mut.Unlock()
	if v, ok := c.lru.Get(key); ok {
		return v.(V), true
	}
	return zero, false
}

// Add stores value under key, evicting the least recently used entry when full.
func (c *Cache[K, V]) Add(key K, value V) {
	if c.lru == nil {
		return
	}
	c.mut.Lock()
	defer c.mut.Unlock()
	c.lru.Add(key, value)
}

// Remove drops key from the cache.
func (c *Cache[K, V]) Remove(key K) {
	if c.lru == nil {
		return
	}
	c.mut.Lock()
	defer c.mut.Unlock()
	c.lru.Remove(key)
}

// Len returns the number of cached entries.
func (c *Cache[K, V]) Len() int {
	if c.lru == nil {
		return 0
	}
	c.mut.Lock()
	defer c.mut.Unlock()
	return c.lru.Len()
}

// Loads run outside the lock; concurrent misses may load the same key.
func (c *Cache[K, V]) GetOrLoad(key K, load func() (V, error)) (V, error) {
	if v, ok := c.Get(key); ok {
		return v, nil
	}
	v, err := load()
	if err != nil {
		return v, err
	}
	c.Add(key, v)
	return v, nil
}

// IndexCaches bounds the in-memory index lookups every backend performs.
type IndexCaches struct {
	Files   *Cache[xet.FileHash, string]   // file hash -> shard hash
	Shards  *Cache[string, *shard.Shard]   // shard hash -> decoded shard
	Chunks  *Cache[xet.ChunkHash, string]  // chunk hash -> shard hash
	SHA256  *Cache[[32]byte, string]       // SHA-256 -> shard hash
	Offsets *Cache[xet.XorbHash, []uint64] // xorb hash -> packed chunk end-offsets
}

// NewIndexCaches returns index caches with the default sizes.
func NewIndexCaches() *IndexCaches {
	return &IndexCaches{
		Files:   NewCache[xet.FileHash, string](DefaultCacheSize),
		Shards:  NewCache[string, *shard.Shard](DefaultCacheSize),
		Chunks:  NewCache[xet.ChunkHash, string](DefaultCacheSize),
		SHA256:  NewCache[[32]byte, string](DefaultCacheSize),
		Offsets: NewCache[xet.XorbHash, []uint64](DefaultCacheSize),
	}
}
