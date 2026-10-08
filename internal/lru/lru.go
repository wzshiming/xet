package lru

import (
	"container/list"
	"sync"
)

// Cache is a goroutine-safe LRU cache; the zero value is an empty, unbounded cache.
type Cache[K comparable, V any] struct {
	// MaxEntries bounds the cache; zero or negative means no limit. Lowering it takes effect on the next Add.
	MaxEntries int

	// OnEvicted, if set, runs under the cache lock for every purged entry: eviction, Remove, RemoveOldest and Clear alike.
	OnEvicted func(key K, value V)

	ll    *list.List
	cache map[K]*list.Element
	mut   sync.Mutex
}

type entry[K comparable, V any] struct {
	key   K
	value V
}

// New creates a new Cache; maxEntries zero or negative means no limit.
func New[K comparable, V any](maxEntries int) *Cache[K, V] {
	return &Cache[K, V]{
		MaxEntries: maxEntries,
		ll:         list.New(),
		cache:      make(map[K]*list.Element),
	}
}

// Add adds a value to the cache.
func (c *Cache[K, V]) Add(key K, value V) {
	c.mut.Lock()
	defer c.mut.Unlock()

	if c.cache == nil {
		c.cache = make(map[K]*list.Element)
		c.ll = list.New()
	}
	if ee, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ee)
		ee.Value.(*entry[K, V]).value = value
	} else {
		c.cache[key] = c.ll.PushFront(&entry[K, V]{key, value})
	}
	c.evictLocked()
}

// evictLocked drops least recently used entries down to MaxEntries; the caller holds the mutex.
func (c *Cache[K, V]) evictLocked() {
	for c.MaxEntries > 0 && c.ll.Len() > c.MaxEntries {
		c.removeOldestLocked()
	}
}

// Get looks up a key's value from the cache.
func (c *Cache[K, V]) Get(key K) (value V, ok bool) {
	c.mut.Lock()
	defer c.mut.Unlock()

	if c.cache == nil {
		return
	}
	if ele, hit := c.cache[key]; hit {
		c.ll.MoveToFront(ele)
		return ele.Value.(*entry[K, V]).value, true
	}
	return
}

// GetOrNew looks up a key's value from the cache, or creates it using the provided function if it doesn't exist.
// newFunc runs under the cache lock, so it is never called twice for one key but stalls every other caller meanwhile.
func (c *Cache[K, V]) GetOrNew(key K, newFunc func() (V, bool)) (value V, ok bool) {
	c.mut.Lock()
	defer c.mut.Unlock()

	if c.cache == nil {
		c.cache = make(map[K]*list.Element)
		c.ll = list.New()
	}
	if ele, hit := c.cache[key]; hit {
		c.ll.MoveToFront(ele)
		return ele.Value.(*entry[K, V]).value, true
	}
	value, ok = newFunc()
	if !ok {
		return value, false
	}
	c.cache[key] = c.ll.PushFront(&entry[K, V]{key, value})
	c.evictLocked()
	return value, true
}

// Remove removes the provided key from the cache and returns its value.
func (c *Cache[K, V]) Remove(key K) (value V, ok bool) {
	c.mut.Lock()
	defer c.mut.Unlock()

	if c.cache == nil {
		return
	}
	ele, hit := c.cache[key]
	if !hit {
		return
	}
	return c.removeElement(ele).value, true
}

// RemoveOldest removes the oldest item from the cache and returns it.
func (c *Cache[K, V]) RemoveOldest() (key K, value V, ok bool) {
	c.mut.Lock()
	defer c.mut.Unlock()

	kv := c.removeOldestLocked()
	if kv == nil {
		return
	}
	return kv.key, kv.value, true
}

// removeOldestLocked removes the oldest item, nil when the cache is empty; the caller holds the mutex.
func (c *Cache[K, V]) removeOldestLocked() *entry[K, V] {
	if c.cache == nil {
		return nil
	}
	ele := c.ll.Back()
	if ele == nil {
		return nil
	}
	return c.removeElement(ele)
}

func (c *Cache[K, V]) removeElement(e *list.Element) *entry[K, V] {
	c.ll.Remove(e)
	kv := e.Value.(*entry[K, V])
	delete(c.cache, kv.key)
	if c.OnEvicted != nil {
		c.OnEvicted(kv.key, kv.value)
	}
	return kv
}

// Len returns the number of items in the cache.
func (c *Cache[K, V]) Len() int {
	c.mut.Lock()
	defer c.mut.Unlock()

	if c.cache == nil {
		return 0
	}
	return c.ll.Len()
}

// Clear purges all stored items from the cache.
func (c *Cache[K, V]) Clear() {
	c.mut.Lock()
	defer c.mut.Unlock()

	if c.OnEvicted != nil {
		for _, e := range c.cache {
			kv := e.Value.(*entry[K, V])
			c.OnEvicted(kv.key, kv.value)
		}
	}
	c.ll = nil
	c.cache = nil
}
