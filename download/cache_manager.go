package download

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/golang/groupcache/lru"
	"github.com/wzshiming/xet/internal/flock"
)

// DefaultCacheSize is the default chunk cache capacity in bytes, matching
// xet-core's default chunk cache size (HF_XET_CHUNK_CACHE_SIZE_BYTES).
const DefaultCacheSize int64 = 10_000_000_000

// cacheEntry is the LRU bookkeeping for one published cache file.
type cacheEntry struct {
	size int64
	refs int // active in-process users; entries with refs > 0 are never evicted
}

// evictedEntry is one (key, entry) pair reported by the LRU's OnEvicted.
type evictedEntry struct {
	key   string
	entry *cacheEntry
}

// CacheManager bounds the total size of one chunk cache directory.
//
// Entries are tracked in an in-memory LRU that is touched once when a cache
// file is acquired and once more when its user is done. Eviction is
// evaluated when an entry is published or released, never on the read path,
// and every evaluation first reconciles the tracked state with the directory
// so the bound holds even when several managers or processes share it.
// Cross-process (and cross-manager) safety relies on the per-entry .partial
// flock: names are only unlinked while holding that lock, so active writers
// are never disturbed, and already-open readers keep their data through the
// open handles even after the names are removed.
type CacheManager struct {
	mu       sync.Mutex
	dir      string
	capacity int64 // bytes; <= 0 disables eviction
	lru      *lru.Cache
	total    int64

	// evicted accumulates entries popped from the LRU via RemoveOldest;
	// evaluateLocked drains it for eviction candidates and reconcileLocked
	// uses it to rebuild the LRU.
	evicted []evictedEntry
}

// NewCacheManager creates a manager for the chunk cache at cacheDir (empty
// means the default directory). capacity bounds the cache size in bytes;
// zero or negative disables eviction. Callers should create one manager per
// cache directory and pass it to every reader sharing that directory.
func NewCacheManager(cacheDir string, capacity int64) *CacheManager {
	m := &CacheManager{
		dir:      defaultCacheDir(cacheDir),
		capacity: capacity,
		lru:      lru.New(0),
	}
	m.lru.OnEvicted = func(key lru.Key, value any) {
		k, _ := key.(string)
		e, _ := value.(*cacheEntry)
		m.evicted = append(m.evicted, evictedEntry{key: k, entry: e})
	}
	return m
}

// prepare reconciles tracked state with the cache directory (adopting
// pre-existing entries and removing orphaned .partial files) and evicts any
// overage before a download starts.
func (m *CacheManager) prepare() {
	m.evaluate()
}

// acquire records one use of the cache file at path and refreshes its LRU
// position, adding it if not yet tracked.
func (m *CacheManager) acquire(path string, size int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v, ok := m.lru.Get(path); ok {
		v.(*cacheEntry).refs++
		return
	}
	m.lru.Add(path, &cacheEntry{size: size, refs: 1})
	m.total += size
}

// release drops one use of path and refreshes its LRU position.
func (m *CacheManager) release(path string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v, ok := m.lru.Get(path); ok {
		if e := v.(*cacheEntry); e.refs > 0 {
			e.refs--
		}
	}
}

// evaluate reconciles tracked state with the cache directory, then evicts
// least-recently-used entries until the total fits the capacity. Reconciling
// first keeps the bound directory-level even when other managers or
// processes write to the same directory.
func (m *CacheManager) evaluate() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reconcileLocked()
	m.evaluateLocked()
}

func (m *CacheManager) evaluateLocked() {
	if m.capacity <= 0 {
		return
	}
	var skipped []evictedEntry
	for m.total > m.capacity && m.lru.Len() > 0 {
		m.lru.RemoveOldest()
		if len(m.evicted) == 0 {
			break
		}
		batch := m.evicted
		m.evicted = nil
		for _, ev := range batch {
			if ev.entry == nil {
				continue
			}
			// Entries in use here or locked by another process are kept and
			// become most recently used.
			if ev.entry.refs > 0 || !removeCacheEntryFiles(ev.key) {
				skipped = append(skipped, ev)
				continue
			}
			m.total -= ev.entry.size
		}
	}
	for _, s := range skipped {
		m.lru.Add(s.key, s.entry)
	}
}

// reconcileLocked re-reads the cache directory so the tracked state matches
// disk: entries created by other managers or processes are adopted, entries
// removed elsewhere are dropped, sizes are refreshed, and orphaned .partial
// files left behind by crashed downloads are cleaned up. The recency order
// and refs of already-tracked entries are preserved.
func (m *CacheManager) reconcileLocked() {
	type diskEntry struct {
		path    string
		size    int64
		modTime time.Time
	}
	var found []diskEntry

	prefixes, err := os.ReadDir(m.dir)
	if err != nil && !os.IsNotExist(err) {
		return
	}
	for _, prefix := range prefixes {
		if !prefix.IsDir() {
			continue
		}
		hashDirs, err := os.ReadDir(filepath.Join(m.dir, prefix.Name()))
		if err != nil {
			continue
		}
		for _, hd := range hashDirs {
			if !hd.IsDir() {
				continue
			}
			dir := filepath.Join(m.dir, prefix.Name(), hd.Name())
			entries, err := os.ReadDir(dir)
			if err != nil {
				continue
			}
			published := make(map[string]bool)
			var partials []string
			for _, de := range entries {
				name := de.Name()
				if de.IsDir() {
					continue
				}
				if strings.HasSuffix(name, ".partial") {
					partials = append(partials, name)
					continue
				}
				if _, _, _, _, _, ok := parseCacheFileName(name); !ok {
					continue
				}
				info, err := de.Info()
				if err != nil {
					continue
				}
				published[name[:strings.LastIndex(name, "_")]] = true
				// Account the real on-disk size; a corrupt entry whose size
				// does not match its name must not inflate the total.
				found = append(found, diskEntry{
					path:    filepath.Join(dir, name),
					size:    info.Size(),
					modTime: info.ModTime(),
				})
			}
			for _, name := range partials {
				if published[strings.TrimSuffix(name, ".partial")] {
					continue
				}
				removeOrphanPartial(filepath.Join(dir, name))
			}
		}
	}

	onDisk := make(map[string]int64, len(found))
	for _, de := range found {
		onDisk[de.path] = de.size
	}

	// Rebuild the LRU by popping everything oldest-first and re-adding the
	// entries whose names are still on disk, preserving their recency order.
	m.evicted = nil
	for m.lru.Len() > 0 {
		m.lru.RemoveOldest()
	}
	tracked := m.evicted
	m.evicted = nil
	m.total = 0
	kept := make(map[string]bool, len(tracked))
	for _, ev := range tracked {
		if ev.entry == nil {
			continue
		}
		size, ok := onDisk[ev.key]
		if !ok {
			// The name vanished (evicted by another process); open handles
			// keep the data alive but it no longer occupies the directory.
			continue
		}
		ev.entry.size = size
		m.lru.Add(ev.key, ev.entry)
		m.total += size
		kept[ev.key] = true
	}

	// Adopt entries this manager has not seen yet, oldest first so they are
	// evicted in modification-time order.
	sort.Slice(found, func(i, j int) bool { return found[i].modTime.Before(found[j].modTime) })
	for _, de := range found {
		if kept[de.path] {
			continue
		}
		m.lru.Add(de.path, &cacheEntry{size: de.size})
		m.total += de.size
	}
}

// cachePartialPathFor returns the .partial flock target for a published cache
// file path by stripping the trailing _<fileSize> component.
func cachePartialPathFor(finalPath string) (string, bool) {
	name := filepath.Base(finalPath)
	idx := strings.LastIndex(name, "_")
	if idx <= 0 {
		return "", false
	}
	return filepath.Join(filepath.Dir(finalPath), name[:idx]+".partial"), true
}

// removeCacheEntryFiles unlinks a published cache file together with its
// .partial flock target (both names share one inode). Names are only removed
// while holding the entry's flock, so a writer that is mid-publish is never
// disturbed. Returns false when the entry is in use elsewhere and must be
// kept.
func removeCacheEntryFiles(finalPath string) bool {
	partialPath, ok := cachePartialPathFor(finalPath)
	if !ok {
		return false
	}
	lockFile, err := os.OpenFile(partialPath, os.O_RDWR, 0)
	if err != nil {
		if !os.IsNotExist(err) {
			return false
		}
		// No flock target left, so no writer can be active on this entry.
		if err := os.Remove(finalPath); err != nil && !os.IsNotExist(err) {
			return false
		}
		removeEmptyCacheDirs(finalPath)
		return true
	}
	defer lockFile.Close()
	if err := flock.TryLock(lockFile); err != nil {
		return false
	}
	defer flock.Unlock(lockFile) //nolint:errcheck
	// Only remove names while the locked handle still backs partialPath;
	// otherwise another process already recycled this entry.
	pathInfo, statErr := os.Stat(partialPath)
	fileInfo, fstatErr := lockFile.Stat()
	if statErr != nil || fstatErr != nil || !os.SameFile(pathInfo, fileInfo) {
		return false
	}
	if err := os.Remove(finalPath); err != nil && !os.IsNotExist(err) {
		return false
	}
	os.Remove(partialPath) //nolint:errcheck
	removeEmptyCacheDirs(finalPath)
	return true
}

// removeOrphanPartial removes a .partial file that has no published sibling,
// but only if no writer holds its flock.
func removeOrphanPartial(path string) {
	lockFile, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return
	}
	defer lockFile.Close()
	if err := flock.TryLock(lockFile); err != nil {
		return
	}
	defer flock.Unlock(lockFile) //nolint:errcheck
	pathInfo, statErr := os.Stat(path)
	fileInfo, fstatErr := lockFile.Stat()
	if statErr != nil || fstatErr != nil || !os.SameFile(pathInfo, fileInfo) {
		return
	}
	os.Remove(path) //nolint:errcheck
	removeEmptyCacheDirs(path)
}

// removeEmptyCacheDirs opportunistically removes the hash directory and its
// two-character parent once they become empty. Removal fails harmlessly while
// they still contain entries.
func removeEmptyCacheDirs(path string) {
	hashDir := filepath.Dir(path)
	if os.Remove(hashDir) != nil {
		return
	}
	os.Remove(filepath.Dir(hashDir)) //nolint:errcheck
}
