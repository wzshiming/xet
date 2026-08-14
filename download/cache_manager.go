package download

import (
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/golang/groupcache/lru"
	"github.com/wzshiming/xet/internal/flock"
)

// DefaultCacheSize is the default chunk cache capacity in bytes, matching
// xet-core's default chunk cache size (HF_XET_CHUNK_CACHE_SIZE_BYTES).
const DefaultCacheSize int64 = 10_000_000_000

// reconcileInterval is the minimum time between full directory walks on the
// eviction slow path. The walk re-syncs tracked state with entries created
// or removed by other managers and processes; throttling it keeps a cache
// hovering at capacity from re-scanning the tree on every published range.
const reconcileInterval = time.Minute

// evictBackoff pauses eviction after an attempt that could not get back
// under capacity because every candidate was in use or flocked by another
// process. Retrying sooner would drain the whole LRU and take one file lock
// per entry again for nothing; a release that makes an entry evictable
// lifts the pause immediately.
const evictBackoff = time.Second

// mergeDebounce is how long a hash directory must stay quiet after its last
// merge hint before the background merger compacts it; hints from ranges
// still being written keep pushing the work back.
const mergeDebounce = 2 * time.Second

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
// file is acquired and once more when its user is done. Publishing or
// releasing a range is O(1) in-memory bookkeeping while the tracked total
// stays under capacity; eviction only runs once it reaches capacity. The
// full directory walk that keeps the bound directory-wide — adopting entries
// created by other managers or processes and cleaning up crashed leftovers —
// runs once when the manager is first prepared and afterwards only right
// before evicting, at most once per reconcileInterval, never on the
// per-range path.
// Cross-process (and cross-manager) safety relies on the flock on the entry
// file itself: names are only unlinked while holding that lock, so active
// writers are never disturbed, and already-open readers keep their data
// through the open handles even after the names are removed.
type CacheManager struct {
	mu       sync.Mutex
	dir      string
	capacity int64 // bytes; <= 0 disables eviction
	lru      *lru.Cache
	total    int64

	// lastReconcile is when reconcileLocked last walked the directory; the
	// zero value means the initial scan has not happened yet.
	lastReconcile time.Time

	// lastEvictFailed is when evictLocked last ended still over capacity
	// with every candidate pinned or flocked; eviction is paused until
	// evictBackoff elapses or a release frees a candidate.
	lastEvictFailed time.Time

	// evicted accumulates entries popped from the LRU via RemoveOldest;
	// evictLocked drains it for eviction candidates and reconcileLocked
	// uses it to rebuild the LRU.
	evicted []evictedEntry

	// verified remembers published entries whose checksum already passed,
	// keyed by final path, so each entry is verified at most once per
	// manager.
	verified sync.Map

	// mergeQuiet is the per-hash quiet period before compaction runs.
	mergeQuiet time.Duration

	// pendingMerge maps a xorb hash to its last merge hint time; drained by
	// mergeWorker once the hash has been quiet for mergeQuiet.
	pendingMerge map[string]time.Time

	// mergeRunning reports whether a mergeWorker goroutine is active.
	mergeRunning bool
}

// NewCacheManager creates a manager for the chunk cache at cacheDir (empty
// means the default directory). capacity bounds the cache size in bytes;
// zero or negative disables eviction. Callers should create one manager per
// cache directory and pass it to every reader sharing that directory.
func NewCacheManager(cacheDir string, capacity int64) *CacheManager {
	m := &CacheManager{
		dir:        defaultCacheDir(cacheDir),
		capacity:   capacity,
		lru:        lru.New(0),
		mergeQuiet: mergeDebounce,
	}
	m.lru.OnEvicted = func(key lru.Key, value any) {
		k, _ := key.(string)
		e, _ := value.(*cacheEntry)
		m.evicted = append(m.evicted, evictedEntry{key: k, entry: e})
	}
	return m
}

// prepare runs the one-time directory scan that adopts pre-existing entries
// and removes crashed incomplete entries, then evicts any overage before a
// download starts. Calls after the first cost the same as evaluate.
func (m *CacheManager) prepare() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.lastReconcile.IsZero() {
		m.reconcileLocked()
	}
	m.evaluateLocked()
}

// acquire records one use of the cache file at path and refreshes its LRU
// position, adding it if not yet tracked.
func (m *CacheManager) acquire(path string, size int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v, ok := m.lru.Get(path); ok {
		e := v.(*cacheEntry)
		e.refs++
		// The name may back a recreated file; refresh the tracked size.
		m.total += size - e.size
		e.size = size
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
			if e.refs == 0 {
				// A candidate became evictable; lift the eviction pause.
				m.lastEvictFailed = time.Time{}
			}
		}
	}
}

// noteMergeCandidate schedules a background compaction of hash's cache
// directory once it has been quiet for mergeQuiet.
func (m *CacheManager) noteMergeCandidate(hash string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(hash) < 2 {
		return
	}
	if m.pendingMerge == nil {
		m.pendingMerge = make(map[string]time.Time)
	}
	m.pendingMerge[hash] = time.Now()
	if !m.mergeRunning {
		m.mergeRunning = true
		go m.mergeWorker()
	}
}

// mergeWorker drains pendingMerge, compacting each hash directory after it
// has been quiet for mergeQuiet, and exits once nothing is pending.
func (m *CacheManager) mergeWorker() {
	for {
		m.mu.Lock()
		var hash string
		var oldest time.Time
		for h, t := range m.pendingMerge {
			if hash == "" || t.Before(oldest) {
				hash, oldest = h, t
			}
		}
		if hash == "" {
			m.mergeRunning = false
			m.mu.Unlock()
			return
		}
		wait := m.mergeQuiet - time.Since(oldest)
		if wait <= 0 {
			delete(m.pendingMerge, hash)
		}
		m.mu.Unlock()
		if wait > 0 {
			time.Sleep(wait)
			continue
		}
		mergeHashDir(m, hash) //nolint:errcheck
	}
}

// forgetIfIdle unlinks the sealed entry at path and drops it from tracking,
// but only when no in-process user holds a reference; in-use or flocked
// entries are kept for normal eviction. Returns true when the name is gone.
func (m *CacheManager) forgetIfIdle(path string) bool {
	m.mu.Lock()
	if v, ok := m.lru.Get(path); ok {
		if e := v.(*cacheEntry); e.refs > 0 {
			m.mu.Unlock()
			return false
		}
	}
	m.mu.Unlock()
	if !removeCacheEntry(path) {
		return false
	}
	m.mu.Lock()
	if v, ok := m.lru.Get(path); ok {
		e := v.(*cacheEntry)
		m.total -= e.size
		m.lru.Remove(path)
		// Remove fires OnEvicted; drop the bookkeeping entry it queued so
		// the next eviction pass does not double-count this file.
		kept := m.evicted[:0]
		for _, ev := range m.evicted {
			if ev.key != path {
				kept = append(kept, ev)
			}
		}
		m.evicted = kept
	}
	m.verified.Delete(path)
	m.mu.Unlock()
	return true
}

// wasVerified reports whether path already passed checksum verification.
func (m *CacheManager) wasVerified(path string) bool {
	_, ok := m.verified.Load(path)
	return ok
}

// markVerified remembers that path passed checksum verification.
func (m *CacheManager) markVerified(path string) {
	m.verified.Store(path, struct{}{})
}

// evaluate evicts least-recently-used entries when the tracked total has
// reached capacity; below capacity it is O(1) and touches no directory.
func (m *CacheManager) evaluate() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.evaluateLocked()
}

// evaluateLocked is the eviction slow path, entered only once the tracked
// total reaches capacity. Before evicting it reconciles the tracked state
// with the directory — throttled to reconcileInterval — so entries written
// by other managers or processes are counted, stale names are dropped, and
// the bound stays directory-wide.
func (m *CacheManager) evaluateLocked() {
	if m.capacity <= 0 || m.total < m.capacity {
		return
	}
	// The last attempt freed nothing and nothing has changed since; skip
	// the rescan until the backoff elapses or a release lifts the pause.
	if !m.lastEvictFailed.IsZero() && time.Since(m.lastEvictFailed) < evictBackoff {
		return
	}
	if time.Since(m.lastReconcile) >= reconcileInterval {
		m.reconcileLocked()
	}
	m.evictLocked()
}

func (m *CacheManager) evictLocked() {
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
			if ev.entry.refs > 0 || !removeCacheEntry(ev.key) {
				skipped = append(skipped, ev)
				continue
			}
			// A future file reusing this name must be verified afresh.
			m.verified.Delete(ev.key)
			m.total -= ev.entry.size
		}
	}
	for _, s := range skipped {
		m.lru.Add(s.key, s.entry)
	}
	if m.total > m.capacity {
		// Everything left is pinned or flocked elsewhere; pause eviction.
		m.lastEvictFailed = time.Now()
	} else {
		m.lastEvictFailed = time.Time{}
	}
}

// reconcileLocked re-reads the cache directory so the tracked state matches
// disk: entries created by other managers or processes are adopted, entries
// removed elsewhere are dropped, sizes are refreshed, and incomplete entries
// left behind by crashed downloads are cleaned up. The recency order
// and refs of already-tracked entries are preserved.
func (m *CacheManager) reconcileLocked() {
	m.lastReconcile = time.Now()

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
			for _, de := range entries {
				name := de.Name()
				if de.IsDir() {
					continue
				}
				cs, ce, _, _, ok := parseCacheFileName(name)
				if !ok {
					continue
				}
				info, err := de.Info()
				if err != nil {
					continue
				}
				path := filepath.Join(dir, name)
				if !isCompleteCacheFile(path, cs, ce, info.Size()) {
					// Either an active download (protected by its flock) or a
					// crashed leftover; only the latter can be removed.
					removeIncompleteCacheEntry(path, cs, ce)
					continue
				}
				found = append(found, diskEntry{
					path:    path,
					size:    info.Size(),
					modTime: info.ModTime(),
				})
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
			m.verified.Delete(ev.key)
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

// isCompleteCacheFile reports whether the file at path is a sealed cache
// entry for the chunk range in its name.
func isCompleteCacheFile(path string, chunkStart, chunkEnd uint32, size int64) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	_, err = readCacheFileLayout(f, chunkStart, chunkEnd, size)
	return err == nil
}

// removeCacheEntry unlinks a cache entry file. The name is only removed
// while holding the entry's flock, so an active writer is never disturbed.
// Returns false when the entry is in use elsewhere and must be kept.
func removeCacheEntry(path string) bool {
	lockFile, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		if !os.IsNotExist(err) {
			return false
		}
		// The name is already gone (removed by another process).
		removeEmptyCacheDirs(path)
		return true
	}
	defer lockFile.Close()
	if err := flock.TryLock(lockFile); err != nil {
		return false
	}
	defer flock.Unlock(lockFile) //nolint:errcheck
	// Only remove the name while the locked handle still backs it; otherwise
	// another process already recycled this entry.
	pathInfo, statErr := os.Stat(path)
	fileInfo, fstatErr := lockFile.Stat()
	if statErr != nil || fstatErr != nil || !os.SameFile(pathInfo, fileInfo) {
		return false
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return false
	}
	removeEmptyCacheDirs(path)
	return true
}

// removeIncompleteCacheEntry removes a crashed leftover, but only if no
// writer holds its flock and it is still incomplete once the lock is held.
func removeIncompleteCacheEntry(path string, chunkStart, chunkEnd uint32) {
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
	// The writer may have sealed the file between the check and the lock.
	if _, err := readCacheFileLayout(lockFile, chunkStart, chunkEnd, fileInfo.Size()); err == nil {
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
