package upload

import (
	"encoding/hex"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/golang/groupcache/lru"

	"github.com/wzshiming/xet"
)

// DefaultCacheSize is the default capacity of the chunk location cache in
// bytes.
const DefaultCacheSize int64 = 1_000_000_000

// leftoverAge is the age from which a reconcile treats a staged xorb or a
// Store temp file as left behind by a crashed process rather than one still
// being set up.
const leftoverAge = 24 * time.Hour

// reconcileInterval is the minimum time between full directory walks on the
// eviction slow path.
const reconcileInterval = time.Minute

// entryKey identifies one cached chunk location by the endpoint tag and
// chunk hash that name its file.
type entryKey struct {
	tag  [8]byte
	hash xet.ChunkHash
}

// CacheManager owns one directory: xorbs awaiting upload under <dir>/staging
// and cached chunk locations under <dir>/chunks/<endpoint>, bounded through
// an in-memory LRU.
type CacheManager struct {
	mu       sync.Mutex
	dir      string
	capacity int64 // bytes; <= 0 disables tracking and eviction

	// lru holds the tracked entries; Add on a known key refreshes its recency.
	lru *lru.Cache

	// evicted collects the keys popped via RemoveOldest; evictLocked unlinks
	// them and reconcileLocked rebuilds the LRU from them.
	evicted []entryKey

	// lastReconcile is when reconcileLocked last walked the directory; zero
	// means the initial scan has not happened yet.
	lastReconcile time.Time
}

// NewCacheManager creates a manager for cacheDir ("" = the default
// directory); capacity bounds <dir>/chunks in bytes (<= 0 unbounded),
// charging each entry entryCost.
func NewCacheManager(cacheDir string, capacity int64) *CacheManager {
	m := &CacheManager{dir: defaultCacheDir(cacheDir), capacity: capacity, lru: lru.New(0)}
	m.lru.OnEvicted = func(key lru.Key, _ any) { m.evicted = append(m.evicted, key.(entryKey)) }
	return m
}

func defaultCacheDir(cacheDir string) string {
	if cacheDir == "" {
		return filepath.Join(os.TempDir(), "xet-cache", "upload")
	}
	return cacheDir
}

// create opens a new xorb file flat under <dir>/staging.
func (m *CacheManager) create() (*os.File, error) {
	dir := filepath.Join(m.dir, "staging")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return os.CreateTemp(dir, "xorb-*")
}

// prepare runs the one-time directory scan that adopts pre-existing entries
// and removes crashed leftovers, then evicts any overage before an upload
// starts.
func (m *CacheManager) prepare() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.lastReconcile.IsZero() {
		m.reconcileLocked()
	}
	m.evaluateLocked()
}

// evaluateLocked is the eviction slow path, entered once the tracked entries
// reach capacity; it first reconciles with the directory, at most once per
// reconcileInterval, so the bound stays directory-wide.
func (m *CacheManager) evaluateLocked() {
	if m.capacity <= 0 || int64(m.lru.Len())*entryCost < m.capacity {
		return
	}
	if time.Since(m.lastReconcile) >= reconcileInterval {
		m.reconcileLocked()
	}
	m.evictLocked()
}

// evictLocked unlinks the least recently used entries down to 90% of
// capacity, so a manager at the cap is not due for another walk after every
// store.
func (m *CacheManager) evictLocked() {
	if m.capacity <= 0 {
		return
	}
	kept := int(m.capacity / entryCost)
	kept -= kept / 10
	for m.lru.Len() > kept {
		m.lru.RemoveOldest()
	}
	for _, key := range m.evicted {
		path := m.entryPath(key)
		_ = os.Remove(path)
		removeEmptyCacheDirs(path)
	}
	m.evicted = nil
}

// reconcileLocked removes stale staging xorbs and, for a capped manager,
// re-reads chunks/ so the tracked state matches disk: entries from other
// managers are adopted, vanished ones dropped and crashed Stores' temp files
// removed, preserving the recency order of tracked entries.
func (m *CacheManager) reconcileLocked() {
	m.lastReconcile = time.Now()
	now := m.lastReconcile

	staging := filepath.Join(m.dir, "staging")
	if des, err := os.ReadDir(staging); err == nil {
		for _, de := range des {
			if !de.Type().IsRegular() || !strings.HasPrefix(de.Name(), "xorb-") {
				continue
			}
			if info, err := de.Info(); err == nil && now.Sub(info.ModTime()) >= leftoverAge {
				_ = os.Remove(filepath.Join(staging, de.Name()))
			}
		}
	}
	if m.capacity <= 0 {
		return
	}

	type diskEntry struct {
		key     entryKey
		modTime time.Time
	}
	var found []diskEntry
	chunks := filepath.Join(m.dir, "chunks")
	_ = filepath.WalkDir(chunks, func(path string, d fs.DirEntry, err error) error {
		if err != nil || !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		// Only a file at the exact path this manager would write is an entry,
		// and only a temp file in such an entry's directory is a crashed
		// Store's; anything else is foreign.
		rel, err := filepath.Rel(chunks, path)
		if err != nil {
			return nil
		}
		parts := strings.Split(rel, string(filepath.Separator))
		if len(parts) != 4 {
			return nil
		}
		var key entryKey
		tag, err := hex.DecodeString(parts[0])
		if err != nil || len(tag) != len(key.tag) {
			return nil
		}
		copy(key.tag[:], tag)
		if strings.HasSuffix(d.Name(), ".tmp") {
			fanout, err := hex.DecodeString(parts[1] + parts[2])
			if err != nil || len(fanout) != 2 || filepath.Dir(path) != filepath.Join(chunks, hex.EncodeToString(tag), hex.EncodeToString(fanout[:1]), hex.EncodeToString(fanout[1:])) {
				return nil
			}
			if now.Sub(info.ModTime()) >= leftoverAge {
				_ = os.Remove(path)
				removeEmptyCacheDirs(path)
			}
			return nil
		}
		if key.hash, err = xet.ParseChunkHash(parts[1] + parts[2] + parts[3]); err != nil || m.entryPath(key) != path {
			return nil
		}
		found = append(found, diskEntry{key, info.ModTime()})
		return nil
	})

	// Rebuild the LRU: pop everything oldest first, re-add the tracked keys
	// still on disk in that order, then adopt the untracked ones oldest first.
	onDisk := make(map[entryKey]bool, len(found))
	for _, de := range found {
		onDisk[de.key] = true
	}
	m.evicted = nil
	for m.lru.Len() > 0 {
		m.lru.RemoveOldest()
	}
	tracked := m.evicted
	m.evicted = nil
	for _, key := range tracked {
		if onDisk[key] {
			m.lru.Add(key, nil)
			delete(onDisk, key)
		}
	}
	slices.SortFunc(found, func(a, b diskEntry) int { return a.modTime.Compare(b.modTime) })
	for _, de := range found {
		if onDisk[de.key] {
			m.lru.Add(de.key, nil)
		}
	}
}
