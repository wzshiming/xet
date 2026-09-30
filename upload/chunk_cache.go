package upload

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
)

// entrySize is the xorb hash followed by the little-endian chunk index.
const entrySize = 32 + 4

// entryCost charges every entry one filesystem block against the cap.
const entryCost = 4 << 10

// entryWorkers bounds the concurrent entry files one Store writes or one
// Lookup reads.
const entryWorkers = 16

// endpointTag names the per-CAS subdirectory of chunks/: a xorb location is
// only valid on the CAS it was learned from.
func endpointTag(endpoint string) [8]byte {
	sum := sha256.Sum256([]byte(endpoint))
	return [8]byte(sum[:8])
}

// Store caches every location for endpoint.
func (m *CacheManager) Store(endpoint string, locations map[xet.ChunkHash]shard.ChunkLocation) error {
	if m == nil {
		return nil
	}

	tag := endpointTag(endpoint)
	var g errgroup.Group
	g.SetLimit(entryWorkers)
	var mu sync.Mutex
	var stored []entryKey
	for h, loc := range locations {
		g.Go(func() error {
			var entry [entrySize]byte
			copy(entry[:], loc.XorbHash[:])
			binary.LittleEndian.PutUint32(entry[32:], loc.ChunkIndex)
			key := entryKey{tag: tag, hash: h}
			if err := writeFile(m.entryPath(key), entry[:]); err != nil {
				return err
			}
			mu.Lock()
			stored = append(stored, key)
			mu.Unlock()
			return nil
		})
	}
	err := g.Wait()
	m.mu.Lock()
	if m.capacity > 0 {
		for _, key := range stored {
			m.lru.Add(key, nil)
		}
		m.evaluateLocked()
	}
	m.mu.Unlock()
	return err
}

// Lookup returns the locations cached for endpoint among hashes; a hit counts
// as recently used.
func (m *CacheManager) Lookup(ctx context.Context, endpoint string, hashes []xet.ChunkHash) map[xet.ChunkHash]shard.ChunkLocation {
	if m == nil || len(hashes) == 0 || ctx.Err() != nil {
		return nil
	}

	tag := endpointTag(endpoint)
	results := make(map[xet.ChunkHash]shard.ChunkLocation)
	var mu sync.Mutex
	var g errgroup.Group
	g.SetLimit(entryWorkers)
	seen := make(map[xet.ChunkHash]bool, len(hashes))
	for _, h := range hashes {
		if ctx.Err() != nil {
			break
		}
		if seen[h] {
			continue
		}
		seen[h] = true
		g.Go(func() error {
			path := m.entryPath(entryKey{tag: tag, hash: h})
			entry, err := os.ReadFile(path)
			if err != nil {
				return nil
			}
			if len(entry) != entrySize {
				_ = os.Remove(path)
				removeEmptyCacheDirs(path)
				return nil
			}
			mu.Lock()
			results[h] = shard.ChunkLocation{XorbHash: xet.XorbHash(entry[:32]), ChunkIndex: binary.LittleEndian.Uint32(entry[32:])}
			mu.Unlock()
			return nil
		})
	}
	_ = g.Wait()
	m.mu.Lock()
	if m.capacity > 0 {
		for h := range results {
			m.lru.Add(entryKey{tag: tag, hash: h}, nil)
		}
	}
	m.mu.Unlock()
	return results
}

// removeEmptyCacheDirs opportunistically removes the entry's two fanout
// parents once they become empty, never the chunks/<tag> directory. Removal
// stops at the first directory that is missing or not empty.
func removeEmptyCacheDirs(path string) {
	dir := filepath.Dir(path)
	for range 2 {
		if os.Remove(dir) != nil {
			return
		}
		dir = filepath.Dir(dir)
	}
}

// entryPath is key's entry under chunks/<tag>, fanned out like the download
// cache.
func (m *CacheManager) entryPath(key entryKey) string {
	name := key.hash.String()
	return filepath.Join(m.dir, "chunks", hex.EncodeToString(key.tag[:]), name[:2], name[2:4], name[4:])
}

// writeFile publishes data at path through a temp file in the same directory
// so readers never see a partial file.
func writeFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, "*.tmp")
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(f.Name(), path)
	}
	if err != nil {
		_ = os.Remove(f.Name())
	}
	return err
}
