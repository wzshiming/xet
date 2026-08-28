package download

import (
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/wzshiming/xet/internal/flock"
)

// mergeCandidate is one sealed cache entry considered for compaction.
type mergeCandidate struct {
	path       string
	start, end uint32
	bytesStart int64
	bytesEnd   int64
}

// cachedChunkReader feeds already-cached chunks to a chunkCache writer, one
// chunk per Read call, matching the decoder contract of load.
type cachedChunkReader struct {
	src   *chunkCache
	idx   uint32
	count uint32
}

func (r *cachedChunkReader) Read(p []byte) (int, error) {
	if r.idx >= r.count {
		return 0, io.EOF
	}
	n, err := r.src.Chunk(r.idx, p)
	if err != nil {
		return 0, err
	}
	r.idx++
	return int(n), nil
}

// mergeHashDir compacts the cache directory of one xorb hash: runs of sealed
// entries whose chunk ranges touch or overlap are rewritten as one covering
// entry and the now-redundant sources are unlinked. Entries vanishing
// mid-pass are left to normal eviction; sources kept because they are in use
// or locked elsewhere re-arm the merge hint so a later pass retries.
func mergeHashDir(m *CacheManager, hash string) error {
	if len(hash) < 2 {
		return nil
	}
	hashDir := filepath.Join(m.dir, hash[:2], hash[2:])
	files, err := os.ReadDir(hashDir)
	if err != nil {
		if os.IsNotExist(err) || os.IsPermission(err) {
			return nil
		}
		return err
	}

	var cands []mergeCandidate
	for _, de := range files {
		if de.IsDir() || strings.HasPrefix(de.Name(), ".") {
			continue
		}
		cs, ce, bs, be, ok := parseCacheFileName(de.Name())
		if !ok {
			continue
		}
		info, err := de.Info()
		if err != nil {
			continue
		}
		path := filepath.Join(hashDir, de.Name())
		if !isCompleteCacheFile(path, cs, ce, info.Size()) {
			continue
		}
		cands = append(cands, mergeCandidate{path: path, start: cs, end: ce, bytesStart: bs, bytesEnd: be})
	}
	if len(cands) < 2 {
		return nil
	}

	sort.Slice(cands, func(i, j int) bool {
		if cands[i].start != cands[j].start {
			return cands[i].start < cands[j].start
		}
		return cands[i].end > cands[j].end
	})

	// Walk maximal runs of touching or overlapping chunk ranges.
	var firstErr error
	retry := false
	for i := 0; i < len(cands); {
		j := i + 1
		end := cands[i].end
		for j < len(cands) && cands[j].start <= end {
			end = max(end, cands[j].end)
			j++
		}
		if j-i >= 2 {
			kept, err := mergeRun(m, hash, cands[i:j], cands[i].start, end)
			if err != nil && firstErr == nil {
				firstErr = err
			}
			retry = retry || kept
		}
		i = j
	}
	if retry {
		// Readers may pin a source across this pass and never hint again
		// once the covering entry serves them; retry after another quiet
		// period so the directory still converges to one entry.
		m.noteMergeCandidate(hash)
	}
	return firstErr
}

// mergeRun replaces the entries in run with one entry covering
// [chunkStart, chunkEnd), reusing an existing covering entry when present.
// kept reports that a redundant source had to be left behind because it is
// still in use or locked elsewhere.
func mergeRun(m *CacheManager, hash string, run []mergeCandidate, chunkStart, chunkEnd uint32) (kept bool, err error) {
	bytesStart := run[0].bytesStart
	bytesEnd := run[0].bytesEnd
	covering := ""
	for _, c := range run {
		bytesStart = min(bytesStart, c.bytesStart)
		bytesEnd = max(bytesEnd, c.bytesEnd)
		if covering == "" && c.start == chunkStart && c.end == chunkEnd {
			covering = c.path
		}
	}
	if covering == "" {
		covering, err = writeMergedEntry(m, hash, chunkStart, chunkEnd, bytesStart, bytesEnd)
		if err != nil || covering == "" {
			return false, err
		}
	}
	for _, c := range run {
		if c.path == covering {
			continue
		}
		if !m.forgetIfIdle(c.path) {
			kept = true
		}
	}
	return kept, nil
}

// writeMergedEntry rewrites the already-cached chunks [chunkStart, chunkEnd)
// as one sealed entry and returns its path, or "" when the range can no
// longer be assembled because a source was evicted mid-pass.
func writeMergedEntry(m *CacheManager, hash string, chunkStart, chunkEnd uint32, bytesStart, bytesEnd int64) (string, error) {
	lockFile, err := lockChunkCache(m.dir, hash, chunkStart, chunkEnd, bytesStart, bytesEnd)
	if err != nil {
		return "", err
	}
	path := cacheFilePath(m.dir, hash, chunkStart, chunkEnd, bytesStart, bytesEnd)

	// A contender may have sealed the same merged entry while this one
	// waited on the lock.
	if info, statErr := os.Stat(path); statErr == nil && isCompleteCacheFile(path, chunkStart, chunkEnd, info.Size()) {
		flock.Unlock(lockFile) //nolint:errcheck
		lockFile.Close()
		return path, nil
	}

	source, err := openCachedRange(m, hash, chunkStart, chunkEnd)
	if err != nil || source == nil {
		// Discard the placeholder created by lockChunkCache; the held flock
		// keeps eviction and other writers away during removal.
		os.Remove(path) //nolint:errcheck
		removeEmptyCacheDirs(path)
		flock.Unlock(lockFile) //nolint:errcheck
		lockFile.Close()
		return "", err
	}

	dec := &cachedChunkReader{src: source, count: chunkEnd - chunkStart}
	merged, err := newLockedChunkCache(dec, m, hash, chunkStart, chunkEnd, bytesStart, bytesEnd, lockFile)
	if err != nil {
		source.Done()
		flock.Unlock(lockFile) //nolint:errcheck
		lockFile.Close()
		return "", err
	}
	err = merged.LoadAll()
	merged.Done()
	source.Done()
	if err != nil {
		return "", err
	}
	return path, nil
}
