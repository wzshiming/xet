package storage

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/wzshiming/xet/shard"
)

// ListStore is the read-only storage surface ListFiles needs.
type ListStore interface {
	GetShardByHash(ctx context.Context, shardHash string) (*shard.Shard, error)
	WalkFileIndex(ctx context.Context, fn func(fileHash, shardHash string, modTime time.Time) error) error
	WalkSHA256Index(ctx context.Context, fn func(sha256Hex, fileHash string, modTime time.Time) error) error
}

// FileListEntry groups the file hashes sharing one content identity: several
// xet hashes can carry the same SHA-256 when the same bytes were chunked
// differently (e.g. across chunker versions), while the SHA-256 index can
// point at only one of them. The SHA-256 comes from each file's shard
// metadata, falling back to the index entry when the shard is gone.
type FileListEntry struct {
	// SHA256 is empty when no SHA-256 is recorded for the file; files
	// without one are never grouped, each gets its own entry.
	SHA256     string   `json:"sha256,omitempty"`
	FileHashes []string `json:"file_hashes"`
	// Size is the unpacked content size, counted once for the entry.
	Size int64 `json:"size"`
	// Missing marks an entry none of whose file hashes still resolves to a
	// shard holding it, so its size is unknown; a sweep repairs such entries.
	Missing bool `json:"missing,omitempty"`
}

// ListFiles reports every file reachable through the file index, grouped by
// SHA-256 and sorted, joining each entry with its size from the owning shard
// and loading each shard once. It is read-only and takes no GC guard, so a
// listing concurrent with a sweep or with uploads reflects a mid-flight
// state.
func ListFiles(ctx context.Context, st ListStore) ([]FileListEntry, error) {
	// Index fallback for files whose shard is gone; the index holds at most
	// one file hash per SHA-256, so shard metadata is joined first below.
	sha256ByFile := map[string]string{}
	err := st.WalkSHA256Index(ctx, func(sha256Hex, fileHash string, _ time.Time) error {
		sha256ByFile[fileHash] = sha256Hex
		return nil
	})
	if err != nil {
		return nil, err
	}

	filesByShard := map[string][]string{}
	totalFiles := 0
	err = st.WalkFileIndex(ctx, func(fileHash, shardHash string, _ time.Time) error {
		filesByShard[shardHash] = append(filesByShard[shardHash], fileHash)
		totalFiles++
		return nil
	})
	if err != nil {
		return nil, err
	}

	// bySHA256 holds indices into entries; index-based grouping avoids
	// per-entry pointer allocations and a final copy into a value slice.
	entries := make([]FileListEntry, 0, totalFiles)
	bySHA256 := make(map[string]int, totalFiles)
	addFile := func(fileHash, sha256 string, size int64, found bool) {
		idx, ok := bySHA256[sha256]
		if !ok {
			entries = append(entries, FileListEntry{SHA256: sha256, Missing: true})
			idx = len(entries) - 1
			if sha256 != "" {
				bySHA256[sha256] = idx
			}
		}
		entry := &entries[idx]
		entry.FileHashes = append(entry.FileHashes, fileHash)
		if found && entry.Missing {
			entry.Missing = false
			entry.Size = size
		}
	}

	for shardHash, fileHashes := range filesByShard {
		sh, err := st.GetShardByHash(ctx, shardHash)
		if err != nil && !isNotExist(err) {
			return nil, fmt.Errorf("load shard %s: %w", shardHash, err)
		}
		// One pass over the shard, hex-encoding each file hash once;
		// matched hashes are swap-removed from the wanted list.
		remaining := fileHashes
		if sh != nil {
			for i := range sh.Files {
				if len(remaining) == 0 {
					break
				}
				fb := &sh.Files[i]
				fileHash := fb.FileHash.String()
				j := slices.Index(remaining, fileHash)
				if j < 0 {
					continue
				}
				remaining[j] = remaining[len(remaining)-1]
				remaining = remaining[:len(remaining)-1]

				var size int64
				for _, entry := range fb.Entries {
					size += int64(entry.UnpackedSegBytes)
				}
				sha256 := sha256ByFile[fileHash]
				if fb.MetadataExt != nil {
					sha256 = fb.MetadataExt.SHA256Hash.String()
				}
				addFile(fileHash, sha256, size, true)
			}
		}
		for _, fileHash := range remaining {
			addFile(fileHash, sha256ByFile[fileHash], 0, false)
		}
	}

	for i := range entries {
		fileHashs := entries[i].FileHashes
		if len(fileHashs) > 1 {
			slices.Sort(fileHashs)
		}
	}
	slices.SortFunc(entries, func(a, b FileListEntry) int {
		if c := cmp.Compare(a.Size, b.Size); c != 0 {
			return c
		}
		if c := cmp.Compare(a.SHA256, b.SHA256); c != 0 {
			return c
		}
		return cmp.Compare(a.FileHashes[0], b.FileHashes[0])
	})
	return entries, nil
}
