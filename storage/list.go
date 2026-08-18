package storage

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"
)

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
func ListFiles(ctx context.Context, st SweepStore) ([]*FileListEntry, error) {
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
	err = st.WalkFileIndex(ctx, func(fileHash, shardHash string, _ time.Time) error {
		filesByShard[shardHash] = append(filesByShard[shardHash], fileHash)
		return nil
	})
	if err != nil {
		return nil, err
	}

	type fileInfo struct {
		sha256 string
		size   int64
		found  bool
	}
	infos := map[string]fileInfo{}
	for shardHash, fileHashes := range filesByShard {
		sh, err := st.GetShardByHash(ctx, shardHash)
		if err != nil && !isNotExist(err) {
			return nil, fmt.Errorf("load shard %s: %w", shardHash, err)
		}
		for _, fileHash := range fileHashes {
			info := fileInfo{sha256: sha256ByFile[fileHash]}
			if sh != nil {
				for i := range sh.Files {
					fb := &sh.Files[i]
					if fb.FileHash.String() != fileHash {
						continue
					}
					info.found = true
					for _, entry := range fb.Entries {
						info.size += int64(entry.UnpackedSegBytes)
					}
					if fb.MetadataExt != nil {
						info.sha256 = fb.MetadataExt.SHA256Hash.String()
					}
					break
				}
			}
			infos[fileHash] = info
		}
	}

	bySHA256 := map[string]*FileListEntry{}
	entries := make([]*FileListEntry, 0, len(infos))
	for fileHash, info := range infos {
		entry := bySHA256[info.sha256]
		if entry == nil {
			entry = &FileListEntry{SHA256: info.sha256, Missing: true}
			entries = append(entries, entry)
			if info.sha256 != "" {
				bySHA256[info.sha256] = entry
			}
		}
		entry.FileHashes = append(entry.FileHashes, fileHash)
		if info.found && entry.Missing {
			entry.Missing = false
			entry.Size = info.size
		}
	}

	for _, entry := range entries {
		slices.Sort(entry.FileHashes)
	}
	slices.SortFunc(entries, func(a, b *FileListEntry) int {
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
