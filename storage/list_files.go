package storage

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	iofs "io/fs"
	"slices"
	"sync"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/xorb"
	"golang.org/x/sync/errgroup"
)

// FileListEntry describes one stored content: every xet file hash that
// reconstructs the same bytes, keyed by the SHA-256 recorded at ingest.
type FileListEntry struct {
	// SHA256 is the hex content digest; empty for empty files (stored with
	// an all-zero marker) and for entries whose owning shard is missing.
	SHA256 string `json:"sha256,omitempty"`
	// FileHashes are the xet file hashes resolving to this content; one
	// SHA-256 can map to several since the xet hash depends on chunking.
	FileHashes []string `json:"file_hashes"`
	// OriginalSize is the original file size in bytes, counted once per entry.
	OriginalSize uint64 `json:"original_size"`
	// UniqueSize is the stored (compressed) bytes referenced only by this
	// entry, i.e. what deleting it would free: exact per-chunk packed sizes
	// including chunk headers. Per-xorb footer bytes are not attributed and
	// chunks whose xorb has vanished contribute nothing.
	UniqueSize uint64 `json:"unique_size"`
	// SharedSize is the stored bytes this entry shares with other entries;
	// overlapping bytes are attributed to every sharing entry.
	SharedSize uint64 `json:"shared_size"`
	// Missing marks entries that no longer resolve to stored data: the
	// owning shard is gone or its chunk metadata is invalid.
	Missing bool `json:"missing,omitempty"`
}

// ListStore is the thin per-backend surface needed to enumerate stored
// files; the aggregation lives in ListFiles.
type ListStore interface {
	// WalkFileIndex calls fn for every index/files entry, passing the hex
	// file hash and the owning shard hash.
	WalkFileIndex(ctx context.Context, fn func(fileHash, shardHash string) error) error

	// GetShardByHash loads a stored shard by the hash of its serialized
	// bytes; the error wraps fs.ErrNotExist when the shard is absent.
	GetShardByHash(ctx context.Context, shardHash string) (*shard.Shard, error)

	// GetXorbChunkOffsets returns the xorb's chunk offset table: the
	// cumulative packed end-offset of every chunk in the stored xorb.
	GetXorbChunkOffsets(ctx context.Context, xorbHash xet.XorbHash) ([]uint64, error)
}

// listFilesConcurrency bounds parallel shard and xorb offset-table reads
// while building the listing.
const listFilesConcurrency = 4

// chunkRange is one reconstruction term: chunks [start, end) of one xorb.
type chunkRange struct {
	hash       xet.XorbHash
	start, end uint32
}

// ListFiles enumerates every file recorded in the file index, grouped by
// content: one entry per SHA-256 carrying all file hashes that map to it.
// The result is deterministically sorted and never nil.
func ListFiles(ctx context.Context, st ListStore) ([]FileListEntry, error) {
	// Group by shard first so each shard is loaded exactly once and can be
	// released after its group; only the flattened terms are retained.
	byShard := map[string][]string{} // shard hash -> file hashes
	err := st.WalkFileIndex(ctx, func(fileHash, shardHash string) error {
		byShard[shardHash] = append(byShard[shardHash], fileHash)
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Resolve shard groups in parallel; the SHA-256 grouping below stays
	// sequential so it needs no locks.
	shardHashes := make([]string, 0, len(byShard))
	for shardHash := range byShard {
		shardHashes = append(shardHashes, shardHash)
	}
	resolved := make([][]resolvedFile, len(shardHashes))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(listFilesConcurrency)
	for i, shardHash := range shardHashes {
		g.Go(func() error {
			files, err := resolveShardFiles(gctx, st, shardHash, byShard[shardHash])
			if err != nil {
				return err
			}
			resolved[i] = files
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	entries := []FileListEntry{}
	var entryTerms [][]chunkRange // parallel to entries
	bySHA256 := map[string]int{}  // sha256 -> index in entries
	for _, files := range resolved {
		for _, rf := range files {
			if rf.missing {
				entries = append(entries, FileListEntry{FileHashes: []string{rf.fileHash}, Missing: true})
				entryTerms = append(entryTerms, nil)
				continue
			}
			if rf.sha == "" {
				entries = append(entries, FileListEntry{FileHashes: []string{rf.fileHash}, OriginalSize: rf.size})
				entryTerms = append(entryTerms, rf.terms)
				continue
			}
			if i, ok := bySHA256[rf.sha]; ok {
				entries[i].FileHashes = append(entries[i].FileHashes, rf.fileHash)
				entryTerms[i] = append(entryTerms[i], rf.terms...)
				continue
			}
			bySHA256[rf.sha] = len(entries)
			entries = append(entries, FileListEntry{SHA256: rf.sha, FileHashes: []string{rf.fileHash}, OriginalSize: rf.size})
			entryTerms = append(entryTerms, rf.terms)
		}
	}

	if err := computeStoredSizes(ctx, st, entries, entryTerms); err != nil {
		return nil, err
	}

	for i := range entries {
		slices.Sort(entries[i].FileHashes)
	}
	slices.SortFunc(entries, func(a, b FileListEntry) int {
		if c := cmp.Compare(b.OriginalSize, a.OriginalSize); c != 0 {
			return c
		}
		if c := cmp.Compare(a.SHA256, b.SHA256); c != 0 {
			return c
		}
		return cmp.Compare(a.FileHashes[0], b.FileHashes[0])
	})
	return entries, nil
}

// resolvedFile is one file-index entry resolved against its owning shard:
// the flattened reconstruction terms plus the grouping metadata.
type resolvedFile struct {
	fileHash string
	sha      string
	size     uint64
	terms    []chunkRange
	missing  bool
}

// resolveShardFiles loads one shard and resolves its file-index entries; a
// missing shard is not an error, its files come back marked missing, as do
// files whose chunk metadata is out of bounds.
func resolveShardFiles(ctx context.Context, st ListStore, shardHash string, fileHashes []string) ([]resolvedFile, error) {
	sh, err := st.GetShardByHash(ctx, shardHash)
	if err != nil {
		if !errors.Is(err, iofs.ErrNotExist) {
			return nil, err
		}
		sh = nil
	}
	var blocks map[xet.FileHash]*shard.FileBlock
	if sh != nil {
		blocks = make(map[xet.FileHash]*shard.FileBlock, len(sh.Files))
		for i := range sh.Files {
			blocks[sh.Files[i].FileHash] = &sh.Files[i]
		}
	}
	files := make([]resolvedFile, 0, len(fileHashes))
	for _, fileHash := range fileHashes {
		var file *shard.FileBlock
		if want, err := xet.ParseFileHash(fileHash); err == nil {
			file = blocks[want]
		}
		if file == nil {
			files = append(files, resolvedFile{fileHash: fileHash, missing: true})
			continue
		}
		rf := resolvedFile{fileHash: fileHash, terms: make([]chunkRange, 0, len(file.Entries))}
		valid := true
		for _, entry := range file.Entries {
			// Dedup terms escape shard-ingest validation, so bound the chunk
			// index here before it sizes per-chunk usage arrays.
			if entry.ChunkIndexEnd > xet.MaxChunksPerXorb {
				valid = false
				break
			}
			rf.size += uint64(entry.UnpackedSegBytes)
			if entry.ChunkIndexEnd > entry.ChunkIndexStart {
				rf.terms = append(rf.terms, chunkRange{hash: entry.CASHash, start: entry.ChunkIndexStart, end: entry.ChunkIndexEnd})
			}
		}
		if !valid {
			files = append(files, resolvedFile{fileHash: fileHash, missing: true})
			continue
		}
		// The all-zero digest is the empty-file marker, not a real SHA-256,
		// so those entries stay ungrouped.
		if file.MetadataExt != nil && file.MetadataExt.SHA256Hash != (shard.SHA256Hash{}) {
			rf.sha = file.MetadataExt.SHA256Hash.String()
		}
		files = append(files, rf)
	}
	return files, nil
}

// xorbUsage tracks, per chunk of one xorb, how many entries reference it and
// the last entry counted (for per-entry dedup).
type xorbUsage struct {
	distinct []uint32
	last     []int32
}

func (u *xorbUsage) grow(n int) {
	for len(u.last) < n {
		u.distinct = append(u.distinct, 0)
		u.last = append(u.last, -1)
	}
}

// computeStoredSizes fills UniqueSize and SharedSize on every entry with the
// exact stored bytes of each referenced chunk. Every referenced xorb's chunk
// offset table is fetched once, in parallel (the same cached table
// reconstruction uses; footer-less xorbs cost a one-time header scan), then
// all range math is local. Chunk reference counts across entries decide
// unique vs shared; consecutive chunks with the same classification are
// coalesced into one range. Chunks whose xorb is gone or inconsistent with
// the shard contribute nothing.
func computeStoredSizes(ctx context.Context, st ListStore, entries []FileListEntry, entryTerms [][]chunkRange) error {
	usage := map[xet.XorbHash]*xorbUsage{}
	for e := range entries {
		for _, term := range entryTerms[e] {
			u := usage[term.hash]
			if u == nil {
				u = &xorbUsage{}
				usage[term.hash] = u
			}
			u.grow(int(term.end))
			for k := term.start; k < term.end; k++ {
				if u.last[k] != int32(e) {
					u.last[k] = int32(e)
					u.distinct[k]++
				}
			}
		}
	}
	// Fetch every referenced xorb's offset table once, in parallel.
	offsets := make(map[xet.XorbHash][]uint64, len(usage))
	var offsetsMut sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(listFilesConcurrency)
	for hash := range usage {
		g.Go(func() error {
			offs, err := st.GetXorbChunkOffsets(gctx, hash)
			if err != nil {
				// A vanished xorb has no stored bytes to attribute.
				if errors.Is(err, iofs.ErrNotExist) {
					return nil
				}
				return fmt.Errorf("chunk offsets of xorb %s: %w", hash.String(), err)
			}
			offsetsMut.Lock()
			offsets[hash] = offs
			offsetsMut.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	for _, u := range usage {
		for k := range u.last {
			u.last[k] = -1
		}
	}
	for e := range entries {
		var unique, shared uint64
		for _, term := range entryTerms[e] {
			// Skip terms the stored xorb cannot satisfy (vanished or shorter
			// than the shard claims); their stored bytes are unknown.
			if uint64(term.end) > uint64(len(offsets[term.hash])) {
				continue
			}
			u := usage[term.hash]
			for k := term.start; k < term.end; {
				if u.last[k] == int32(e) {
					k++
					continue
				}
				sharedRun := u.distinct[k] > 1
				runEnd := k
				for runEnd < term.end && u.last[runEnd] != int32(e) && (u.distinct[runEnd] > 1) == sharedRun {
					u.last[runEnd] = int32(e)
					runEnd++
				}
				start, end, err := xorb.ChunkDataRangeFromOffsets(offsets[term.hash], k, runEnd)
				if err != nil {
					return fmt.Errorf("stored size of xorb %s chunks [%d,%d): %w", term.hash.String(), k, runEnd, err)
				}
				if sharedRun {
					shared += uint64(end - start + 1)
				} else {
					unique += uint64(end - start + 1)
				}
				k = runEnd
			}
		}
		entries[e].UniqueSize = unique
		entries[e].SharedSize = shared
	}
	return nil
}
