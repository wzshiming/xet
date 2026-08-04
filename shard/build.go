package shard

import (
	"sort"

	"github.com/wzshiming/xet"
)

// FileInfo contains per-file information needed to build a Shard.
type FileInfo struct {
	Hash         xet.FileHash
	SHA256       [32]byte // zero value means not available
	ChunkIndices []int    // indices into the chunks slice passed to BuildShard
}

// ChunkInfo contains the per-chunk information needed to build a Shard.
type ChunkInfo struct {
	Hash       xet.ChunkHash
	Size       uint32
	IsNew      bool
	XorbHash   xet.XorbHash
	ChunkIndex uint32
}

// BuildShard constructs a Shard from the provided file and chunk information.
func BuildShard(files []FileInfo, chunks []ChunkInfo) *Shard {
	sh := NewShard()

	// Build file blocks.
	for _, file := range files {
		fileChunkHashes := make([]xet.ChunkHash, len(file.ChunkIndices))
		for i, idx := range file.ChunkIndices {
			fileChunkHashes[i] = chunks[idx].Hash
		}

		fileBlock := FileBlock{
			FileHash:     file.Hash,
			Flags:        FileWithVerification,
			Entries:      make([]FileDataSequenceEntry, 0),
			Verification: make([]xet.VerificationHash, 0),
		}
		if file.SHA256 != ([32]byte{}) {
			fileBlock.MetadataExt = &FileMetadataExt{
				SHA256Hash: NewSHA256Hash(file.SHA256),
			}
			fileBlock.Flags |= FileWithMetadataExt
		}

		// Group consecutive chunks by xorb into terms.
		type term struct {
			xorbHash   xet.XorbHash
			startIndex uint32
			endIndex   uint32 // exclusive, tracks next expected chunk index
			bytes      uint32
			chunkStart int
			chunkEnd   int
		}
		var terms []term
		var cur term
		for i, idx := range file.ChunkIndices {
			chunk := chunks[idx]
			if i == 0 || chunk.XorbHash != cur.xorbHash || chunk.ChunkIndex != cur.endIndex {
				if i > 0 {
					cur.chunkEnd = i
					terms = append(terms, cur)
				}
				cur = term{
					xorbHash:   chunk.XorbHash,
					startIndex: chunk.ChunkIndex,
					endIndex:   chunk.ChunkIndex + 1,
					bytes:      chunk.Size,
					chunkStart: i,
				}
			} else {
				cur.endIndex++
				cur.bytes += chunk.Size
			}
		}
		if len(file.ChunkIndices) > 0 {
			cur.chunkEnd = len(file.ChunkIndices)
			terms = append(terms, cur)
		}

		for _, t := range terms {
			fileBlock.Entries = append(fileBlock.Entries, FileDataSequenceEntry{
				CASHash:          t.xorbHash,
				UnpackedSegBytes: t.bytes,
				ChunkIndexStart:  t.startIndex,
				ChunkIndexEnd:    t.endIndex,
			})
			fileBlock.Verification = append(fileBlock.Verification,
				xet.ComputeVerificationHash(fileChunkHashes[t.chunkStart:t.chunkEnd]))
		}

		sh.Files = append(sh.Files, fileBlock)
	}

	// Build CAS blocks.
	casBlocks := make(map[xet.XorbHash]*CASBlock)
	seen := make(map[xet.ChunkHash]bool)
	for _, chunk := range chunks {
		if !chunk.IsNew || seen[chunk.Hash] {
			continue
		}
		seen[chunk.Hash] = true
		cb, ok := casBlocks[chunk.XorbHash]
		if !ok {
			cb = &CASBlock{CASHash: chunk.XorbHash}
			casBlocks[chunk.XorbHash] = cb
		}
		cb.Chunks = append(cb.Chunks, CASChunkSequenceEntry{
			ChunkHash:        chunk.Hash,
			ByteRangeStart:   cb.NumBytesInCAS,
			UnpackedSegBytes: chunk.Size,
		})
		cb.NumBytesInCAS += chunk.Size
	}

	for _, cb := range casBlocks {
		sh.CASInfos = append(sh.CASInfos, *cb)
	}

	// Sort CAS blocks by hash for a deterministic shard layout that matches
	// the reference implementation (xet-go).
	sort.Slice(sh.CASInfos, func(i, j int) bool {
		return sh.CASInfos[i].CASHash.String() < sh.CASInfos[j].CASHash.String()
	})

	return sh
}
