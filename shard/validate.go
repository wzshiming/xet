package shard

import (
	"fmt"
)

// Validate checks the structural integrity of the shard.
// CAS blocks present in the shard are fully validated. File entries that
// reference a CAS block not included in the shard (e.g. already on the
// server via deduplication) are skipped rather than treated as errors.
func (s *Shard) Validate() error {
	const allowedChunkFlags = ChunkGlobalDedupEligible
	const allowedFileFlags = FileWithVerification | FileWithMetadataExt

	// Build a lookup from CASHash -> CASBlock index.
	casIndex := make(map[[32]byte]int, len(s.CASInfos))
	for i, cb := range s.CASInfos {
		if _, dup := casIndex[cb.CASHash]; dup {
			return fmt.Errorf("CAS block %d: duplicate CAS hash %x", i, cb.CASHash)
		}
		casIndex[cb.CASHash] = i
	}

	// Validate each CAS block.
	for i, cb := range s.CASInfos {
		if cb.CASFlags != 0 {
			return fmt.Errorf("CAS block %d: non-zero reserved CASFlags %d", i, cb.CASFlags)
		}
		var byteOffset uint32
		for j, chunk := range cb.Chunks {
			if chunk.Flags&^allowedChunkFlags != 0 {
				return fmt.Errorf("CAS block %d chunk %d: unknown chunk flags %d", i, j, chunk.Flags)
			}
			if chunk.ByteRangeStart != byteOffset {
				return fmt.Errorf("CAS block %d chunk %d: ByteRangeStart %d, expected %d",
					i, j, chunk.ByteRangeStart, byteOffset)
			}
			byteOffset += chunk.UnpackedSegBytes
		}
		if byteOffset != cb.NumBytesInCAS {
			return fmt.Errorf("CAS block %d: NumBytesInCAS %d != sum of chunk sizes %d",
				i, cb.NumBytesInCAS, byteOffset)
		}
	}

	// Validate each file block.
	for i, fb := range s.Files {
		if fb.Flags&^allowedFileFlags != 0 {
			return fmt.Errorf("file block %d: unknown flags %d", i, fb.Flags)
		}
		if fb.Flags&FileWithVerification != 0 {
			if len(fb.Verification) != len(fb.Entries) {
				return fmt.Errorf("file block %d: verification entry count %d != entry count %d",
					i, len(fb.Verification), len(fb.Entries))
			}
		} else if len(fb.Verification) != 0 {
			return fmt.Errorf("file block %d: verification entries present without flag", i)
		}
		if fb.Flags&FileWithMetadataExt != 0 {
			if fb.MetadataExt == nil {
				return fmt.Errorf("file block %d: FileWithMetadataExt set but MetadataExt is nil", i)
			}
		} else if fb.MetadataExt != nil {
			return fmt.Errorf("file block %d: MetadataExt present without flag", i)
		}

		for j, entry := range fb.Entries {
			if entry.CASFlags != 0 {
				return fmt.Errorf("file block %d entry %d: non-zero reserved CASFlags %d", i, j, entry.CASFlags)
			}
			if entry.ChunkIndexEnd <= entry.ChunkIndexStart {
				return fmt.Errorf("file block %d entry %d: invalid chunk index range [%d, %d)",
					i, j, entry.ChunkIndexStart, entry.ChunkIndexEnd)
			}
			casIdx, ok := casIndex[entry.CASHash]
			if !ok {
				// CAS block not in this shard (already on server) — skip.
				continue
			}
			cb := &s.CASInfos[casIdx]
			if entry.ChunkIndexEnd > uint32(len(cb.Chunks)) {
				return fmt.Errorf("file block %d entry %d: ChunkIndexEnd %d out of range (CAS block has %d chunks)",
					i, j, entry.ChunkIndexEnd, len(cb.Chunks))
			}
			var termBytes uint32
			for k := entry.ChunkIndexStart; k < entry.ChunkIndexEnd; k++ {
				termBytes += cb.Chunks[k].UnpackedSegBytes
			}
			if termBytes != entry.UnpackedSegBytes {
				return fmt.Errorf("file block %d entry %d: UnpackedSegBytes %d != sum of chunk sizes %d",
					i, j, entry.UnpackedSegBytes, termBytes)
			}
		}
	}

	return nil
}
