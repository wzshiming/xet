package shard

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/wzshiming/xet"
)

// Decode deserializes a shard from an io.Reader.
// The reader is consumed incrementally without buffering the entire stream.
// If withFooter is true, the shard must contain a footer; if false, it must not.
func (s *Shard) Decode(r io.Reader, withFooter bool) error {
	var buf [48]byte

	// Read 48-byte header
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}
	if !bytes.Equal(buf[15:32], shardMagicSequence[:]) {
		return fmt.Errorf("invalid shard magic sequence")
	}
	version := binary.LittleEndian.Uint64(buf[32:40])
	if version != 2 {
		return fmt.Errorf("unsupported shard version: %d", version)
	}
	s.FooterSize = binary.LittleEndian.Uint64(buf[40:48])
	if withFooter && s.FooterSize == 0 {
		return fmt.Errorf("footer expected but not present")
	}
	if !withFooter && s.FooterSize != 0 {
		return fmt.Errorf("footer unexpected: FooterSize=%d", s.FooterSize)
	}

	// Read file blocks until bookend
	s.Files = make([]FileBlock, 0)
	for {
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return fmt.Errorf("failed to read file section: %w", err)
		}
		if isBookend(&buf) {
			break
		}

		fb := FileBlock{}
		copy(fb.FileHash[:], buf[:32])
		fb.Flags = FileFlags(binary.LittleEndian.Uint32(buf[32:36]))
		numEntries := binary.LittleEndian.Uint32(buf[36:40])
		// buf[40:48] reserved

		fb.Entries = make([]FileDataSequenceEntry, numEntries)
		for i := range numEntries {
			if _, err := io.ReadFull(r, buf[:]); err != nil {
				return fmt.Errorf("failed to read file entry %d: %w", i, err)
			}
			entry := &fb.Entries[i]
			copy(entry.CASHash[:], buf[:32])
			entry.CASFlags = binary.LittleEndian.Uint32(buf[32:36])
			entry.UnpackedSegBytes = binary.LittleEndian.Uint32(buf[36:40])
			entry.ChunkIndexStart = binary.LittleEndian.Uint32(buf[40:44])
			entry.ChunkIndexEnd = binary.LittleEndian.Uint32(buf[44:48])
		}

		if fb.Flags&FileWithVerification != 0 {
			fb.Verification = make([]xet.Hash, numEntries)
			for i := range numEntries {
				if _, err := io.ReadFull(r, buf[:]); err != nil {
					return fmt.Errorf("failed to read verification entry %d: %w", i, err)
				}
				copy(fb.Verification[i][:], buf[:32])
				// buf[32:48] reserved
			}
		}

		if fb.Flags&FileWithMetadataExt != 0 {
			if _, err := io.ReadFull(r, buf[:]); err != nil {
				return fmt.Errorf("failed to read metadata ext: %w", err)
			}
			fb.MetadataExt = &FileMetadataExt{}
			copy(fb.MetadataExt.SHA256Hash[:], buf[:32])
			// buf[32:48] reserved
		}

		s.Files = append(s.Files, fb)
	}

	// Read CAS blocks until bookend
	s.CASInfos = make([]CASBlock, 0)
	for {
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return fmt.Errorf("failed to read CAS section: %w", err)
		}
		if isBookend(&buf) {
			break
		}

		cb := CASBlock{}
		copy(cb.CASHash[:], buf[:32])
		cb.CASFlags = binary.LittleEndian.Uint32(buf[32:36])
		numEntries := binary.LittleEndian.Uint32(buf[36:40])
		cb.NumBytesInCAS = binary.LittleEndian.Uint32(buf[40:44])
		cb.NumBytesOnDisk = binary.LittleEndian.Uint32(buf[44:48])

		cb.Chunks = make([]CASChunkSequenceEntry, numEntries)
		for i := range numEntries {
			if _, err := io.ReadFull(r, buf[:]); err != nil {
				return fmt.Errorf("failed to read chunk entry %d: %w", i, err)
			}
			chunk := &cb.Chunks[i]
			copy(chunk.ChunkHash[:], buf[:32])
			chunk.ByteRangeStart = binary.LittleEndian.Uint32(buf[32:36])
			chunk.UnpackedSegBytes = binary.LittleEndian.Uint32(buf[36:40])
			chunk.Flags = ChunkFlags(binary.LittleEndian.Uint32(buf[40:44]))
			// buf[44:48] reserved
		}

		s.CASInfos = append(s.CASInfos, cb)
	}

	// Read footer if present
	if s.FooterSize > 0 {
		if s.FooterSize < 200 {
			return fmt.Errorf("footer too small: %d", s.FooterSize)
		}

		s.Footer = &Footer{}
		f := s.Footer

		if _, err := io.ReadFull(r, buf[:8]); err != nil {
			return fmt.Errorf("failed to read footer version: %w", err)
		}
		f.Version = binary.LittleEndian.Uint64(buf[:8])
		if f.Version != 1 {
			return fmt.Errorf("unsupported footer version: %d", f.Version)
		}

		if _, err := io.ReadFull(r, buf[:8]); err != nil {
			return fmt.Errorf("failed to read footer file info offset: %w", err)
		}
		f.FileInfoOffset = binary.LittleEndian.Uint64(buf[:8])

		if _, err := io.ReadFull(r, buf[:8]); err != nil {
			return fmt.Errorf("failed to read footer cas info offset: %w", err)
		}
		f.CASInfoOffset = binary.LittleEndian.Uint64(buf[:8])

		if _, err := io.ReadFull(r, buf[:8]); err != nil {
			return fmt.Errorf("failed to read footer file lookup offset: %w", err)
		}
		f.FileLookupOffset = binary.LittleEndian.Uint64(buf[:8])

		if _, err := io.ReadFull(r, buf[:8]); err != nil {
			return fmt.Errorf("failed to read footer file lookup num entries: %w", err)
		}
		f.FileLookupNumEntries = binary.LittleEndian.Uint64(buf[:8])

		if _, err := io.ReadFull(r, buf[:8]); err != nil {
			return fmt.Errorf("failed to read footer cas lookup offset: %w", err)
		}
		f.CASLookupOffset = binary.LittleEndian.Uint64(buf[:8])

		if _, err := io.ReadFull(r, buf[:8]); err != nil {
			return fmt.Errorf("failed to read footer cas lookup num entries: %w", err)
		}
		f.CASLookupNumEntries = binary.LittleEndian.Uint64(buf[:8])

		if _, err := io.ReadFull(r, buf[:8]); err != nil {
			return fmt.Errorf("failed to read footer chunk lookup offset: %w", err)
		}
		f.ChunkLookupOffset = binary.LittleEndian.Uint64(buf[:8])

		if _, err := io.ReadFull(r, buf[:8]); err != nil {
			return fmt.Errorf("failed to read footer chunk lookup num entries: %w", err)
		}
		f.ChunkLookupNumEntries = binary.LittleEndian.Uint64(buf[:8])

		if _, err := io.ReadFull(r, buf[:32]); err != nil {
			return fmt.Errorf("failed to read footer chunk hash key: %w", err)
		}
		copy(f.ChunkHashKey[:], buf[:32])

		if _, err := io.ReadFull(r, buf[:8]); err != nil {
			return fmt.Errorf("failed to read footer shard creation timestamp: %w", err)
		}
		f.ShardCreationTimestamp = binary.LittleEndian.Uint64(buf[:8])

		if _, err := io.ReadFull(r, buf[:8]); err != nil {
			return fmt.Errorf("failed to read footer shard key expiry: %w", err)
		}
		f.ShardKeyExpiry = binary.LittleEndian.Uint64(buf[:8])

		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return fmt.Errorf("failed to read footer reserved: %w", err)
		}
		copy(f.Reserved[:], buf[:])

		if _, err := io.ReadFull(r, buf[:8]); err != nil {
			return fmt.Errorf("failed to read footer stored bytes on disk: %w", err)
		}
		f.StoredBytesOnDisk = binary.LittleEndian.Uint64(buf[:8])

		if _, err := io.ReadFull(r, buf[:8]); err != nil {
			return fmt.Errorf("failed to read footer materialized bytes: %w", err)
		}
		f.MaterializedBytes = binary.LittleEndian.Uint64(buf[:8])

		if _, err := io.ReadFull(r, buf[:8]); err != nil {
			return fmt.Errorf("failed to read footer stored bytes: %w", err)
		}
		f.StoredBytes = binary.LittleEndian.Uint64(buf[:8])

		if _, err := io.ReadFull(r, buf[:8]); err != nil {
			return fmt.Errorf("failed to read footer offset: %w", err)
		}
		f.FooterOffset = binary.LittleEndian.Uint64(buf[:8])

		if s.FooterSize > 200 {
			if _, err := io.CopyN(io.Discard, r, int64(s.FooterSize)-200); err != nil {
				return fmt.Errorf("failed to drain footer: %w", err)
			}
		}
	}

	return nil
}

// isBookend checks if a 48-byte slice is a bookend entry
func isBookend(data *[48]byte) bool {
	// Check bytes 0-31 are all 0xFF
	for i := range 32 {
		if data[i] != 0xFF {
			return false
		}
	}

	// Check bytes 32-47 are all 0x00
	for i := 32; i < 48; i++ {
		if data[i] != 0x00 {
			return false
		}
	}

	return true
}
