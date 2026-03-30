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
func Decode(r io.Reader) (*Shard, error) {
	s := &Shard{}
	var buf [48]byte

	// Read 48-byte header
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}
	copy(s.Header.Tag[:], buf[:32])
	if !bytes.Equal(s.Header.Tag[15:32], shardMagicSequence[:]) {
		return nil, fmt.Errorf("invalid shard magic sequence")
	}
	s.Header.Version = binary.LittleEndian.Uint64(buf[32:40])
	if s.Header.Version != 2 {
		return nil, fmt.Errorf("unsupported shard version: %d", s.Header.Version)
	}
	s.Header.FooterSize = binary.LittleEndian.Uint64(buf[40:48])

	// Read file blocks until bookend
	s.Files = make([]FileBlock, 0)
	for {
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return nil, fmt.Errorf("failed to read file section: %w", err)
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
				return nil, fmt.Errorf("failed to read file entry %d: %w", i, err)
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
					return nil, fmt.Errorf("failed to read verification entry %d: %w", i, err)
				}
				copy(fb.Verification[i][:], buf[:32])
				// buf[32:48] reserved
			}
		}

		if fb.Flags&FileWithMetadataExt != 0 {
			if _, err := io.ReadFull(r, buf[:]); err != nil {
				return nil, fmt.Errorf("failed to read metadata ext: %w", err)
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
			return nil, fmt.Errorf("failed to read CAS section: %w", err)
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
				return nil, fmt.Errorf("failed to read chunk entry %d: %w", i, err)
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
	if s.Header.FooterSize > 0 {
		footerBuf := make([]byte, s.Header.FooterSize)
		if _, err := io.ReadFull(r, footerBuf); err != nil {
			return nil, fmt.Errorf("failed to read footer: %w", err)
		}
		if err := s.readFooter(footerBuf, 0); err != nil {
			return nil, fmt.Errorf("failed to parse footer: %w", err)
		}
	}

	return s, nil
}

// readFooter reads the 200-byte footer
func (s *Shard) readFooter(data []byte, offset int) error {
	if offset+200 > len(data) {
		return fmt.Errorf("data too short for footer")
	}

	s.Footer = &Footer{}
	f := s.Footer

	// Read all footer fields in order
	f.Version = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	if f.Version != 1 {
		return fmt.Errorf("unsupported footer version: %d", f.Version)
	}

	f.FileInfoOffset = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	f.CASInfoOffset = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	f.FileLookupOffset = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	f.FileLookupNumEntries = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	f.CASLookupOffset = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	f.CASLookupNumEntries = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	f.ChunkLookupOffset = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	f.ChunkLookupNumEntries = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	copy(f.ChunkHashKey[:], data[offset:offset+32])
	offset += 32

	f.ShardCreationTimestamp = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	f.ShardKeyExpiry = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	copy(f.Reserved[:], data[offset:offset+48])
	offset += 48

	f.StoredBytesOnDisk = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	f.MaterializedBytes = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	f.StoredBytes = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	f.FooterOffset = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

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
