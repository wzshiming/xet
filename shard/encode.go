package shard

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Encode returns a streaming reader for the shard serialization.
func (s *Shard) Encode(withFooter bool) (io.Reader, error) {
	if withFooter {
		if s.Footer == nil {
			s.SetFooter()
		}
	}
	return &shardReader{
		shard:      s,
		withFooter: withFooter,
	}, nil
}

// shardReader implements io.Reader for shard serialization.
// Sections: header (0), file blocks (1..numFiles), file bookend (numFiles+1),
// CAS blocks (numFiles+2..numFiles+1+numCAS), CAS bookend (numFiles+2+numCAS).
// Footer follows all sections if withFooter is true.
type shardReader struct {
	shard         *Shard
	withFooter    bool
	sectionIdx    int
	buffer        []byte
	bufOffset     int
	footerWritten bool
	bytesWritten  int64
	// Offsets recorded for footer construction.
	fileInfoOffset uint64
	casInfoOffset  uint64
	footerOffset   uint64
}

func (r *shardReader) Read(p []byte) (n int, err error) {
	numFiles := len(r.shard.Files)
	numCAS := len(r.shard.CASInfos)
	totalSections := numFiles + numCAS + 3

	for n < len(p) {
		// Write all sections (header, file blocks, file bookend, CAS blocks, CAS bookend).
		if r.sectionIdx < totalSections {
			if r.bufOffset >= len(r.buffer) {
				if err := r.loadSection(r.sectionIdx, numFiles, numCAS); err != nil {
					return n, err
				}
			}

			copied := copy(p[n:], r.buffer[r.bufOffset:])
			n += copied
			r.bufOffset += copied
			r.bytesWritten += int64(copied)

			if r.bufOffset >= len(r.buffer) {
				// Record byte offsets needed by the footer.
				switch r.sectionIdx {
				case 0: // header complete
					r.fileInfoOffset = uint64(r.bytesWritten)
				case numFiles + 1: // file bookend complete
					r.casInfoOffset = uint64(r.bytesWritten)
				case numFiles + numCAS + 2: // CAS bookend complete
					r.footerOffset = uint64(r.bytesWritten)
				}
				r.sectionIdx++
				r.buffer = r.buffer[:0]
				r.bufOffset = 0
			}
			continue
		}

		// Write footer if requested.
		if r.withFooter {
			if !r.footerWritten {
				r.buildFooterBuffer()
				r.bufOffset = 0
				r.footerWritten = true
			}
			if r.bufOffset < len(r.buffer) {
				copied := copy(p[n:], r.buffer[r.bufOffset:])
				n += copied
				r.bufOffset += copied
				r.bytesWritten += int64(copied)
				if r.bufOffset < len(r.buffer) {
					continue
				}
			}
		}

		if n > 0 {
			return n, nil
		}
		return 0, io.EOF
	}

	return n, nil
}

func (r *shardReader) loadSection(idx, numFiles, numCAS int) error {
	casBookendIdx := numFiles + numCAS + 2
	switch {
	case idx == 0:
		// Header (48 bytes).
		if r.withFooter {
			r.shard.FooterSize = 200
		} else {
			r.shard.FooterSize = 0
		}
		r.buildHeader()

	case idx >= 1 && idx <= numFiles:
		// File block.
		r.buildFileBlock(r.shard.Files[idx-1])

	case idx == numFiles+1:
		// File bookend (48 bytes).
		r.buildBookend()

	case idx >= numFiles+2 && idx < casBookendIdx:
		// CAS block.
		r.buildCASBlock(r.shard.CASInfos[idx-numFiles-2])

	case idx == casBookendIdx:
		// CAS bookend (48 bytes).
		r.buildBookend()

	default:
		return fmt.Errorf("invalid section index: %d", idx)
	}
	r.bufOffset = 0
	return nil
}

func (r *shardReader) buildFooterBuffer() {
	f := r.shard.Footer
	f.FileInfoOffset = r.fileInfoOffset
	f.CASInfoOffset = r.casInfoOffset
	f.FooterOffset = r.footerOffset
	r.buffer = r.buffer[:0]
	r.buffer = binary.LittleEndian.AppendUint64(r.buffer, f.Version)
	r.buffer = binary.LittleEndian.AppendUint64(r.buffer, f.FileInfoOffset)
	r.buffer = binary.LittleEndian.AppendUint64(r.buffer, f.CASInfoOffset)
	r.buffer = binary.LittleEndian.AppendUint64(r.buffer, f.FileLookupOffset)
	r.buffer = binary.LittleEndian.AppendUint64(r.buffer, f.FileLookupNumEntries)
	r.buffer = binary.LittleEndian.AppendUint64(r.buffer, f.CASLookupOffset)
	r.buffer = binary.LittleEndian.AppendUint64(r.buffer, f.CASLookupNumEntries)
	r.buffer = binary.LittleEndian.AppendUint64(r.buffer, f.ChunkLookupOffset)
	r.buffer = binary.LittleEndian.AppendUint64(r.buffer, f.ChunkLookupNumEntries)
	r.buffer = append(r.buffer, f.ChunkHashKey[:]...)
	r.buffer = binary.LittleEndian.AppendUint64(r.buffer, f.ShardCreationTimestamp)
	r.buffer = binary.LittleEndian.AppendUint64(r.buffer, f.ShardKeyExpiry)
	r.buffer = append(r.buffer, f.Reserved[:]...)
	r.buffer = binary.LittleEndian.AppendUint64(r.buffer, f.StoredBytesOnDisk)
	r.buffer = binary.LittleEndian.AppendUint64(r.buffer, f.MaterializedBytes)
	r.buffer = binary.LittleEndian.AppendUint64(r.buffer, f.StoredBytes)
	r.buffer = binary.LittleEndian.AppendUint64(r.buffer, f.FooterOffset)
}

func (r *shardReader) buildHeader() {
	const version uint64 = 2

	r.buffer = r.buffer[:0]
	r.buffer = append(r.buffer, hfApplicationID[:]...)
	r.buffer = append(r.buffer, 0)
	r.buffer = append(r.buffer, shardMagicSequence[:]...)
	r.buffer = binary.LittleEndian.AppendUint64(r.buffer, version)
	r.buffer = binary.LittleEndian.AppendUint64(r.buffer, r.shard.FooterSize)
}

func (r *shardReader) buildFileBlock(fb FileBlock) {
	r.buffer = r.buffer[:0]

	// FileDataSequenceHeader (48 bytes)
	r.buffer = append(r.buffer, fb.FileHash[:]...)
	r.buffer = binary.LittleEndian.AppendUint32(r.buffer, uint32(fb.Flags))
	r.buffer = binary.LittleEndian.AppendUint32(r.buffer, uint32(len(fb.Entries)))
	r.buffer = append(r.buffer, 0, 0, 0, 0, 0, 0, 0, 0) // 8 bytes reserved

	// FileDataSequenceEntry entries (48 bytes each)
	for _, entry := range fb.Entries {
		r.buffer = append(r.buffer, entry.CASHash[:]...)
		r.buffer = binary.LittleEndian.AppendUint32(r.buffer, entry.CASFlags)
		r.buffer = binary.LittleEndian.AppendUint32(r.buffer, entry.UnpackedSegBytes)
		r.buffer = binary.LittleEndian.AppendUint32(r.buffer, entry.ChunkIndexStart)
		r.buffer = binary.LittleEndian.AppendUint32(r.buffer, entry.ChunkIndexEnd)
	}

	// FileVerificationEntry entries (48 bytes each) if flag set
	if fb.Flags&FileWithVerification != 0 {
		for _, verif := range fb.Verification {
			r.buffer = append(r.buffer, verif[:]...)
			r.buffer = append(r.buffer, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0) // 16 bytes reserved
		}
	}

	// FileMetadataExt (48 bytes) if flag set
	if fb.Flags&FileWithMetadataExt != 0 {
		if fb.MetadataExt != nil {
			wireHash := transformSHA256ByteOrder(fb.MetadataExt.SHA256Hash)
			r.buffer = append(r.buffer, wireHash[:]...)
			r.buffer = append(r.buffer, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0) // 16 bytes reserved
		}
	}
}

func (r *shardReader) buildCASBlock(cb CASBlock) {
	r.buffer = r.buffer[:0]

	// CASChunkSequenceHeader (48 bytes)
	r.buffer = append(r.buffer, cb.CASHash[:]...)
	r.buffer = binary.LittleEndian.AppendUint32(r.buffer, cb.CASFlags)
	r.buffer = binary.LittleEndian.AppendUint32(r.buffer, uint32(len(cb.Chunks)))
	r.buffer = binary.LittleEndian.AppendUint32(r.buffer, cb.NumBytesInCAS)
	r.buffer = binary.LittleEndian.AppendUint32(r.buffer, cb.NumBytesOnDisk)

	// CASChunkSequenceEntry entries (48 bytes each)
	for _, chunk := range cb.Chunks {
		r.buffer = append(r.buffer, chunk.ChunkHash[:]...)
		r.buffer = binary.LittleEndian.AppendUint32(r.buffer, chunk.ByteRangeStart)
		r.buffer = binary.LittleEndian.AppendUint32(r.buffer, chunk.UnpackedSegBytes)
		r.buffer = binary.LittleEndian.AppendUint32(r.buffer, uint32(chunk.Flags))
		r.buffer = append(r.buffer, 0, 0, 0, 0) // 4 bytes reserved
	}
}

func (r *shardReader) buildBookend() {
	r.buffer = r.buffer[:0]
	r.buffer = append(r.buffer,
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	)
}
