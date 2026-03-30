package shard

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Encode serializes the shard to binary format (without footer for upload API).
// Returns an io.Reader that streams the data directly without buffering everything in memory.
func Encode(s *Shard, includeFooter bool) (io.Reader, error) {
	if includeFooter && s.Footer == nil {
		return nil, fmt.Errorf("footer is required but not set")
	}
	return &shardReader{
		shard:         s,
		state:         stateHeader,
		includeFooter: includeFooter,
	}, nil
}

// shardReader implements io.Reader for shard serialization
type shardReader struct {
	shard         *Shard
	state         readerState
	includeFooter bool
	fileIdx       int
	casIdx        int
	buffer        []byte
	bufOffset     int
	bytesWritten  int64
	// Footer tracking
	fileInfoOffset uint64
	casInfoOffset  uint64
	footerOffset   uint64
}

// readerState represents the current state of the reader
type readerState int

const (
	stateHeader readerState = iota
	stateFileBlocks
	stateFileBookend
	stateCASBlocks
	stateCASBookend
	stateFooter
	stateDone
)

func (r *shardReader) Read(p []byte) (n int, err error) {
	for n < len(p) {
		// Check if we're done
		if r.state == stateDone {
			if n > 0 {
				return n, nil
			}
			return 0, io.EOF
		}

		// If buffer is empty or consumed, prepare next data based on state
		if len(r.buffer) == 0 || r.bufOffset >= len(r.buffer) {
			if err := r.prepareNextBuffer(); err != nil {
				if err == io.EOF {
					r.state = stateDone
					if n > 0 {
						return n, nil
					}
					return 0, io.EOF
				}
				return n, err
			}
		}

		// Copy from buffer to output
		copied := copy(p[n:], r.buffer[r.bufOffset:])
		n += copied
		r.bufOffset += copied
		r.bytesWritten += int64(copied)
	}

	return n, nil
}

func (r *shardReader) prepareNextBuffer() error {
	switch r.state {
	case stateHeader:
		// Set footer size based on includeFooter
		if r.includeFooter {
			r.shard.Header.FooterSize = 200
		} else {
			r.shard.Header.FooterSize = 0
		}

		r.buffer = make([]byte, 48)
		r.buildHeader(r.buffer)
		r.bufOffset = 0
		r.fileInfoOffset = uint64(r.bytesWritten) + 48
		r.state = stateFileBlocks
		return nil

	case stateFileBlocks:
		if r.fileIdx < len(r.shard.Files) {
			fb := r.shard.Files[r.fileIdx]
			r.buffer = r.buildFileBlock(fb)
			r.bufOffset = 0
			r.fileIdx++
			return nil
		}
		r.state = stateFileBookend
		return r.prepareNextBuffer()

	case stateFileBookend:
		r.buffer = r.buildBookend()
		r.bufOffset = 0
		r.casInfoOffset = uint64(r.bytesWritten) + 48
		r.state = stateCASBlocks
		return nil

	case stateCASBlocks:
		if r.casIdx < len(r.shard.CASInfos) {
			cb := r.shard.CASInfos[r.casIdx]
			r.buffer = r.buildCASBlock(cb)
			r.bufOffset = 0
			r.casIdx++
			return nil
		}
		r.state = stateCASBookend
		return r.prepareNextBuffer()

	case stateCASBookend:
		r.buffer = r.buildBookend()
		r.bufOffset = 0
		if r.includeFooter {
			r.footerOffset = uint64(r.bytesWritten) + 48
			r.state = stateFooter
		} else {
			r.state = stateDone
		}
		return nil

	case stateFooter:
		// Update footer with offsets
		r.shard.Footer.FileInfoOffset = r.fileInfoOffset
		r.shard.Footer.CASInfoOffset = r.casInfoOffset
		r.shard.Footer.FooterOffset = r.footerOffset

		r.buffer = make([]byte, 200)
		r.buildFooter(r.buffer)
		r.bufOffset = 0
		r.state = stateDone
		return nil

	case stateDone:
		return io.EOF

	default:
		return fmt.Errorf("invalid reader state: %d", r.state)
	}
}

func (r *shardReader) buildHeader(buf []byte) {
	// Tag (32 bytes)
	copy(buf[0:32], r.shard.Header.Tag[:])

	// Version (8 bytes)
	binary.LittleEndian.PutUint64(buf[32:40], r.shard.Header.Version)

	// FooterSize (8 bytes)
	binary.LittleEndian.PutUint64(buf[40:48], r.shard.Header.FooterSize)
}

func (r *shardReader) buildFileBlock(fb FileBlock) []byte {
	// Calculate size: header (48) + entries (48 each) + verification (48 each if flag set) + metadata ext (48 if flag set)
	size := 48 + len(fb.Entries)*48
	if fb.Flags&FileWithVerification != 0 {
		size += len(fb.Verification) * 48
	}
	if fb.Flags&FileWithMetadataExt != 0 {
		size += 48
	}

	buf := make([]byte, size)
	offset := 0

	// FileDataSequenceHeader (48 bytes)
	copy(buf[offset:offset+32], fb.FileHash[:])
	offset += 32
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(fb.Flags))
	offset += 4
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(fb.Entries)))
	offset += 4
	// 8 bytes reserved (already zeroed)
	offset += 8

	// FileDataSequenceEntry entries (48 bytes each)
	for _, entry := range fb.Entries {
		copy(buf[offset:offset+32], entry.CASHash[:])
		offset += 32
		binary.LittleEndian.PutUint32(buf[offset:offset+4], entry.CASFlags)
		offset += 4
		binary.LittleEndian.PutUint32(buf[offset:offset+4], entry.UnpackedSegBytes)
		offset += 4
		binary.LittleEndian.PutUint32(buf[offset:offset+4], entry.ChunkIndexStart)
		offset += 4
		binary.LittleEndian.PutUint32(buf[offset:offset+4], entry.ChunkIndexEnd)
		offset += 4
	}

	// FileVerificationEntry entries (48 bytes each) if flag set
	if fb.Flags&FileWithVerification != 0 {
		for _, verif := range fb.Verification {
			copy(buf[offset:offset+32], verif[:])
			offset += 32
			// 16 bytes reserved (already zeroed)
			offset += 16
		}
	}

	// FileMetadataExt (48 bytes) if flag set
	if fb.Flags&FileWithMetadataExt != 0 {
		if fb.MetadataExt != nil {
			copy(buf[offset:offset+32], fb.MetadataExt.SHA256Hash[:])
			offset += 32
			// 16 bytes reserved (already zeroed)
			offset += 16
		}
	}

	return buf
}

func (r *shardReader) buildCASBlock(cb CASBlock) []byte {
	// Calculate size: header (48) + entries (48 each)
	size := 48 + len(cb.Chunks)*48

	buf := make([]byte, size)
	offset := 0

	// CASChunkSequenceHeader (48 bytes)
	copy(buf[offset:offset+32], cb.CASHash[:])
	offset += 32
	binary.LittleEndian.PutUint32(buf[offset:offset+4], cb.CASFlags)
	offset += 4
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(cb.Chunks)))
	offset += 4
	binary.LittleEndian.PutUint32(buf[offset:offset+4], cb.NumBytesInCAS)
	offset += 4
	binary.LittleEndian.PutUint32(buf[offset:offset+4], cb.NumBytesOnDisk)
	offset += 4

	// CASChunkSequenceEntry entries (48 bytes each)
	for _, chunk := range cb.Chunks {
		copy(buf[offset:offset+32], chunk.ChunkHash[:])
		offset += 32
		binary.LittleEndian.PutUint32(buf[offset:offset+4], chunk.ByteRangeStart)
		offset += 4
		binary.LittleEndian.PutUint32(buf[offset:offset+4], chunk.UnpackedSegBytes)
		offset += 4
		binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(chunk.Flags))
		offset += 4
		// 4 bytes reserved (already zeroed)
		offset += 4
	}

	return buf
}

func (r *shardReader) buildBookend() []byte {
	buf := make([]byte, 48)
	// Bytes 0-31: All 0xFF
	for i := 0; i < 32; i++ {
		buf[i] = 0xFF
	}
	// Bytes 32-47: All 0x00 (already zeroed)
	return buf
}

func (r *shardReader) buildFooter(buf []byte) {
	f := r.shard.Footer
	offset := 0

	// Write all footer fields in order (200 bytes total)
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.Version)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.FileInfoOffset)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.CASInfoOffset)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.FileLookupOffset)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.FileLookupNumEntries)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.CASLookupOffset)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.CASLookupNumEntries)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.ChunkLookupOffset)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.ChunkLookupNumEntries)
	offset += 8
	copy(buf[offset:offset+32], f.ChunkHashKey[:])
	offset += 32
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.ShardCreationTimestamp)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.ShardKeyExpiry)
	offset += 8
	copy(buf[offset:offset+48], f.Reserved[:])
	offset += 48
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.StoredBytesOnDisk)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.MaterializedBytes)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.StoredBytes)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.FooterOffset)
	offset += 8
}
