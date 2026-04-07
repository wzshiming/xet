package xorb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/wzshiming/xet"
)

// Validate reads the xorb stream and verifies its structural and hash integrity.
// Chunks are processed without buffering all raw data; only chunk hashes and sizes
// are accumulated for hash verification against the footer.
// For chunk-only format (no footer), only structural validity is checked.
// Returns nil if the stream is valid, or a descriptive error otherwise.
func Validate(r io.Reader, xorbHash xet.Hash) error {
	var tmpBuf [xet.MaxChunkSize]byte
	var headerBuf [8]byte
	var chunkHashes []xet.Hash
	var chunkSizes []uint64
	var uncompressedBuf []byte
	var packedEndOffset uint64   // cumulative compressed bytes (header + data) per chunk
	var unpackedEndOffset uint64 // cumulative uncompressed bytes per chunk

	for {
		n, err := io.ReadFull(r, headerBuf[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// Chunk-only format: structural validation passed.
			return nil
		}
		if err != nil {
			return fmt.Errorf("failed to read chunk header: %w", err)
		}

		if n >= 7 && bytes.Equal(headerBuf[:7], xorbIdentifier[:]) {
			return validateWithFooter(r, tmpBuf[:], headerBuf, xorbHash, chunkHashes, chunkSizes, packedEndOffset, unpackedEndOffset)
		}

		version := headerBuf[0]
		if version != 0 {
			return fmt.Errorf("unsupported chunk version: %d", version)
		}
		compressedSize := uint32(headerBuf[1]) | uint32(headerBuf[2])<<8 | uint32(headerBuf[3])<<16
		ct := compressionType(headerBuf[4])
		uncompressedSize := uint32(headerBuf[5]) | uint32(headerBuf[6])<<8 | uint32(headerBuf[7])<<16

		if _, err := io.ReadFull(r, tmpBuf[:compressedSize]); err != nil {
			return fmt.Errorf("failed to read compressed chunk data: %w", err)
		}
		uncompressedBuf, err = decompressChunk(uncompressedBuf[:0], tmpBuf[:compressedSize], ct, int(uncompressedSize))
		if err != nil {
			return fmt.Errorf("decompress chunk: %w", err)
		}
		h := xet.ComputeChunkHash(uncompressedBuf)
		chunkHashes = append(chunkHashes, h)
		chunkSizes = append(chunkSizes, uint64(uncompressedSize))
		packedEndOffset += 8 + uint64(compressedSize)
		unpackedEndOffset += uint64(uncompressedSize)
	}
}

// validateWithFooter reads the footer from the stream and validates it against
// the provided chunk data (hashes, sizes, offsets) and expected xorb hash.
// headerBuf contains the first 8 bytes already read (XETBLOB identifier + version byte).
func validateWithFooter(
	r io.Reader, buf []byte, headerBuf [8]byte,
	xorbHash xet.Hash,
	chunkHashes []xet.Hash, chunkSizes []uint64,
	packedEndOffset, unpackedEndOffset uint64,
) error {
	info, err := readFooter(r, buf, headerBuf)
	if err != nil {
		return fmt.Errorf("invalid footer: %w", err)
	}

	// Verify chunk count matches.
	if len(info.ChunkHashes) != len(chunkHashes) {
		return fmt.Errorf("chunk count mismatch: footer has %d, stream has %d", len(info.ChunkHashes), len(chunkHashes))
	}
	// Verify each chunk hash against the footer.
	for i, h := range chunkHashes {
		if h != info.ChunkHashes[i] {
			return fmt.Errorf("chunk %d hash mismatch: footer claims %x, computed %x", i, info.ChunkHashes[i], h)
		}
	}

	// Verify cumulative packed/unpacked offsets against boundary section.
	if len(info.ChunkPackedEndOffsets) > 0 {
		last := len(info.ChunkPackedEndOffsets) - 1
		if info.ChunkPackedEndOffsets[last] != packedEndOffset {
			return fmt.Errorf("packed offset mismatch: footer claims %d, stream has %d", info.ChunkPackedEndOffsets[last], packedEndOffset)
		}
		if info.ChunkUnpackedEndOffsets[last] != unpackedEndOffset {
			return fmt.Errorf("unpacked offset mismatch: footer claims %d, stream has %d", info.ChunkUnpackedEndOffsets[last], unpackedEndOffset)
		}
	}

	computed := xet.ComputeXorbHash(chunkHashes, chunkSizes)
	if info.Hash != computed {
		return fmt.Errorf("xorb hash mismatch: footer claims %x, computed %x", info.Hash, computed)
	}
	if info.Hash != xorbHash {
		return fmt.Errorf("xorb hash mismatch: expected %x, computed %x", xorbHash, computed)
	}
	return nil
}

// footerInfo holds the integrity data parsed from an xorb footer.
type footerInfo struct {
	// Hash is the xorb-level hash covering all chunks.
	Hash xet.Hash
	// ChunkHashes lists each chunk's content hash in order.
	ChunkHashes []xet.Hash
	// ChunkPackedEndOffsets holds the cumulative end byte offset of each chunk
	// in the packed (compressed) stream, as recorded in the boundary section.
	ChunkPackedEndOffsets []uint64
	// ChunkUnpackedEndOffsets holds the cumulative end byte offset of each chunk
	// in the uncompressed data stream, as recorded in the boundary section.
	ChunkUnpackedEndOffsets []uint64
}

// readFooter reads and validates the footer from the stream.
// headerBuf contains the first 8 bytes already read (XETBLOB identifier + version byte).
//
// Footer layout (all sizes in bytes):
//   - XETBLOB (7) + version (1) + xorb hash (32)          — already in headerBuf + fixedBuf
//   - Hash section: XBLBHSH (7) + version (1) + N (4) + hashes (32*N)
//   - Boundary section: XBLBBND (7) + version (1) + N (4) + packed offsets (4*N) + unpacked offsets (4*N)
//   - Trailer (28) + footer length (4)
func readFooter(r io.Reader, buf []byte, headerBuf [8]byte) (footerInfo, error) {
	// Validate main header version.
	if headerBuf[7] != 1 {
		return footerInfo{}, fmt.Errorf("unsupported xorb version: %d (expected 1)", headerBuf[7])
	}

	// Read xorb hash (32) + hash section header: ident (7) + version (1) + num_chunks (4) = 44 bytes.
	var fixedBuf [44]byte
	if _, err := io.ReadFull(r, fixedBuf[:]); err != nil {
		return footerInfo{}, fmt.Errorf("failed to read xorb hash and hash section header: %w", err)
	}

	if !bytes.Equal(fixedBuf[32:39], hashSectionIdent[:]) {
		return footerInfo{}, fmt.Errorf("invalid hash section identifier: got %q, expected %q", string(fixedBuf[32:39]), string(hashSectionIdent[:]))
	}
	if fixedBuf[39] != 0 {
		return footerInfo{}, fmt.Errorf("unsupported hash section version: %d", fixedBuf[39])
	}

	numChunks := binary.LittleEndian.Uint32(fixedBuf[40:44])

	// Remaining: chunk hashes (32*N) + boundary section (12 + 8*N) + trailer (28) + length field (4)
	// = 40*N + 44 bytes.
	remainingSize := int(numChunks)*40 + 44
	remaining := buf[:remainingSize]
	if _, err := io.ReadFull(r, remaining); err != nil {
		return footerInfo{}, fmt.Errorf("failed to read remaining footer data: %w", err)
	}

	// Verify footer length (total footer bytes, excluding the final 4-byte field itself).
	footerLen := binary.LittleEndian.Uint32(remaining[remainingSize-4:])
	expectedLen := 8 + len(fixedBuf) + remainingSize - 4
	if int(footerLen) != expectedLen {
		return footerInfo{}, fmt.Errorf("footer length mismatch: expected %d, got %d", expectedLen, footerLen)
	}

	// Parse per-chunk hashes from the hash section (first 32*N bytes of remaining).
	chunkHashes := make([]xet.Hash, numChunks)
	for i := range chunkHashes {
		chunkHashes[i] = *(*xet.Hash)(remaining[i*32:])
	}

	// Parse boundary section: XBLBBND (7) + version (1) + num_chunks (4) + packed offsets (4*N) + unpacked offsets (4*N)
	bOff := int(numChunks) * 32
	if !bytes.Equal(remaining[bOff:bOff+7], boundarySectionIdent[:]) {
		return footerInfo{}, fmt.Errorf("invalid boundary section identifier: got %q, expected %q", string(remaining[bOff:bOff+7]), string(boundarySectionIdent[:]))
	}
	if remaining[bOff+7] != 1 {
		return footerInfo{}, fmt.Errorf("unsupported boundary section version: %d", remaining[bOff+7])
	}
	if binary.LittleEndian.Uint32(remaining[bOff+8:bOff+12]) != numChunks {
		return footerInfo{}, fmt.Errorf("boundary section num_chunks mismatch")
	}
	packedBase := bOff + 12
	unpackedBase := packedBase + int(numChunks)*4
	packedOffsets := make([]uint64, numChunks)
	unpackedOffsets := make([]uint64, numChunks)
	for i := range packedOffsets {
		packedOffsets[i] = uint64(binary.LittleEndian.Uint32(remaining[packedBase+i*4:]))
		unpackedOffsets[i] = uint64(binary.LittleEndian.Uint32(remaining[unpackedBase+i*4:]))
	}

	// Parse trailer: num_chunks (4) + hash_offset_from_end (4) + boundary_offset_from_end (4) + reserved (16)
	trailerOff := unpackedBase + int(numChunks)*4
	if binary.LittleEndian.Uint32(remaining[trailerOff:trailerOff+4]) != numChunks {
		return footerInfo{}, fmt.Errorf("trailer num_chunks mismatch")
	}

	return footerInfo{
		Hash:                    *(*xet.Hash)(fixedBuf[:32]),
		ChunkHashes:             chunkHashes,
		ChunkPackedEndOffsets:   packedOffsets,
		ChunkUnpackedEndOffsets: unpackedOffsets,
	}, nil
}
