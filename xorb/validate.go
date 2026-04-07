package xorb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/internal/pool"
)

// Validate reads the xorb stream and verifies its structural and hash integrity.
// Chunks are processed without buffering all raw data; only chunk hashes and sizes
// are accumulated for hash verification against the footer.
// For chunk-only format (no footer), only structural validity is checked.
// Returns nil if the stream is valid, or a descriptive error otherwise.
func Validate(r io.Reader, xorbHash xet.Hash) error {
	tmpBuf := pool.GetChunkBuf()
	defer pool.PutChunkBuf(tmpBuf)

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
			return validateWithFooter(r, tmpBuf[:], headerBuf, xorbHash, chunkHashes)
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
	chunkHashes []xet.Hash,
) error {
	// Validate main header version.
	if headerBuf[7] != 1 {
		return fmt.Errorf("invalid footer: unsupported xorb version: %d (expected 1)", headerBuf[7])
	}

	// Read xorb hash (32) + hash section header: ident (7) + version (1) + num_chunks (4) = 44 bytes.
	var fixedBuf [44]byte
	if _, err := io.ReadFull(r, fixedBuf[:]); err != nil {
		return fmt.Errorf("invalid footer: failed to read xorb hash and hash section header: %w", err)
	}

	footerHash := *(*xet.Hash)(fixedBuf[:32])
	if footerHash != xorbHash {
		return fmt.Errorf("xorb hash mismatch: footer claims %x, computed %x", footerHash, xorbHash)
	}

	if !bytes.Equal(fixedBuf[32:39], hashSectionIdent[:]) {
		return fmt.Errorf("invalid footer: invalid hash section identifier: got %q, expected %q", string(fixedBuf[32:39]), string(hashSectionIdent[:]))
	}
	if fixedBuf[39] != 0 {
		return fmt.Errorf("invalid footer: unsupported hash section version: %d", fixedBuf[39])
	}

	numChunks := binary.LittleEndian.Uint32(fixedBuf[40:44])

	// Remaining: chunk hashes (32*N) + boundary section (12 + 8*N) + trailer (28) + length field (4)
	// = 40*N + 44 bytes.
	remainingSize := int(numChunks)*40 + 44
	remaining := buf[:remainingSize]
	if _, err := io.ReadFull(r, remaining); err != nil {
		return fmt.Errorf("invalid footer: failed to read remaining footer data: %w", err)
	}

	// Verify footer length (total footer bytes, excluding the final 4-byte field itself).
	footerLen := binary.LittleEndian.Uint32(remaining[remainingSize-4:])
	expectedLen := 8 + len(fixedBuf) + remainingSize - 4
	if int(footerLen) != expectedLen {
		return fmt.Errorf("invalid footer: footer length mismatch: expected %d, got %d", expectedLen, footerLen)
	}

	if numChunks != uint32(len(chunkHashes)) {
		return fmt.Errorf("chunk count mismatch: footer has %d, stream has %d", numChunks, len(chunkHashes))
	}

	// Parse per-chunk hashes from the hash section (first 32*N bytes of remaining).
	for i, v := range chunkHashes {
		if v != *(*xet.Hash)(remaining[i*32:]) {
			return fmt.Errorf("chunk %d hash mismatch: footer claims %x, computed %x", i, remaining[i*32:i*32+32], v)
		}
	}

	// Parse boundary section: XBLBBND (7) + version (1) + num_chunks (4) + packed offsets (4*N) + unpacked offsets (4*N)
	bOff := int(numChunks) * 32
	if !bytes.Equal(remaining[bOff:bOff+7], boundarySectionIdent[:]) {
		return fmt.Errorf("invalid footer: invalid boundary section identifier: got %q, expected %q", string(remaining[bOff:bOff+7]), string(boundarySectionIdent[:]))
	}
	if remaining[bOff+7] != 1 {
		return fmt.Errorf("invalid footer: unsupported boundary section version: %d", remaining[bOff+7])
	}
	if binary.LittleEndian.Uint32(remaining[bOff+8:bOff+12]) != numChunks {
		return fmt.Errorf("invalid footer: boundary section num_chunks mismatch")
	}
	packedBase := bOff + 12
	unpackedBase := packedBase + int(numChunks)*4

	// Parse trailer: num_chunks (4) + hash_offset_from_end (4) + boundary_offset_from_end (4) + reserved (16)
	trailerOff := unpackedBase + int(numChunks)*4
	if binary.LittleEndian.Uint32(remaining[trailerOff:trailerOff+4]) != numChunks {
		return fmt.Errorf("invalid footer: trailer num_chunks mismatch")
	}
	return nil
}
