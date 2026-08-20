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
func Validate(r io.Reader, xorbHash xet.XorbHash) error {
	tmp := pool.GetChunkBuf()
	defer pool.PutChunkBuf(tmp)

	var headerBuf [8]byte
	var chunkHashes []xet.ChunkHash
	var chunkSizes []uint64
	var uncompressedBuf []byte
	var packedEndOffset uint64   // cumulative compressed bytes (header + data) per chunk
	var unpackedEndOffset uint64 // cumulative uncompressed bytes per chunk

	for {
		n, err := io.ReadFull(r, headerBuf[:])
		if err == io.EOF {
			// Chunk-only format: no trusted footer, so bind the decoded
			// content to the expected hash directly.
			if computed := xet.ComputeXorbHash(chunkHashes, chunkSizes); computed != xorbHash {
				return fmt.Errorf("xorb hash mismatch: computed %x, expected %x", computed, xorbHash)
			}
			return nil
		}
		if err == io.ErrUnexpectedEOF {
			return fmt.Errorf("failed to read chunk header: %w", err)
		}
		if err != nil {
			return fmt.Errorf("failed to read chunk header: %w", err)
		}

		if n >= 7 && bytes.Equal(headerBuf[:7], xorbIdentifier[:]) {
			return validateWithFooter(r, tmp[:], headerBuf, xorbHash, chunkHashes, chunkSizes)
		}

		version := headerBuf[0]
		if version != 0 {
			return fmt.Errorf("unsupported chunk version: %d", version)
		}
		compressedSize := uint32(headerBuf[1]) | uint32(headerBuf[2])<<8 | uint32(headerBuf[3])<<16
		ct := compressionType(headerBuf[4])
		uncompressedSize := uint32(headerBuf[5]) | uint32(headerBuf[6])<<8 | uint32(headerBuf[7])<<16
		if compressedSize > uint32(len(tmp)) {
			return fmt.Errorf("invalid compressed chunk size: %d exceeds maximum %d", compressedSize, len(tmp))
		}
		if uncompressedSize > xet.MaxChunkSize {
			return fmt.Errorf("invalid uncompressed chunk size: %d exceeds maximum %d", uncompressedSize, xet.MaxChunkSize)
		}
		if len(chunkHashes) >= xet.MaxChunksPerXorb {
			return fmt.Errorf("chunk count exceeds maximum %d", xet.MaxChunksPerXorb)
		}
		if unpackedEndOffset > xet.MaxXorbSize || uint64(uncompressedSize) > xet.MaxXorbSize-unpackedEndOffset {
			return fmt.Errorf("raw payload size exceeds maximum %d", xet.MaxXorbSize)
		}

		if _, err := io.ReadFull(r, tmp[:compressedSize]); err != nil {
			return fmt.Errorf("failed to read compressed chunk data: %w", err)
		}
		uncompressedBuf, err = decompressChunk(uncompressedBuf[:0], tmp[:compressedSize], ct, int(uncompressedSize))
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
	xorbHash xet.XorbHash,
	chunkHashes []xet.ChunkHash,
	chunkSizes []uint64,
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

	footerHash := *(*xet.XorbHash)(fixedBuf[:32])
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
	if numChunks > xet.MaxChunksPerXorb {
		return fmt.Errorf("invalid footer: chunk count %d exceeds maximum %d", numChunks, xet.MaxChunksPerXorb)
	}

	// Remaining: chunk hashes (32*N) + boundary section (12 + 8*N) + trailer (28) + length field (4)
	// = 40*N + 44 bytes.
	remainingSize := int(numChunks)*40 + 44
	remaining := buf
	if remainingSize > len(remaining) {
		remaining = make([]byte, remainingSize)
	} else {
		remaining = remaining[:remainingSize]
	}
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

	// The footer's claimed hashes are writer-controlled (direct uploads bypass
	// the server), so recompute the xorb hash from the decoded chunks.
	if computed := xet.ComputeXorbHash(chunkHashes, chunkSizes); computed != xorbHash {
		return fmt.Errorf("xorb hash mismatch: computed %x, expected %x", computed, xorbHash)
	}

	// Parse per-chunk hashes from the hash section (first 32*N bytes of remaining).
	for i, v := range chunkHashes {
		if v != *(*xet.ChunkHash)(remaining[i*32:]) {
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

	// Parse trailer: num_chunks (4) + hash_offset_from_end (4) +
	// boundary_offset_from_end (4) + footer buffer (16). Readers ignore the
	// first four footer-buffer bytes (the uniqueness nonce); the remaining 12
	// reserved bytes must be zero.
	trailerOff := unpackedBase + int(numChunks)*4
	if binary.LittleEndian.Uint32(remaining[trailerOff:trailerOff+4]) != numChunks {
		return fmt.Errorf("invalid footer: trailer num_chunks mismatch")
	}
	reserved := remaining[trailerOff+16 : trailerOff+28]
	var zeroReserved [12]byte
	if !bytes.Equal(reserved, zeroReserved[:]) {
		return fmt.Errorf("invalid footer: reserved footer buffer bytes must be zero")
	}
	return nil
}
