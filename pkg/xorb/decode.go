package xorb

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/wzshiming/xet"
)

// Decode deserializes an xorb from an io.Reader.
// If chunkOnly is true, only the chunk data region is expected (without footer).
// If chunkOnly is false, the full XETBLOB format with footer is expected.
//
// This reads from the stream progressively and deserializes the xorb without
// buffering the entire stream in memory first.
func Decode(r io.Reader, chunkOnly bool) (*Xorb, error) {
	xorb := NewXorb()

	// Read chunks until we hit EOF (chunk-only) or footer marker (full format)
	for {
		// Try to peek at the next 8 bytes to see if it's a chunk header or footer marker
		headerBuf := make([]byte, 8)
		n, err := io.ReadFull(r, headerBuf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// End of stream - this is expected for chunk-only format
			if chunkOnly {
				break
			}
			// For full format, we shouldn't hit EOF yet
			if err == io.EOF {
				return nil, fmt.Errorf("unexpected EOF: expected footer")
			}
			return nil, fmt.Errorf("failed to read data: %w", err)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read chunk header: %w", err)
		}

		// Check if this is the start of the footer (XETBLOB identifier)
		if n >= 7 && string(headerBuf[:7]) == XorbIdentifier {
			// We've hit the footer marker
			if chunkOnly {
				// Chunk-only format shouldn't have a footer
				break
			}

			// Read the rest of the footer
			if err := xorb.readFooterFromStream(r, headerBuf); err != nil {
				return nil, fmt.Errorf("failed to read footer: %w", err)
			}
			break
		}

		// Parse chunk header (8 bytes)
		header := ChunkHeader{
			Version:          headerBuf[0],
			CompressedSize:   uint32(headerBuf[1]) | uint32(headerBuf[2])<<8 | uint32(headerBuf[3])<<16,
			CompressionType:  CompressionType(headerBuf[4]),
			UncompressedSize: uint32(headerBuf[5]) | uint32(headerBuf[6])<<8 | uint32(headerBuf[7])<<16,
		}

		if header.Version != 0 {
			return nil, fmt.Errorf("unsupported chunk version: %d", header.Version)
		}

		// Read compressed data
		compressedData := make([]byte, header.CompressedSize)
		if _, err := io.ReadFull(r, compressedData); err != nil {
			return nil, fmt.Errorf("failed to read compressed chunk data: %w", err)
		}

		// Decompress chunk
		uncompressedData, err := decompressChunk(compressedData, header.CompressionType, int(header.UncompressedSize))
		if err != nil {
			return nil, fmt.Errorf("failed to decompress chunk: %w", err)
		}

		// Compute chunk hash
		chunkHash := xet.ChunkBytes(uncompressedData).Hash()

		xorb.Chunks = append(xorb.Chunks, ChunkData{
			UncompressedData: uncompressedData,
			CompressedData:   compressedData,
			CompressionType:  header.CompressionType,
			Hash:             chunkHash,
		})
		xorb.ChunkHashes = append(xorb.ChunkHashes, chunkHash)
	}

	// Compute xorb hash from chunks
	if len(xorb.Chunks) > 0 {
		chunkSizes := make([]uint64, len(xorb.Chunks))
		for i, chunk := range xorb.Chunks {
			chunkSizes[i] = uint64(len(chunk.UncompressedData))
		}
		xorb.Hash = xet.ComputeXorbHash(xorb.ChunkHashes, chunkSizes)
	}

	return xorb, nil
}

// readFooterFromStream reads the footer from the stream.
// headerBuf contains the first 8 bytes we already read (starting with XETBLOB).
func (x *Xorb) readFooterFromStream(r io.Reader, headerBuf []byte) error {
	// We've already read 8 bytes (first 7 are "XETBLOB", 8th is version byte)
	// The footer structure is:
	// - XETBLOB (7 bytes) - already in headerBuf[0:7]
	// - version (1 byte) - already in headerBuf[7]
	// - xorb hash (32 bytes)
	// - Hash section header (7 bytes "XBLBHSH")
	// - Hash section version (1 byte)
	// - num_chunks (4 bytes)
	// - chunk hashes (32 * num_chunks bytes)
	// - Boundary section header (7 bytes "XBLBBND")
	// - ... rest of boundary section
	// - Trailer (28 bytes)
	// - footer length (4 bytes) at the very end

	// We need to read the entire footer to parse it properly
	// The footer length is at the very END, so we need to buffer it
	// Let's read everything remaining and parse from that buffer
	footerData, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("failed to read footer data: %w", err)
	}

	// Combine headerBuf with footerData
	fullFooter := make([]byte, 0, len(headerBuf)+len(footerData))
	fullFooter = append(fullFooter, headerBuf...)
	fullFooter = append(fullFooter, footerData...)

	// The last 4 bytes should be the footer length
	if len(fullFooter) < 4 {
		return fmt.Errorf("footer too short")
	}

	footerLen := binary.LittleEndian.Uint32(fullFooter[len(fullFooter)-4:])

	// Verify the footer length matches what we have
	// The footer length does NOT include the final 4-byte length field itself
	if int(footerLen) != len(fullFooter)-4 {
		return fmt.Errorf("footer length mismatch: expected %d, got %d", len(fullFooter)-4, footerLen)
	}

	// Parse the footer (excluding the final 4-byte length)
	return x.parseFooter(fullFooter[:len(fullFooter)-4])
}

// parseFooter parses the CasObjectInfo footer
func (x *Xorb) parseFooter(data []byte) error {
	offset := 0

	// Main Header: XETBLOB (7), version (1), xorb hash (32)
	if offset+40 > len(data) {
		return fmt.Errorf("footer too short for main header")
	}

	if string(data[offset:offset+7]) != XorbIdentifier {
		return fmt.Errorf("invalid xorb identifier")
	}
	offset += 7

	version := data[offset]
	if version != 1 {
		return fmt.Errorf("unsupported xorb version: %d (expected 1)", version)
	}
	offset++

	copy(x.Hash[:], data[offset:offset+32])
	offset += 32

	// Hash Section: XBLBHSH (7), version (1), num_chunks (4), hashes
	if offset+12 > len(data) {
		return fmt.Errorf("footer too short for hash section header")
	}

	if string(data[offset:offset+7]) != HashSectionIdent {
		return fmt.Errorf("invalid hash section identifier")
	}
	offset += 7

	version = data[offset]
	if version != 0 {
		return fmt.Errorf("unsupported hash section version: %d", version)
	}
	offset++

	numChunks := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	// Read chunk hashes
	if offset+int(numChunks)*32 > len(data) {
		return fmt.Errorf("footer too short for chunk hashes")
	}

	x.ChunkHashes = make([]xet.Hash, numChunks)
	for i := range numChunks {
		copy(x.ChunkHashes[i][:], data[offset:offset+32])
		offset += 32
	}

	// We can skip parsing the boundary section for now
	// It's mainly used for reconstruction and not needed for basic deserialization

	return nil
}
