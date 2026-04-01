package xorb

import (
	"bytes"
	"fmt"
	"io"
)

// Decode deserializes a xorb from the given io.ReadSeeker.
func (x *Xorb) Decode(r io.ReadSeeker, withFooter bool) error {
	var headerBuf [8]byte
	for {
		n, err := io.ReadFull(r, headerBuf[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			if !withFooter {
				break
			}
			if err == io.EOF {
				return fmt.Errorf("unexpected EOF: expected footer")
			}
			return fmt.Errorf("failed to read data: %w", err)
		}
		if err != nil {
			return fmt.Errorf("failed to read chunk header: %w", err)
		}

		// Check if this is the start of the footer (XETBLOB identifier)
		if n >= 7 && bytes.Equal(headerBuf[:7], xorbIdentifier[:]) {
			if !withFooter {
				break
			}
			info, err := readFooter(r, headerBuf[:])
			if err != nil {
				return fmt.Errorf("failed to read footer: %w", err)
			}
			x.hash = &info.Hash
			// Pre-populate each chunk's hash from the footer, avoiding lazy decompression.
			for i := range x.chunks {
				if i < len(info.ChunkHashes) {
					h := info.ChunkHashes[i]
					x.chunks[i].hash = &h
				}
			}
			break
		}

		// Parse chunk header (8 bytes):
		//   [0]    version
		//   [1-3]  compressed size (little-endian, 3 bytes)
		//   [4]    compression type
		//   [5-7]  uncompressed size (little-endian, 3 bytes)
		version := headerBuf[0]
		if version != 0 {
			return fmt.Errorf("unsupported chunk version: %d", version)
		}
		compressedSize := uint32(headerBuf[1]) | uint32(headerBuf[2])<<8 | uint32(headerBuf[3])<<16
		ct := compressionType(headerBuf[4])
		uncompressedSize := uint32(headerBuf[5]) | uint32(headerBuf[6])<<8 | uint32(headerBuf[7])<<16

		// Current position (after reading the 8-byte header) is the start of compressed data.
		dataOffset, err := r.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("failed to get stream position: %w", err)
		}

		// Skip over the compressed data without reading it.
		if _, err := r.Seek(int64(compressedSize), io.SeekCurrent); err != nil {
			return fmt.Errorf("failed to skip chunk data: %w", err)
		}

		x.chunks = append(x.chunks, Chunk{
			compressedSrc:    r,
			compressedOffset: dataOffset,
			compressedSize:   compressedSize,
			compressedType:   ct,
			uncompressedSize: uncompressedSize,
		})
	}

	return nil
}
