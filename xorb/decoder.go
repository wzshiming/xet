package xorb

import (
	"bytes"
	"fmt"
	"io"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/internal/pool"
)

// Decoder reads xorb data chunk-by-chunk from an io.Reader.
// Call Decode repeatedly to get each chunk's uncompressed data.
// After Decode returns io.EOF, call SummoryHash to retrieve the overall xorb hash.
// Call Close to release any associated resources (e.g. the HTTP response body).
type Decoder struct {
	r          io.Reader
	withFooter bool

	chunkHashes []xet.ChunkHash
	chunkSizes  []uint64
	rawSize     uint64

	done     bool
	xorbHash *xet.XorbHash
	err      error
}

func NewDecoder(r io.Reader, withFooter bool) *Decoder {
	return &Decoder{
		r:          r,
		withFooter: withFooter,
	}
}

// Decode reads and returns the next chunk's uncompressed data.
// Returns io.EOF when all chunks have been consumed.
func (d *Decoder) Read(p []byte) (int, error) {
	if d.err != nil {
		return 0, d.err
	}
	if d.done {
		return 0, io.EOF
	}

	var headerBuf [8]byte
	_, err := io.ReadFull(d.r, headerBuf[:])
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		if d.withFooter {
			if err == io.EOF {
				d.err = fmt.Errorf("unexpected EOF: expected footer")
				return 0, d.err
			}
			d.err = fmt.Errorf("failed to read data: %w", err)
			return 0, d.err
		}
		d.done = true
		return 0, io.EOF
	}
	if err != nil {
		d.err = fmt.Errorf("failed to read chunk header: %w", err)
		return 0, d.err
	}

	// Check for footer start (XETBLOB identifier)
	if bytes.Equal(headerBuf[:7], xorbIdentifier[:]) {
		if !d.withFooter {
			d.done = true
			return 0, io.EOF
		}

		tmp := pool.GetChunkBuf()
		defer pool.PutChunkBuf(tmp)
		err := validateWithFooter(d.r, tmp[:], headerBuf, d.SummoryHash(), d.chunkHashes, d.chunkSizes)
		if err != nil {
			d.err = fmt.Errorf("validate footer: %w", err)
			return 0, d.err
		}

		d.done = true
		return 0, io.EOF
	}

	// Parse chunk header (8 bytes):
	//   [0]    version
	//   [1-3]  compressed size (little-endian, 3 bytes)
	//   [4]    compression type
	//   [5-7]  uncompressed size (little-endian, 3 bytes)
	if headerBuf[0] != 0 {
		d.err = fmt.Errorf("unsupported chunk version: %d", headerBuf[0])
		return 0, d.err
	}
	compressedSize := uint32(headerBuf[1]) | uint32(headerBuf[2])<<8 | uint32(headerBuf[3])<<16
	ct := compressionType(headerBuf[4])
	uncompressedSize := uint32(headerBuf[5]) | uint32(headerBuf[6])<<8 | uint32(headerBuf[7])<<16
	if compressedSize > uint32(len(p)) {
		d.err = fmt.Errorf("input buffer too small for compressed chunk: need %d bytes, have %d", compressedSize, len(p))
		return 0, d.err
	}
	if uncompressedSize > xet.MaxChunkSize {
		d.err = fmt.Errorf("invalid uncompressed chunk size: %d exceeds maximum %d", uncompressedSize, xet.MaxChunkSize)
		return 0, d.err
	}
	if len(d.chunkHashes) >= xet.MaxChunksPerXorb {
		d.err = fmt.Errorf("chunk count exceeds maximum %d", xet.MaxChunksPerXorb)
		return 0, d.err
	}
	if d.rawSize > xet.MaxXorbSize || uint64(uncompressedSize) > xet.MaxXorbSize-d.rawSize {
		d.err = fmt.Errorf("raw payload size exceeds maximum %d", xet.MaxXorbSize)
		return 0, d.err
	}

	if _, err := io.ReadFull(d.r, p[:compressedSize]); err != nil {
		d.err = fmt.Errorf("failed to read chunk data: %w", err)
		return 0, d.err
	}

	tmp := pool.GetChunkBuf()
	defer pool.PutChunkBuf(tmp)
	uncompressed, err := decompressChunk(tmp[:0], p[:compressedSize], ct, int(uncompressedSize))
	if err != nil {
		d.err = fmt.Errorf("decompress chunk: %w", err)
		return 0, d.err
	}

	h := xet.ComputeChunkHash(uncompressed)
	d.chunkHashes = append(d.chunkHashes, h)
	d.chunkSizes = append(d.chunkSizes, uint64(uncompressedSize))
	d.rawSize += uint64(uncompressedSize)

	if len(uncompressed) > len(p) {
		d.err = fmt.Errorf("output buffer too small: need %d bytes", len(uncompressed))
		return 0, d.err
	}

	return copy(p, uncompressed), nil
}

// SummoryHash returns the overall xorb hash.
// When withFooter is true, the hash is taken directly from the footer after Decode returns io.EOF.
// Otherwise it is computed from all decoded chunks.
func (d *Decoder) SummoryHash() xet.XorbHash {
	if d.xorbHash == nil {
		hash := xet.ComputeXorbHash(d.chunkHashes, d.chunkSizes)
		d.xorbHash = &hash
	}
	return *d.xorbHash
}
