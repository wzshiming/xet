package xorb

import (
	"bytes"
	"fmt"
	"io"

	"github.com/wzshiming/xet"
)

// Decoder reads xorb data chunk-by-chunk from an io.Reader.
// Call Decode repeatedly to get each chunk's uncompressed data.
// After Decode returns io.EOF, call SummoryHash to retrieve the overall xorb hash.
// Call Close to release any associated resources (e.g. the HTTP response body).
type Decoder struct {
	r          io.Reader
	withFooter bool

	chunkHashes []xet.Hash
	chunkSizes  []uint64

	done     bool
	xorbHash *xet.Hash
	err      error
}

func NewDecoder(r io.Reader, withFooter bool) *Decoder {
	return &Decoder{
		r:          r,
		withFooter: withFooter,
	}
}

// Close releases any resources held by the Decoder, in particular the Closer set via SetCloser.
func (d *Decoder) Close() error {
	if closer, ok := d.r.(io.Closer); ok {
		closer.Close()
	}
	return nil
}

// Decode reads and returns the next chunk's uncompressed data.
// Returns io.EOF when all chunks have been consumed.
func (d *Decoder) Decode() ([]byte, error) {
	if d.err != nil {
		return nil, d.err
	}
	if d.done {
		return nil, io.EOF
	}

	var headerBuf [8]byte
	_, err := io.ReadFull(d.r, headerBuf[:])
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		if d.withFooter {
			if err == io.EOF {
				d.err = fmt.Errorf("unexpected EOF: expected footer")
				return nil, d.err
			}
			d.err = fmt.Errorf("failed to read data: %w", err)
			return nil, d.err
		}
		d.done = true
		return nil, io.EOF
	}
	if err != nil {
		d.err = fmt.Errorf("failed to read chunk header: %w", err)
		return nil, d.err
	}

	// Check for footer start (XETBLOB identifier)
	if bytes.Equal(headerBuf[:7], xorbIdentifier[:]) {
		if !d.withFooter {
			d.done = true
			return nil, io.EOF
		}
		info, err := readFooter(d.r, headerBuf[:])
		if err != nil {
			d.err = fmt.Errorf("failed to read footer: %w", err)
			return nil, d.err
		}
		d.xorbHash = &info.Hash
		d.done = true
		return nil, io.EOF
	}

	// Parse chunk header (8 bytes):
	//   [0]    version
	//   [1-3]  compressed size (little-endian, 3 bytes)
	//   [4]    compression type
	//   [5-7]  uncompressed size (little-endian, 3 bytes)
	if headerBuf[0] != 0 {
		d.err = fmt.Errorf("unsupported chunk version: %d", headerBuf[0])
		return nil, d.err
	}
	compressedSize := uint32(headerBuf[1]) | uint32(headerBuf[2])<<8 | uint32(headerBuf[3])<<16
	ct := compressionType(headerBuf[4])
	uncompressedSize := uint32(headerBuf[5]) | uint32(headerBuf[6])<<8 | uint32(headerBuf[7])<<16

	chunkData := make([]byte, compressedSize)
	if _, err := io.ReadFull(d.r, chunkData); err != nil {
		d.err = fmt.Errorf("failed to read chunk data: %w", err)
		return nil, d.err
	}

	uncompressed, err := decompressChunk(nil, chunkData, ct, int(uncompressedSize))
	if err != nil {
		d.err = fmt.Errorf("decompress chunk: %w", err)
		return nil, d.err
	}

	h := xet.ComputeChunkHash(uncompressed)
	d.chunkHashes = append(d.chunkHashes, h)
	d.chunkSizes = append(d.chunkSizes, uint64(uncompressedSize))

	return uncompressed, nil
}

// SummoryHash returns the overall xorb hash.
// When withFooter is true, the hash is taken directly from the footer after Decode returns io.EOF.
// Otherwise it is computed from all decoded chunks.
func (d *Decoder) SummoryHash() xet.Hash {
	if d.xorbHash == nil {
		hash := xet.ComputeXorbHash(d.chunkHashes, d.chunkSizes)
		d.xorbHash = &hash
	}
	return *d.xorbHash
}
