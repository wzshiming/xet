package xorb

import (
	"encoding/binary"
	"fmt"
	"io"
	"slices"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/internal/pool"
)

// Encoder writes xorb data chunk-by-chunk to an io.Writer.
// Call Encode for each chunk, then Close to flush the footer (if withFooter),
// then SummoryHash to retrieve the overall xorb hash.
type Encoder struct {
	w               io.Writer
	withFooter      bool
	uniquenessNonce [4]byte

	// Accumulated per-chunk metadata
	chunkHashes     []xet.ChunkHash
	chunkSizes      []uint64
	chunkOffsets    []uint64 // cumulative packed byte end-positions (footer only)
	unpackedOffsets []uint64 // cumulative unpacked byte end-positions (footer only)

	packedPos   uint64
	unpackedPos uint64

	finalized bool
	xorbHash  *xet.XorbHash
	err       error
}

func NewEncoder(w io.Writer, withFooter bool) *Encoder {
	return &Encoder{
		w:          w,
		withFooter: withFooter,
	}
}

// SetUniquenessNonce stores nonce in the first four bytes of the footer
// buffer. It may be called before Close and does not affect the xorb hash.
func (e *Encoder) SetUniquenessNonce(nonce [4]byte) error {
	if e.err != nil {
		return e.err
	}
	if e.finalized {
		return fmt.Errorf("encoder already finalized")
	}
	e.uniquenessNonce = nonce
	return nil
}

// Write compresses chunk and writes the 8-byte header followed by compressed data to w.
func (e *Encoder) Write(chunk []byte) (int, error) {
	if e.err != nil {
		return 0, e.err
	}
	if e.finalized {
		e.err = fmt.Errorf("encoder already finalized")
		return 0, e.err
	}
	if len(chunk) > xet.MaxChunkSize {
		e.err = fmt.Errorf("chunk size %d exceeds maximum %d", len(chunk), xet.MaxChunkSize)
		return 0, e.err
	}
	if len(e.chunkHashes) >= xet.MaxChunksPerXorb {
		e.err = fmt.Errorf("chunk count would exceed maximum %d", xet.MaxChunksPerXorb)
		return 0, e.err
	}
	if e.unpackedPos > xet.MaxXorbSize || uint64(len(chunk)) > xet.MaxXorbSize-e.unpackedPos {
		e.err = fmt.Errorf("raw payload size would exceed maximum %d", xet.MaxXorbSize)
		return 0, e.err
	}

	tmp := pool.GetChunkBuf()
	defer pool.PutChunkBuf(tmp)
	compressed, compressionType, err := selectBestCompression(tmp[8:8], chunk)
	if err != nil {
		e.err = fmt.Errorf("failed to compress chunk: %w", err)
		return 0, e.err
	}

	compressedSize := uint32(len(compressed))
	uncompressedSize := uint32(len(chunk))

	// 8-byte chunk header
	tmp[0] = 0 // version
	tmp[1] = byte(compressedSize)
	tmp[2] = byte(compressedSize >> 8)
	tmp[3] = byte(compressedSize >> 16)
	tmp[4] = byte(compressionType)
	tmp[5] = byte(uncompressedSize)
	tmp[6] = byte(uncompressedSize >> 8)
	tmp[7] = byte(uncompressedSize >> 16)

	if _, err := e.w.Write(tmp[:8+compressedSize]); err != nil {
		e.err = fmt.Errorf("failed to write chunk data: %w", err)
		return 0, e.err
	}

	e.packedPos += 8 + uint64(compressedSize)
	e.unpackedPos += uint64(uncompressedSize)

	h := xet.ComputeChunkHash(chunk)
	e.chunkHashes = append(e.chunkHashes, h)
	e.chunkSizes = append(e.chunkSizes, uint64(uncompressedSize))

	if e.withFooter {
		e.chunkOffsets = append(e.chunkOffsets, e.packedPos)
		e.unpackedOffsets = append(e.unpackedOffsets, e.unpackedPos)
	}

	return len(chunk), nil
}

// Close writes the footer to w (if withFooter was set) and finalizes the encoder.
// It must be called exactly once after all Encode calls.
func (e *Encoder) Close() error {
	if e.err != nil {
		return e.err
	}
	if e.finalized {
		return nil
	}
	e.finalized = true

	if e.withFooter {
		if err := e.writeFooter(); err != nil {
			e.err = err
			return err
		}
	}

	return nil
}

// SummoryHash returns the overall xorb hash computed from all encoded chunks.
func (e *Encoder) SummoryHash() xet.XorbHash {
	if e.xorbHash == nil {
		hash := xet.ComputeXorbHash(e.chunkHashes, e.chunkSizes)
		e.xorbHash = &hash
	}
	return *e.xorbHash
}

// ChunkHashes returns a copy of the hashes of the chunks written so far.
func (e *Encoder) ChunkHashes() []xet.ChunkHash {
	return slices.Clone(e.chunkHashes)
}

// ChunkSizes returns a copy of the unpacked sizes of the chunks written so far.
func (e *Encoder) ChunkSizes() []uint64 {
	return slices.Clone(e.chunkSizes)
}

// writeFooter serializes and writes the full CasObjectInfo footer.
func (e *Encoder) writeFooter() error {
	numChunks := uint32(len(e.chunkHashes))

	// Pre-allocate exact footer size to avoid reallocation.
	// Layout: main header (40) + hash section (12 + numChunks*32) +
	//         boundary section (12 + numChunks*8) + trailer (28) + length (4)

	tmp := pool.GetChunkBuf()
	defer pool.PutChunkBuf(tmp)

	buf := tmp[:0]

	hash := e.SummoryHash()

	// Main Header: XETBLOB ident (7), version (1), xorb hash (32)
	buf = append(buf, xorbIdentifier[:]...)
	buf = append(buf, 1) // version - MUST be 1 per spec
	buf = append(buf, hash[:]...)

	// Hash Section: XBLBHSH ident (7), version (1), num_chunks (4), chunk hashes
	buf = append(buf, hashSectionIdent[:]...)
	buf = append(buf, 0) // version
	buf = binary.LittleEndian.AppendUint32(buf, numChunks)
	for _, h := range e.chunkHashes {
		buf = append(buf, h[:]...)
	}

	// Boundary Section: XBLBBND ident (7), version (1), num_chunks (4), packed offsets, unpacked offsets
	buf = append(buf, boundarySectionIdent[:]...)
	buf = append(buf, 1) // version - MUST be 1 per spec
	buf = binary.LittleEndian.AppendUint32(buf, numChunks)
	for _, offset := range e.chunkOffsets {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(offset))
	}
	for _, offset := range e.unpackedOffsets {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(offset))
	}

	// Trailer: num_chunks (4), hash_offset_from_end (4),
	// boundary_offset_from_end (4), footer buffer (16). The footer buffer starts
	// with an optional 4-byte uniqueness nonce followed by 12 reserved zero bytes.
	footerEndPos := len(buf) + 4 + 4 + 4 + 16

	hashOffsetFromEnd := uint32(footerEndPos - 40)
	boundarySectionStart := 40 + 8 + 4 + int(numChunks)*32
	boundaryOffsetFromEnd := uint32(footerEndPos - boundarySectionStart)

	buf = binary.LittleEndian.AppendUint32(buf, numChunks)
	buf = binary.LittleEndian.AppendUint32(buf, hashOffsetFromEnd)
	buf = binary.LittleEndian.AppendUint32(buf, boundaryOffsetFromEnd)
	buf = append(buf, e.uniquenessNonce[:]...)
	var reserved [12]byte
	buf = append(buf, reserved[:]...)

	// Trailing footer length (4 bytes)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(buf)))

	_, err := e.w.Write(buf)
	return err
}
