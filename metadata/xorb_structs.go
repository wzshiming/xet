package metadata

import (
	"io"

	"github.com/wzshiming/xet/merklehash"
)

// XorbChunkSequenceHeader represents the header for a XORB chunk sequence.
type XorbChunkSequenceHeader struct {
	XorbHash       merklehash.MerkleHash
	XorbFlags      uint32
	NumEntries     uint32
	NumBytesInXorb uint32
	NumBytesOnDisk uint32
}

// NewXorbChunkSequenceHeader creates a new XorbChunkSequenceHeader.
func NewXorbChunkSequenceHeader(xorbHash merklehash.MerkleHash, numEntries uint32, numBytesInXorb uint32) XorbChunkSequenceHeader {
	return XorbChunkSequenceHeader{
		XorbHash:       xorbHash,
		NumEntries:     numEntries,
		NumBytesInXorb: numBytesInXorb,
	}
}

// BookendXorbHeader returns a bookend XORB header (marker hash, all 1s).
func BookendXorbHeader() XorbChunkSequenceHeader {
	return XorbChunkSequenceHeader{
		XorbHash: merklehash.Marker(),
	}
}

// IsBookend returns true if this header is a bookend (marker hash).
func (h XorbChunkSequenceHeader) IsBookend() bool {
	return h.XorbHash == merklehash.Marker()
}

// Serialize writes the header to the writer. Returns bytes written and any error.
func (h XorbChunkSequenceHeader) Serialize(w io.Writer) (int, error) {
	if err := writeHash(w, h.XorbHash); err != nil {
		return 0, err
	}
	if err := writeU32(w, h.XorbFlags); err != nil {
		return 32, err
	}
	if err := writeU32(w, h.NumEntries); err != nil {
		return 36, err
	}
	if err := writeU32(w, h.NumBytesInXorb); err != nil {
		return 40, err
	}
	if err := writeU32(w, h.NumBytesOnDisk); err != nil {
		return 44, err
	}
	return MDBFileInfoEntrySize, nil
}

// DeserializeXorbChunkSequenceHeader reads a XorbChunkSequenceHeader from the reader.
func DeserializeXorbChunkSequenceHeader(r io.Reader) (XorbChunkSequenceHeader, error) {
	h := XorbChunkSequenceHeader{}
	var err error
	if h.XorbHash, err = readHash(r); err != nil {
		return h, err
	}
	if h.XorbFlags, err = readU32(r); err != nil {
		return h, err
	}
	if h.NumEntries, err = readU32(r); err != nil {
		return h, err
	}
	if h.NumBytesInXorb, err = readU32(r); err != nil {
		return h, err
	}
	if h.NumBytesOnDisk, err = readU32(r); err != nil {
		return h, err
	}
	return h, nil
}

// XorbChunkSequenceEntry represents an entry in a XORB chunk sequence.
type XorbChunkSequenceEntry struct {
	ChunkHash            merklehash.MerkleHash
	ChunkByteRangeStart  uint32
	UnpackedSegmentBytes uint32
	Flags                uint32
	Unused               uint32
}

// NewXorbChunkSequenceEntry creates a new XorbChunkSequenceEntry.
func NewXorbChunkSequenceEntry(chunkHash merklehash.MerkleHash, unpackedSegmentBytes uint32, chunkByteRangeStart uint32) XorbChunkSequenceEntry {
	return XorbChunkSequenceEntry{
		ChunkHash:            chunkHash,
		ChunkByteRangeStart:  chunkByteRangeStart,
		UnpackedSegmentBytes: unpackedSegmentBytes,
	}
}

// WithGlobalDedupFlag returns a copy of the entry with the global dedup flag set or cleared.
func (e XorbChunkSequenceEntry) WithGlobalDedupFlag(isGlobalDedup bool) XorbChunkSequenceEntry {
	if isGlobalDedup {
		e.Flags |= MDBChunkWithGlobalDedupFlag
	} else {
		e.Flags &^= MDBChunkWithGlobalDedupFlag
	}
	return e
}

// IsGlobalDedupEligible returns true if the entry is eligible for global dedup,
// either by flag or by hash modulus.
func (e XorbChunkSequenceEntry) IsGlobalDedupEligible() bool {
	if e.Flags&MDBChunkWithGlobalDedupFlag != 0 {
		return true
	}
	return HashIsGlobalDedupEligible(e.ChunkHash)
}

// Serialize writes the entry to the writer. Returns bytes written and any error.
func (e XorbChunkSequenceEntry) Serialize(w io.Writer) (int, error) {
	if err := writeHash(w, e.ChunkHash); err != nil {
		return 0, err
	}
	if err := writeU32(w, e.ChunkByteRangeStart); err != nil {
		return 32, err
	}
	if err := writeU32(w, e.UnpackedSegmentBytes); err != nil {
		return 36, err
	}
	if err := writeU32(w, e.Flags); err != nil {
		return 40, err
	}
	if err := writeU32(w, e.Unused); err != nil {
		return 44, err
	}
	return MDBFileInfoEntrySize, nil
}

// DeserializeXorbChunkSequenceEntry reads a XorbChunkSequenceEntry from the reader.
func DeserializeXorbChunkSequenceEntry(r io.Reader) (XorbChunkSequenceEntry, error) {
	e := XorbChunkSequenceEntry{}
	var err error
	if e.ChunkHash, err = readHash(r); err != nil {
		return e, err
	}
	if e.ChunkByteRangeStart, err = readU32(r); err != nil {
		return e, err
	}
	if e.UnpackedSegmentBytes, err = readU32(r); err != nil {
		return e, err
	}
	if e.Flags, err = readU32(r); err != nil {
		return e, err
	}
	if e.Unused, err = readU32(r); err != nil {
		return e, err
	}
	return e, nil
}

// ChunkBoundary represents a chunk boundary with its hash and byte range.
type ChunkBoundary struct {
	ChunkHash merklehash.MerkleHash
	Start     uint32
	End       uint32
}

// MDBXorbInfo represents the complete XORB info structure in an MDB shard.
type MDBXorbInfo struct {
	Metadata XorbChunkSequenceHeader
	Chunks   []XorbChunkSequenceEntry
}

// NumBytes returns the total serialized size in bytes.
func (x *MDBXorbInfo) NumBytes() uint64 {
	return uint64(1+len(x.Chunks)) * MDBFileInfoEntrySize
}

// Serialize writes the complete XORB info to the writer.
func (x *MDBXorbInfo) Serialize(w io.Writer) (int, error) {
	total := 0
	n, err := x.Metadata.Serialize(w)
	total += n
	if err != nil {
		return total, err
	}
	for _, chunk := range x.Chunks {
		n, err = chunk.Serialize(w)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// DeserializeMDBXorbInfo reads a complete MDBXorbInfo from the reader.
// Returns nil if the header is a bookend.
func DeserializeMDBXorbInfo(r io.Reader) (*MDBXorbInfo, error) {
	header, err := DeserializeXorbChunkSequenceHeader(r)
	if err != nil {
		return nil, err
	}
	if header.IsBookend() {
		return nil, nil
	}

	info := &MDBXorbInfo{
		Metadata: header,
		Chunks:   make([]XorbChunkSequenceEntry, header.NumEntries),
	}

	for i := uint32(0); i < header.NumEntries; i++ {
		if info.Chunks[i], err = DeserializeXorbChunkSequenceEntry(r); err != nil {
			return nil, err
		}
	}

	return info, nil
}

// ChunksAndBoundaries returns the chunk boundaries for all chunks in the XORB.
func (x *MDBXorbInfo) ChunksAndBoundaries() []ChunkBoundary {
	boundaries := make([]ChunkBoundary, len(x.Chunks))
	for i, chunk := range x.Chunks {
		var end uint32
		if i+1 < len(x.Chunks) {
			end = x.Chunks[i+1].ChunkByteRangeStart
		} else {
			end = x.Metadata.NumBytesInXorb
		}
		boundaries[i] = ChunkBoundary{
			ChunkHash: chunk.ChunkHash,
			Start:     chunk.ChunkByteRangeStart,
			End:       end,
		}
	}
	return boundaries
}
