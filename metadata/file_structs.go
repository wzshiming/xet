package metadata

import (
	"encoding/binary"
	"io"

	"github.com/wzshiming/xet/merklehash"
)

// Helper serialization functions

func writeHash(w io.Writer, h merklehash.DataHash) error {
	_, err := w.Write(h.AsBytes())
	return err
}

func writeU32(w io.Writer, v uint32) error {
	return binary.Write(w, binary.LittleEndian, v)
}

func writeU64(w io.Writer, v uint64) error {
	return binary.Write(w, binary.LittleEndian, v)
}

func readHash(r io.Reader) (merklehash.DataHash, error) {
	var buf [32]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return merklehash.DataHash{}, err
	}
	return merklehash.FromSlice(buf[:])
}

func readU32(r io.Reader) (uint32, error) {
	var v uint32
	err := binary.Read(r, binary.LittleEndian, &v)
	return v, err
}

func readU64(r io.Reader) (uint64, error) {
	var v uint64
	err := binary.Read(r, binary.LittleEndian, &v)
	return v, err
}

// FileDataSequenceHeader represents the header for a file data sequence in the MDB shard.
type FileDataSequenceHeader struct {
	FileHash   merklehash.MerkleHash
	FileFlags  uint32
	NumEntries uint32
	Unused     uint64
}

// NewFileDataSequenceHeader creates a new FileDataSequenceHeader.
func NewFileDataSequenceHeader(fileHash merklehash.MerkleHash, numEntries uint32, containsVerification bool, containsMetadataExt bool) FileDataSequenceHeader {
	flags := MDBDefaultFileFlag
	if containsVerification {
		flags |= MDBFileFlagWithVerification
	}
	if containsMetadataExt {
		flags |= MDBFileFlagWithMetadataExt
	}
	return FileDataSequenceHeader{
		FileHash:   fileHash,
		FileFlags:  flags,
		NumEntries: numEntries,
	}
}

// BookendFileHeader returns a bookend file header (marker hash, all 1s).
func BookendFileHeader() FileDataSequenceHeader {
	return FileDataSequenceHeader{
		FileHash: merklehash.Marker(),
	}
}

// IsBookend returns true if this header is a bookend (marker hash).
func (h FileDataSequenceHeader) IsBookend() bool {
	return h.FileHash == merklehash.Marker()
}

// ContainsVerification returns true if the verification flag is set.
func (h FileDataSequenceHeader) ContainsVerification() bool {
	return h.FileFlags&MDBFileFlagVerificationMask != 0
}

// ContainsMetadataExt returns true if the metadata extension flag is set.
func (h FileDataSequenceHeader) ContainsMetadataExt() bool {
	return h.FileFlags&MDBFileFlagMetadataExtMask != 0
}

// NumInfoEntryFollowing returns the number of info entries following this header.
func (h FileDataSequenceHeader) NumInfoEntryFollowing() uint32 {
	n := h.NumEntries
	if h.ContainsVerification() {
		n *= 2
	}
	if h.ContainsMetadataExt() {
		n++
	}
	return n
}

// Serialize writes the header to the writer. Returns bytes written and any error.
func (h FileDataSequenceHeader) Serialize(w io.Writer) (int, error) {
	if err := writeHash(w, h.FileHash); err != nil {
		return 0, err
	}
	if err := writeU32(w, h.FileFlags); err != nil {
		return 32, err
	}
	if err := writeU32(w, h.NumEntries); err != nil {
		return 36, err
	}
	if err := writeU64(w, h.Unused); err != nil {
		return 40, err
	}
	return MDBFileInfoEntrySize, nil
}

// DeserializeFileDataSequenceHeader reads a FileDataSequenceHeader from the reader.
func DeserializeFileDataSequenceHeader(r io.Reader) (FileDataSequenceHeader, error) {
	h := FileDataSequenceHeader{}
	var err error
	if h.FileHash, err = readHash(r); err != nil {
		return h, err
	}
	if h.FileFlags, err = readU32(r); err != nil {
		return h, err
	}
	if h.NumEntries, err = readU32(r); err != nil {
		return h, err
	}
	if h.Unused, err = readU64(r); err != nil {
		return h, err
	}
	return h, nil
}

// FileDataSequenceEntry represents a segment entry in a file data sequence.
type FileDataSequenceEntry struct {
	XorbHash             merklehash.MerkleHash
	XorbFlags            uint32
	UnpackedSegmentBytes uint32
	ChunkIndexStart      uint32
	ChunkIndexEnd        uint32
}

// Serialize writes the entry to the writer. Returns bytes written and any error.
func (e FileDataSequenceEntry) Serialize(w io.Writer) (int, error) {
	if err := writeHash(w, e.XorbHash); err != nil {
		return 0, err
	}
	if err := writeU32(w, e.XorbFlags); err != nil {
		return 32, err
	}
	if err := writeU32(w, e.UnpackedSegmentBytes); err != nil {
		return 36, err
	}
	if err := writeU32(w, e.ChunkIndexStart); err != nil {
		return 40, err
	}
	if err := writeU32(w, e.ChunkIndexEnd); err != nil {
		return 44, err
	}
	return MDBFileInfoEntrySize, nil
}

// DeserializeFileDataSequenceEntry reads a FileDataSequenceEntry from the reader.
func DeserializeFileDataSequenceEntry(r io.Reader) (FileDataSequenceEntry, error) {
	e := FileDataSequenceEntry{}
	var err error
	if e.XorbHash, err = readHash(r); err != nil {
		return e, err
	}
	if e.XorbFlags, err = readU32(r); err != nil {
		return e, err
	}
	if e.UnpackedSegmentBytes, err = readU32(r); err != nil {
		return e, err
	}
	if e.ChunkIndexStart, err = readU32(r); err != nil {
		return e, err
	}
	if e.ChunkIndexEnd, err = readU32(r); err != nil {
		return e, err
	}
	return e, nil
}

// FileVerificationEntry represents a verification entry for a file segment.
type FileVerificationEntry struct {
	RangeHash merklehash.MerkleHash
	Unused    [2]uint64
}

// Serialize writes the entry to the writer. Returns bytes written and any error.
func (e FileVerificationEntry) Serialize(w io.Writer) (int, error) {
	if err := writeHash(w, e.RangeHash); err != nil {
		return 0, err
	}
	if err := writeU64(w, e.Unused[0]); err != nil {
		return 32, err
	}
	if err := writeU64(w, e.Unused[1]); err != nil {
		return 40, err
	}
	return MDBFileInfoEntrySize, nil
}

// DeserializeFileVerificationEntry reads a FileVerificationEntry from the reader.
func DeserializeFileVerificationEntry(r io.Reader) (FileVerificationEntry, error) {
	e := FileVerificationEntry{}
	var err error
	if e.RangeHash, err = readHash(r); err != nil {
		return e, err
	}
	if e.Unused[0], err = readU64(r); err != nil {
		return e, err
	}
	if e.Unused[1], err = readU64(r); err != nil {
		return e, err
	}
	return e, nil
}

// FileMetadataExt represents extended metadata for a file.
type FileMetadataExt struct {
	SHA256 merklehash.DataHash
	Unused [2]uint64
}

// Serialize writes the entry to the writer. Returns bytes written and any error.
func (e FileMetadataExt) Serialize(w io.Writer) (int, error) {
	if err := writeHash(w, e.SHA256); err != nil {
		return 0, err
	}
	if err := writeU64(w, e.Unused[0]); err != nil {
		return 32, err
	}
	if err := writeU64(w, e.Unused[1]); err != nil {
		return 40, err
	}
	return MDBFileInfoEntrySize, nil
}

// DeserializeFileMetadataExt reads a FileMetadataExt from the reader.
func DeserializeFileMetadataExt(r io.Reader) (FileMetadataExt, error) {
	e := FileMetadataExt{}
	var err error
	if e.SHA256, err = readHash(r); err != nil {
		return e, err
	}
	if e.Unused[0], err = readU64(r); err != nil {
		return e, err
	}
	if e.Unused[1], err = readU64(r); err != nil {
		return e, err
	}
	return e, nil
}

// MDBFileInfo represents the complete file info structure in an MDB shard.
type MDBFileInfo struct {
	Metadata     FileDataSequenceHeader
	Segments     []FileDataSequenceEntry
	Verification []FileVerificationEntry
	MetadataExt  *FileMetadataExt
}

// NumBytes returns the total serialized size in bytes.
func (f *MDBFileInfo) NumBytes() uint64 {
	n := uint64(MDBFileInfoEntrySize) // header
	n += uint64(len(f.Segments)) * MDBFileInfoEntrySize
	n += uint64(len(f.Verification)) * MDBFileInfoEntrySize
	if f.MetadataExt != nil {
		n += MDBFileInfoEntrySize
	}
	return n
}

// FileSize returns the total uncompressed file size (sum of segment UnpackedSegmentBytes).
func (f *MDBFileInfo) FileSize() uint64 {
	var total uint64
	for _, seg := range f.Segments {
		total += uint64(seg.UnpackedSegmentBytes)
	}
	return total
}

// Serialize writes the complete file info to the writer.
func (f *MDBFileInfo) Serialize(w io.Writer) (int, error) {
	total := 0
	n, err := f.Metadata.Serialize(w)
	total += n
	if err != nil {
		return total, err
	}
	for _, seg := range f.Segments {
		n, err = seg.Serialize(w)
		total += n
		if err != nil {
			return total, err
		}
	}
	for _, ver := range f.Verification {
		n, err = ver.Serialize(w)
		total += n
		if err != nil {
			return total, err
		}
	}
	if f.MetadataExt != nil {
		n, err = f.MetadataExt.Serialize(w)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// DeserializeMDBFileInfo reads a complete MDBFileInfo from the reader.
// Returns nil if the header is a bookend.
func DeserializeMDBFileInfo(r io.Reader) (*MDBFileInfo, error) {
	header, err := DeserializeFileDataSequenceHeader(r)
	if err != nil {
		return nil, err
	}
	if header.IsBookend() {
		return nil, nil
	}

	info := &MDBFileInfo{
		Metadata: header,
		Segments: make([]FileDataSequenceEntry, header.NumEntries),
	}

	for i := uint32(0); i < header.NumEntries; i++ {
		if info.Segments[i], err = DeserializeFileDataSequenceEntry(r); err != nil {
			return nil, err
		}
	}

	if header.ContainsVerification() {
		info.Verification = make([]FileVerificationEntry, header.NumEntries)
		for i := uint32(0); i < header.NumEntries; i++ {
			if info.Verification[i], err = DeserializeFileVerificationEntry(r); err != nil {
				return nil, err
			}
		}
	}

	if header.ContainsMetadataExt() {
		ext, err := DeserializeFileMetadataExt(r)
		if err != nil {
			return nil, err
		}
		info.MetadataExt = &ext
	}

	return info, nil
}
