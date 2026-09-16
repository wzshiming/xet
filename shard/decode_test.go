package shard

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/wzshiming/xet"
)

// blockHeader builds a 48-byte file or CAS block header declaring numEntries.
func blockHeader(flags, numEntries uint32) []byte {
	var header [48]byte
	binary.LittleEndian.PutUint32(header[32:36], flags)
	binary.LittleEndian.PutUint32(header[36:40], numEntries)
	return header[:]
}

func encodedBytes(t *testing.T, s *Shard) []byte {
	t.Helper()
	r, err := s.Encode(false)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func heapBytesAllocated(fn func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

func TestDecodeDoesNotPreallocateUnreadEntries(t *testing.T) {
	const declared = 1 << 18
	empty := encodedBytes(t, NewShard()) // header, file bookend, CAS bookend
	for _, test := range []struct {
		name   string
		prefix []byte
	}{
		{"file entries", empty[:48]},
		{"CAS chunks", empty[:96]},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := append(bytes.Clone(test.prefix), blockHeader(0, declared)...)
			var err error
			allocated := heapBytesAllocated(func() {
				err = NewShard().Decode(bytes.NewReader(data), false)
			})
			if !errors.Is(err, io.EOF) {
				t.Fatalf("Decode() error = %v, want truncated stream", err)
			}
			if allocated > 1<<20 {
				t.Fatalf("Decode() allocated %d bytes for %d declared entries that were never read", allocated, declared)
			}
		})
	}
}

func TestDecodeTruncatedVerificationEntry(t *testing.T) {
	s := NewShard()
	s.AddFile(FileBlock{
		FileHash: [32]byte{1},
		Flags:    FileWithVerification,
		Entries: []FileDataSequenceEntry{
			{CASHash: [32]byte{2}, UnpackedSegBytes: 10, ChunkIndexEnd: 1},
			{CASHash: [32]byte{3}, UnpackedSegBytes: 20, ChunkIndexStart: 1, ChunkIndexEnd: 3},
		},
		Verification: []xet.VerificationHash{{4}, {5}},
	})
	// Header, file header, two entries, one verification entry, then half of the second.
	truncated := encodedBytes(t, s)[:48*5+16]
	err := NewShard().Decode(bytes.NewReader(truncated), false)
	if !errors.Is(err, io.ErrUnexpectedEOF) || !strings.Contains(err.Error(), "verification entry 1") {
		t.Fatalf("Decode() error = %v, want unexpected EOF in verification entry 1", err)
	}
}

func TestDecodeRoundTripsMultipleEntries(t *testing.T) {
	s := NewShard()
	s.AddFile(FileBlock{
		FileHash: [32]byte{1},
		Flags:    FileWithVerification,
		Entries: []FileDataSequenceEntry{
			{CASHash: [32]byte{2}, UnpackedSegBytes: 10, ChunkIndexEnd: 1},
			{CASHash: [32]byte{3}, UnpackedSegBytes: 20, ChunkIndexStart: 1, ChunkIndexEnd: 3},
		},
		Verification: []xet.VerificationHash{{4}, {5}},
	})
	s.AddCASBlock(CASBlock{
		CASHash:       [32]byte{2},
		NumBytesInCAS: 30,
		Chunks: []CASChunkSequenceEntry{
			{ChunkHash: [32]byte{6}, UnpackedSegBytes: 10},
			{ChunkHash: [32]byte{7}, ByteRangeStart: 10, UnpackedSegBytes: 20, Flags: ChunkGlobalDedupEligible},
		},
	})
	decoded := NewShard()
	if err := decoded.Decode(bytes.NewReader(encodedBytes(t, s)), false); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded.Files, s.Files) || !reflect.DeepEqual(decoded.CASInfos, s.CASInfos) {
		t.Fatalf("decoded files %+v CAS %+v, want %+v %+v", decoded.Files, decoded.CASInfos, s.Files, s.CASInfos)
	}
}
