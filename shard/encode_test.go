package shard

import (
	"bytes"
	"io"
	"sync"
	"testing"
	"time"
)

func encodeTestShard() *Shard {
	s := NewShard()
	s.AddFile(FileBlock{
		FileHash: [32]byte{1},
		Entries:  []FileDataSequenceEntry{{CASHash: [32]byte{2}, UnpackedSegBytes: 10, ChunkIndexEnd: 1}},
	})
	s.AddCASBlock(CASBlock{
		CASHash:       [32]byte{2},
		Chunks:        []CASChunkSequenceEntry{{ChunkHash: [32]byte{3}, UnpackedSegBytes: 10}},
		NumBytesInCAS: 10,
	})
	return s
}

func TestEncodeLeavesShardUnmodified(t *testing.T) {
	s := encodeTestShard()
	r, err := s.Encode(true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(r); err != nil {
		t.Fatal(err)
	}
	if s.Footer != nil {
		t.Fatal("Encode() created a footer on the shard")
	}
	if s.FooterSize != 0 {
		t.Fatalf("Encode() set FooterSize = %d", s.FooterSize)
	}

	// An existing footer keeps its offsets: encoding fills in a private copy.
	s.SetFooter(time.Now())
	before := *s.Footer
	r, err = s.Encode(true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(r); err != nil {
		t.Fatal(err)
	}
	if *s.Footer != before {
		t.Fatalf("Encode() modified the footer: %+v -> %+v", before, *s.Footer)
	}
}

// TestEncodeIsConcurrencySafe serves one shard from several goroutines, as the
// dedup endpoints do with a cached shard. Run with -race.
func TestEncodeIsConcurrencySafe(t *testing.T) {
	s := encodeTestShard()
	s.SetFooter(time.Now())

	want, err := s.Encode(true)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := io.ReadAll(want)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	results := make([][]byte, 8)
	for i := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := s.Encode(true)
			if err != nil {
				t.Error(err)
				return
			}
			data, err := io.ReadAll(r)
			if err != nil {
				t.Error(err)
				return
			}
			results[i] = data
		}()
	}
	wg.Wait()

	for i, got := range results {
		if !bytes.Equal(got, expected) {
			t.Fatalf("concurrent encode %d produced different bytes", i)
		}
	}
}
