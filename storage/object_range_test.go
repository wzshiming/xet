package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"slices"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/xorb"
)

// rangeStore serves one stored xorb, recording each GetXorbReadSeekCloser call and the seekers it returned.
type rangeStore struct {
	Storage
	hash    xet.XorbHash
	data    []byte
	seekErr error // every Seek fails with it when set
	calls   int
	seekers []*rangeSeeker
}

// newRangeStore stores a footered three-chunk xorb, so every chunk range has stored bytes after it.
func newRangeStore(t *testing.T) *rangeStore {
	t.Helper()
	var buf bytes.Buffer
	enc := xorb.NewEncoder(&buf, true)
	for _, chunk := range []string{"first", "a second, longer chunk", "3"} {
		if _, err := enc.Write([]byte(chunk)); err != nil {
			t.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return &rangeStore{hash: enc.SummoryHash(), data: buf.Bytes()}
}

func (s *rangeStore) GetXorbReadSeekCloser(_ context.Context, h xet.XorbHash) (io.ReadSeekCloser, error) {
	s.calls++
	if h != s.hash {
		return nil, fmt.Errorf("xorb %s: %w", h, fs.ErrNotExist)
	}
	rs := &rangeSeeker{Reader: bytes.NewReader(s.data), seekErr: s.seekErr}
	s.seekers = append(s.seekers, rs)
	return rs, nil
}

// closes reports how often each returned seeker was closed, in opening order.
func (s *rangeStore) closes() []int {
	n := make([]int, len(s.seekers))
	for i, rs := range s.seekers {
		n[i] = rs.closes
	}
	return n
}

type rangeSeeker struct {
	*bytes.Reader
	seekErr error
	closes  int
}

func (r *rangeSeeker) Seek(offset int64, whence int) (int64, error) {
	if r.seekErr != nil {
		return 0, r.seekErr
	}
	return r.Reader.Seek(offset, whence)
}

func (r *rangeSeeker) Close() error {
	r.closes++
	return nil
}

func TestOpenXorbRangeReadsExactlyTheInclusiveRange(t *testing.T) {
	st := newRangeStore(t)
	offsets, err := ReadXorbChunkOffsets(bytes.NewReader(st.data))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name       string
		start, end int64
	}{
		{"one byte", 3, 3},
		{"single chunk", int64(offsets[0]), int64(offsets[1]) - 1},
		{"span", 0, int64(offsets[1]) - 1},
		{"whole xorb", 0, int64(len(st.data)) - 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rc, err := openXorbRange(t.Context(), st, st.hash, tc.start, tc.end)
			if err != nil {
				t.Fatalf("openXorbRange(%d, %d): %v", tc.start, tc.end, err)
			}
			defer rc.Close()
			want := st.data[tc.start : tc.end+1]
			got := make([]byte, len(want))
			if _, err := io.ReadFull(rc, got); err != nil || !bytes.Equal(got, want) {
				t.Fatalf("read of [%d, %d] = %x, %v; want the stored bytes %x", tc.start, tc.end, got, err, want)
			}
			if n, err := rc.Read(make([]byte, 1)); n != 0 || err != io.EOF {
				t.Fatalf("Read after byte %d = %d, %v; want 0, io.EOF", tc.end, n, err)
			}
		})
	}
}

func TestOpenXorbRangeRejectsInvalidRangesWithoutOpening(t *testing.T) {
	for _, tc := range []struct {
		name       string
		start, end int64
	}{
		{"negative start", -1, 3},
		{"start after end", 4, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := newRangeStore(t)
			if rc, err := openXorbRange(t.Context(), st, st.hash, tc.start, tc.end); err == nil {
				rc.Close()
				t.Fatalf("openXorbRange(%d, %d) succeeded; want an invalid range error", tc.start, tc.end)
			}
			if st.calls != 0 {
				t.Fatalf("openXorbRange(%d, %d) called GetXorbReadSeekCloser %d times; want 0", tc.start, tc.end, st.calls)
			}
		})
	}
}

func TestOpenXorbRangeShortXorbIsUnexpectedEOF(t *testing.T) {
	st := newRangeStore(t)
	start, end := int64(len(st.data))-4, int64(len(st.data))+5
	rc, err := openXorbRange(t.Context(), st, st.hash, start, end)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	got := make([]byte, end-start+1)
	n, err := io.ReadFull(rc, got)
	if err != io.ErrUnexpectedEOF || !bytes.Equal(got[:n], st.data[start:]) {
		t.Fatalf("ReadFull of [%d, %d] over %d stored bytes = %x, %v; want %x, io.ErrUnexpectedEOF", start, end, len(st.data), got[:n], err, st.data[start:])
	}
}

func TestOpenXorbRangeSeekErrorClosesTheSeekerOnce(t *testing.T) {
	errSeek := errors.New("seek failed")
	st := newRangeStore(t)
	st.seekErr = errSeek
	rc, err := openXorbRange(t.Context(), st, st.hash, 1, 2)
	if rc != nil || !errors.Is(err, errSeek) {
		t.Fatalf("openXorbRange with a failing Seek = %v, %v; want nil and an error wrapping %q", rc, err, errSeek)
	}
	if got := st.closes(); !slices.Equal(got, []int{1}) {
		t.Fatalf("closes of the seekers opened = %v; want [1]", got)
	}
}

func TestOpenXorbRangeMissingXorbIsNotExist(t *testing.T) {
	st := newRangeStore(t)
	if _, err := openXorbRange(t.Context(), st, xet.XorbHash{0xde, 0xad}, 1, 2); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("openXorbRange(missing xorb) = %v; want an error wrapping fs.ErrNotExist", err)
	}
}

func TestOpenXorbRangeCloseClosesItsOwnSeekerOnce(t *testing.T) {
	st := newRangeStore(t)
	var readers []io.ReadCloser
	for range 2 {
		rc, err := openXorbRange(t.Context(), st, st.hash, 1, 2)
		if err != nil {
			t.Fatal(err)
		}
		readers = append(readers, rc)
	}
	want := []int{0, 0}
	if got := st.closes(); !slices.Equal(got, want) {
		t.Fatalf("seeker closes before any Close = %v; want %v", got, want)
	}
	for i, rc := range readers {
		if err := rc.Close(); err != nil {
			t.Fatalf("Close of reader %d: %v", i, err)
		}
		want[i] = 1
		if got := st.closes(); !slices.Equal(got, want) {
			t.Fatalf("seeker closes after closing reader %d = %v; want %v", i, got, want)
		}
	}
}
