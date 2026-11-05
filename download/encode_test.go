package download

import (
	"bytes"
	"fmt"
	"reflect"
	"slices"
	"testing"

	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage/storagetest"
	"github.com/wzshiming/xet/xorb"
)

// fetches is the fetch info the builders answer with for ranges of chunks: an entry or descriptor of each, per xorb.
func (f *tsFixture) fetches(t *testing.T, ranges []tsTerm) (map[string][]FetchInfoEntry, map[string][]XorbMultiRangeFetch) {
	t.Helper()
	v1, v2 := map[string][]FetchInfoEntry{}, map[string][]XorbMultiRangeFetch{}
	for _, p := range ranges {
		hash := f.xorbs[p.x].String()
		url := "https://cas.example/xorbs/" + hash
		start, end, err := xorb.ChunkDataRangeFromOffsets(f.st.offsets[f.xorbs[p.x]], p.start, p.end)
		if err != nil {
			t.Fatal(err)
		}
		chunks, packed := ChunkRange{Start: p.start, End: p.end}, ByteRange{Start: start, End: end}
		v1[hash] = append(v1[hash], FetchInfoEntry{Range: chunks, URL: url, URLRange: packed})
		if v2[hash] == nil {
			v2[hash] = []XorbMultiRangeFetch{{URL: url}}
		}
		v2[hash][0].Ranges = append(v2[hash][0].Ranges, XorbRangeDescriptor{Chunks: chunks, Bytes: packed})
	}
	return v1, v2
}

// wantBuilt fails t, with the rule why, unless both builders answer the Range header with offset,
// the pieces as terms, and fetch info of the ranges.
func (f *tsFixture) wantBuilt(t *testing.T, rangeHeader string, offset int64, pieces, ranges []tsTerm, why string) {
	t.Helper()
	rng := f.parse(rangeHeader)
	terms := f.terms(pieces...)
	wantV1, wantV2 := f.fetches(t, ranges)
	v1, err := BuildReconstructionResponseV1(t.Context(), f.st, f.sh, f.file, rng)
	if err != nil {
		t.Fatal(err)
	}
	v2, err := BuildReconstructionResponseV2(t.Context(), f.st, f.sh, f.file, rng)
	if err != nil {
		t.Fatal(err)
	}
	if v1.OffsetIntoFirstRange != offset || !slices.Equal(v1.Terms, terms) || !reflect.DeepEqual(v1.FetchInfo, wantV1) {
		t.Errorf("BuildReconstructionResponseV1 with Range %q = offset %d, %s, fetch_info %s\nwant offset %d, %s, fetch_info %s\n(%s)", rangeHeader, v1.OffsetIntoFirstRange, f.format(v1.Terms), f.formatFetchInfo(v1.FetchInfo), offset, f.format(terms), f.formatFetchInfo(wantV1), why)
	}
	if v2.OffsetIntoFirstRange != offset || !slices.Equal(v2.Terms, terms) || !reflect.DeepEqual(v2.Xorbs, wantV2) {
		t.Errorf("BuildReconstructionResponseV2 with Range %q = offset %d, %s, fetch info %s\nwant offset %d, %s, fetch info %s\n(%s)", rangeHeader, v2.OffsetIntoFirstRange, f.format(v2.Terms), f.formatXorbs(v2.Xorbs), offset, f.format(terms), f.formatXorbs(wantV2), why)
	}
}

func TestBuildReconstructionTrimsToTheRange(t *testing.T) {
	// The file of #312: 4 xorbs of 1024 chunks of 64 KiB, a term each, under hf_xet's first three windows.
	issue := tsNewFixture(slices.Repeat([][]uint32{tsSizes(1024, 64<<10)}, 4), tsTerm{0, 0, 1024}, tsTerm{1, 0, 1024}, tsTerm{2, 0, 1024}, tsTerm{3, 0, 1024})
	// Chunks of 1000, 2000, 3000 and 4000 bytes: the whole xorb at [0, 10000), then chunks 1 and 2 at [10000, 15000).
	sizes := [][]uint32{{1000, 2000, 3000, 4000}}
	varied := tsNewFixture(sizes, tsTerm{0, 0, 4}, tsTerm{0, 1, 3})
	// Two terms of 16 chunks of 64 KiB, the first's xorb without a CAS block.
	noCAS := tsNewFixture([][]uint32{tsSizes(16, 64<<10), tsSizes(16, 64<<10)}, tsTerm{0, 0, 16}, tsTerm{1, 0, 16})
	noCAS.sh.CASInfos = noCAS.sh.CASInfos[1:]
	longCAS := tsNewFixture(sizes, tsTerm{0, 0, 4})
	longCAS.sh.CASInfos[0].Chunks[0].UnpackedSegBytes++
	shortCAS := tsNewFixture(sizes, tsTerm{0, 0, 4})
	shortCAS.sh.CASInfos[0].Chunks = shortCAS.sh.CASInfos[0].Chunks[:3]
	for _, c := range []struct {
		name        string
		f           *tsFixture
		rangeHeader string
		offset      int64
		want        []tsTerm
		ranges      []tsTerm // fetch ranges, if not one of each term
	}{
		{"whole file", varied, "", 0, []tsTerm{{0, 0, 4}, {0, 1, 3}}, nil},
		{"window ending in a term", issue, "bytes=0-255999999", 0, []tsTerm{{0, 0, 1024}, {1, 0, 1024}, {2, 0, 1024}, {3, 0, 835}}, nil},
		{"window starting in a term", issue, "bytes=256000000-767999999", 16384, []tsTerm{{3, 834, 1024}}, nil},
		{"window past the end", issue, "bytes=768000000-1023999999", 0, nil, nil},
		// The first term trimmed to start at chunk 1, as the second does, takes the second's range.
		{"start inside a later chunk", varied, "bytes=2500-", 1500, []tsTerm{{0, 1, 4}, {0, 1, 3}}, []tsTerm{{0, 1, 4}}},
		{"start at a chunk boundary", varied, "bytes=3000-", 0, []tsTerm{{0, 2, 4}, {0, 1, 3}}, nil},
		{"start inside a term's first chunk", varied, "bytes=11000-", 1000, []tsTerm{{0, 1, 3}}, nil},
		{"end at a chunk's last byte", varied, "bytes=0-2999", 0, []tsTerm{{0, 0, 2}}, nil},
		{"end at a chunk's first byte", varied, "bytes=0-3000", 0, []tsTerm{{0, 0, 3}}, nil},
		{"one byte", varied, "bytes=4000-4000", 1000, []tsTerm{{0, 2, 3}}, nil},
		{"across two terms", varied, "bytes=9999-10000", 3999, []tsTerm{{0, 3, 4}, {0, 1, 2}}, nil},
		{"suffix", varied, "bytes=-1", 2999, []tsTerm{{0, 2, 3}}, nil},
		{"xorb without CAS block", noCAS, "bytes=100000-1500000", 100000, []tsTerm{{0, 0, 16}, {1, 0, 7}}, nil},
		{"CAS block one byte past the term", longCAS, "bytes=2500-4000", 2500, []tsTerm{{0, 0, 4}}, nil},
		{"CAS block shorter than the term", shortCAS, "bytes=2500-4000", 2500, []tsTerm{{0, 0, 4}}, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			ranges := c.ranges
			if ranges == nil {
				ranges = c.want
			}
			c.f.wantBuilt(t, c.rangeHeader, c.offset, c.want, ranges, "the first and the last term keep only the chunks holding the first and the last byte requested, unless their xorb's CAS block in the shard is missing or disagrees with them")
		})
	}
}

func TestBuildReconstructionKeepsTheLongestRangePerStartChunk(t *testing.T) {
	// Xorb 0 has 150 chunks of 64 KiB, xorb 1 has 10.
	sizes := [][]uint32{tsSizes(150, 64<<10), tsSizes(10, 64<<10)}
	for _, c := range []struct {
		name        string
		terms       []tsTerm
		rangeHeader string
		offset      int64
		want        []tsTerm // the terms answered, if not the file's
		ranges      []tsTerm
	}{
		{"longer term later", []tsTerm{{0, 0, 100}, {1, 0, 10}, {0, 0, 150}}, "", 0, nil, []tsTerm{{0, 0, 150}, {1, 0, 10}}},
		{"shorter term later", []tsTerm{{0, 0, 150}, {1, 0, 10}, {0, 0, 100}}, "", 0, nil, []tsTerm{{0, 0, 150}, {1, 0, 10}}},
		{"identical terms", []tsTerm{{0, 0, 100}, {0, 0, 100}}, "", 0, nil, []tsTerm{{0, 0, 100}}},
		{"other start chunks in their order", []tsTerm{{0, 50, 100}, {0, 0, 50}, {0, 25, 75}}, "", 0, nil, []tsTerm{{0, 50, 100}, {0, 0, 50}, {0, 25, 75}}},
		// The Range starts in chunk 90 of the first term, the start chunk of the third.
		{"first term trimmed to a later term's start", []tsTerm{{0, 0, 100}, {1, 0, 10}, {0, 90, 150}}, "bytes=5899240-", 1000, []tsTerm{{0, 90, 100}, {1, 0, 10}, {0, 90, 150}}, []tsTerm{{0, 90, 150}, {1, 0, 10}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := tsNewFixture(sizes, c.terms...)
			want := c.want
			if want == nil {
				want = c.terms
			}
			f.wantBuilt(t, c.rangeHeader, c.offset, want, c.ranges, "hf_xet keys a xorb's blocks by start chunk: of the ranges sharing one, the longest is kept in place of the first")
		})
	}
}

// chunkBounds is the file offset of each chunk of f, then f's size.
func (f *tsStored) chunkBounds() []int64 {
	bounds := []int64{0}
	for _, e := range f.sh.Files[0].Entries {
		k := slices.IndexFunc(f.sh.CASInfos, func(b shard.CASBlock) bool { return b.CASHash == e.CASHash })
		for _, c := range f.sh.CASInfos[k].Chunks[e.ChunkIndexStart:e.ChunkIndexEnd] {
			bounds = append(bounds, bounds[len(bounds)-1]+int64(c.UnpackedSegBytes))
		}
	}
	return bounds
}

func TestBuildReconstructionReadersReadTheChunksRequested(t *testing.T) {
	for _, c := range storagetest.ChunkClasses {
		if c.Compression != xorb.CompressionNone && c.Compression != xorb.CompressionLZ4 {
			continue
		}
		t.Run(c.Name, func(t *testing.T) {
			// Xorb 0 recurs at its start and from chunk 2, so trimmed terms share a xorb with whole ones.
			f := tsNewStored(t, c, []int{1992294, 5 << 18}, tsTerm{0, 0, 3}, tsTerm{x: 0}, tsTerm{x: 1}, tsTerm{0, 2, 14})
			bounds := f.chunkBounds()
			if len(bounds) <= 31 {
				t.Fatalf("precondition: %d chunks, want more than 30 to pick ranges from", len(bounds)-1)
			}
			for _, rangeHeader := range []string{
				"",
				fmt.Sprintf("bytes=%d-%d", bounds[5]+1, bounds[20]),
				fmt.Sprintf("bytes=%d-%d", bounds[7], bounds[9]-1),
				fmt.Sprintf("bytes=%d-%d", bounds[10]+5, bounds[10]+5),
				fmt.Sprintf("bytes=%d-", bounds[30]+100),
				"bytes=-1000",
			} {
				rng := ParseByteRange(rangeHeader, int64(len(f.content)))
				first, _ := slices.BinarySearch(bounds, rng.Start+1)
				last, _ := slices.BinarySearch(bounds, rng.End+1)
				from, to := bounds[first-1], bounds[last]
				v1, err := BuildReconstructionResponseV1(t.Context(), f.st, f.sh, f.file, rng)
				if err != nil {
					t.Fatal(err)
				}
				v2, err := BuildReconstructionResponseV2(t.Context(), f.st, f.sh, f.file, rng)
				if err != nil {
					t.Fatal(err)
				}
				for _, got := range []struct {
					version        string
					offset, length int64
					data           []byte
				}{
					{"V1", v1.OffsetIntoFirstRange, ExpectedLengthV1(v1), f.readV1(t, v1)},
					{"V2", v2.OffsetIntoFirstRange, ExpectedLengthV2(v2), f.read(t, v2)},
				} {
					if got.offset != rng.Start-from || got.length != to-rng.Start || !bytes.Equal(got.data, f.content[rng.Start:to]) {
						t.Errorf("New%s over the builder's response with Range %q: offset %d, expected length %d, read %d bytes; want offset %d into the chunk holding byte %d, then the file bytes through the end of the chunk holding byte %d: %d", got.version, rangeHeader, got.offset, got.length, len(got.data), rng.Start-from, rng.Start, rng.End, to-rng.Start)
					}
				}
			}
		})
	}
}
