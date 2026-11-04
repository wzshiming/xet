package download

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage/memory"
	"github.com/wzshiming/xet/storage/storagetest"
	"github.com/wzshiming/xet/xorb"
)

// tsTerm is chunks [start, end) of fixture xorb x; tsNewStored reads end 0 as the xorb's end.
type tsTerm struct {
	x          int
	start, end uint32
}

// tsSizes is n chunks of size unpacked bytes.
func tsSizes(n int, size uint32) []uint32 {
	return slices.Repeat([]uint32{size}, n)
}

// tsEvery cuts chunks [start, end) of xorb x into pieces of step chunks.
func tsEvery(x int, start, end, step uint32) []tsTerm {
	var pieces []tsTerm
	for s := start; s < end; s += step {
		pieces = append(pieces, tsTerm{x, s, min(s+step, end)})
	}
	return pieces
}

// tsAt cuts xorb x at the chunk bounds.
func tsAt(x int, bounds ...uint32) []tsTerm {
	pieces := make([]tsTerm, 0, len(bounds)-1)
	for i := range len(bounds) - 1 {
		pieces = append(pieces, tsTerm{x, bounds[i], bounds[i+1]})
	}
	return pieces
}

// tsAdapter serves the packed chunk offsets of xorbs that hold no bytes, so a 64 MiB term costs nothing.
type tsAdapter struct{ offsets map[xet.XorbHash][]uint64 }

func (a *tsAdapter) GetXorbURL(_ context.Context, xorbHash xet.XorbHash) (string, error) {
	return "https://cas.example/xorbs/" + xorbHash.String(), nil
}

func (a *tsAdapter) GetXorbChunkOffsets(_ context.Context, xorbHash xet.XorbHash) ([]uint64, error) {
	offsets, ok := a.offsets[xorbHash]
	if !ok {
		return nil, fmt.Errorf("no xorb %s", xorbHash)
	}
	return slices.Clone(offsets), nil
}

// tsFixture is a file of fileSize bytes in terms over xorbs of the given unpacked chunk sizes, their CAS blocks in its shard.
type tsFixture struct {
	st       *tsAdapter
	sh       *shard.Shard
	file     xet.FileHash
	fileSize int64
	xorbs    []xet.XorbHash
	sizes    [][]uint32
}

func tsNewFixture(sizes [][]uint32, terms ...tsTerm) *tsFixture {
	f := &tsFixture{st: &tsAdapter{offsets: map[xet.XorbHash][]uint64{}}, sh: shard.NewShard(), file: xet.FileHash{0xf1}, sizes: sizes}
	for x, chunks := range sizes {
		hash := xet.XorbHash{byte(x + 1), 0x5a}
		cb := shard.CASBlock{CASHash: hash}
		var packed uint64
		for i, size := range chunks {
			cb.Chunks = append(cb.Chunks, shard.CASChunkSequenceEntry{ByteRangeStart: cb.NumBytesInCAS, UnpackedSegBytes: size})
			cb.NumBytesInCAS += size
			// Packed sizes unlike the unpacked ones expose descriptors built from the wrong offsets.
			packed += 8 + uint64(size)/3 + uint64(i%7)
			f.st.offsets[hash] = append(f.st.offsets[hash], packed)
		}
		f.sh.AddCASBlock(cb)
		f.xorbs = append(f.xorbs, hash)
	}
	file := shard.FileBlock{FileHash: f.file}
	for _, term := range terms {
		entry := shard.FileDataSequenceEntry{CASHash: f.xorbs[term.x], ChunkIndexStart: term.start, ChunkIndexEnd: term.end}
		for _, size := range sizes[term.x][term.start:term.end] {
			entry.UnpackedSegBytes += size
		}
		file.Entries = append(file.Entries, entry)
		f.fileSize += int64(entry.UnpackedSegBytes)
	}
	f.sh.AddFile(file)
	return f
}

// tsTwoMiBTerms is a file of n terms of 2 MiB, each the whole of its own xorb of 64 KiB chunks.
func tsTwoMiBTerms(n int) (*tsFixture, []tsTerm) {
	sizes := make([][]uint32, n)
	terms := make([]tsTerm, n)
	for x := range n {
		sizes[x] = tsSizes(32, 64<<10)
		terms[x] = tsTerm{x, 0, 32}
	}
	return tsNewFixture(sizes, terms...), terms
}

// parse is the Range header parsed against the file's size, as the handlers parse it once per request.
func (f *tsFixture) parse(rangeHeader string) ByteRange {
	return ParseByteRange(rangeHeader, f.fileSize)
}

func (f *tsFixture) build(t *testing.T, rng ByteRange) *ReconstructionResponseV2 {
	t.Helper()
	in, err := BuildReconstructionResponseV2(t.Context(), f.st, f.sh, f.file, rng)
	if err != nil {
		t.Fatalf("BuildReconstructionResponseV2 with range %+v: %v", rng, err)
	}
	return in
}

// split returns SplitTermsV2's response, failing t on an error or a nil response.
func (f *tsFixture) split(t *testing.T, in *ReconstructionResponseV2, rng ByteRange, minTerms int, pieceBytes int64) *ReconstructionResponseV2 {
	t.Helper()
	out, err := SplitTermsV2(t.Context(), f.st, f.sh, in, rng, minTerms, pieceBytes)
	if err != nil || out == nil {
		t.Fatalf("SplitTermsV2(%d, %d) with range %+v = %v, %v; want a response", minTerms, pieceBytes, rng, out, err)
	}
	return out
}

// terms is the pieces as terms of their chunks' unpacked bytes.
func (f *tsFixture) terms(pieces ...tsTerm) []Term {
	terms := make([]Term, 0, len(pieces))
	for _, p := range pieces {
		term := Term{Hash: f.xorbs[p.x].String(), Range: ChunkRange{Start: p.start, End: p.end}}
		for _, size := range f.sizes[p.x][p.start:p.end] {
			term.UnpackedLength += uint64(size)
		}
		terms = append(terms, term)
	}
	return terms
}

// format names terms by fixture xorb, chunk range and unpacked bytes.
func (f *tsFixture) format(terms []Term) string {
	parts := make([]string, 0, len(terms))
	for _, term := range terms {
		x := slices.IndexFunc(f.xorbs, func(h xet.XorbHash) bool { return h.String() == term.Hash })
		parts = append(parts, fmt.Sprintf("x%d[%d,%d):%d", x, term.Range.Start, term.Range.End, term.UnpackedLength))
	}
	return strings.Join(parts, " ")
}

// wantTerms reports whether out's terms are the pieces, failing t with the rule why if not.
func (f *tsFixture) wantTerms(t *testing.T, call string, out *ReconstructionResponseV2, why string, pieces ...tsTerm) bool {
	t.Helper()
	want := f.terms(pieces...)
	if slices.Equal(out.Terms, want) {
		return true
	}
	t.Errorf("%s = %s\nwant %s\n(%s)", call, f.format(out.Terms), f.format(want), why)
	return false
}

// fetch is xorb x's fetch entry under in's URL: a descriptor per piece with the packed bytes of its chunks.
func (f *tsFixture) fetch(t *testing.T, in *ReconstructionResponseV2, x int, pieces ...tsTerm) []XorbMultiRangeFetch {
	t.Helper()
	hash := f.xorbs[x]
	fetch := XorbMultiRangeFetch{URL: in.Xorbs[hash.String()][0].URL}
	for _, p := range pieces {
		start, end, err := xorb.ChunkDataRangeFromOffsets(f.st.offsets[hash], p.start, p.end)
		if err != nil {
			t.Fatal(err)
		}
		fetch.Ranges = append(fetch.Ranges, XorbRangeDescriptor{Chunks: ChunkRange{Start: p.start, End: p.end}, Bytes: ByteRange{Start: start, End: end}})
	}
	return []XorbMultiRangeFetch{fetch}
}

// formatXorbs names fetch entries by fixture xorb.
func (f *tsFixture) formatXorbs(xorbs map[string][]XorbMultiRangeFetch) string {
	parts := []string{fmt.Sprintf("%d xorbs", len(xorbs))}
	for x, hash := range f.xorbs {
		for _, fetch := range xorbs[hash.String()] {
			var b strings.Builder
			fmt.Fprintf(&b, "x%d %s", x, fetch.URL)
			for _, rg := range fetch.Ranges {
				fmt.Fprintf(&b, " [%d,%d)@%d-%d", rg.Chunks.Start, rg.Chunks.End, rg.Bytes.Start, rg.Bytes.End)
			}
			parts = append(parts, b.String())
		}
	}
	return strings.Join(parts, "; ")
}

func tsSnapshot(t *testing.T, resp *ReconstructionResponseV2) []byte {
	t.Helper()
	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// tsStored is a file over xorbs stored in memory, for readers.
type tsStored struct {
	st      *memory.Storage
	sh      *shard.Shard
	file    xet.FileHash
	content []byte
}

// tsVariedSizes cuts total into chunks of 32 to 128 KiB, so cuts miss round sizes.
func tsVariedSizes(seed uint64, total int) []int {
	r := rand.New(rand.NewPCG(seed, seed))
	var sizes []int
	for total > 0 {
		size := min(total, 32<<10+r.IntN(96<<10))
		sizes = append(sizes, size)
		total -= size
	}
	return sizes
}

// tsNewStored stores xorb x as totals[x] bytes of class c in varied chunks, its CAS block in the shard.
func tsNewStored(t *testing.T, c storagetest.ChunkClass, totals []int, terms ...tsTerm) *tsStored {
	t.Helper()
	f := &tsStored{st: memory.NewStorage(), sh: shard.NewShard(), file: xet.FileHash{0xf5}}
	chunks := make([][][]byte, len(totals))
	var hashes []xet.XorbHash
	for x, total := range totals {
		var cb shard.CASBlock
		for i, size := range tsVariedSizes(uint64(x), total) {
			chunk := make([]byte, size)
			c.Chunk(x, i, chunk)
			chunks[x] = append(chunks[x], chunk)
			cb.Chunks = append(cb.Chunks, shard.CASChunkSequenceEntry{ChunkHash: xet.ComputeChunkHash(chunk), ByteRangeStart: cb.NumBytesInCAS, UnpackedSegBytes: uint32(size)})
			cb.NumBytesInCAS += uint32(size)
		}
		encoded, hash := storagetest.EncodeXorb(t, true, chunks[x]...)
		if _, err := f.st.PutXorb(t.Context(), hash, bytes.NewReader(encoded)); err != nil {
			t.Fatal(err)
		}
		cb.CASHash = hash
		f.sh.AddCASBlock(cb)
		hashes = append(hashes, hash)
	}
	file := shard.FileBlock{FileHash: f.file}
	for _, term := range terms {
		if term.end == 0 {
			term.end = uint32(len(chunks[term.x]))
		}
		entry := shard.FileDataSequenceEntry{CASHash: hashes[term.x], ChunkIndexStart: term.start, ChunkIndexEnd: term.end}
		for _, chunk := range chunks[term.x][term.start:term.end] {
			entry.UnpackedSegBytes += uint32(len(chunk))
			f.content = append(f.content, chunk...)
		}
		file.Entries = append(file.Entries, entry)
	}
	f.sh.AddFile(file)
	return f
}

func (f *tsStored) read(t *testing.T, resp *ReconstructionResponseV2) []byte {
	t.Helper()
	r, err := NewReaderV2(t.Context(), storageClient{f.st}, resp, WithCacheManager(NewCacheManager(t.TempDir(), 0)))
	if err != nil {
		t.Fatalf("NewReaderV2: %v", err)
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("NewReaderV2 read: %v", err)
	}
	return data
}

func TestSplitTermsV2MinTermsUpTo1IsOff(t *testing.T) {
	f := tsNewFixture([][]uint32{tsSizes(256, 64<<10)}, tsTerm{0, 0, 256})
	rng := f.parse("")
	in := f.build(t, rng)
	for _, minTerms := range []int{-1, 0, 1} {
		if out := f.split(t, in, rng, minTerms, 1<<20); out != in {
			t.Errorf("SplitTermsV2(%d, 1 MiB) of one 16 MiB term = %s, want its input itself: minTerms <= 1 is off", minTerms, f.format(out.Terms))
		}
	}
}

func TestSplitTermsV2GateReturnsInput(t *testing.T) {
	f, terms := tsTwoMiBTerms(17)
	for _, c := range []struct {
		rangeHeader string
		minTerms    int
	}{{"", 16}, {"", 17}, {"bytes=2097152-", 16}} {
		rng := f.parse(c.rangeHeader)
		in := f.build(t, rng)
		if out := f.split(t, in, rng, c.minTerms, 1<<20); out != in {
			t.Errorf("SplitTermsV2(%d, 1 MiB) of %d terms of 2 MiB = %s, want its input itself: the gate holds at len(terms) >= minTerms", c.minTerms, len(in.Terms), f.format(out.Terms))
		}
	}
	rng := f.parse("")
	out := f.split(t, f.build(t, rng), rng, 18, 1<<20)
	f.wantTerms(t, "SplitTermsV2(18, 1 MiB) of 17 terms of 2 MiB", out, "under the gate one cut, of the first tied term, reaches 18 terms", slices.Concat(tsAt(0, 0, 16, 32), terms[1:])...)
}

func TestSplitTermsV2GateAllocatesNothing(t *testing.T) {
	f, _ := tsTwoMiBTerms(17)
	rng := f.parse("")
	in := f.build(t, rng)
	ctx := t.Context()
	var st StorageAdapter = f.st
	for _, minTerms := range []int{0, 1, 16, 17} {
		var out *ReconstructionResponseV2
		allocs := testing.AllocsPerRun(100, func() { out, _ = SplitTermsV2(ctx, st, f.sh, in, rng, minTerms, 1<<20) })
		if out != in || allocs != 0 {
			t.Errorf("SplitTermsV2(%d, 1 MiB) of 17 terms returned its input %t after %v allocations, want it after none", minTerms, out == in, allocs)
		}
	}
}

func TestSplitTermsV2WithoutValidCutReturnsInput(t *testing.T) {
	big := tsNewFixture([][]uint32{tsSizes(256, 64<<10)}, tsTerm{0, 0, 256})
	noCAS := tsNewFixture([][]uint32{tsSizes(256, 64<<10)}, tsTerm{0, 0, 256})
	noCAS.sh.CASInfos = nil
	for _, c := range []struct {
		name        string
		f           *tsFixture
		rangeHeader string
	}{
		{"1 MiB term of 16 chunks", tsNewFixture([][]uint32{tsSizes(16, 64<<10)}, tsTerm{0, 0, 16}), ""},
		{"16 MiB term without CAS block", noCAS, ""},
		{"1.5 MiB requested of a 16 MiB term", big, "bytes=0-1572863"},
		{"no term in the range", big, "bytes=99999999-"},
	} {
		t.Run(c.name, func(t *testing.T) {
			rng := c.f.parse(c.rangeHeader)
			in := c.f.build(t, rng)
			if out := c.f.split(t, in, rng, 16, 1<<20); out != in {
				t.Errorf("SplitTermsV2(16, 1 MiB) with Range %q = %s, want its input itself: no cut leaves both pieces 1 MiB of requested bytes", c.rangeHeader, c.f.format(out.Terms))
			}
		})
	}
}

func TestSplitTermsV2HalvesOneTerm(t *testing.T) {
	for _, c := range []struct {
		name          string
		chunks, piece uint32
	}{
		{"16 MiB into 16 of 1 MiB", 256, 16},
		{"64 MiB into 16 of 4 MiB", 1024, 64},
		{"24 MiB into 16 of 1.5 MiB", 384, 24},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := tsNewFixture([][]uint32{tsSizes(int(c.chunks), 64<<10)}, tsTerm{0, 0, c.chunks})
			rng := f.parse("")
			out := f.split(t, f.build(t, rng), rng, 16, 1<<20)
			f.wantTerms(t, "SplitTermsV2(16, 1 MiB) of one term of 64 KiB chunks", out, "the largest term halves at its middle chunk boundary until there are 16 terms", tsEvery(0, 0, c.chunks, c.piece)...)
		})
	}
}

func TestSplitTermsV2CutsOnlyTheLargestTerm(t *testing.T) {
	// Fifteen terms: fourteen of 2 MiB around one of 64 MiB at index 7.
	sizes := make([][]uint32, 15)
	terms := make([]tsTerm, 15)
	for x := range 15 {
		n := uint32(32)
		if x == 7 {
			n = 1024
		}
		sizes[x] = tsSizes(int(n), 64<<10)
		terms[x] = tsTerm{x, 0, n}
	}
	f := tsNewFixture(sizes, terms...)
	rng := f.parse("")
	out := f.split(t, f.build(t, rng), rng, 16, 1<<20)
	f.wantTerms(t, "SplitTermsV2(16, 1 MiB) of fourteen 2 MiB terms and one of 64 MiB", out, "one cut of the 64 MiB term into 32 + 32 MiB reaches 16 terms", slices.Concat(terms[:7], tsAt(7, 0, 512, 1024), terms[8:])...)
}

func TestSplitTermsV2CutsLargestFirstLowestIndexOnTie(t *testing.T) {
	f := tsNewFixture([][]uint32{tsSizes(128, 64<<10), tsSizes(32, 64<<10)}, tsTerm{0, 0, 128}, tsTerm{1, 0, 32})
	rng := f.parse("")
	out := f.split(t, f.build(t, rng), rng, 4, 1<<20)
	f.wantTerms(t, "SplitTermsV2(4, 1 MiB) of an 8 and a 2 MiB term", out, "8 -> 4 + 4, then the first of the tied 4s -> 2 + 2: 2, 2, 4, 2 MiB", tsTerm{0, 0, 32}, tsTerm{0, 32, 64}, tsTerm{0, 64, 128}, tsTerm{1, 0, 32})
}

func TestSplitTermsV2CutsLargestRequestedLengthFirst(t *testing.T) {
	for _, c := range []struct {
		name        string
		chunks      [2]uint32
		rangeHeader string
		want        []tsTerm
	}{
		// 2 MiB of the 8 MiB term are requested, all 4 MiB of the next one.
		{"range starting late in the longer term", [2]uint32{128, 64}, "bytes=6291456-", []tsTerm{{0, 0, 128}, {1, 0, 32}, {1, 32, 64}}},
		// All 4 MiB of the first term are requested, 2 MiB of the 8 MiB one.
		{"range ending early in the longer term", [2]uint32{64, 128}, "bytes=0-6291455", []tsTerm{{0, 0, 32}, {0, 32, 64}, {1, 0, 128}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := tsNewFixture([][]uint32{tsSizes(int(c.chunks[0]), 64<<10), tsSizes(int(c.chunks[1]), 64<<10)}, tsTerm{0, 0, c.chunks[0]}, tsTerm{1, 0, c.chunks[1]})
			rng := f.parse(c.rangeHeader)
			out := f.split(t, f.build(t, rng), rng, 3, 1<<20)
			f.wantTerms(t, "SplitTermsV2(3, 1 MiB) with Range "+c.rangeHeader, out, "the term with the most requested bytes is cut, not the longest", c.want...)
		})
	}
}

func TestSplitTermsV2CutsAtBoundaryNearestMidpoint(t *testing.T) {
	f := tsNewFixture([][]uint32{{1 << 20, 1 << 20, 1 << 20, 5 << 20}}, tsTerm{0, 0, 4})
	rng := f.parse("")
	out := f.split(t, f.build(t, rng), rng, 2, 1<<20)
	f.wantTerms(t, "SplitTermsV2(2, 1 MiB) of chunks of 1, 1, 1 and 5 MiB", out, "of the boundaries at 1, 2 and 3 MiB, 3 MiB is nearest the 4 MiB midpoint", tsAt(0, 0, 3, 4)...)
}

func TestSplitTermsV2MidpointTieCutsAtSmallerBoundary(t *testing.T) {
	f := tsNewFixture([][]uint32{{1 << 20, 2 << 20, 1 << 20}}, tsTerm{0, 0, 3})
	rng := f.parse("")
	out := f.split(t, f.build(t, rng), rng, 2, 1<<20)
	f.wantTerms(t, "SplitTermsV2(2, 1 MiB) of chunks of 1, 2 and 1 MiB", out, "the boundaries at 1 and 3 MiB tie around the 2 MiB midpoint; the smaller wins", tsAt(0, 0, 1, 3)...)
}

func TestSplitTermsV2FloorBoundsEachPiece(t *testing.T) {
	// The 4 MiB term's only cut leaves a 0.5 MiB piece, so the 2 MiB term is cut instead.
	f := tsNewFixture([][]uint32{{7 << 19, 1 << 19}, tsSizes(32, 64<<10)}, tsTerm{0, 0, 2}, tsTerm{1, 0, 32})
	rng := f.parse("")
	out := f.split(t, f.build(t, rng), rng, 16, 1<<20)
	f.wantTerms(t, "SplitTermsV2(16, 1 MiB) of a 3.5 + 0.5 MiB term and a 2 MiB term", out, "a cut is valid only when both pieces hold pieceBytes; terms without a valid cut are passed over", tsTerm{0, 0, 2}, tsTerm{1, 0, 16}, tsTerm{1, 16, 32})
}

func TestSplitTermsV2PieceBytesUpTo0CutsDownToChunks(t *testing.T) {
	f := tsNewFixture([][]uint32{tsSizes(16, 64<<10)}, tsTerm{0, 0, 16})
	rng := f.parse("")
	in := f.build(t, rng)
	for _, pieceBytes := range []int64{0, -1} {
		out := f.split(t, in, rng, 16, pieceBytes)
		f.wantTerms(t, fmt.Sprintf("SplitTermsV2(16, %d) of one 1 MiB term", pieceBytes), out, "pieceBytes <= 0 sets no floor", tsEvery(0, 0, 16, 1)...)
	}
}

func TestSplitTermsV2FirstPieceCoversOffsetPlusOne(t *testing.T) {
	f := tsNewFixture([][]uint32{tsSizes(32, 64<<10)}, tsTerm{0, 0, 32})
	// OffsetIntoFirstRange is 1 MiB, the boundary after chunk 15.
	const rangeHeader = "bytes=1048576-"
	rng := f.parse(rangeHeader)
	out := f.split(t, f.build(t, rng), rng, 16, 0)
	f.wantTerms(t, "SplitTermsV2(16, 0) of one 2 MiB term with Range "+rangeHeader, out, "every cut lies past OffsetIntoFirstRange, so the first piece holds a requested byte", slices.Concat([]tsTerm{{0, 0, 17}}, tsEvery(0, 17, 32, 1))...)
}

func TestSplitTermsV2LastPieceHoldsTheRangeEnd(t *testing.T) {
	f := tsNewFixture([][]uint32{tsSizes(32, 64<<10)}, tsTerm{0, 0, 32})
	// The cap is 1 MiB, the boundary after chunk 15; minTerms 32 cuts until no valid cut is left.
	const rangeHeader = "bytes=0-1048575"
	rng := f.parse(rangeHeader)
	out := f.split(t, f.build(t, rng), rng, 32, 0)
	f.wantTerms(t, "SplitTermsV2(32, 0) of one 2 MiB term with Range "+rangeHeader, out, "every cut lies before the cap, so no piece starts past the last requested byte: hf_xet trims only the last term", slices.Concat(tsEvery(0, 0, 15, 1), []tsTerm{{0, 15, 32}})...)
}

func TestSplitTermsV2CutsOnlyInsideRequestedRange(t *testing.T) {
	f := tsNewFixture([][]uint32{tsSizes(64, 64<<10), tsSizes(64, 64<<10), tsSizes(64, 64<<10)}, tsTerm{0, 0, 64}, tsTerm{1, 0, 64}, tsTerm{2, 0, 64})
	// File bytes [1, 9) MiB of three 4 MiB terms: OffsetIntoFirstRange 1 MiB, cap 9 MiB.
	const rangeHeader = "bytes=1048576-9437183"
	rng := f.parse(rangeHeader)
	out := f.split(t, f.build(t, rng), rng, 6, 1<<20)
	if out.OffsetIntoFirstRange != 1<<20 {
		t.Errorf("SplitTermsV2(6, 1 MiB) with Range %s: OffsetIntoFirstRange %d, want the builder's 1 MiB", rangeHeader, out.OffsetIntoFirstRange)
	}
	// Term 0 is cut at 2.5 MiB, the middle of its requested [1, 4) MiB; term 2 holds 1 MiB requested and byte 9 MiB - 1.
	f.wantTerms(t, "SplitTermsV2(6, 1 MiB) of three 4 MiB terms with Range "+rangeHeader, out, "cuts lie inside [1, 9) MiB, each at the middle of its term's requested bytes; the piece holding the last requested byte runs to its term's end",
		slices.Concat(tsAt(0, 0, 40, 64), tsAt(1, 0, 16, 32, 64), []tsTerm{{2, 0, 64}})...)
}

func TestSplitTermsV2RangeHeaderSetsTheCap(t *testing.T) {
	f := tsNewFixture([][]uint32{tsSizes(256, 64<<10)}, tsTerm{0, 0, 256})
	whole := tsEvery(0, 0, 256, 16)
	// Requested [1, 16) MiB halves into 8 pieces of 1.875 MiB requested; halves of those fall under the floor.
	toEnd := tsAt(0, 0, 46, 76, 106, 136, 166, 196, 226, 256)
	for _, c := range []struct {
		rangeHeader string
		want        []tsTerm
	}{
		{"", whole},
		{"bytes=oops", whole},
		{"bytes=1048576-", toEnd},
		{"bytes=-15728640", toEnd},
		{"bytes=1048576-99999999", toEnd},
		// Requested [1, 7) MiB halves into 4 pieces of 1.5 MiB requested, the last running to the term's end.
		{"bytes=1048576-7340031", tsAt(0, 0, 40, 64, 88, 256)},
		// Requested [1, 3) MiB is exactly two floors, so a cap one byte short leaves no cut at 2 MiB.
		{"bytes=1048576-3145727", tsAt(0, 0, 32, 256)},
	} {
		rng := f.parse(c.rangeHeader)
		out := f.split(t, f.build(t, rng), rng, 16, 1<<20)
		f.wantTerms(t, "SplitTermsV2(16, 1 MiB) of one 16 MiB term with Range "+strconv.Quote(c.rangeHeader), out, "an explicit end caps the requested bytes at OffsetIntoFirstRange + end - start + 1; no, open-ended, suffix or invalid Ranges request through the last term", c.want...)
	}
}

func TestSplitTermsV2CapComesFromTheRangeAlone(t *testing.T) {
	f := tsNewFixture([][]uint32{tsSizes(256, 64<<10)}, tsTerm{0, 0, 256})
	// Requested [1, 7) MiB of the 16 MiB file: the cap stands at 7 MiB.
	rng := f.parse("bytes=1048576-7340031")
	in := f.build(t, rng)
	file := f.sh.Files[0]
	chunk := []shard.FileDataSequenceEntry{{CASHash: f.xorbs[0], UnpackedSegBytes: 64 << 10, ChunkIndexEnd: 1}}
	for _, c := range []struct {
		name  string
		files []shard.FileBlock
	}{
		// One-chunk files first and last and a 32 MiB one around the 16 MiB file.
		{"among smaller and larger files", []shard.FileBlock{
			{FileHash: xet.FileHash{0xf2}, Entries: chunk},
			{FileHash: xet.FileHash{0xf3}, Entries: slices.Concat(file.Entries, file.Entries)},
			file,
			{FileHash: xet.FileHash{0xf4}, Entries: chunk},
		}},
		{"without file blocks", nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			f.sh.Files = c.files
			out := f.split(t, in, rng, 16, 1<<20)
			f.wantTerms(t, fmt.Sprintf("SplitTermsV2(16, 1 MiB) of a 16 MiB term with range %+v in a shard of %d files", rng, len(c.files)), out, "the cap is OffsetIntoFirstRange + rng.End - rng.Start + 1 whatever files the shard holds", tsAt(0, 0, 40, 64, 88, 256)...)
		})
	}
}

func TestSplitTermsV2CapIsInResponseBytes(t *testing.T) {
	f := tsNewFixture([][]uint32{tsSizes(64, 64<<10), tsSizes(64, 64<<10)}, tsTerm{0, 0, 64}, tsTerm{1, 0, 64})
	// File bytes [5, 7) MiB of two 4 MiB terms lie in the second: OffsetIntoFirstRange 1 MiB, cap 3 MiB.
	const rangeHeader = "bytes=5242880-7340031"
	rng := f.parse(rangeHeader)
	out := f.split(t, f.build(t, rng), rng, 16, 1<<20)
	f.wantTerms(t, "SplitTermsV2(16, 1 MiB) of two 4 MiB terms with Range "+rangeHeader, out, "the cap counts response bytes, which start at the first selected term: OffsetIntoFirstRange + end - start + 1, not end + 1", tsAt(1, 0, 32, 64)...)
}

// tsDescriptorCase is a split without Range whose pieces and fetch entries are known.
type tsDescriptorCase struct {
	name     string
	sizes    [][]uint32
	terms    []tsTerm
	minTerms int
	want     []tsTerm
	ranges   map[int][]tsTerm // descriptors of the xorbs of cut terms; the others keep in's
}

func tsDescriptorCases() []tsDescriptorCase {
	return []tsDescriptorCase{
		{"pieces sorted by start chunk", [][]uint32{tsSizes(64, 64<<10)}, []tsTerm{{0, 32, 64}, {0, 0, 32}}, 4,
			[]tsTerm{{0, 32, 48}, {0, 48, 64}, {0, 0, 16}, {0, 16, 32}}, map[int][]tsTerm{0: tsEvery(0, 0, 64, 16)}},
		{"identical pieces once", [][]uint32{tsSizes(32, 64<<10)}, []tsTerm{{0, 0, 32}, {0, 0, 32}}, 4,
			[]tsTerm{{0, 0, 16}, {0, 16, 32}, {0, 0, 16}, {0, 16, 32}}, map[int][]tsTerm{0: tsEvery(0, 0, 32, 16)}},
		{"uncut twin keeps the original range", [][]uint32{tsSizes(32, 64<<10)}, []tsTerm{{0, 0, 32}, {0, 0, 32}}, 3,
			[]tsTerm{{0, 0, 16}, {0, 16, 32}, {0, 0, 32}}, map[int][]tsTerm{0: {{0, 0, 32}, {0, 16, 32}}}},
		{"longer of a shared start", [][]uint32{tsSizes(64, 64<<10)}, []tsTerm{{0, 0, 64}, {0, 0, 8}}, 3,
			[]tsTerm{{0, 0, 32}, {0, 32, 64}, {0, 0, 8}}, map[int][]tsTerm{0: tsEvery(0, 0, 64, 32)}},
		{"uncut term beside pieces of its xorb", [][]uint32{tsSizes(40, 64<<10)}, []tsTerm{{0, 0, 32}, {0, 32, 40}}, 16,
			[]tsTerm{{0, 0, 16}, {0, 16, 32}, {0, 32, 40}}, map[int][]tsTerm{0: {{0, 0, 16}, {0, 16, 32}, {0, 32, 40}}}},
		{"xorb of uncut terms only", [][]uint32{tsSizes(32, 64<<10), tsSizes(16, 64<<10)}, []tsTerm{{1, 8, 16}, {1, 0, 8}, {1, 8, 16}, {0, 0, 32}}, 16,
			[]tsTerm{{1, 8, 16}, {1, 0, 8}, {1, 8, 16}, {0, 0, 16}, {0, 16, 32}}, map[int][]tsTerm{0: tsEvery(0, 0, 32, 16)}},
	}
}

func TestSplitTermsV2Descriptors(t *testing.T) {
	for _, c := range tsDescriptorCases() {
		t.Run(c.name, func(t *testing.T) {
			f := tsNewFixture(c.sizes, c.terms...)
			rng := f.parse("")
			in := f.build(t, rng)
			out := f.split(t, in, rng, c.minTerms, 1<<20)
			if !f.wantTerms(t, fmt.Sprintf("SplitTermsV2(%d, 1 MiB)", c.minTerms), out, "the pieces this case's fetch info is built for", c.want...) {
				return
			}
			want := maps.Clone(in.Xorbs)
			for x, pieces := range c.ranges {
				want[f.xorbs[x].String()] = f.fetch(t, in, x, pieces...)
			}
			if !reflect.DeepEqual(out.Xorbs, want) {
				t.Errorf("SplitTermsV2(%d, 1 MiB) fetch info = %s\nwant %s\n(a xorb of a cut term keeps its URL and gets a descriptor per output term with its chunks' packed bytes, identical ones once and of a shared start the longest, sorted by start; other xorbs keep theirs)", c.minTerms, f.formatXorbs(out.Xorbs), f.formatXorbs(want))
			}
		})
	}
}

func TestSplitTermsV2TermWithoutCASBlockStaysWhole(t *testing.T) {
	f, _ := tsTwoMiBTerms(2)
	f.sh.CASInfos = f.sh.CASInfos[1:]
	rng := f.parse("")
	in := f.build(t, rng)
	out := f.split(t, in, rng, 16, 1<<20)
	f.wantTerms(t, "SplitTermsV2(16, 1 MiB) of two 2 MiB terms, the first's xorb without CAS block", out, "a term whose xorb has no CAS block in the shard stays whole; the other one is cut", tsTerm{0, 0, 32}, tsTerm{1, 0, 16}, tsTerm{1, 16, 32})
	if hash := f.xorbs[0].String(); !reflect.DeepEqual(out.Xorbs[hash], in.Xorbs[hash]) {
		t.Errorf("SplitTermsV2 fetch info of the xorb without CAS block = %+v, want it unchanged: %+v", out.Xorbs[hash], in.Xorbs[hash])
	}
}

func TestSplitTermsV2InconsistentDataIsAnError(t *testing.T) {
	f := tsNewFixture([][]uint32{tsSizes(32, 64<<10)}, tsTerm{0, 0, 32})
	rng := f.parse("")
	in := f.build(t, rng)
	hash := f.xorbs[0]
	shortOffsets := &tsAdapter{offsets: map[xet.XorbHash][]uint64{hash: f.st.offsets[hash][:31]}}
	shortCAS, longCAS := *f.sh, *f.sh
	shortCAS.CASInfos = slices.Clone(f.sh.CASInfos)
	shortCAS.CASInfos[0].Chunks = shortCAS.CASInfos[0].Chunks[:31]
	longCAS.CASInfos = slices.Clone(f.sh.CASInfos)
	longCAS.CASInfos[0].Chunks = slices.Clone(longCAS.CASInfos[0].Chunks)
	longCAS.CASInfos[0].Chunks[0].UnpackedSegBytes++
	for _, c := range []struct {
		name string
		st   StorageAdapter
		sh   *shard.Shard
	}{
		{"chunk offsets shorter than the term", shortOffsets, f.sh},
		{"CAS block shorter than the term", f.st, &shortCAS},
		{"CAS chunks one byte past the term", f.st, &longCAS},
	} {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("SplitTermsV2 panicked: %v", r)
				}
			}()
			if out, err := SplitTermsV2(t.Context(), c.st, c.sh, in, rng, 16, 1<<20); err == nil || out != nil {
				t.Errorf("SplitTermsV2(16, 1 MiB) of a 2 MiB term = %v, %v; want no response and an error, not a guessed split", out, err)
			}
		})
	}
}

func TestSplitTermsV2OfItsOutputReturnsIt(t *testing.T) {
	sixteen := tsNewFixture([][]uint32{tsSizes(256, 64<<10)}, tsTerm{0, 0, 256})
	two, _ := tsTwoMiBTerms(1)
	for _, c := range []struct {
		name        string
		f           *tsFixture
		rangeHeader string
	}{
		{"16 terms reached", sixteen, ""},
		{"no valid cut left", two, ""},
		{"no valid cut left before the Range end", sixteen, "bytes=1048576-7340031"},
		{"no valid cut left after the Range start", sixteen, "bytes=1048576-"},
	} {
		t.Run(c.name, func(t *testing.T) {
			rng := c.f.parse(c.rangeHeader)
			out := c.f.split(t, c.f.build(t, rng), rng, 16, 1<<20)
			before := tsSnapshot(t, out)
			if again := c.f.split(t, out, rng, 16, 1<<20); again != out {
				t.Errorf("SplitTermsV2(16, 1 MiB) with Range %q of its own output %s = %s, want that output itself: idempotent", c.rangeHeader, c.f.format(out.Terms), c.f.format(again.Terms))
			}
			if !bytes.Equal(tsSnapshot(t, out), before) {
				t.Error("SplitTermsV2 of its own output changed it")
			}
		})
	}
}

func TestSplitTermsV2IsDeterministic(t *testing.T) {
	f := tsNewFixture([][]uint32{tsSizes(64, 64<<10), tsSizes(64, 64<<10), tsSizes(64, 64<<10)}, tsTerm{0, 0, 64}, tsTerm{1, 0, 64}, tsTerm{2, 0, 64}, tsTerm{1, 16, 48})
	rng := f.parse("")
	in := f.build(t, rng)
	first := f.split(t, in, rng, 16, 1<<20)
	for range 10 {
		if again := f.split(t, in, rng, 16, 1<<20); !reflect.DeepEqual(again, first) {
			t.Fatalf("SplitTermsV2(16, 1 MiB) of one input = %s, then %s; want one response", tsSnapshot(t, first), tsSnapshot(t, again))
		}
	}
}

func TestSplitTermsV2LeavesInputUnchanged(t *testing.T) {
	for _, c := range tsDescriptorCases() {
		t.Run(c.name, func(t *testing.T) {
			f := tsNewFixture(c.sizes, c.terms...)
			rng := f.parse("")
			in := f.build(t, rng)
			before := tsSnapshot(t, in)
			f.split(t, in, rng, c.minTerms, 1<<20)
			if after := tsSnapshot(t, in); !bytes.Equal(after, before) {
				t.Errorf("SplitTermsV2(%d, 1 MiB) changed its input to %s from %s", c.minTerms, after, before)
			}
		})
	}
}

func TestSplitTermsV2ReaderReadsTheSameBytes(t *testing.T) {
	for _, c := range storagetest.ChunkClasses {
		if c.Compression != xorb.CompressionNone && c.Compression != xorb.CompressionLZ4 {
			continue
		}
		t.Run(c.Name, func(t *testing.T) {
			// Xorb 0 recurs at its start and from chunk 2, so pieces share a xorb and start chunks.
			f := tsNewStored(t, c, []int{1992294, 5 << 18}, tsTerm{0, 0, 3}, tsTerm{x: 0}, tsTerm{x: 1}, tsTerm{0, 2, 14})
			for _, r := range []struct {
				rangeHeader string
				start       int
			}{
				{"", 0},
				{"bytes=300000-", 300000},
				{fmt.Sprintf("bytes=300000-%d", len(f.content)-300000), 300000},
			} {
				rng := ParseByteRange(r.rangeHeader, int64(len(f.content)))
				in, err := BuildReconstructionResponseV2(t.Context(), f.st, f.sh, f.file, rng)
				if err != nil {
					t.Fatal(err)
				}
				out, err := SplitTermsV2(t.Context(), f.st, f.sh, in, rng, 16, 256<<10)
				if err != nil || out == in {
					t.Fatalf("SplitTermsV2(16, 256 KiB) with Range %q = %v, %v; want a split", r.rangeHeader, out, err)
				}
				want := f.content[r.start:][:ExpectedLengthV2(in)]
				if got := f.read(t, in); !bytes.Equal(got, want) {
					t.Fatalf("NewReaderV2 over the builder's response with Range %q read %d bytes, want %d of the file", r.rangeHeader, len(got), len(want))
				}
				if got := f.read(t, out); !bytes.Equal(got, want) || ExpectedLengthV2(out) != ExpectedLengthV2(in) {
					t.Errorf("NewReaderV2 over SplitTermsV2's response with Range %q read %d bytes of expected length %d, want the builder's %d bytes", r.rangeHeader, len(got), ExpectedLengthV2(out), len(want))
				}
			}
		})
	}
}
