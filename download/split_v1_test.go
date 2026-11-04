package download

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage/storagetest"
	"github.com/wzshiming/xet/xorb"
)

func (f *tsFixture) buildV1(t *testing.T, rng ByteRange) *ReconstructionResponseV1 {
	t.Helper()
	in, err := BuildReconstructionResponseV1(t.Context(), f.st, f.sh, f.file, rng)
	if err != nil {
		t.Fatalf("BuildReconstructionResponseV1 with range %+v: %v", rng, err)
	}
	return in
}

// splitV1 returns SplitTermsV1's response, failing t on an error or a nil response.
func (f *tsFixture) splitV1(t *testing.T, in *ReconstructionResponseV1, rng ByteRange, minTerms int, pieceBytes int64) *ReconstructionResponseV1 {
	t.Helper()
	out, err := SplitTermsV1(t.Context(), f.st, f.sh, in, rng, minTerms, pieceBytes)
	if err != nil || out == nil {
		t.Fatalf("SplitTermsV1(%d, %d) with range %+v = %v, %v; want a response", minTerms, pieceBytes, rng, out, err)
	}
	return out
}

// wantTermsV1 reports whether out's terms are the pieces, failing t with the rule why if not.
func (f *tsFixture) wantTermsV1(t *testing.T, call string, out *ReconstructionResponseV1, why string, pieces ...tsTerm) bool {
	t.Helper()
	want := f.terms(pieces...)
	if slices.Equal(out.Terms, want) {
		return true
	}
	t.Errorf("%s = %s\nwant %s\n(%s)", call, f.format(out.Terms), f.format(want), why)
	return false
}

// fetchInfo is xorb x's fetch_info entries under url: one per piece with the packed bytes of its chunks.
func (f *tsFixture) fetchInfo(t *testing.T, url string, x int, pieces ...tsTerm) []FetchInfoEntry {
	t.Helper()
	entries := make([]FetchInfoEntry, 0, len(pieces))
	for _, p := range pieces {
		start, end, err := xorb.ChunkDataRangeFromOffsets(f.st.offsets[f.xorbs[x]], p.start, p.end)
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, FetchInfoEntry{Range: ChunkRange{Start: p.start, End: p.end}, URL: url, URLRange: ByteRange{Start: start, End: end}})
	}
	return entries
}

// formatFetchInfo names fetch_info entries by fixture xorb, showing URLs other than tsAdapter's.
func (f *tsFixture) formatFetchInfo(fetchInfo map[string][]FetchInfoEntry) string {
	parts := []string{fmt.Sprintf("%d xorbs", len(fetchInfo))}
	for x, hash := range f.xorbs {
		entries, ok := fetchInfo[hash.String()]
		if !ok {
			continue
		}
		var b strings.Builder
		fmt.Fprintf(&b, "x%d", x)
		for _, e := range entries {
			fmt.Fprintf(&b, " [%d,%d)@%d-%d", e.Range.Start, e.Range.End, e.URLRange.Start, e.URLRange.End)
			if url := strings.TrimPrefix(e.URL, "https://cas.example/xorbs/"+hash.String()); url != "" {
				b.WriteString(" " + url)
			}
		}
		parts = append(parts, b.String())
	}
	return strings.Join(parts, "; ")
}

// wantParity reports whether v1 has v2's offset and pieces and, per chunk range, v2's bytes, failing t with both if not.
func (f *tsFixture) wantParity(t *testing.T, call string, v1 *ReconstructionResponseV1, v2 *ReconstructionResponseV2) bool {
	t.Helper()
	if v1.OffsetIntoFirstRange != v2.OffsetIntoFirstRange || !slices.Equal(v1.Terms, v2.Terms) {
		t.Errorf("%s:\nSplitTermsV1 = offset %d, %s\nSplitTermsV2 = offset %d, %s\n(both cut the same pieces)", call, v1.OffsetIntoFirstRange, f.format(v1.Terms), v2.OffsetIntoFirstRange, f.format(v2.Terms))
		return false
	}
	if !maps.Equal(tsChunkBytesV1(v1.FetchInfo), tsChunkBytesV2(v2.Xorbs)) {
		t.Errorf("%s:\nSplitTermsV1 fetch_info = %s\nSplitTermsV2 descriptors = %s\n(each url_range is the bytes of the V2 descriptor of the same chunks)", call, f.formatFetchInfo(v1.FetchInfo), f.formatXorbs(v2.Xorbs))
		return false
	}
	return true
}

func tsSnapshotV1(t *testing.T, resp *ReconstructionResponseV1) []byte {
	t.Helper()
	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func (f *tsStored) readV1(t *testing.T, resp *ReconstructionResponseV1) []byte {
	t.Helper()
	r, err := NewReaderV1(t.Context(), storageClient{f.st}, resp, WithCacheManager(NewCacheManager(t.TempDir(), 0)))
	if err != nil {
		t.Fatalf("NewReaderV1: %v", err)
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("NewReaderV1 read: %v", err)
	}
	return data
}

// tsChunkKey is a chunk range of a xorb.
type tsChunkKey struct {
	hash   string
	chunks ChunkRange
}

// tsChunkBytesV1 maps the chunk ranges of V1 fetch_info entries to their url_range.
func tsChunkBytesV1(fetchInfo map[string][]FetchInfoEntry) map[tsChunkKey]ByteRange {
	m := map[tsChunkKey]ByteRange{}
	for hash, entries := range fetchInfo {
		for _, e := range entries {
			m[tsChunkKey{hash, e.Range}] = e.URLRange
		}
	}
	return m
}

// tsChunkBytesV2 maps the chunk ranges of V2 descriptors to their bytes.
func tsChunkBytesV2(xorbs map[string][]XorbMultiRangeFetch) map[tsChunkKey]ByteRange {
	m := map[tsChunkKey]ByteRange{}
	for hash, fetches := range xorbs {
		for _, fetch := range fetches {
			for _, rg := range fetch.Ranges {
				m[tsChunkKey{hash, rg.Chunks}] = rg.Bytes
			}
		}
	}
	return m
}

// tsOneTerm is a file of one term over a xorb of n chunks of 64 KiB.
func tsOneTerm(n uint32) *tsFixture {
	return tsNewFixture([][]uint32{tsSizes(int(n), 64<<10)}, tsTerm{0, 0, n})
}

// tsVariedFixture is the file tsNewStored stores for the readers, without bytes.
func tsVariedFixture() *tsFixture {
	sizes := make([][]uint32, 2)
	for x, total := range []int{1992294, 5 << 18} {
		for _, size := range tsVariedSizes(uint64(x), total) {
			sizes[x] = append(sizes[x], uint32(size))
		}
	}
	return tsNewFixture(sizes, tsTerm{0, 0, 3}, tsTerm{0, 0, uint32(len(sizes[0]))}, tsTerm{1, 0, uint32(len(sizes[1]))}, tsTerm{0, 2, 14})
}

// tsRandomFile is 1 to 6 terms, some repeating or overlapping, over 1 to 3 xorbs of 16 to 128 KiB chunks,
// with a Range header of each form over it.
func tsRandomFile(seed uint64) (*tsFixture, []string) {
	r := rand.New(rand.NewPCG(seed, seed))
	sizes := make([][]uint32, 1+r.IntN(3))
	for x := range sizes {
		for range 4 + r.IntN(60) {
			sizes[x] = append(sizes[x], uint32(16<<10+r.IntN(112<<10)))
		}
	}
	terms := make([]tsTerm, 1+r.IntN(6))
	size := 0
	for i := range terms {
		x := r.IntN(len(sizes))
		start := r.IntN(len(sizes[x]))
		end := start + 1 + r.IntN(len(sizes[x])-start)
		terms[i] = tsTerm{x, uint32(start), uint32(end)}
		for _, s := range sizes[x][start:end] {
			size += int(s)
		}
	}
	first := r.IntN(size)
	return tsNewFixture(sizes, terms...), []string{"", fmt.Sprintf("bytes=%d-", first), fmt.Sprintf("bytes=%d-%d", first, first+r.IntN(size-first)), fmt.Sprintf("bytes=-%d", 1+r.IntN(size))}
}

// tsRuleCase is a request and the pieces the rule cuts its response into; want nil means the input itself.
type tsRuleCase struct {
	name        string
	f           *tsFixture
	rangeHeader string
	minTerms    int
	pieceBytes  int64
	want        []tsTerm
	why         string
}

func tsRuleCases() []tsRuleCase {
	const mib = 1 << 20
	oneMiB, twoMiB, sixteenMiB := tsOneTerm(16), tsOneTerm(32), tsOneTerm(256)
	seventeen, twoMiBTerms := tsTwoMiBTerms(17)
	noCAS := tsOneTerm(256)
	noCAS.sh.CASInfos = nil
	// Fourteen 2 MiB terms around one of 64 MiB at index 7.
	sizes, terms := make([][]uint32, 15), make([]tsTerm, 15)
	for x := range 15 {
		n := uint32(32)
		if x == 7 {
			n = 1024
		}
		sizes[x], terms[x] = tsSizes(int(n), 64<<10), tsTerm{x, 0, n}
	}
	largest := tsNewFixture(sizes, terms...)
	twoXorbs := func(a, b uint32) *tsFixture {
		return tsNewFixture([][]uint32{tsSizes(int(a), 64<<10), tsSizes(int(b), 64<<10)}, tsTerm{0, 0, a}, tsTerm{1, 0, b})
	}
	threeFourMiB := tsNewFixture([][]uint32{tsSizes(64, 64<<10), tsSizes(64, 64<<10), tsSizes(64, 64<<10)}, tsTerm{0, 0, 64}, tsTerm{1, 0, 64}, tsTerm{2, 0, 64})
	// One-chunk files first and last and a 32 MiB one around the 16 MiB file.
	manyFiles := tsOneTerm(256)
	file := manyFiles.sh.Files[0]
	chunk := []shard.FileDataSequenceEntry{{CASHash: manyFiles.xorbs[0], UnpackedSegBytes: 64 << 10, ChunkIndexEnd: 1}}
	manyFiles.sh.Files = []shard.FileBlock{
		{FileHash: xet.FileHash{0xf2}, Entries: chunk},
		{FileHash: xet.FileHash{0xf3}, Entries: slices.Concat(file.Entries, file.Entries)},
		file,
		{FileHash: xet.FileHash{0xf4}, Entries: chunk},
	}
	// Requested [1, 16) MiB halves into 8 pieces of 1.875 MiB requested; halves of those fall under the floor.
	toEnd := tsAt(0, 0, 46, 76, 106, 136, 166, 196, 226, 256)
	const halve = "the largest term halves at its middle chunk boundary until there are 16 terms"
	const noCut = "no cut leaves both pieces 1 MiB of requested bytes"
	const toLast = "open-ended, suffix, past-the-end and invalid Ranges request through the last term"
	return []tsRuleCase{
		{"minTerms -1 is off", sixteenMiB, "", -1, mib, nil, "minTerms <= 1 disables splitting"},
		{"minTerms 0 is off", sixteenMiB, "", 0, mib, nil, "minTerms <= 1 disables splitting"},
		{"minTerms 1 is off", sixteenMiB, "", 1, mib, nil, "minTerms <= 1 disables splitting"},
		{"gate at minTerms terms", seventeen, "", 17, mib, nil, "the gate holds at len(terms) >= minTerms"},
		{"gate past minTerms terms", seventeen, "", 16, mib, nil, "the gate holds at len(terms) >= minTerms"},
		{"gate counts the ranged response's terms", seventeen, "bytes=2097152-", 16, mib, nil, "the Range leaves 16 terms, so the gate holds"},
		{"one cut short of the gate", seventeen, "", 18, mib, slices.Concat(tsAt(0, 0, 16, 32), twoMiBTerms[1:]), "one cut, of the first of the tied terms, reaches 18 terms"},
		{"1 MiB term stays whole", oneMiB, "", 16, mib, nil, noCut},
		{"term without CAS block stays whole", noCAS, "", 16, mib, nil, "a term whose xorb has no CAS block in the shard has no cut"},
		{"1.5 MiB requested stays whole", sixteenMiB, "bytes=0-1572863", 16, mib, nil, noCut},
		{"no term in the range", sixteenMiB, "bytes=99999999-", 16, mib, nil, "a response without terms has nothing to cut"},
		{"16 MiB into 16 of 1 MiB", sixteenMiB, "", 16, mib, tsEvery(0, 0, 256, 16), halve},
		{"64 MiB into 16 of 4 MiB", tsOneTerm(1024), "", 16, mib, tsEvery(0, 0, 1024, 64), halve},
		{"24 MiB into 16 of 1.5 MiB", tsOneTerm(384), "", 16, mib, tsEvery(0, 0, 384, 24), halve},
		{"only the largest term is cut", largest, "", 16, mib, slices.Concat(terms[:7], tsAt(7, 0, 512, 1024), terms[8:]), "one cut of the 64 MiB term into 32 + 32 MiB reaches 16 terms"},
		{"largest first, lowest index on a tie", twoXorbs(128, 32), "", 4, mib, []tsTerm{{0, 0, 32}, {0, 32, 64}, {0, 64, 128}, {1, 0, 32}}, "8 -> 4 + 4, then the first of the tied 4s -> 2 + 2: 2, 2, 4, 2 MiB"},
		{"most requested bytes first, Range starting late in the longer term", twoXorbs(128, 64), "bytes=6291456-", 3, mib, []tsTerm{{0, 0, 128}, {1, 0, 32}, {1, 32, 64}}, "2 MiB of the 8 MiB term are requested and all 4 MiB of the next: the term with the most requested bytes is cut, not the longest"},
		{"most requested bytes first, Range ending early in the longer term", twoXorbs(64, 128), "bytes=0-6291455", 3, mib, []tsTerm{{0, 0, 32}, {0, 32, 64}, {1, 0, 128}}, "all 4 MiB of the first term are requested and 2 MiB of the 8 MiB one: the term with the most requested bytes is cut, not the longest"},
		{"cut at the boundary nearest the midpoint", tsNewFixture([][]uint32{{mib, mib, mib, 5 * mib}}, tsTerm{0, 0, 4}), "", 2, mib, tsAt(0, 0, 3, 4), "of the boundaries at 1, 2 and 3 MiB, 3 MiB is nearest the 4 MiB midpoint"},
		{"midpoint tie cuts at the smaller boundary", tsNewFixture([][]uint32{{mib, 2 * mib, mib}}, tsTerm{0, 0, 3}), "", 2, mib, tsAt(0, 0, 1, 3), "the boundaries at 1 and 3 MiB tie around the 2 MiB midpoint; the smaller wins"},
		{"floor bounds each piece", tsNewFixture([][]uint32{{7 << 19, 1 << 19}, tsSizes(32, 64<<10)}, tsTerm{0, 0, 2}, tsTerm{1, 0, 32}), "", 16, mib, []tsTerm{{0, 0, 2}, {1, 0, 16}, {1, 16, 32}}, "the 4 MiB term's only cut leaves a 0.5 MiB piece, so the 2 MiB term is cut instead"},
		{"pieceBytes 0 sets no floor", oneMiB, "", 16, 0, tsEvery(0, 0, 16, 1), "pieceBytes <= 0 sets no floor"},
		{"pieceBytes -1 sets no floor", oneMiB, "", 16, -1, tsEvery(0, 0, 16, 1), "pieceBytes <= 0 sets no floor"},
		// OffsetIntoFirstRange is 1 MiB, the boundary after chunk 15.
		{"first piece covers OffsetIntoFirstRange + 1", twoMiB, "bytes=1048576-", 16, 0, slices.Concat([]tsTerm{{0, 0, 17}}, tsEvery(0, 17, 32, 1)), "every cut lies past OffsetIntoFirstRange, so the first piece holds a requested byte"},
		// The cap is 1 MiB, the boundary after chunk 15; minTerms 32 cuts until no valid cut is left.
		{"last piece holds the Range end", twoMiB, "bytes=0-1048575", 32, 0, slices.Concat(tsEvery(0, 0, 15, 1), []tsTerm{{0, 15, 32}}), "every cut lies before the cap, so no piece starts past the last requested byte: hf_xet trims only the last term"},
		// File bytes [1, 9) MiB of three 4 MiB terms: OffsetIntoFirstRange 1 MiB, cap 9 MiB.
		{"cuts only inside the requested range", threeFourMiB, "bytes=1048576-9437183", 6, mib, slices.Concat(tsAt(0, 0, 40, 64), tsAt(1, 0, 16, 32, 64), []tsTerm{{2, 0, 64}}), "cuts lie inside [1, 9) MiB, each at the middle of its term's requested bytes; the piece holding the last requested byte runs to its term's end"},
		{"invalid Range requests through the last term", sixteenMiB, "bytes=oops", 16, mib, tsEvery(0, 0, 256, 16), toLast},
		{"open-ended Range requests through the last term", sixteenMiB, "bytes=1048576-", 16, mib, toEnd, toLast},
		{"suffix Range requests through the last term", sixteenMiB, "bytes=-15728640", 16, mib, toEnd, toLast},
		{"Range end past the file requests through the last term", sixteenMiB, "bytes=1048576-99999999", 16, mib, toEnd, toLast},
		// Requested [1, 7) MiB halves into 4 pieces of 1.5 MiB requested, the last running to the term's end.
		{"Range end caps the requested bytes", sixteenMiB, "bytes=1048576-7340031", 16, mib, tsAt(0, 0, 40, 64, 88, 256), "an explicit end caps the requested bytes at OffsetIntoFirstRange + end - start + 1"},
		// Requested [1, 3) MiB is exactly two floors, so a cap one byte short leaves no cut at 2 MiB.
		{"Range of two floors cuts once", sixteenMiB, "bytes=1048576-3145727", 16, mib, tsAt(0, 0, 32, 256), "an explicit end caps the requested bytes at OffsetIntoFirstRange + end - start + 1"},
		{"Range end caps the requested bytes whatever files the shard holds", manyFiles, "bytes=1048576-7340031", 16, mib, tsAt(0, 0, 40, 64, 88, 256), "the cap comes from the range alone: the shard's smaller and larger other files leave it at 7 MiB"},
		// File bytes [5, 7) MiB of two 4 MiB terms lie in the second: OffsetIntoFirstRange 1 MiB, cap 3 MiB.
		{"Range inside a later term caps response bytes", twoXorbs(64, 64), "bytes=5242880-7340031", 16, mib, tsAt(1, 0, 32, 64), "the cap counts response bytes, which start at the first selected term: OffsetIntoFirstRange + end - start + 1, not end + 1"},
	}
}

func TestSplitTermsV1Pieces(t *testing.T) {
	for _, c := range tsRuleCases() {
		t.Run(c.name, func(t *testing.T) {
			rng := c.f.parse(c.rangeHeader)
			in := c.f.buildV1(t, rng)
			out := c.f.splitV1(t, in, rng, c.minTerms, c.pieceBytes)
			call := fmt.Sprintf("SplitTermsV1(%d, %d) with Range %q", c.minTerms, c.pieceBytes, c.rangeHeader)
			if c.want == nil {
				if out != in {
					t.Errorf("%s = %s, want its input itself: %s", call, c.f.format(out.Terms), c.why)
				}
				return
			}
			c.f.wantTermsV1(t, call, out, c.why, c.want...)
			if out.OffsetIntoFirstRange != in.OffsetIntoFirstRange {
				t.Errorf("%s: OffsetIntoFirstRange %d, want the builder's %d: it still belongs to the first term", call, out.OffsetIntoFirstRange, in.OffsetIntoFirstRange)
			}
		})
	}
}

func TestSplitTermsV1GateAllocatesNothing(t *testing.T) {
	f, _ := tsTwoMiBTerms(17)
	rng := f.parse("")
	in := f.buildV1(t, rng)
	ctx := t.Context()
	var st StorageAdapter = f.st
	for _, minTerms := range []int{0, 1, 16, 17} {
		var out *ReconstructionResponseV1
		allocs := testing.AllocsPerRun(100, func() { out, _ = SplitTermsV1(ctx, st, f.sh, in, rng, minTerms, 1<<20) })
		if out != in || allocs != 0 {
			t.Errorf("SplitTermsV1(%d, 1 MiB) of 17 terms returned its input %t after %v allocations, want it after none", minTerms, out == in, allocs)
		}
	}
}

func TestSplitTermsV1TermWithoutCASBlockStaysWhole(t *testing.T) {
	f, _ := tsTwoMiBTerms(2)
	f.sh.CASInfos = f.sh.CASInfos[1:]
	rng := f.parse("")
	in := f.buildV1(t, rng)
	out := f.splitV1(t, in, rng, 16, 1<<20)
	f.wantTermsV1(t, "SplitTermsV1(16, 1 MiB) of two 2 MiB terms, the first's xorb without CAS block", out, "a term whose xorb has no CAS block in the shard stays whole; the other one is cut", tsTerm{0, 0, 32}, tsTerm{1, 0, 16}, tsTerm{1, 16, 32})
	if hash := f.xorbs[0].String(); !reflect.DeepEqual(out.FetchInfo[hash], in.FetchInfo[hash]) {
		t.Errorf("SplitTermsV1 fetch_info of the xorb without CAS block = %+v, want it unchanged: %+v", out.FetchInfo[hash], in.FetchInfo[hash])
	}
}

func TestSplitTermsV1FetchInfo(t *testing.T) {
	for _, c := range tsDescriptorCases() {
		t.Run(c.name, func(t *testing.T) {
			f := tsNewFixture(c.sizes, c.terms...)
			rng := f.parse("")
			in := f.buildV1(t, rng)
			out := f.splitV1(t, in, rng, c.minTerms, 1<<20)
			if !f.wantTermsV1(t, fmt.Sprintf("SplitTermsV1(%d, 1 MiB)", c.minTerms), out, "the pieces this case's fetch_info is built for", c.want...) {
				return
			}
			want := maps.Clone(in.FetchInfo)
			for x, pieces := range c.ranges {
				hash := f.xorbs[x].String()
				want[hash] = f.fetchInfo(t, in.FetchInfo[hash][0].URL, x, pieces...)
			}
			if !reflect.DeepEqual(out.FetchInfo, want) {
				t.Errorf("SplitTermsV1(%d, 1 MiB) fetch_info = %s\nwant %s\n(a xorb of a cut term gets an entry per output term with its chunks' packed bytes in place of the cut term's, identical ones once and of a shared start the longest, sorted by start; other xorbs keep theirs as they were)", c.minTerms, f.formatFetchInfo(out.FetchInfo), f.formatFetchInfo(want))
			}
		})
	}
}

func TestSplitTermsV1UncutXorbKeepsItsFetchInfoInOrder(t *testing.T) {
	// Xorb 1's half-MiB terms stay whole while xorb 0's 2 MiB term halves.
	f := tsNewFixture([][]uint32{tsSizes(32, 64<<10), tsSizes(16, 64<<10)}, tsTerm{1, 0, 8}, tsTerm{1, 8, 16}, tsTerm{1, 4, 12}, tsTerm{0, 0, 32})
	rng := f.parse("")
	in := f.buildV1(t, rng)
	hash := f.xorbs[1].String()
	// Its entries [0,8) [8,16) [4,12) change if reversed or sorted.
	want := slices.Clone(in.FetchInfo[hash])
	out := f.splitV1(t, in, rng, 16, 1<<20)
	if !f.wantTermsV1(t, "SplitTermsV1(16, 1 MiB) of three half-MiB terms and a 2 MiB one", out, "only the 2 MiB term has a cut leaving both pieces 1 MiB", tsTerm{1, 0, 8}, tsTerm{1, 8, 16}, tsTerm{1, 4, 12}, tsTerm{0, 0, 16}, tsTerm{0, 16, 32}) {
		return
	}
	if !reflect.DeepEqual(out.FetchInfo[hash], want) {
		t.Errorf("SplitTermsV1(16, 1 MiB) fetch_info of the xorb of uncut terms = %s\nwant %s\n(a xorb without a cut term keeps its entries as they were, in their order)", f.formatFetchInfo(map[string][]FetchInfoEntry{hash: out.FetchInfo[hash]}), f.formatFetchInfo(map[string][]FetchInfoEntry{hash: want}))
	}
}

func TestSplitTermsV1PiecesKeepTheirTermsURL(t *testing.T) {
	// Two 2 MiB terms of one xorb, its second half first.
	f := tsNewFixture([][]uint32{tsSizes(64, 64<<10)}, tsTerm{0, 32, 64}, tsTerm{0, 0, 32})
	rng := f.parse("")
	in := f.buildV1(t, rng)
	// Signed URLs may differ per entry: tell the terms' entries apart.
	hash := f.xorbs[0].String()
	var urls []string
	for k := range in.FetchInfo[hash] {
		in.FetchInfo[hash][k].URL += fmt.Sprintf("?term=%d", k)
		urls = append(urls, in.FetchInfo[hash][k].URL)
	}
	out := f.splitV1(t, in, rng, 4, 1<<20)
	if !f.wantTermsV1(t, "SplitTermsV1(4, 1 MiB) of two 2 MiB terms", out, "each term halves once", slices.Concat(tsAt(0, 32, 48, 64), tsAt(0, 0, 16, 32))...) {
		return
	}
	want := map[string][]FetchInfoEntry{hash: slices.Concat(f.fetchInfo(t, urls[1], 0, tsAt(0, 0, 16, 32)...), f.fetchInfo(t, urls[0], 0, tsAt(0, 32, 48, 64)...))}
	if !reflect.DeepEqual(out.FetchInfo, want) {
		t.Errorf("SplitTermsV1(4, 1 MiB) fetch_info = %s\nwant %s\n(each piece's entry takes the URL of its term's entry)", f.formatFetchInfo(out.FetchInfo), f.formatFetchInfo(want))
	}
}

func TestSplitTermsV1InconsistentDataIsAnError(t *testing.T) {
	f := tsOneTerm(32)
	rng := f.parse("")
	in := f.buildV1(t, rng)
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
					t.Fatalf("SplitTermsV1 panicked: %v", r)
				}
			}()
			if out, err := SplitTermsV1(t.Context(), c.st, c.sh, in, rng, 16, 1<<20); err == nil || out != nil {
				t.Errorf("SplitTermsV1(16, 1 MiB) of a 2 MiB term = %v, %v; want no response and an error, not a guessed split", out, err)
			}
		})
	}
}

func TestSplitTermsV1TermWithoutFetchInfoEntryIsAnError(t *testing.T) {
	f := tsOneTerm(32)
	hash := f.xorbs[0].String()
	rng := f.parse("")
	for _, c := range []struct {
		name    string
		entries []tsTerm // chunk ranges of the xorb's fetch_info entries, nil for none
	}{
		{"no fetch_info for its xorb", nil},
		{"only an entry of half the term", []tsTerm{{0, 0, 16}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			in := f.buildV1(t, rng)
			url := in.FetchInfo[hash][0].URL
			delete(in.FetchInfo, hash)
			if c.entries != nil {
				in.FetchInfo[hash] = f.fetchInfo(t, url, 0, c.entries...)
			}
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("SplitTermsV1 panicked: %v", r)
				}
			}()
			if out, err := SplitTermsV1(t.Context(), f.st, f.sh, in, rng, 16, 1<<20); err == nil || out != nil {
				t.Errorf("SplitTermsV1(16, 1 MiB) of a 2 MiB term with %s = %v, %v; want no response and an error: the term has no entry for its pieces to take the URL of", c.name, out, err)
			}
		})
	}
}

func TestSplitTermsV1OfItsOutputReturnsIt(t *testing.T) {
	sixteen := tsOneTerm(256)
	for _, c := range []struct {
		name        string
		f           *tsFixture
		rangeHeader string
	}{
		{"16 terms reached", sixteen, ""},
		{"no valid cut left", tsOneTerm(32), ""},
		{"no valid cut left before the Range end", sixteen, "bytes=1048576-7340031"},
		{"no valid cut left after the Range start", sixteen, "bytes=1048576-"},
	} {
		t.Run(c.name, func(t *testing.T) {
			rng := c.f.parse(c.rangeHeader)
			out := c.f.splitV1(t, c.f.buildV1(t, rng), rng, 16, 1<<20)
			before := tsSnapshotV1(t, out)
			if again := c.f.splitV1(t, out, rng, 16, 1<<20); again != out {
				t.Errorf("SplitTermsV1(16, 1 MiB) with Range %q of its own output %s = %s, want that output itself: idempotent", c.rangeHeader, c.f.format(out.Terms), c.f.format(again.Terms))
			}
			if !bytes.Equal(tsSnapshotV1(t, out), before) {
				t.Error("SplitTermsV1 of its own output changed it")
			}
		})
	}
}

func TestSplitTermsV1IsDeterministic(t *testing.T) {
	f := tsNewFixture([][]uint32{tsSizes(64, 64<<10), tsSizes(64, 64<<10), tsSizes(64, 64<<10)}, tsTerm{0, 0, 64}, tsTerm{1, 0, 64}, tsTerm{2, 0, 64}, tsTerm{1, 16, 48})
	rng := f.parse("")
	in := f.buildV1(t, rng)
	first := f.splitV1(t, in, rng, 16, 1<<20)
	for range 10 {
		if again := f.splitV1(t, in, rng, 16, 1<<20); !reflect.DeepEqual(again, first) {
			t.Fatalf("SplitTermsV1(16, 1 MiB) of one input = %s, then %s; want one response", tsSnapshotV1(t, first), tsSnapshotV1(t, again))
		}
	}
}

func TestSplitTermsV1LeavesInputUnchanged(t *testing.T) {
	for _, c := range tsDescriptorCases() {
		t.Run(c.name, func(t *testing.T) {
			f := tsNewFixture(c.sizes, c.terms...)
			rng := f.parse("")
			in := f.buildV1(t, rng)
			before := tsSnapshotV1(t, in)
			f.splitV1(t, in, rng, c.minTerms, 1<<20)
			if after := tsSnapshotV1(t, in); !bytes.Equal(after, before) {
				t.Errorf("SplitTermsV1(%d, 1 MiB) changed its input to %s from %s", c.minTerms, after, before)
			}
		})
	}
}

func TestSplitTermsV1ReaderReadsTheSameBytes(t *testing.T) {
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
				in, err := BuildReconstructionResponseV1(t.Context(), f.st, f.sh, f.file, rng)
				if err != nil {
					t.Fatal(err)
				}
				out, err := SplitTermsV1(t.Context(), f.st, f.sh, in, rng, 16, 256<<10)
				if err != nil || out == in {
					t.Fatalf("SplitTermsV1(16, 256 KiB) with Range %q = %v, %v; want a split", r.rangeHeader, out, err)
				}
				want := f.content[r.start:][:ExpectedLengthV1(in)]
				if got := f.readV1(t, in); !bytes.Equal(got, want) {
					t.Fatalf("NewReaderV1 over the builder's response with Range %q read %d bytes, want %d of the file", r.rangeHeader, len(got), len(want))
				}
				if got := f.readV1(t, out); !bytes.Equal(got, want) || ExpectedLengthV1(out) != ExpectedLengthV1(in) {
					t.Errorf("NewReaderV1 over SplitTermsV1's response with Range %q read %d bytes of expected length %d, want the builder's %d bytes", r.rangeHeader, len(got), ExpectedLengthV1(out), len(want))
				}
			}
		})
	}
}

// tsRequest is a Range header with the split parameters.
type tsRequest struct {
	rangeHeader string
	minTerms    int
	pieceBytes  int64
}

func TestSplitTermsV1CutsTheSamePiecesAsSplitTermsV2(t *testing.T) {
	type parityCase struct {
		name     string
		f        *tsFixture
		requests []tsRequest
	}
	var cases []parityCase
	for _, c := range tsRuleCases() {
		cases = append(cases, parityCase{c.name, c.f, []tsRequest{{c.rangeHeader, c.minTerms, c.pieceBytes}}})
	}
	for _, c := range tsDescriptorCases() {
		cases = append(cases, parityCase{c.name, tsNewFixture(c.sizes, c.terms...), []tsRequest{{"", c.minTerms, 1 << 20}}})
	}
	varied := tsVariedFixture()
	size := ExpectedLengthV1(varied.buildV1(t, varied.parse("")))
	cases = append(cases, parityCase{"varied chunks", varied, []tsRequest{{"", 16, 256 << 10}, {"bytes=300000-", 16, 256 << 10}, {fmt.Sprintf("bytes=300000-%d", size-300000), 16, 256 << 10}}})
	for seed := range uint64(16) {
		f, ranges := tsRandomFile(seed)
		var requests []tsRequest
		for _, rangeHeader := range ranges {
			for _, minTerms := range []int{3, 16} {
				for _, pieceBytes := range []int64{0, 256 << 10, 1 << 20} {
					requests = append(requests, tsRequest{rangeHeader, minTerms, pieceBytes})
				}
			}
		}
		cases = append(cases, parityCase{fmt.Sprintf("random file %d", seed), f, requests})
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, r := range c.requests {
				rng := c.f.parse(r.rangeHeader)
				call := fmt.Sprintf("(%d, %d) with Range %q of %s", r.minTerms, r.pieceBytes, r.rangeHeader, c.f.format(c.f.buildV1(t, c.f.parse("")).Terms))
				v1, err := SplitTermsV1(t.Context(), c.f.st, c.f.sh, c.f.buildV1(t, rng), rng, r.minTerms, r.pieceBytes)
				if err != nil {
					t.Fatalf("SplitTermsV1%s: %v", call, err)
				}
				v2 := c.f.split(t, c.f.build(t, rng), rng, r.minTerms, r.pieceBytes)
				if !c.f.wantParity(t, "Split"+call, v1, v2) {
					return
				}
			}
		})
	}
}
