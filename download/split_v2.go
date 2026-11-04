package download

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/xorb"
)

// SplitTermsV2 halves the term with the most requested bytes at the chunk boundary nearest their middle
// until the response has minTerms terms or every cut would leave a side under pieceBytes requested bytes.
// minTerms <= 1 disables splitting and pieceBytes <= 0 means no floor.
// Terms whose xorb has no CAS block in sh stay whole; no piece starts past rng.End, rng being the range
// passed to the builder. It returns in itself if it cuts nothing and never modifies in.
func SplitTermsV2(ctx context.Context, st StorageAdapter, sh *shard.Shard, in *ReconstructionResponseV2, rng ByteRange, minTerms int, pieceBytes int64) (*ReconstructionResponseV2, error) {
	terms, xorbs, err := splitTerms(ctx, st, sh, in.Terms, in.OffsetIntoFirstRange, rng, minTerms, pieceBytes)
	if err != nil {
		return nil, err
	}
	if terms == nil {
		return in, nil
	}

	out := *in
	out.Terms = terms
	out.Xorbs = make(map[string][]XorbMultiRangeFetch, len(in.Xorbs))
	for hash, fetches := range in.Xorbs {
		if _, ok := xorbs[hash]; !ok {
			out.Xorbs[hash] = fetches
		}
	}
	for hash, pieces := range xorbs {
		if len(in.Xorbs[hash]) == 0 {
			return nil, fmt.Errorf("no fetch info for xorb %s", hash)
		}
		rgs := make([]XorbRangeDescriptor, len(pieces))
		for i, p := range pieces {
			rgs[i] = XorbRangeDescriptor{Chunks: p.Range, Bytes: p.packed}
		}
		out.Xorbs[hash] = []XorbMultiRangeFetch{{URL: in.Xorbs[hash][0].URL, Ranges: rgs}}
	}
	return &out, nil
}

// splitTerms cuts the terms of a response starting offset bytes into the first by the rule of SplitTermsV2.
// It returns the output terms and, for each xorb with a cut term, that xorb's pieces with their packed byte
// ranges, sorted by start chunk and keeping only the longest per start; both are nil when nothing is cut.
func splitTerms(ctx context.Context, st StorageAdapter, sh *shard.Shard, terms []Term, offset int64, rng ByteRange, minTerms int, pieceBytes int64) ([]Term, map[string][]splitPiece, error) {
	if minTerms <= 1 || len(terms) >= minTerms {
		return nil, nil, nil
	}

	pieces := make([]splitPiece, 0, len(terms))
	var size int64
	for i, term := range terms {
		p := splitPiece{Term: term, src: i, start: size}
		size += int64(term.UnpackedLength)
		var err error
		if p.xorbHash, err = xet.ParseXorbHash(term.Hash); err != nil {
			return nil, nil, fmt.Errorf("term %d: %w", i, err)
		}
		if k := slices.IndexFunc(sh.CASInfos, func(b shard.CASBlock) bool { return b.CASHash == p.xorbHash }); k >= 0 {
			p.chunks = sh.CASInfos[k].Chunks
			if uint64(term.Range.End) > uint64(len(p.chunks)) {
				return nil, nil, fmt.Errorf("term %d: chunk range [%d, %d) out of bounds: CAS block has %d chunks", i, term.Range.Start, term.Range.End, len(p.chunks))
			}
			var sum uint64
			for c := term.Range.Start; c < term.Range.End; c++ {
				sum += uint64(p.chunks[c].UnpackedSegBytes)
			}
			if sum != term.UnpackedLength {
				return nil, nil, fmt.Errorf("term %d: chunks [%d, %d) hold %d unpacked bytes, term has %d", i, term.Range.Start, term.Range.End, sum, term.UnpackedLength)
			}
		}
		pieces = append(pieces, p)
	}

	w := splitWindow{lo: offset, hi: offset + max(rng.End-rng.Start+1, 0), floor: max(pieceBytes, 1)}

	for i := range pieces {
		w.plan(&pieces[i])
	}
	for len(pieces) < minTerms {
		k := -1
		for i := range pieces {
			if pieces[i].cut > 0 && (k < 0 || pieces[i].want > pieces[k].want) {
				k = i
			}
		}
		if k < 0 {
			break
		}
		head, tail := pieces[k], pieces[k]
		head.Range.End, head.UnpackedLength = head.cut, uint64(head.at-head.start)
		tail.Range.Start, tail.UnpackedLength, tail.start = tail.cut, tail.UnpackedLength-head.UnpackedLength, tail.at
		head.split, tail.split = true, true
		w.plan(&head)
		w.plan(&tail)
		pieces = slices.Replace(pieces, k, k+1, head, tail)
	}
	if len(pieces) == len(terms) {
		return nil, nil, nil
	}

	out := make([]Term, len(pieces))
	xorbs := map[string][]splitPiece{}
	for i, p := range pieces {
		out[i] = p.Term
		if p.split {
			xorbs[p.Hash] = nil
		}
	}
	offsets := map[string][]uint64{}
	for _, p := range pieces {
		if _, ok := xorbs[p.Hash]; !ok {
			continue
		}
		if _, ok := offsets[p.Hash]; !ok {
			o, err := st.GetXorbChunkOffsets(ctx, p.xorbHash)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to get xorb chunk offsets: %w", err)
			}
			offsets[p.Hash] = o
		}
		var err error
		if p.packed.Start, p.packed.End, err = xorb.ChunkDataRangeFromOffsets(offsets[p.Hash], p.Range.Start, p.Range.End); err != nil {
			return nil, nil, fmt.Errorf("failed to get xorb data range: %w", err)
		}
		xorbs[p.Hash] = append(xorbs[p.Hash], p)
	}
	for hash, ps := range xorbs {
		// hf_xet keys a xorb's blocks by start chunk: of the ranges sharing one, keep the longest.
		slices.SortStableFunc(ps, func(a, b splitPiece) int {
			return cmp.Or(cmp.Compare(a.Range.Start, b.Range.Start), cmp.Compare(b.Range.End, a.Range.End))
		})
		xorbs[hash] = slices.CompactFunc(ps, func(a, b splitPiece) bool { return a.Range.Start == b.Range.Start })
	}
	return out, xorbs, nil
}

// splitPiece is a term at response offset start with its chosen cut: chunk cut at response offset at, cut 0 for none.
type splitPiece struct {
	Term
	src      int // index of the input term it is a piece of
	xorbHash xet.XorbHash
	chunks   []shard.CASChunkSequenceEntry // its xorb's CAS block, nil without one
	start    int64
	split    bool
	want     int64 // requested bytes
	cut      uint32
	at       int64
	packed   ByteRange // packed bytes of its chunks, set only for xorbs with a cut term
}

// splitWindow holds the requested bytes [lo, hi) of a response and the fewest a piece may keep.
type splitWindow struct{ lo, hi, floor int64 }

func (w splitWindow) bytes(start, end int64) int64 {
	return max(min(end, w.hi)-max(start, w.lo), 0)
}

// plan sets p's requested bytes and its valid cut nearest their middle, the lower on a tie.
func (w splitWindow) plan(p *splitPiece) {
	end := p.start + int64(p.UnpackedLength)
	mid := max(p.start, w.lo) + min(end, w.hi) // twice the middle
	p.want, p.cut = w.bytes(p.start, end), 0
	best, at := int64(-1), p.start
	for c := int64(p.Range.Start) + 1; c < int64(p.Range.End) && p.chunks != nil; c++ {
		at += int64(p.chunks[c-1].UnpackedSegBytes)
		if at <= w.lo || at >= w.hi || w.bytes(p.start, at) < w.floor || w.bytes(at, end) < w.floor {
			continue
		}
		if d := max(2*at-mid, mid-2*at); best < 0 || d < best {
			best, p.cut, p.at = d, uint32(c), at
		}
	}
}
