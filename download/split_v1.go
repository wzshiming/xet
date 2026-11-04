package download

import (
	"context"
	"fmt"
	"slices"

	"github.com/wzshiming/xet/shard"
)

// SplitTermsV1 halves the term with the most requested bytes at the chunk boundary nearest their middle
// until the response has minTerms terms or every cut would leave a side under pieceBytes requested bytes.
// minTerms <= 1 disables splitting and pieceBytes <= 0 means no floor.
// Terms whose xorb has no CAS block in sh stay whole; no piece starts past rng.End, rng being the range
// passed to the builder. Pieces keep the url of their term's fetch info entry.
// It returns in itself if it cuts nothing and never modifies in.
func SplitTermsV1(ctx context.Context, st StorageAdapter, sh *shard.Shard, in *ReconstructionResponseV1, rng ByteRange, minTerms int, pieceBytes int64) (*ReconstructionResponseV1, error) {
	terms, xorbs, err := splitTerms(ctx, st, sh, in.Terms, in.OffsetIntoFirstRange, rng, minTerms, pieceBytes)
	if err != nil {
		return nil, err
	}
	if terms == nil {
		return in, nil
	}

	// A term's entry is the first of its xorb holding its chunks, as clients look it up.
	urls := make([]string, len(in.Terms))
	for i, term := range in.Terms {
		if _, ok := xorbs[term.Hash]; !ok {
			continue
		}
		entries := in.FetchInfo[term.Hash]
		k := slices.IndexFunc(entries, func(e FetchInfoEntry) bool {
			return e.Range.Start <= term.Range.Start && term.Range.End <= e.Range.End
		})
		if k < 0 {
			return nil, fmt.Errorf("term %d: no fetch info for chunks [%d, %d) of xorb %s", i, term.Range.Start, term.Range.End, term.Hash)
		}
		urls[i] = entries[k].URL
	}

	out := *in
	out.Terms = terms
	out.FetchInfo = make(map[string][]FetchInfoEntry, len(in.FetchInfo))
	for hash, entries := range in.FetchInfo {
		if _, ok := xorbs[hash]; !ok {
			out.FetchInfo[hash] = entries
		}
	}
	for hash, pieces := range xorbs {
		entries := make([]FetchInfoEntry, len(pieces))
		for i, p := range pieces {
			entries[i] = FetchInfoEntry{Range: p.Range, URL: urls[p.src], URLRange: p.packed}
		}
		out.FetchInfo[hash] = entries
	}
	return &out, nil
}
