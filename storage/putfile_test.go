package storage

import (
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/xorb"
)

// The grouper closes the groups GroupChunkIndicesBySize forms over the ids never removed.
func TestXorbGrouperMatchesGroupChunkIndicesBySize(t *testing.T) {
	rng := rand.New(rand.NewChaCha8([32]byte{}))
	targets := []uint64{xet.MaxXorbSize, 1, 1000, xet.MaxChunkSize, 3 * xet.MaxChunkSize, 1 << 20}
	maxSizes := []int{xet.MaxChunkSize, 8 << 10, 64, 1}
	for round := range 1000 {
		target := targets[rng.IntN(len(targets))]
		maxSize := maxSizes[rng.IntN(len(maxSizes))]
		n, removeEvery := rng.IntN(300), 4
		if rng.IntN(10) == 0 {
			n, removeEvery = xet.MaxChunksPerXorb+1+rng.IntN(2*xet.MaxChunksPerXorb), 256
		}

		g := newXorbGrouper(target)
		sizes := make([]uint32, n)
		removed := map[int]bool{}
		var got [][]int
		// Ids whose add closed a group stay: removing one leaves a close the survivors alone would not make.
		var removable []int
		for id := range sizes {
			sizes[id] = uint32(1 + rng.IntN(maxSize))
			if closed := g.add(id, sizes[id]); len(closed) > 0 {
				got = append(got, closed)
				removable = removable[:0]
			} else {
				removable = append(removable, id)
			}
			if len(removable) == 0 || rng.IntN(removeEvery) != 0 {
				continue
			}
			k := rng.IntN(len(removable))
			victim := removable[k]
			removable[k] = removable[len(removable)-1]
			removable = removable[:len(removable)-1]
			if !g.remove(victim) {
				t.Fatalf("round %d: remove(%d) of a pending id = false", round, victim)
			}
			if g.remove(victim) {
				t.Fatalf("round %d: second remove(%d) = true", round, victim)
			}
			removed[victim] = true
		}
		if len(got) > 0 {
			group := got[rng.IntN(len(got))]
			if id := group[rng.IntN(len(group))]; g.remove(id) {
				t.Fatalf("round %d: remove(%d) of a closed id = true", round, id)
			}
		}
		if last := g.flush(); len(last) > 0 {
			got = append(got, last)
		}
		if again := g.flush(); len(again) > 0 {
			t.Fatalf("round %d: flush after flush = %v, want nothing", round, again)
		}

		var survivors []int
		var survivorSizes []uint32
		for id, size := range sizes {
			if !removed[id] {
				survivors = append(survivors, id)
				survivorSizes = append(survivorSizes, size)
			}
		}
		var want [][]int
		for _, group := range xorb.GroupChunkIndicesBySize(survivorSizes, target) {
			ids := make([]int, len(group))
			for i, pos := range group {
				ids[i] = survivors[pos]
			}
			want = append(want, ids)
		}
		if !slices.EqualFunc(got, want, slices.Equal[[]int]) {
			first := 0
			for first < min(len(got), len(want)) && slices.Equal(got[first], want[first]) {
				first++
			}
			t.Fatalf("round %d (target %d, %d ids, %d removed): %d closed groups, want %d, first differing at group %d",
				round, target, n, len(removed), len(got), len(want), first)
		}
	}
}
