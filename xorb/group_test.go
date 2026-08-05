package xorb

import (
	"testing"

	"github.com/wzshiming/xet"
)

func TestGroupChunkIndicesAllowsExactRawPayloadLimit(t *testing.T) {
	groups := GroupChunkIndicesBySize([]uint32{40, 60, 1}, 100)
	if len(groups) != 2 {
		t.Fatalf("group count = %d, want 2", len(groups))
	}
	if len(groups[0]) != 2 || groups[0][0] != 0 || groups[0][1] != 1 {
		t.Fatalf("first group = %v, want [0 1]", groups[0])
	}
	if len(groups[1]) != 1 || groups[1][0] != 2 {
		t.Fatalf("second group = %v, want [2]", groups[1])
	}
}

func TestGroupChunkIndicesEnforcesChunkCountLimit(t *testing.T) {
	sizes := make([]uint32, xet.MaxChunksPerXorb+1)
	for i := range sizes {
		sizes[i] = 1
	}

	groups := GroupChunkIndicesBySize(sizes, xet.MaxXorbSize)
	if len(groups) != 2 {
		t.Fatalf("group count = %d, want 2", len(groups))
	}
	if len(groups[0]) != xet.MaxChunksPerXorb || len(groups[1]) != 1 {
		t.Fatalf("group sizes = [%d %d], want [%d 1]", len(groups[0]), len(groups[1]), xet.MaxChunksPerXorb)
	}
}
