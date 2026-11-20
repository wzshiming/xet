package storage

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
)

// walks compare mtimes against the cutoff truncated to whole seconds. For
// every sub-second cutoff phase, an object written at or after the raw
// cutoff must still read in-grace when its mtime is reported at S3's second
// precision — truncation may only widen the shield, never shrink it.
func TestObjectCutoffTruncationInvariant(t *testing.T) {
	base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	for phaseMs := 0; phaseMs < 1000; phaseMs += 25 {
		cutoff := base.Add(time.Duration(phaseMs) * time.Millisecond)
		objCutoff := cutoff.Truncate(time.Second)
		for deltaMs := 0; deltaMs < 2500; deltaMs += 25 {
			event := cutoff.Add(time.Duration(deltaMs) * time.Millisecond)
			reported := event.Truncate(time.Second)
			if reported.Before(objCutoff) {
				t.Fatalf("object at cutoff+%dms (phase %dms) reads dead: reported %v < objCutoff %v",
					deltaMs, phaseMs, reported, objCutoff)
			}
		}
	}
}

// A chunk entry another actor removes between the sweep's read and its Delete is not counted.
func TestSweepCountsChunkEntryOnlyWhenDeleteFindsIt(t *testing.T) {
	const shardHash = "dead-shard"
	found, vanished := xet.ChunkHash{0: 1}, xet.ChunkHash{0: 2}
	content := shard.NewShard()
	content.AddCASBlock(shard.CASBlock{CASHash: xet.XorbHash{0: 1}, Chunks: []shard.CASChunkSequenceEntry{
		{ChunkHash: found, Flags: shard.ChunkGlobalDedupEligible},
		{ChunkHash: vanished, Flags: shard.ChunkGlobalDedupEligible},
	}})
	st := &sweepStore{
		shards:  []SweptObject{{Hash: shardHash, Size: 1}},
		content: content,
		chunks:  map[xet.ChunkHash]string{found: shardHash, vanished: shardHash},
		gone:    map[string]bool{vanished.String(): true},
	}
	res, err := Sweep(context.Background(), st, SweepOptions{})
	if err != nil {
		t.Fatalf("Sweep() = %v", err)
	}
	if res.DeletedChunkEntries != 1 {
		t.Fatalf("DeletedChunkEntries = %d, want 1: the entry another actor removed first counts for nothing", res.DeletedChunkEntries)
	}
}

// A shard another actor removes first is not listed, reclaims nothing and charges no MaxDeletes slot.
func TestSweepStepCountsShardOnlyWhenDeleteFindsIt(t *testing.T) {
	// vanished walks first, so charging it would spend the only slot.
	vanished := SweptObject{Hash: "vanished-shard", Size: 10}
	found := SweptObject{Hash: "found-shard", Size: 100}
	st := &sweepStore{
		shards:  []SweptObject{vanished, found},
		content: shard.NewShard(),
		gone:    map[string]bool{vanished.Hash: true},
	}
	res, err := NewGC(st).SweepStep(context.Background(), SweepOptions{MaxDeletes: 1})
	if err != nil {
		t.Fatalf("SweepStep() = %v", err)
	}
	if want := []SweptObject{found}; !slices.Equal(res.SweptShards, want) || res.ReclaimedBytes != found.Size {
		t.Fatalf("SweepStep() swept shards %v reclaiming %d bytes, want %v reclaiming %d", res.SweptShards, res.ReclaimedBytes, want, found.Size)
	}
}

// A xorb another actor removes first is not listed, reclaims nothing and charges no MaxDeletes slot.
func TestSweepStepCountsXorbOnlyWhenDeleteFindsIt(t *testing.T) {
	// vanished walks first, so charging it would spend the only slot.
	vanished := SweptObject{Hash: xet.XorbHash{0: 1}.String(), Size: 10}
	found := SweptObject{Hash: xet.XorbHash{0: 2}.String(), Size: 100}
	st := &sweepStore{
		xorbs: []SweptObject{vanished, found},
		gone:  map[string]bool{vanished.Hash: true},
	}
	res, err := NewGC(st).SweepStep(context.Background(), SweepOptions{MaxDeletes: 1})
	if err != nil {
		t.Fatalf("SweepStep() = %v", err)
	}
	if want := []SweptObject{found}; !slices.Equal(res.SweptXorbs, want) || res.ReclaimedBytes != found.Size {
		t.Fatalf("SweepStep() swept xorbs %v reclaiming %d bytes, want %v reclaiming %d", res.SweptXorbs, res.ReclaimedBytes, want, found.Size)
	}
}

// sweepStore holds long-unreferenced shards, each loading as content, xorbs
// and chunk entries; another actor removes the names in gone just before the
// sweep's Delete reaches them.
type sweepStore struct {
	Storage
	shards  []SweptObject
	content *shard.Shard
	xorbs   []SweptObject
	chunks  map[xet.ChunkHash]string
	gone    map[string]bool
}

func (s *sweepStore) WalkFileIndex(context.Context, func(fileHash, shardHash string) error) error {
	return nil
}

func (s *sweepStore) WalkSHA256Index(context.Context, func(sha256Hex, shardHash string) error) error {
	return nil
}

func (s *sweepStore) WalkShards(_ context.Context, fn func(shardHash string, size int64, modTime time.Time) error) error {
	return walkLongAgo(s.shards, fn)
}

func (s *sweepStore) WalkXorbs(_ context.Context, fn func(xorbHash string, size int64, modTime time.Time) error) error {
	return walkLongAgo(s.xorbs, fn)
}

// walkLongAgo reports every object as written at the zero time, outside any grace window.
func walkLongAgo(objs []SweptObject, fn func(hash string, size int64, modTime time.Time) error) error {
	for _, o := range objs {
		if err := fn(o.Hash, o.Size, time.Time{}); err != nil {
			return err
		}
	}
	return nil
}

func (s *sweepStore) LoadShard(context.Context, string) (*shard.Shard, error) {
	return s.content, nil
}

func (s *sweepStore) GetChunkIndexEntry(_ context.Context, chunkHash xet.ChunkHash) (string, error) {
	return s.chunks[chunkHash], nil
}

func (s *sweepStore) DeleteChunkIndexEntry(_ context.Context, chunkHash xet.ChunkHash) (bool, error) {
	return !s.gone[chunkHash.String()], nil
}

func (s *sweepStore) DeleteShard(_ context.Context, shardHash string) (bool, error) {
	return !s.gone[shardHash], nil
}

func (s *sweepStore) DeleteXorb(_ context.Context, xorbHash xet.XorbHash) (bool, error) {
	return !s.gone[xorbHash.String()], nil
}
