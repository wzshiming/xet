package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/wzshiming/xet/mirror"
	"github.com/wzshiming/xet/storage"
)

// runPeriodicGC runs one in-process collection per interval on the serving
// storage. With a mirror, retention is applied through the handler first so
// memory and disk stay consistent, and its live entries become the roots;
// otherwise every uploaded file is a root and only orphans are collected.
func runPeriodicGC(st *storage.FileStorage, m *mirror.Handler, interval, grace, pruneAge time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		opts := []storage.GCOption{storage.WithGCGracePeriod(grace)}
		if m != nil {
			if pruneAge > 0 {
				pruned, err := m.PruneIndex(pruneAge)
				if err != nil {
					fmt.Fprintf(os.Stderr, "gc: prune mirror index: %v\n", err)
					continue
				}
				if pruned.RemovedEntries > 0 || pruned.RemovedBranches > 0 {
					fmt.Printf("gc: pruned %d index entries, %d branch pins\n", pruned.RemovedEntries, pruned.RemovedBranches)
				}
			}
			opts = append(opts, storage.WithGCRoots(m.GCRoots()))
		}
		res, err := st.GC(context.Background(), opts...)
		if err != nil {
			fmt.Fprintf(os.Stderr, "gc: %v\n", err)
			continue
		}
		if removed := res.RemovedFiles + res.RemovedShards + res.RemovedXorbs + res.RemovedChunks + res.RemovedSHA256s + res.RemovedTemps; removed > 0 {
			fmt.Printf("gc: removed %d objects, reclaimed %d bytes (%d files, %d shards, %d xorbs live)\n",
				removed, res.ReclaimedBytes, res.LiveFiles, res.LiveShards, res.LiveXorbs)
		}
	}
}

