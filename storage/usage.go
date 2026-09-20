package storage

import (
	"context"
	"time"
)

// ObjectUsage counts stored objects of one kind and their stored payload bytes.
type ObjectUsage struct {
	Count int64
	Bytes int64
}

type Usage struct {
	Xorbs       ObjectUsage
	Shards      ObjectUsage
	FileIndex   ObjectUsage
	ChunkIndex  ObjectUsage
	SHA256Index ObjectUsage
}

// ComputeUsage walks kinds separately, so the result is not an atomic snapshot.
func ComputeUsage(ctx context.Context, walk func(context.Context, string, func(string, int64, time.Time) error) error) (Usage, error) {
	var usage Usage
	kinds := []struct {
		kind string
		dst  *ObjectUsage
	}{
		{"xorbs", &usage.Xorbs},
		{"shards", &usage.Shards},
		{"index/files", &usage.FileIndex},
		{"index/chunks", &usage.ChunkIndex},
		{"index/sha256", &usage.SHA256Index},
	}
	for _, entry := range kinds {
		if err := ctx.Err(); err != nil {
			return Usage{}, err
		}
		if err := walk(ctx, entry.kind, func(_ string, size int64, _ time.Time) error {
			entry.dst.Count++
			entry.dst.Bytes += size
			return nil
		}); err != nil {
			return Usage{}, err
		}
	}
	return usage, nil
}
