package spool

import (
	"context"
	"io/fs"

	"github.com/wzshiming/xet/storage"
)

// Usage sums the spool files on disk, idle or in flight; it is read-only and does not provide an atomic snapshot.
func (s *Spool) Usage(ctx context.Context) (storage.ObjectUsage, error) {
	var usage storage.ObjectUsage
	err := s.walkSpools(ctx, func(_ string, info fs.FileInfo) error {
		usage.Count++
		usage.Bytes += info.Size()
		return nil
	})
	if err != nil {
		return storage.ObjectUsage{}, err
	}
	return usage, nil
}
