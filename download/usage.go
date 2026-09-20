package download

import (
	"context"
	"errors"
	"io/fs"
	"path/filepath"
)

// CacheUsage includes every regular file under the cache directory.
type CacheUsage struct {
	Count int64
	Bytes int64
}

// Usage is read-only and does not provide an atomic snapshot.
func (m *CacheManager) Usage(ctx context.Context) (CacheUsage, error) {
	if err := ctx.Err(); err != nil {
		return CacheUsage{}, err
	}
	var usage CacheUsage
	err := filepath.WalkDir(m.dir, func(_ string, entry fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		info, err := entry.Info()
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		usage.Count++
		usage.Bytes += info.Size()
		return nil
	})
	if err != nil {
		return CacheUsage{}, err
	}
	return usage, nil
}
