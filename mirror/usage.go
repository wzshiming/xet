package mirror

import (
	"context"
	"errors"
	"io/fs"

	"github.com/wzshiming/xet/storage"
)

// Usage is read-only and does not provide an atomic snapshot.
func (m *Mirror) Usage(ctx context.Context) (storage.ObjectUsage, error) {
	var usage storage.ObjectUsage
	err := m.walkIndex(ctx, func(_, _ string, ent fs.DirEntry) error {
		info, err := ent.Info()
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
		return storage.ObjectUsage{}, err
	}
	return usage, nil
}
