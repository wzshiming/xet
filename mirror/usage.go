package mirror

import (
	"context"
	"errors"
	"io/fs"

	"github.com/wzshiming/xet/storage"
)

// Usage covers the index; spool usage is the Spool's, and the shared CAS and client cache directories are excluded.
type Usage struct {
	Index storage.ObjectUsage
}

// Usage is read-only and does not provide an atomic snapshot.
func (m *Mirror) Usage(ctx context.Context) (Usage, error) {
	var usage Usage
	err := m.walkIndex(ctx, func(_, _ string, ent fs.DirEntry) error {
		info, err := ent.Info()
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		usage.Index.Count++
		usage.Index.Bytes += info.Size()
		return nil
	})
	if err != nil {
		return Usage{}, err
	}
	return usage, nil
}
