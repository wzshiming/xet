package mirror

import (
	"context"
	"errors"
	"io/fs"
	"path/filepath"

	"github.com/wzshiming/xet/storage"
)

// Usage excludes the shared CAS and client cache directories.
type Usage struct {
	Index storage.ObjectUsage
	Spool storage.ObjectUsage
}

// Usage is read-only and does not provide an atomic snapshot.
func (m *Mirror) Usage(ctx context.Context) (Usage, error) {
	var usage Usage
	if err := sumFiles(ctx, m.indexDir, &usage.Index); err != nil {
		return Usage{}, err
	}
	if err := sumFiles(ctx, m.spoolDir, &usage.Spool); err != nil {
		return Usage{}, err
	}
	return usage, nil
}

func sumFiles(ctx context.Context, root string, dst *storage.ObjectUsage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info()
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		dst.Count++
		dst.Bytes += info.Size()
		return nil
	})
}
