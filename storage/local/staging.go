package local

import (
	"context"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/internal/crc64nvme"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/xorb"
)

// GrantXorbUpload issues a grant whose upload goes through the CAS server.
func (fs *Storage) GrantXorbUpload(_ context.Context, xorbHash xet.XorbHash, length int64, checksum uint64) (*storage.XorbUploadGrant, error) {
	id := fs.grants.Issue(xorbHash, length, checksum)
	return &storage.XorbUploadGrant{
		ID:      id,
		Method:  http.MethodPut,
		URL:     fs.baseURL + storage.XorbUploadPath(id),
		Headers: map[string]string{},
	}, nil
}

// PutStagedXorb streams at most the granted length plus one byte into a temp
// beside the staging object and renames it into place once the length and
// CRC-64/NVME match the grant.
func (fs *Storage) PutStagedXorb(_ context.Context, grantID string, r io.Reader) error {
	// Only issued, well-formed ids pass Lookup, so grantID is safe in a path below.
	grant, err := fs.grants.Lookup(grantID)
	if err != nil {
		return err
	}
	stagedPath := fs.objectPath("staging", grantID)
	if _, err := os.Stat(stagedPath); err == nil {
		return storage.ErrStagedXorbExists
	} else if !errors.Is(err, iofs.ErrNotExist) {
		return fmt.Errorf("check staged xorb: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(stagedPath), 0755); err != nil {
		return fmt.Errorf("create staging directory: %w", err)
	}
	f, err := os.CreateTemp(filepath.Dir(stagedPath), ".staged-*")
	if err != nil {
		return fmt.Errorf("create staged xorb file: %w", err)
	}
	renamed := false
	defer func() {
		if !renamed {
			f.Close()
			os.Remove(f.Name())
		}
	}()

	sum := crc64nvme.New()
	n, err := io.Copy(io.MultiWriter(f, sum), io.LimitReader(r, grant.Length+1))
	if err != nil {
		return fmt.Errorf("receive staged xorb: %w", err)
	}
	if n != grant.Length || sum.Sum64() != grant.Checksum {
		return fmt.Errorf("%w: got %d bytes with crc64nvme %d, want %d bytes with %d", storage.ErrStagedXorbMismatch, n, sum.Sum64(), grant.Length, grant.Checksum)
	}
	if err := f.Chmod(0644); err != nil {
		return fmt.Errorf("finalize staged xorb file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("finalize staged xorb file: %w", err)
	}
	if err := os.Rename(f.Name(), stagedPath); err != nil {
		return fmt.Errorf("finalize staged xorb file: %w", err)
	}
	renamed = true
	return nil
}

// CommitStagedXorb validates the staged upload as xorbHash and renames it into xorbs/.
func (fs *Storage) CommitStagedXorb(ctx context.Context, grantID string, xorbHash xet.XorbHash) (bool, error) {
	if !storage.ValidGrantID(grantID) {
		return false, storage.ErrStagedXorbNotFound
	}
	stagedPath := fs.objectPath("staging", grantID)
	f, err := os.Open(stagedPath)
	if err != nil {
		if errors.Is(err, iofs.ErrNotExist) {
			return false, storage.ErrStagedXorbNotFound
		}
		return false, fmt.Errorf("open staged xorb: %w", err)
	}
	exists, err := fs.HasXorb(ctx, xorbHash)
	if err != nil {
		f.Close()
		return false, err
	}
	if exists {
		f.Close()
		return false, fs.dropStaged(ctx, grantID)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return false, fmt.Errorf("stat staged xorb: %w", err)
	}
	// Validate stops at a footer, leaving N at the staged bytes past it.
	r := &io.LimitedReader{R: f, N: info.Size()}
	err = xorb.Validate(r, xorbHash)
	f.Close()
	if err == nil && r.N > 0 {
		err = fmt.Errorf("%d trailing bytes after the xorb", r.N)
	}
	if err != nil {
		return false, errors.Join(fmt.Errorf("%w: %w", storage.ErrStagedXorbMismatch, err), fs.dropStaged(ctx, grantID))
	}

	xorbPath := fs.objectPath("xorbs", xorbHash.String())
	if err := os.MkdirAll(filepath.Dir(xorbPath), 0755); err != nil {
		return false, fmt.Errorf("create xorb directory: %w", err)
	}
	// Rename keeps the upload's mtime, but the xorb's GC grace starts at its commit.
	now := time.Now()
	if err := os.Chtimes(stagedPath, now, now); err != nil {
		// A sweep may reclaim the upload after the commit opened it.
		if errors.Is(err, iofs.ErrNotExist) {
			return false, storage.ErrStagedXorbNotFound
		}
		return false, fmt.Errorf("stamp staged xorb: %w", err)
	}
	if err := os.Rename(stagedPath, xorbPath); err != nil {
		if errors.Is(err, iofs.ErrNotExist) {
			return false, storage.ErrStagedXorbNotFound
		}
		return false, fmt.Errorf("publish staged xorb: %w", err)
	}
	fs.grants.Remove(grantID)
	return true, nil
}

// dropStaged forgets the grant and deletes its staged upload.
func (fs *Storage) dropStaged(ctx context.Context, grantID string) error {
	fs.grants.Remove(grantID)
	return fs.DeleteStagedXorb(ctx, grantID)
}

// WalkStagedXorbs calls fn for every completely staged upload, skipping in-flight temps.
func (fs *Storage) WalkStagedXorbs(ctx context.Context, fn func(grantID string, size int64, modTime time.Time) error) error {
	root := filepath.Join(fs.basePath, "staging")
	return filepath.WalkDir(root, func(path string, d iofs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, iofs.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		grantID := strings.ReplaceAll(filepath.ToSlash(rel), "/", "")
		if !storage.ValidGrantID(grantID) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			if errors.Is(err, iofs.ErrNotExist) {
				return nil
			}
			return err
		}
		return fn(grantID, info.Size(), info.ModTime())
	})
}

// DeleteStagedXorb removes a staged upload; a missing one is not an error.
func (fs *Storage) DeleteStagedXorb(_ context.Context, grantID string) error {
	if !storage.ValidGrantID(grantID) {
		return nil
	}
	if err := os.Remove(fs.objectPath("staging", grantID)); err != nil && !errors.Is(err, iofs.ErrNotExist) {
		return fmt.Errorf("delete staged xorb: %w", err)
	}
	return nil
}
