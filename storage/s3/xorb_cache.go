package s3

import (
	"context"
	"fmt"
	"io"
	iofs "io/fs"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/internal/xorbmap"
	"github.com/wzshiming/xet/storage"
)

// xorbCachePath returns where the local copy of h lives, independent of the key prefix.
func (ss *Storage) xorbCachePath(h xet.XorbHash) string {
	return filepath.Join(ss.cacheDir, filepath.FromSlash(storage.ObjectKey("xorbs", h.String())))
}

// downloadXorb copies the whole xorb object into the local cache and opens the copy.
func (ss *Storage) downloadXorb(h xet.XorbHash) (*os.File, error) {
	// Every concurrent reader of h shares this download, so no single request's context may cancel it.
	out, err := ss.client.GetObject(context.Background(), &awss3.GetObjectInput{
		Bucket: aws.String(ss.bucket),
		Key:    aws.String(ss.objectKey("xorbs", h.String())),
	})
	if err != nil {
		if isS3NotFound(err) {
			return nil, fmt.Errorf("xorb %s: %w", h.String(), iofs.ErrNotExist)
		}
		return nil, fmt.Errorf("get xorb object: %w", err)
	}
	defer out.Body.Close()

	path := ss.xorbCachePath(h)
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create xorb cache directory: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".xorb-*")
	if err != nil {
		return nil, fmt.Errorf("create xorb cache file: %w", err)
	}
	_, err = io.Copy(tmp, out.Body)
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		os.Remove(tmp.Name())
		return nil, fmt.Errorf("download xorb: %w", err)
	}

	ss.xorbFiles.Lock()
	defer ss.xorbFiles.Unlock()
	if err := os.Rename(tmp.Name(), path); err != nil {
		os.Remove(tmp.Name())
		return nil, fmt.Errorf("download xorb: %w", err)
	}
	f, err := os.Open(path)
	if err != nil {
		os.Remove(path)
		return nil, err
	}
	ss.xorbLive[h]++
	return f, nil
}

// removeXorbFile is the cache's release hook; it may run under the cache lock, so it never calls the cache.
func (ss *Storage) removeXorbFile(h xet.XorbHash) {
	ss.xorbFiles.Lock()
	defer ss.xorbFiles.Unlock()
	if n := ss.xorbLive[h] - 1; n > 0 {
		ss.xorbLive[h] = n
		return
	}
	delete(ss.xorbLive, h)
	os.Remove(ss.xorbCachePath(h))
}

// copyXorbs reads the cached copies' files for PutShard's pass over every xorb a shard
// names: each copy still downloads once and stays cached, but no page of it is faulted
// in through its mapping, where it would stay resident until the copy is evicted.
type copyXorbs struct{ *Storage }

func (x copyXorbs) GetXorbReadSeekCloser(_ context.Context, xorbHash xet.XorbHash) (io.ReadSeekCloser, error) {
	// The hold keeps the copy on disk until Close.
	m, err := x.xorbCache.Acquire(xorbHash)
	if err != nil {
		return nil, fmt.Errorf("get xorb: %w", err)
	}
	f, err := os.Open(x.xorbCachePath(xorbHash))
	if err != nil {
		m.Release()
		return nil, fmt.Errorf("open xorb copy: %w", err)
	}
	return &copyFile{File: f, held: m}, nil
}

func (x copyXorbs) GetXorbChunkOffsets(ctx context.Context, xorbHash xet.XorbHash) ([]uint64, error) {
	return x.caches.Offsets.GetOrLoad(xorbHash, func() ([]uint64, error) {
		rc, err := x.GetXorbReadSeekCloser(ctx, xorbHash)
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		offsets, err := storage.ReadXorbChunkOffsets(rc)
		if err != nil {
			return nil, fmt.Errorf("read xorb chunk offsets: %w", err)
		}
		return offsets, nil
	})
}

// copyFile reads a cached copy, holding its mapping until the first Close.
type copyFile struct {
	*os.File
	held *xorbmap.Mapping
}

func (c *copyFile) Close() error {
	err := c.File.Close()
	if c.held != nil {
		c.held.Release()
		c.held = nil
	}
	return err
}
