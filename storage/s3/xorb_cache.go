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
