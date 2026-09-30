package client

import (
	"context"
	"os"
	"path/filepath"

	"github.com/wzshiming/xet/download"
	"github.com/wzshiming/xet/upload"
)

// Cache is the cache root shared by every Client built with WithCache:
// <root>/download holds decoded chunks, <root>/upload holds upload staging
// and cached chunk locations.
type Cache struct {
	Download *download.CacheManager
	Upload   *upload.CacheManager
}

// Usage is the space the cache root holds per kind of data.
type Usage struct {
	Download download.CacheUsage
	Upload   upload.CacheUsage
}

// NewCache opens the cache root at dir ("" = <os temp dir>/xet-cache);
// downloadSize bounds the chunk cache and uploadSize the cached chunk
// locations, in bytes (<= 0 unbounded).
func NewCache(dir string, downloadSize, uploadSize int64) *Cache {
	if dir == "" {
		dir = filepath.Join(os.TempDir(), "xet-cache")
	}
	return &Cache{
		Download: download.NewCacheManager(filepath.Join(dir, "download"), downloadSize),
		Upload:   upload.NewCacheManager(filepath.Join(dir, "upload"), uploadSize),
	}
}

// Usage includes temporary and incomplete files in the cache directory; a nil
// manager counts as empty.
func (c *Cache) Usage(ctx context.Context) (Usage, error) {
	var usage Usage
	var err error
	if c.Download != nil {
		if usage.Download, err = c.Download.Usage(ctx); err != nil {
			return Usage{}, err
		}
	}
	if c.Upload != nil {
		if usage.Upload, err = c.Upload.Usage(ctx); err != nil {
			return Usage{}, err
		}
	}
	return usage, nil
}
