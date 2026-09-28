package client

import (
	"context"

	"github.com/wzshiming/xet/download"
)

// Cache is the chunk cache directory shared by every Client built with WithCache.
type Cache struct {
	dir     string
	manager *download.CacheManager
}

// Usage is the space the cache directory holds per kind of data.
type Usage struct {
	Download download.CacheUsage
}

// NewCache opens the chunk cache at dir ("" = the default directory) bounded to size bytes (<= 0 unbounded).
func NewCache(dir string, size int64) *Cache {
	return &Cache{dir: dir, manager: download.NewCacheManager(dir, size)}
}

// Usage includes temporary and incomplete files in the cache directory.
func (c *Cache) Usage(ctx context.Context) (Usage, error) {
	downloadUsage, err := c.manager.Usage(ctx)
	if err != nil {
		return Usage{}, err
	}
	return Usage{Download: downloadUsage}, nil
}
