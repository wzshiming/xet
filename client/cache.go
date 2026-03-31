package client

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type removeOnCloseFile struct {
	*os.File
	path string
}

func (f *removeOnCloseFile) Close() error {
	if f == nil || f.File == nil {
		return nil
	}
	err := f.File.Close()
	_ = os.Remove(f.path)
	return err
}

func closeAndIgnoreError(closer io.Closer) {
	if closer == nil {
		return
	}
	_ = closer.Close()
}

func (c *Client) cacheDir() string {
	if c.cacheDirPath != "" {
		return c.cacheDirPath
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(os.TempDir(), "xet-cache")
	}
	return filepath.Join(homeDir, ".cache/xet")
}

func (c *Client) createCacheFile(pattern string) (*removeOnCloseFile, error) {
	dir := c.cacheDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}

	f, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return nil, fmt.Errorf("create temp cache file: %w", err)
	}

	return &removeOnCloseFile{File: f, path: f.Name()}, nil
}

func (c *Client) spoolReaderToCache(reader io.Reader, pattern string) (*removeOnCloseFile, int64, error) {
	f, err := c.createCacheFile(pattern)
	if err != nil {
		return nil, 0, err
	}

	n, err := io.Copy(f, reader)
	if err != nil {
		_ = f.Close()
		return nil, 0, fmt.Errorf("cache stream to file: %w", err)
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, 0, fmt.Errorf("rewind cache file: %w", err)
	}

	return f, n, nil
}

func stableCacheKey(parts ...string) string {
	for i, part := range parts {
		part = strings.TrimSpace(part)
		part = strings.ReplaceAll(part, string(os.PathSeparator), "_")
		part = strings.ReplaceAll(part, ":", "_")
		parts[i] = part
	}
	return path.Join(parts...)
}

func (c *Client) cacheBucketDir(bucket string) (string, error) {
	bucket = strings.TrimSpace(bucket)
	if bucket == "" {
		return "", fmt.Errorf("cache bucket is empty")
	}
	dir := filepath.Join(c.cacheDir(), "client", bucket)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("create cache bucket dir: %w", err)
	}
	return dir, nil
}

func (c *Client) cacheFilePath(bucket, key, ext string) (string, error) {
	if strings.TrimSpace(key) == "" {
		return "", fmt.Errorf("cache key is empty")
	}
	dir, err := c.cacheBucketDir(bucket)
	if err != nil {
		return "", err
	}
	if ext == "" {
		ext = ".bin"
	}
	return filepath.Join(dir, key+ext), nil
}

func (c *Client) openPersistentCache(bucket, key, ext string) (*os.File, int64, bool, error) {
	path, err := c.cacheFilePath(bucket, key, ext)
	if err != nil {
		return nil, 0, false, err
	}

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, false, nil
		}
		return nil, 0, false, fmt.Errorf("open persistent cache: %w", err)
	}

	info, err := f.Stat()
	if err != nil {
		closeAndIgnoreError(f)
		return nil, 0, false, fmt.Errorf("stat persistent cache: %w", err)
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		closeAndIgnoreError(f)
		return nil, 0, false, fmt.Errorf("rewind persistent cache: %w", err)
	}

	return f, info.Size(), true, nil
}

func (c *Client) writePersistentCache(bucket, key, ext string, reader io.Reader) (*os.File, int64, error) {
	path, err := c.cacheFilePath(bucket, key, ext)
	if err != nil {
		return nil, 0, err
	}

	if f, size, hit, err := c.openPersistentCache(bucket, key, ext); err != nil {
		return nil, 0, err
	} else if hit {
		return f, size, nil
	}

	dir := filepath.Dir(path)

	err = os.MkdirAll(dir, 0o755)
	if err != nil {
		return nil, 0, fmt.Errorf("create cache dir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".xet-client-cache-*")
	if err != nil {
		return nil, 0, fmt.Errorf("create cache temp file: %w", err)
	}
	tmpPath := tmp.Name()

	size, copyErr := io.Copy(tmp, reader)
	if copyErr != nil {
		closeAndIgnoreError(tmp)
		_ = os.Remove(tmpPath)
		return nil, 0, fmt.Errorf("write persistent cache: %w", copyErr)
	}

	if closeErr := tmp.Close(); closeErr != nil {
		_ = os.Remove(tmpPath)
		return nil, 0, fmt.Errorf("close cache temp file: %w", closeErr)
	}

	if renameErr := os.Rename(tmpPath, path); renameErr != nil {
		_ = os.Remove(tmpPath)
		if f, hitSize, hit, err := c.openPersistentCache(bucket, key, ext); err == nil && hit {
			return f, hitSize, nil
		}
		return nil, 0, fmt.Errorf("commit persistent cache: %w", renameErr)
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, 0, fmt.Errorf("open committed cache: %w", err)
	}
	return f, size, nil
}
