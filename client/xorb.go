package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"

	"github.com/wzshiming/xet/progress"
	"github.com/wzshiming/xet/upload"
	"github.com/wzshiming/xet/xorb"
)

// UploadXorb serializes and uploads a xorb to the server
// This is a high-level method that handles serialization and upload of a Xorb object.
func (c *Client) UploadXorb(ctx context.Context, xorbObj *xorb.Xorb) (*upload.XorbUploadResponse, error) {
	xorbHash, err := xorbObj.Hash()
	if err != nil {
		return nil, fmt.Errorf("compute xorb hash: %w", err)
	}
	cacheKey := stableCacheKey(c.namespace, xorbHash.String())
	cacheFile, contentLength, hit, err := c.openPersistentCache("upload-xorb", cacheKey, ".bin")
	if err != nil {
		return nil, fmt.Errorf("open xorb upload cache: %w", err)
	}
	if !hit {
		// Serialize only on cache miss, then persist for future uploads with the same xorb hash.
		reader, encodeErr := xorbObj.Encode(true)
		if encodeErr != nil {
			return nil, fmt.Errorf("serialize xorb: %w", encodeErr)
		}
		cacheFile, contentLength, err = c.writePersistentCache("upload-xorb", cacheKey, ".bin", reader)
		if err != nil {
			return nil, fmt.Errorf("cache serialized xorb: %w", err)
		}
	}
	defer cacheFile.Close()

	url := fmt.Sprintf("%s/v1/xorbs/%s/%s", c.baseURL, c.namespace, xorbHash.String())

	var body io.Reader = cacheFile
	if c.progressFunc != nil {
		body = progress.NewProgressReader(body, url, contentLength, c.progressFunc)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.ContentLength = contentLength
	req.Header.Set("Content-Length", fmt.Sprintf("%d", contentLength))
	req.Header.Set("Content-Type", "application/octet-stream")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if err := reqError(req, resp); err != nil {
		return nil, err
	}

	var uploadResp upload.XorbUploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&uploadResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &uploadResp, nil
}

// DownloadXorb downloads and deserializes a xorb from a URL
// This is a high-level method that handles downloading and deserialization of a Xorb object from a given URL.
func (c *Client) DownloadXorb(ctx context.Context, url string, header http.Header) (*xorb.Xorb, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	for k, v := range header {
		req.Header[k] = v
	}

	withFooter := req.Header.Get("Range") == ""
	cacheKey := stableCacheKey(path.Base(req.URL.Path), strings.TrimPrefix(req.Header.Get("Range"), "bytes="))
	cacheFile, _, hit, err := c.openPersistentCache("download-xorb", cacheKey, ".bin")
	if err != nil {
		return nil, fmt.Errorf("open xorb download cache: %w", err)
	}
	if !hit {
		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("do request: %w", err)
		}
		defer resp.Body.Close()

		if err := reqError(req, resp); err != nil {
			return nil, err
		}

		var body io.Reader = resp.Body
		if c.progressFunc != nil {
			body = progress.NewProgressReader(body, url, resp.ContentLength, c.progressFunc)
		}

		cacheFile, _, err = c.writePersistentCache("download-xorb", cacheKey, ".bin", body)
		if err != nil {
			return nil, fmt.Errorf("cache xorb response: %w", err)
		}
	}

	// Read the file content into memory before closing it.
	// Chunk lazy-loading stores byte offsets into the reader; using a bytes.Reader
	// keeps the data alive for the lifetime of the Xorb without holding the file open.
	data, err := io.ReadAll(cacheFile)
	cacheFile.Close()
	if err != nil {
		return nil, fmt.Errorf("read cached xorb: %w", err)
	}

	xorbObj := xorb.NewXorb()
	err = xorbObj.Decode(bytes.NewReader(data), withFooter)
	if err != nil {
		return nil, fmt.Errorf("deserialize xorb: %w", err)
	}
	return xorbObj, nil
}
