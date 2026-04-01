package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/progress"
	"github.com/wzshiming/xet/upload"
	"github.com/wzshiming/xet/xorb"
)

// UploadXorb serializes and uploads a xorb to the server
// This is a high-level method that handles serialization and upload of a Xorb object.
func (c *Client) UploadXorb(ctx context.Context, xorbObj *xorb.Xorb) (*upload.XorbUploadResponse, error) {
	// Ensure the xorb hash is computed before it is used as a cache key or URL path.
	// The hash is normally set as a side-effect of Encode, so an all-zero hash here
	// means the xorb was freshly built and has not yet been serialized.
	if xorbObj.Hash == (xet.Hash{}) {
		chunkSizes := make([]uint64, len(xorbObj.Chunks))
		for i, chunk := range xorbObj.Chunks {
			chunkSizes[i] = uint64(len(chunk.UncompressedData))
		}
		xorbObj.Hash = xet.ComputeXorbHash(xorbObj.ChunkHashes, chunkSizes)
	}
	cacheKey := stableCacheKey(c.namespace, xorbObj.Hash.String())
	cacheFile, contentLength, hit, err := c.openPersistentCache("upload-xorb", cacheKey, ".bin")
	if err != nil {
		return nil, fmt.Errorf("open xorb upload cache: %w", err)
	}
	if !hit {
		// Serialize only on cache miss, then persist for future uploads with the same xorb hash.
		reader, encodeErr := upload.EncodeXorb(xorbObj)
		if encodeErr != nil {
			return nil, fmt.Errorf("serialize xorb: %w", encodeErr)
		}
		cacheFile, contentLength, err = c.writePersistentCache("upload-xorb", cacheKey, ".bin", reader)
		if err != nil {
			return nil, fmt.Errorf("cache serialized xorb: %w", err)
		}
	}
	defer cacheFile.Close()

	url := fmt.Sprintf("%s/v1/xorbs/%s/%s", c.baseURL, c.namespace, xorbObj.Hash.String())

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

	cacheKey := stableCacheKey(path.Base(req.URL.Path), strings.TrimPrefix(req.Header.Get("Range"), "bytes="))
	cacheFile, _, hit, err := c.openPersistentCache("download-xorb", cacheKey, ".bin")
	if err != nil {
		return nil, fmt.Errorf("open xorb download cache: %w", err)
	}
	if hit {
		defer cacheFile.Close()
		chunkOnly := req.Header.Get("Range") != ""
		xorbObj, decodeErr := xorb.Decode(cacheFile, chunkOnly)
		if decodeErr != nil {
			return nil, fmt.Errorf("deserialize xorb from cache: %w", decodeErr)
		}
		return xorbObj, nil
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if err := reqError(req, resp); err != nil {
		return nil, err
	}

	// Determine if this is a range request (chunks-only format)
	// Check if a Range header was set on the actual request
	chunkOnly := req.Header.Get("Range") != ""

	var body io.Reader = resp.Body
	if c.progressFunc != nil {
		body = progress.NewProgressReader(body, url, resp.ContentLength, c.progressFunc)
	}

	cacheFile, _, err = c.writePersistentCache("download-xorb", cacheKey, ".bin", body)
	if err != nil {
		return nil, fmt.Errorf("cache xorb response: %w", err)
	}
	defer cacheFile.Close()

	// Deserialize the xorb
	xorbObj, err := xorb.Decode(cacheFile, chunkOnly)
	if err != nil {
		return nil, fmt.Errorf("deserialize xorb: %w", err)
	}

	return xorbObj, nil
}
