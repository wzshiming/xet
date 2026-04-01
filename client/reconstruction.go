package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/download"
	"github.com/wzshiming/xet/progress"
)

// GetReconstructionV1 retrieves reconstruction information for a file
func (c *Client) GetReconstructionV1(ctx context.Context, fileHash xet.Hash, header http.Header) (*download.ReconstructionResponse, error) {
	url := fmt.Sprintf("%s/v1/reconstructions/%s", c.baseURL, fileHash.String())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	for k, v := range header {
		req.Header[k] = v
	}

	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	cacheKey := stableCacheKey(fileHash.String(), strings.TrimPrefix(req.Header.Get("Range"), "bytes="))
	cacheFile, _, hit, err := c.openPersistentCache("reconstruction-v1", cacheKey, ".json")
	if err != nil {
		return nil, fmt.Errorf("open reconstruction cache: %w", err)
	}
	if hit {
		defer cacheFile.Close()
		var reconstructionResp download.ReconstructionResponse
		if decodeErr := json.NewDecoder(cacheFile).Decode(&reconstructionResp); decodeErr == nil {
			return &reconstructionResp, nil
		}
	}

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

	cacheFile, _, err = c.writePersistentCache("reconstruction-v1", cacheKey, ".json", body)
	if err != nil {
		return nil, fmt.Errorf("cache reconstruction response: %w", err)
	}
	defer cacheFile.Close()

	var reconstructionResp download.ReconstructionResponse
	if err := json.NewDecoder(cacheFile).Decode(&reconstructionResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &reconstructionResp, nil
}

// GetReconstructionV2 retrieves V2 reconstruction information for a file
func (c *Client) GetReconstructionV2(ctx context.Context, fileHash xet.Hash, header http.Header) (*download.ReconstructionResponseV2, error) {
	url := fmt.Sprintf("%s/v2/reconstructions/%s", c.baseURL, fileHash.String())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	for k, v := range header {
		req.Header[k] = v
	}

	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	cacheKey := stableCacheKey(fileHash.String(), strings.TrimPrefix(req.Header.Get("Range"), "bytes="))
	cacheFile, _, hit, err := c.openPersistentCache("reconstruction-v2", cacheKey, ".json")
	if err != nil {
		return nil, fmt.Errorf("open reconstruction cache: %w", err)
	}
	if hit {
		defer cacheFile.Close()
		var reconstructionResp download.ReconstructionResponseV2
		if decodeErr := json.NewDecoder(cacheFile).Decode(&reconstructionResp); decodeErr == nil {
			return &reconstructionResp, nil
		}
	}

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

	cacheFile, _, err = c.writePersistentCache("reconstruction-v2", cacheKey, ".json", body)
	if err != nil {
		return nil, fmt.Errorf("cache reconstruction response: %w", err)
	}
	defer cacheFile.Close()

	var reconstructionResp download.ReconstructionResponseV2
	if err := json.NewDecoder(cacheFile).Decode(&reconstructionResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &reconstructionResp, nil
}

// BatchGetReconstruction retrieves reconstruction information for multiple files in a single request.
// It calls GET /reconstructions?file_id=<hex>&file_id=<hex>&... and returns the aggregated response.
func (c *Client) BatchGetReconstruction(ctx context.Context, fileHashes []xet.Hash) (*download.BatchReconstructionResponse, error) {
	if len(fileHashes) == 0 {
		return &download.BatchReconstructionResponse{
			Files:     make(map[string][]download.Term),
			FetchInfo: make(map[string][]download.FetchInfoEntry),
		}, nil
	}

	urlStr := c.baseURL + "/reconstructions?"
	for i, h := range fileHashes {
		if i > 0 {
			urlStr += "&"
		}
		urlStr += "file_id=" + h.String()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return nil, fmt.Errorf("create batch reconstruction request: %w", err)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do batch reconstruction request: %w", err)
	}
	defer resp.Body.Close()

	if err := reqError(req, resp); err != nil {
		return nil, err
	}

	var body io.Reader = resp.Body
	if c.progressFunc != nil {
		body = progress.NewProgressReader(body, urlStr, resp.ContentLength, c.progressFunc)
	}

	var batchResp download.BatchReconstructionResponse
	if err := json.NewDecoder(body).Decode(&batchResp); err != nil {
		return nil, fmt.Errorf("decode batch reconstruction response: %w", err)
	}

	return &batchResp, nil
}
