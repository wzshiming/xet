package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/download"
)

// GetReconstructionV1 retrieves reconstruction information for a file
func (c *Client) GetReconstructionV1(ctx context.Context, fileHash xet.Hash, header http.Header) (*download.ReconstructionResponseV1, error) {
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

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if err := reqError(req, resp); err != nil {
		return nil, err
	}

	var reconstructionResp download.ReconstructionResponseV1
	if err := json.NewDecoder(resp.Body).Decode(&reconstructionResp); err != nil {
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

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if err := reqError(req, resp); err != nil {
		return nil, err
	}

	var reconstructionResp download.ReconstructionResponseV2
	if err := json.NewDecoder(resp.Body).Decode(&reconstructionResp); err != nil {
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

	var batchResp download.BatchReconstructionResponse
	if err := json.NewDecoder(resp.Body).Decode(&batchResp); err != nil {
		return nil, fmt.Errorf("decode batch reconstruction response: %w", err)
	}

	return &batchResp, nil
}
