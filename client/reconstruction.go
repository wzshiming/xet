package client

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"strings"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/download"
)

// GetReconstructionV1 retrieves reconstruction information for a file
func (c *Client) GetReconstructionV1(ctx context.Context, fileHash xet.Hash, header http.Header) (*download.ReconstructionResponseV1, error) {
	return c.GetReconstructionV1WithAuthProvider(ctx, nil, fileHash, header)
}

// GetReconstructionV1WithAuthProvider retrieves reconstruction information for
// a file with a per-call auth provider.
func (c *Client) GetReconstructionV1WithAuthProvider(ctx context.Context, provider AuthProvider, fileHash xet.Hash, header http.Header) (*download.ReconstructionResponseV1, error) {
	baseURL, err := c.getBaseURL(ctx, provider)
	if err != nil {
		return nil, fmt.Errorf("get base URL: %w", err)
	}
	url := fmt.Sprintf("%s/v1/reconstructions/%s", baseURL, fileHash.String())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	maps.Copy(req.Header, header)

	if token, err := c.getToken(ctx, provider); err != nil {
		return nil, fmt.Errorf("get token: %w", err)
	} else if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := c.doWithNetworkRetry(req)
	if err != nil {
		return nil, err
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
	return c.GetReconstructionV2WithAuthProvider(ctx, nil, fileHash, header)
}

// GetReconstructionV2WithAuthProvider retrieves V2 reconstruction information
// for a file with a per-call auth provider.
func (c *Client) GetReconstructionV2WithAuthProvider(ctx context.Context, provider AuthProvider, fileHash xet.Hash, header http.Header) (*download.ReconstructionResponseV2, error) {
	baseURL, err := c.getBaseURL(ctx, provider)
	if err != nil {
		return nil, fmt.Errorf("get base URL: %w", err)
	}
	url := fmt.Sprintf("%s/v2/reconstructions/%s", baseURL, fileHash.String())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	maps.Copy(req.Header, header)

	if token, err := c.getToken(ctx, provider); err != nil {
		return nil, fmt.Errorf("get token: %w", err)
	} else if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := c.doWithNetworkRetry(req)
	if err != nil {
		return nil, err
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

// GetBatchReconstruction retrieves reconstruction information for multiple files in a single request.
// It calls GET /reconstructions?file_id=<hex>&file_id=<hex>&... and returns the aggregated response.
func (c *Client) GetBatchReconstruction(ctx context.Context, fileHashes []xet.Hash) (*download.BatchReconstructionResponse, error) {
	return c.GetBatchReconstructionWithAuthProvider(ctx, nil, fileHashes)
}

// GetBatchReconstructionWithAuthProvider retrieves reconstruction information
// for multiple files in a single request with a per-call auth provider.
func (c *Client) GetBatchReconstructionWithAuthProvider(ctx context.Context, provider AuthProvider, fileHashes []xet.Hash) (*download.BatchReconstructionResponse, error) {
	if len(fileHashes) == 0 {
		return &download.BatchReconstructionResponse{
			Files:     make(map[string][]download.Term),
			FetchInfo: make(map[string][]download.FetchInfoEntry),
		}, nil
	}

	baseURL, err := c.getBaseURL(ctx, provider)
	if err != nil {
		return nil, fmt.Errorf("get base URL: %w", err)
	}

	var urlStr strings.Builder
	urlStr.WriteString(baseURL + "/reconstructions?")
	for i, h := range fileHashes {
		if i > 0 {
			urlStr.WriteString("&")
		}
		urlStr.WriteString("file_id=" + h.String())
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create batch reconstruction request: %w", err)
	}
	if token, err := c.getToken(ctx, provider); err != nil {
		return nil, fmt.Errorf("get token: %w", err)
	} else if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := c.doWithNetworkRetry(req)
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
