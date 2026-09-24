package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/download"
)

// GetReconstructionV1 retrieves reconstruction information for a file
func (c *Client) GetReconstructionV1(ctx context.Context, fileHash xet.FileHash, header http.Header) (*download.ReconstructionResponseV1, error) {
	return c.GetReconstructionV1WithAuthProvider(ctx, nil, fileHash, header)
}

// GetReconstructionV1WithAuthProvider retrieves reconstruction information for
// a file with a per-call auth provider.
func (c *Client) GetReconstructionV1WithAuthProvider(ctx context.Context, provider AuthProvider, fileHash xet.FileHash, header http.Header) (*download.ReconstructionResponseV1, error) {
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

	return getJSON[download.ReconstructionResponseV1](c, req, reconstructionError)
}

// Request and body failures share one retry budget.
func getJSON[T any](c *Client, req *http.Request, statusErr func(*http.Request, *http.Response) error) (*T, error) {
	attempts := c.retryAttempts()

	var lastErr error
	for i := range attempts {
		if i > 0 {
			if err := c.waitRetry(req.Context(), i-1, lastErr); err != nil {
				lastErr = err
				break
			}
		}

		resp, err := c.do(req)
		if err != nil {
			if !isNetworkError(err) {
				return nil, fmt.Errorf("do request: %w", err)
			}
			lastErr = err
			continue
		}
		if isRetryableStatus(resp.StatusCode) {
			_ = resp.Body.Close()
			lastErr = fmt.Errorf("server error status %s", resp.Status)
			continue
		}
		if err := statusErr(req, resp); err != nil {
			_ = resp.Body.Close()
			return nil, err
		}

		v := new(T)
		err = json.NewDecoder(resp.Body).Decode(v)
		_ = resp.Body.Close()
		if err == nil {
			return v, nil
		}
		lastErr = fmt.Errorf("decode response: %w", err)
		var syntaxErr *json.SyntaxError
		var typeErr *json.UnmarshalTypeError
		if errors.As(err, &syntaxErr) || errors.As(err, &typeErr) {
			return nil, lastErr
		}
	}

	return nil, fmt.Errorf("network error after %d attempts: %w", attempts, lastErr)
}

// Reconstruction ranges are encoded in JSON even when the status is 200.
func reconstructionError(req *http.Request, resp *http.Response) error {
	if resp.StatusCode == http.StatusOK {
		return nil
	}
	return reqError(req, resp)
}

// GetReconstructionV2 retrieves V2 reconstruction information for a file
func (c *Client) GetReconstructionV2(ctx context.Context, fileHash xet.FileHash, header http.Header) (*download.ReconstructionResponseV2, error) {
	return c.GetReconstructionV2WithAuthProvider(ctx, nil, fileHash, header)
}

// GetReconstructionV2WithAuthProvider retrieves V2 reconstruction information
// for a file with a per-call auth provider.
func (c *Client) GetReconstructionV2WithAuthProvider(ctx context.Context, provider AuthProvider, fileHash xet.FileHash, header http.Header) (*download.ReconstructionResponseV2, error) {
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

	return getJSON[download.ReconstructionResponseV2](c, req, reconstructionError)
}

// GetBatchReconstruction retrieves reconstruction information for multiple files in a single request.
// It calls GET /reconstructions?file_id=<hex>&file_id=<hex>&... and returns the aggregated response.
func (c *Client) GetBatchReconstruction(ctx context.Context, fileHashes []xet.FileHash) (*download.BatchReconstructionResponse, error) {
	return c.GetBatchReconstructionWithAuthProvider(ctx, nil, fileHashes)
}

// GetBatchReconstructionWithAuthProvider retrieves reconstruction information
// for multiple files in a single request with a per-call auth provider.
func (c *Client) GetBatchReconstructionWithAuthProvider(ctx context.Context, provider AuthProvider, fileHashes []xet.FileHash) (*download.BatchReconstructionResponse, error) {
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

	return getJSON[download.BatchReconstructionResponse](c, req, reqError)
}
