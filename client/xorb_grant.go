package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/upload"
)

// maxXorbGrantAttempts bounds the grants one upload requests before it falls back to the legacy POST, as xet-core 1.8.0 does.
const maxXorbGrantAttempts = 3

// xorbUploadGrant is a 201 answer to POST /v1/xorb-grants: send the xorb to URL with Method and exactly Headers, then commit ID.
type xorbUploadGrant struct {
	ID      string            `json:"id"`
	Method  string            `json:"method"`
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
}

// uploadXorbThroughGrants uploads through at most maxXorbGrantAttempts grants, falling back to the legacy POST where xet-core 1.8.0 does.
func (c *Client) uploadXorbThroughGrants(ctx context.Context, baseURL string, xorbHash xet.XorbHash, length int64, checksum uint64, makeBody func() (io.ReadCloser, error)) (*upload.XorbUploadResponse, error) {
grants:
	for range maxXorbGrantAttempts {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		grant, err := c.requestXorbUploadGrant(ctx, baseURL, xorbHash, length, checksum)
		switch {
		case errors.Is(err, errNotFound):
			break grants
		case err != nil:
			return nil, err
		case grant == nil:
			return &upload.XorbUploadResponse{WasInserted: false}, nil
		}

		status, err := c.uploadXorbToGrant(ctx, grant, length, makeBody)
		switch {
		case err != nil:
			break grants
		case status == http.StatusBadRequest, status == http.StatusForbidden, status == http.StatusConflict:
			continue // a fresh grant may pass where this one was refused
		case status/100 != 2 && status != http.StatusPreconditionFailed:
			break grants
		}

		resp, err := c.commitXorbUpload(ctx, baseURL, xorbHash, grant.ID)
		if !errors.Is(err, errNotFound) {
			return resp, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.postXorb(ctx, baseURL, xorbHash, length, makeBody)
}

// requestXorbUploadGrant asks for a grant to upload length bytes with the CRC-64/NVME checksum; a nil grant means the xorb already exists, errNotFound that the server issues no grants.
func (c *Client) requestXorbUploadGrant(ctx context.Context, baseURL string, xorbHash xet.XorbHash, length int64, checksum uint64) (*xorbUploadGrant, error) {
	endpoint := fmt.Sprintf("%s/v1/xorb-grants/default/%s", baseURL, xorbHash.String())
	body := fmt.Sprintf(`{"length":%d,"checksum":{"algo":"crc64nvme","value":"%d"}}`, length, checksum)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.doWithNetworkRetry(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return nil, nil
	case http.StatusCreated:
		var grant xorbUploadGrant
		if err := json.NewDecoder(resp.Body).Decode(&grant); err != nil {
			return nil, fmt.Errorf("decode upload grant: %w", err)
		}
		u, err := url.Parse(grant.URL)
		absolute := err == nil && u.Scheme != "" && u.Host != ""
		// The id becomes a path segment of the commit URL, where "." and ".." would address another route.
		if grant.ID == "" || grant.ID == "." || grant.ID == ".." || grant.Method == "" || !absolute {
			return nil, fmt.Errorf("unusable upload grant: id %q, method %q, absolute url %t", grant.ID, grant.Method, absolute)
		}
		return &grant, nil
	}
	return nil, reqError(req, resp)
}

// uploadXorbToGrant sends the xorb where the grant directs and returns the response status; an error means no status came back.
func (c *Client) uploadXorbToGrant(ctx context.Context, grant *xorbUploadGrant, length int64, makeBody func() (io.ReadCloser, error)) (int, error) {
	body, err := makeBody()
	if err != nil {
		return 0, err
	}
	// The grant itself authorizes the upload: no CAS token, even when the URL is on the CAS origin.
	req, err := http.NewRequestWithContext(context.WithValue(ctx, casAuthKey{}, nil), grant.Method, grant.URL, body)
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}
	req.GetBody = makeBody
	req.ContentLength = length
	for name, value := range grant.Headers {
		req.Header.Set(name, value)
	}

	resp, err := c.doWithNetworkRetry(req)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}

// commitXorbUpload commits the xorb uploaded under grantID; errNotFound means the grant is gone and a new one is needed.
func (c *Client) commitXorbUpload(ctx context.Context, baseURL string, xorbHash xet.XorbHash, grantID string) (*upload.XorbUploadResponse, error) {
	endpoint := fmt.Sprintf("%s/v1/xorb-commits/default/%s/grant/%s", baseURL, xorbHash.String(), escapeGrantID(grantID))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.doWithNetworkRetry(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		return nil, reqError(req, resp)
	}
	committed := &upload.XorbUploadResponse{WasInserted: true}
	// An empty or non-JSON body still reports a commit that landed.
	data, _ := io.ReadAll(resp.Body)
	_ = json.Unmarshal(data, committed)
	return committed, nil
}

// escapeGrantID percent-encodes all but the RFC 3986 unreserved characters, as Rust's urlencoding::encode does.
func escapeGrantID(id string) string {
	return strings.ReplaceAll(url.QueryEscape(id), "+", "%20")
}
