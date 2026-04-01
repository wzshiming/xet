package lfs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// BatchUploadResult holds upload credentials from a successful batch negotiation.
type BatchUploadResult struct {
	NeedsUpload bool
	Upload      Action
	Verify      Action
}

// ResolveOIDUpload negotiates an upload and extracts Xet action headers.
func ResolveOIDUpload(ctx context.Context, hubToken string, target Target, obj BatchObject) ([]BatchUploadResult, error) {
	resp, err := doBatch(ctx, hubToken, target, batchRequest{
		Operation: "upload",
		Transfers: []string{"xet"},
		Objects:   []BatchObject{obj},
		HashAlgo:  "sha256",
	})
	if err != nil {
		return nil, err
	}
	if !strings.EqualFold(resp.Transfer, "xet") {
		return nil, fmt.Errorf("LFS batch selected transfer %q, expected %q", resp.Transfer, "xet")
	}

	if len(resp.Objects) == 0 {
		return nil, nil
	}

	output := make([]BatchUploadResult, len(resp.Objects))
	for i, objResp := range resp.Objects {

		uploadAction, hasUpload := objResp.Actions["upload"]
		if hasUpload {
			output[i].NeedsUpload = true
			output[i].Upload = Action{
				Href:   uploadAction.Href,
				Header: uploadAction.Header,
			}
		}

		verifyAction, hasVerify := objResp.Actions["verify"]
		if hasVerify {
			output[i].Verify = Action{
				Href:   verifyAction.Href,
				Header: verifyAction.Header,
			}
		}
	}

	return output, nil
}

// VerifyObject calls the LFS verify endpoint after a successful upload.
func VerifyObject(ctx context.Context, action Action, obj BatchObject) error {
	if action.Href == "" {
		return nil
	}

	body, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("encode LFS verify request: %w", err)
	}

	const maxAttempts = 5
	backoff := 200 * time.Millisecond
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, action.Href, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("create LFS verify request: %w", err)
		}
		for k, v := range action.Header {
			if k == "" || v == "" {
				continue
			}
			req.Header.Set(k, v)
		}
		req.Header.Set("Content-Type", "application/vnd.git-lfs+json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("LFS verify request: %w", err)
		} else {
			respBody, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			trimmedBody := strings.TrimSpace(string(respBody))

			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent {
				return nil
			}

			lastErr = fmt.Errorf("LFS verify request failed with status %d: %s", resp.StatusCode, trimmedBody)

			if resp.StatusCode != http.StatusNotFound || !isTransientVerifyNotFound(trimmedBody) || attempt == maxAttempts {
				return lastErr
			}
		}

		if attempt < maxAttempts {
			select {
			case <-ctx.Done():
				return fmt.Errorf("LFS verify request canceled: %w", ctx.Err())
			case <-time.After(backoff):
			}
			backoff *= 2
		}
	}

	return lastErr
}

func isTransientVerifyNotFound(respBody string) bool {
	body := strings.ToLower(respBody)
	return strings.Contains(body, "no file uploaded for oid")
}
