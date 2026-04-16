package lfs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// BatchUploadResult holds upload credentials from a successful batch negotiation.
type BatchUploadResult struct {
	Upload *Action
	Verify *Action
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
			output[i].Upload = &Action{
				Href:   uploadAction.Href,
				Header: uploadAction.Header,
			}
		}

		verifyAction, hasVerify := objResp.Actions["verify"]
		if hasVerify {
			output[i].Verify = &Action{
				Href:   verifyAction.Href,
				Header: verifyAction.Header,
			}
		}
	}

	return output, nil
}

// VerifyObject calls the LFS verify endpoint after a successful upload.
func VerifyObject(ctx context.Context, action *Action, obj BatchObject) error {
	body, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("encode LFS verify request: %w", err)
	}

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
		return fmt.Errorf("do LFS verify request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent {
		return nil
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("LFS verify failed with status %d and error reading response body: %w", resp.StatusCode, err)
	}

	return fmt.Errorf("LFS verify failed (status %d): %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
}
