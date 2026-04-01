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

// Target identifies a Hub repository and revision for LFS operations.
type Target struct {
	Endpoint string
	RepoType string
	RepoID   string
	Revision string
}

// BatchObject describes a Git LFS object.
type BatchObject struct {
	OID  string `json:"oid"`
	Size int64  `json:"size"`
}

type batchRequest struct {
	Operation string           `json:"operation"`
	Transfers []string         `json:"transfers,omitempty"`
	Objects   []BatchObject    `json:"objects"`
	HashAlgo  string           `json:"hash_algo,omitempty"`
	Ref       *batchRequestRef `json:"ref,omitempty"`
}

type batchRequestRef struct {
	Name string `json:"name"`
}

type batchObjectAction struct {
	Href   string            `json:"href"`
	Header map[string]string `json:"header,omitempty"`
}

// Action is an LFS action entry containing href and associated headers.
type Action struct {
	Href   string
	Header map[string]string
}

type batchResponse struct {
	Transfer string `json:"transfer"`
	Objects  []struct {
		OID     string                       `json:"oid"`
		Actions map[string]batchObjectAction `json:"actions,omitempty"`
	} `json:"objects"`
	Errors []struct {
		OID     string `json:"oid"`
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"errors"`
}

func doBatch(ctx context.Context, hubToken string, target Target, payload batchRequest) (*batchResponse, error) {
	if hubToken == "" {
		return nil, fmt.Errorf("missing Hugging Face token")
	}
	if strings.TrimRight(target.Endpoint, "/") == "" {
		return nil, fmt.Errorf("missing Hugging Face endpoint")
	}
	if target.RepoType == "" || target.RepoID == "" {
		return nil, fmt.Errorf("missing Hugging Face repo target")
	}
	if len(payload.Objects) == 0 {
		return nil, fmt.Errorf("missing LFS objects")
	}
	for _, obj := range payload.Objects {
		if obj.OID == "" {
			return nil, fmt.Errorf("missing LFS oid")
		}
		if obj.Size < 0 {
			return nil, fmt.Errorf("invalid LFS size %d", obj.Size)
		}
	}
	if target.Revision != "" {
		payload.Ref = &batchRequestRef{Name: target.Revision}
	}

	batchURL := fmt.Sprintf("%s/%s.git/info/lfs/objects/batch",
		strings.TrimRight(target.Endpoint, "/"),
		repoTypeURLPrefix(target.RepoType)+target.RepoID,
	)

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode LFS batch request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, batchURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create LFS batch request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+hubToken)
	req.Header.Set("Accept", "application/vnd.git-lfs+json")
	req.Header.Set("Content-Type", "application/vnd.git-lfs+json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("LFS batch request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("LFS batch request failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	var batchResp batchResponse
	if err := json.NewDecoder(resp.Body).Decode(&batchResp); err != nil {
		return nil, fmt.Errorf("decode LFS batch response: %w", err)
	}

	if len(batchResp.Errors) > 0 {
		e := batchResp.Errors[0]
		return nil, fmt.Errorf("LFS batch object error for oid %s (code %d): %s", e.OID, e.Code, strings.TrimSpace(e.Message))
	}

	return &batchResp, nil
}

func repoTypeURLPrefix(repoType string) string {
	switch normalizeRepoType(repoType) {
	case "dataset":
		return "datasets/"
	case "space":
		return "spaces/"
	default:
		return ""
	}
}

func normalizeRepoType(repoType string) string {
	switch strings.ToLower(strings.TrimSpace(repoType)) {
	case "model", "models", "":
		if strings.TrimSpace(repoType) == "" {
			return ""
		}
		return "model"
	case "dataset", "datasets":
		return "dataset"
	case "space", "spaces":
		return "space"
	default:
		return ""
	}
}
