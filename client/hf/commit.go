package hf

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// CommitFile is one path of a hub commit and the bytes to store at it.
type CommitFile struct {
	Path    string
	Content io.ReadSeeker
}

// Commit identifies a commit the hub created.
type Commit struct {
	OID string
	URL string
}

// ErrNoChanges reports a commit that would change nothing: the hub already holds or ignores every file.
var ErrNoChanges = errors.New("no files changed")

// sampleSize is how many leading bytes preupload sends for the hub's upload mode choice.
const sampleSize = 512

type preuploadFile struct {
	Path   string `json:"path"`
	Sample []byte `json:"sample"`
	Size   int64  `json:"size"`
}

type preuploadRequest struct {
	Files []preuploadFile `json:"files"`
}

type preuploadEntry struct {
	Path         string `json:"path"`
	ShouldIgnore bool   `json:"shouldIgnore"`
	UploadMode   string `json:"uploadMode"`
	OID          string `json:"oid"`
}

type preuploadResponse struct {
	CommitOID string           `json:"commitOid"`
	Files     []preuploadEntry `json:"files"`
}

type commitLine struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
}

type commitHeader struct {
	Description string `json:"description"`
	Summary     string `json:"summary"`
}

type commitLFSFile struct {
	Algo string `json:"algo"`
	OID  string `json:"oid"`
	Path string `json:"path"`
	Size int64  `json:"size"`
}

type commitRegularFile struct {
	Content  []byte `json:"content"`
	Encoding string `json:"encoding"`
	Path     string `json:"path"`
}

type commitResponse struct {
	CommitOID string `json:"commitOid"`
	CommitURL string `json:"commitUrl"`
}

// Commit stores files in one commit with summary at the bound repository revision the way huggingface_hub does: hub preupload, CAS upload of the large files, one NDJSON commit; ErrNoChanges reports a revision that already holds or ignores every file.
func (c *Client) Commit(ctx context.Context, summary string, files ...CommitFile) (*Commit, error) {
	if len(files) == 0 {
		return nil, errors.New("commit needs at least one file")
	}
	hub := &hubAPI{httpClient: c.hub, token: c.token, base: c.repo.apiBase(), rev: url.PathEscape(c.repo.Revision)}
	preupload := preuploadRequest{Files: make([]preuploadFile, 0, len(files))}
	for _, f := range files {
		if err := validatePath(f.Path); err != nil {
			return nil, err
		}
		size, sample, err := describe(f.Content)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", f.Path, err)
		}
		preupload.Files = append(preupload.Files, preuploadFile{Path: f.Path, Sample: sample, Size: size})
	}

	var modes preuploadResponse
	if err := hub.post(ctx, "preupload", "application/json", jsonBody(preupload), &modes); err != nil {
		return nil, err
	}
	entries := make(map[string]preuploadEntry, len(modes.Files))
	for _, e := range modes.Files {
		entries[e.Path] = e
	}

	lines := []commitLine{{Key: "header", Value: commitHeader{Summary: summary}}}
	for i, f := range files {
		entry, ok := entries[f.Path]
		if !ok {
			return nil, fmt.Errorf("preupload response omits %s", f.Path)
		}
		if entry.ShouldIgnore {
			continue
		}
		switch entry.UploadMode {
		case "lfs":
			digest, err := sha256Hex(f.Content)
			if err != nil {
				return nil, fmt.Errorf("digest %s: %w", f.Path, err)
			}
			if digest == entry.OID {
				continue
			}
			if err := rewind(f.Content); err != nil {
				return nil, fmt.Errorf("upload %s: %w", f.Path, err)
			}
			if _, err := c.Client.UploadFileV2(ctx, f.Content); err != nil {
				return nil, fmt.Errorf("upload %s: %w", f.Path, err)
			}
			lines = append(lines, commitLine{Key: "lfsFile", Value: commitLFSFile{Algo: "sha256", OID: digest, Path: f.Path, Size: preupload.Files[i].Size}})
		case "regular":
			if err := rewind(f.Content); err != nil {
				return nil, fmt.Errorf("read %s: %w", f.Path, err)
			}
			content, err := io.ReadAll(f.Content)
			if err != nil {
				return nil, fmt.Errorf("read %s: %w", f.Path, err)
			}
			if blobSHA1(content) == entry.OID {
				continue
			}
			lines = append(lines, commitLine{Key: "file", Value: commitRegularFile{Content: content, Encoding: "base64", Path: f.Path}})
		default:
			return nil, fmt.Errorf("unsupported upload mode %q for %s", entry.UploadMode, f.Path)
		}
	}
	if len(lines) == 1 {
		return nil, ErrNoChanges
	}

	var created commitResponse
	if err := hub.post(ctx, "commit", "application/x-ndjson", ndjsonBody(lines), &created); err != nil {
		return nil, err
	}
	return &Commit{OID: created.CommitOID, URL: created.CommitURL}, nil
}

// hubAPI reaches the endpoints of one repository revision with the hub token.
type hubAPI struct {
	httpClient *http.Client
	token      string
	base       string // origin and path up to the endpoint kind: {endpoint}/api/{type}s/{repo}
	rev        string // escaped as given
}

// url returns the revision's endpoint of the given kind, such as preupload or xet-write-token.
func (h *hubAPI) url(kind string) string {
	return h.base + "/" + kind + "/" + h.rev
}

// post sends body to the revision's endpoint of the given kind and decodes the JSON reply into out.
func (h *hubAPI) post(ctx context.Context, kind, contentType string, body []byte, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url(kind), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create %s request: %w", kind, err)
	}
	req.Header.Set("Content-Type", contentType)
	if h.token != "" {
		req.Header.Set("Authorization", "Bearer "+h.token)
	}
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("%s request: %w", kind, err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s: hub API error (status %s): %s", req.Method, req.URL.Path, resp.Status, hubMessage(resp))
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode %s response: %w", kind, err)
	}
	return nil
}

// hubMessage returns the hub's explanation of a failed response: its X-Error-Message, else the JSON error field, else the body text.
func hubMessage(resp *http.Response) string {
	if msg := resp.Header.Get("X-Error-Message"); msg != "" {
		return msg
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var payload struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &payload); err == nil && payload.Error != "" {
		return payload.Error
	}
	return strings.TrimSpace(string(body))
}

// validatePath accepts clean relative repository paths: no leading slash and no empty, "." or ".." segment.
func validatePath(p string) error {
	if p == "" || strings.HasPrefix(p, "/") {
		return fmt.Errorf("invalid path %q", p)
	}
	for seg := range strings.SplitSeq(p, "/") {
		switch seg {
		case "", ".", "..":
			return fmt.Errorf("invalid path %q", p)
		}
	}
	return nil
}

// describe returns content's size and its first sampleSize bytes.
func describe(content io.ReadSeeker) (int64, []byte, error) {
	size, err := content.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, nil, err
	}
	if err := rewind(content); err != nil {
		return 0, nil, err
	}
	sample := make([]byte, min(size, sampleSize))
	if _, err := io.ReadFull(content, sample); err != nil {
		return 0, nil, err
	}
	return size, sample, nil
}

// sha256Hex digests content from its start.
func sha256Hex(content io.ReadSeeker) (string, error) {
	if err := rewind(content); err != nil {
		return "", err
	}
	h := sha256.New()
	if _, err := io.Copy(h, content); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// blobSHA1 is the git object id of a blob holding content, the oid the hub reports for a regular file it already holds.
func blobSHA1(content []byte) string {
	h := sha1.New()
	_, _ = fmt.Fprintf(h, "blob %d\x00", len(content))
	_, _ = h.Write(content)
	return hex.EncodeToString(h.Sum(nil))
}

// rewind puts content at its start: every pass over a file begins there, whether files share a reader or one arrives mid-way.
func rewind(content io.ReadSeeker) error {
	_, err := content.Seek(0, io.SeekStart)
	return err
}

func jsonBody(v any) []byte {
	body, _ := json.Marshal(v)
	return body
}

// ndjsonBody encodes one line per value with a trailing newline, the commit payload format.
func ndjsonBody(lines []commitLine) []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	for _, line := range lines {
		_ = enc.Encode(line)
	}
	return buf.Bytes()
}
