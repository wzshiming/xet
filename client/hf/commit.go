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
	preupload := preuploadRequest{Files: make([]preuploadFile, 0, len(files))}
	for _, f := range files {
		if err := validatePath(f.Path); err != nil {
			return nil, err
		}
		size, err := f.Content.Seek(0, io.SeekEnd)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", f.Path, err)
		}
		if err := rewind(f.Content); err != nil {
			return nil, fmt.Errorf("read %s: %w", f.Path, err)
		}
		sample := make([]byte, min(size, sampleSize))
		if _, err := io.ReadFull(f.Content, sample); err != nil {
			return nil, fmt.Errorf("read %s: %w", f.Path, err)
		}
		preupload.Files = append(preupload.Files, preuploadFile{Path: f.Path, Sample: sample, Size: size})
	}

	body, _ := json.Marshal(preupload)
	var modes preuploadResponse
	if err := c.post(ctx, "preupload", "application/json", body, &modes); err != nil {
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
			if err := rewind(f.Content); err != nil {
				return nil, fmt.Errorf("digest %s: %w", f.Path, err)
			}
			h := sha256.New()
			if _, err := io.Copy(h, f.Content); err != nil {
				return nil, fmt.Errorf("digest %s: %w", f.Path, err)
			}
			digest := hex.EncodeToString(h.Sum(nil))
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
			// The hub reports a regular file it already holds by its git blob id.
			h := sha1.New()
			_, _ = fmt.Fprintf(h, "blob %d\x00", len(content))
			_, _ = h.Write(content)
			if hex.EncodeToString(h.Sum(nil)) == entry.OID {
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

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	for _, line := range lines {
		_ = enc.Encode(line)
	}
	var created commitResponse
	if err := c.post(ctx, "commit", "application/x-ndjson", buf.Bytes(), &created); err != nil {
		return nil, err
	}
	return &Commit{OID: created.CommitOID, URL: created.CommitURL}, nil
}

// post sends body to the bound revision's endpoint of the given kind and decodes the JSON reply into out.
func (c *Client) post(ctx context.Context, kind, contentType string, body []byte, out any) error {
	req, err := newRequest(ctx, http.MethodPost, c.repo.apiURL(kind), c.token, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create %s request: %w", kind, err)
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := c.hub.Do(req)
	if err != nil {
		return fmt.Errorf("%s request: %w", kind, err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return hubError(req, resp)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode %s response: %w", kind, err)
	}
	return nil
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

// rewind puts content at its start: every pass over a file begins there, whether files share a reader or one arrives mid-way.
func rewind(content io.ReadSeeker) error {
	_, err := content.Seek(0, io.SeekStart)
	return err
}
