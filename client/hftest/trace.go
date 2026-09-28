// Package hftest holds the recorded Hugging Face hub trace format: the redacted exchanges a tool made with a hub and the CAS it named, from which the regression tests model a private hub.
package hftest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
)

// Trace is one recorded scenario of Tool against Repo on Upstream, its exchanges in arrival order.
type Trace struct {
	Scenario  string     `json:"scenario"`
	Recorded  string     `json:"recorded"` // RFC3339 UTC
	Upstream  string     `json:"upstream"`
	Repo      string     `json:"repo"`
	Tool      string     `json:"tool"`
	Exchanges []Exchange `json:"exchanges"`
}

// Exchange is one request as the client sent it to the recorder and the response it got back, headers after the recorder's rewrites, everything redacted.
type Exchange struct {
	Seq             int         `json:"seq"`
	Origin          string      `json:"origin"` // "hub" or "cas"
	Method          string      `json:"method"`
	Path            string      `json:"path"` // path plus query as received
	RequestHeaders  http.Header `json:"request_headers"`
	RequestBody     *Body       `json:"request_body,omitempty"`
	Status          int         `json:"status"`
	ResponseHeaders http.Header `json:"response_headers"`
	ResponseBody    *Body       `json:"response_body,omitempty"`
}

// Body describes a recorded body by the size and sha256 of its raw bytes; Text holds the redacted text of JSON, NDJSON and text/* bodies up to 1 MiB.
type Body struct {
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256,omitempty"` // hex, set when Size > 0
	Text   string `json:"text,omitempty"`
}

// Load reads the trace saved at path.
func Load(path string) (*Trace, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var t Trace
	if err := json.Unmarshal(data, &t); err != nil {
		return nil, fmt.Errorf("decode trace %s: %w", path, err)
	}
	return &t, nil
}

// Save writes t to path as indented JSON, creating the parent directory.
func (t *Trace) Save(path string) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(t); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, buf.Bytes(), 0o644)
}
