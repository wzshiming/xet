package mirror

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
)

// treeLFS is the lfs pointer block of a hub tree entry; OID is the sha256
// naming the file bytes. The entry itself stays a raw map because expand
// requests add fields (lastCommit, securityFileStatus, ...) that must pass
// through untouched.
type treeLFS struct {
	OID         string `json:"oid"`
	PointerSize int64  `json:"pointerSize"`
	Size        int64  `json:"size"`
}

// handleTree proxies a tree listing API request, rewriting each entry's
// xetHash: entries whose lfs sha256 oid resolves in local storage advertise
// the mirror's own hash so xet clients reconstruct them from the mirror CAS;
// all other entries lose the hash so clients fall back to the resolve flow,
// which ingests through the mirror. Left untouched, upstream hashes would
// send clients directly to the upstream CAS, bypassing the mirror entirely.
// Matching by content makes revision and repo irrelevant. The array is
// rewritten in a streaming pass, so a listing is never buffered whole; no
// upstream header is forwarded, only the body is relayed.
func (h *Handler) handleTree(w http.ResponseWriter, r *http.Request) {
	u := h.upstreamURL(r.URL.EscapedPath())
	if r.URL.RawQuery != "" {
		u += "?" + r.URL.RawQuery
	}
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, u, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp, err := h.fetchClient.Do(req)
	if err != nil {
		http.Error(w, "upstream tree fetch failed", http.StatusBadGateway)
		return
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	body := bufio.NewReader(resp.Body)
	if resp.StatusCode != http.StatusOK || !startsJSONArray(body) {
		// Error status or unexpected shape: relay the bytes untouched.
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, body)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	dec := json.NewDecoder(body)
	if _, err := dec.Token(); err != nil {
		return
	}
	_, _ = io.WriteString(w, "[")
	for first := true; dec.More(); first = false {
		// Raw values keep every untouched field byte-exact through the re-encode.
		var item map[string]json.RawMessage
		if err := dec.Decode(&item); err != nil {
			// Mid-stream failure: stop without the closing bracket so the
			// client sees invalid JSON rather than a truncated listing.
			return
		}
		if _, ok := item["xetHash"]; ok {
			var lfs treeLFS
			if raw, ok := item["lfs"]; ok {
				_ = json.Unmarshal(raw, &lfs)
			}
			if hash, ok := h.lookupXetHash(r.Context(), lfs.OID); ok {
				quoted, _ := json.Marshal(hash)
				item["xetHash"] = quoted
			} else {
				delete(item, "xetHash")
			}
		}
		out, err := json.Marshal(item)
		if err != nil {
			return
		}
		if !first {
			_, _ = io.WriteString(w, ",")
		}
		if _, err := w.Write(out); err != nil {
			return
		}
	}
	_, _ = io.WriteString(w, "]")
}

// startsJSONArray peeks past leading whitespace and reports whether the next
// byte opens a JSON array, consuming nothing.
func startsJSONArray(br *bufio.Reader) bool {
	for i := 1; ; i++ {
		buf, _ := br.Peek(i)
		if len(buf) < i {
			return false
		}
		switch buf[i-1] {
		case ' ', '\t', '\r', '\n':
		default:
			return buf[i-1] == '['
		}
	}
}

// lookupXetHash resolves an lfs sha256 oid to the local xet file hash through
// the storage sha256 index; ok is false when the content is not held locally.
func (h *Handler) lookupXetHash(ctx context.Context, oid string) (string, bool) {
	digest, err := hex.DecodeString(oid)
	if err != nil || len(digest) != sha256.Size {
		return "", false
	}
	fileHash, err := h.storage.GetFileHashBySHA256(ctx, "default", [32]byte(digest))
	if err != nil {
		return "", false
	}
	return fileHash.String(), true
}
