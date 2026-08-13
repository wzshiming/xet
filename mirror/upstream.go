package mirror

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/wzshiming/xet/hf"
)

// authInjector adds the mirror's upstream credential to requests that target
// the upstream hub host, and never anywhere else, so the token cannot leak to
// CDN or CAS hosts reached through redirects.
type authInjector struct {
	inner http.RoundTripper
	host  string
	token string
}

func (t *authInjector) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.token != "" && req.URL.Host == t.host && req.Header.Get("Authorization") == "" {
		req = req.Clone(req.Context())
		req.Header.Set("Authorization", "Bearer "+t.token)
	}
	return t.inner.RoundTrip(req)
}

// upstreamURL maps a local resolve path to the upstream equivalent.
func (h *Handler) upstreamURL(key string) string {
	return strings.TrimRight(h.upstream.String(), "/") + key
}

// probeResult captures upstream metadata for one resolve path. Everything is
// taken from response headers only, so no platform-specific adapters exist.
type probeResult struct {
	status int
	size   int64 // -1 when unknown
	etag   string
	sha256 string // set when the upstream etag looks like a SHA-256
	commit string
	// realCommit records that commit came from the upstream rather than
	// being synthesized; only real commits may pin branch revisions.
	realCommit bool
	xet        bool // upstream advertised xet link headers on the resolve response
}

var hexSHA256Re = regexp.MustCompile(`^[0-9a-f]{64}$`)

// probe issues HEAD requests for the resolve key, following redirects
// manually so that metadata headers from the hub hop are retained while later
// hops can still supply the content length. Upstreams whose HEAD responses
// carry no size at all (e.g. modelscope.cn) leave size at -1; the ingest
// download learns it from its first response headers and resolve replies wait
// for that (task.sized).
func (h *Handler) probe(ctx context.Context, key string) (*probeResult, error) {
	res := &probeResult{size: -1}

	cur := h.upstreamURL(key)
	for hop := 0; hop < 8; hop++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodHead, cur, nil)
		if err != nil {
			return nil, fmt.Errorf("create probe request: %w", err)
		}
		// Disable transparent gzip so sizes describe the raw bytes.
		req.Header.Set("Accept-Encoding", "identity")

		resp, err := h.probeClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("probe %s: %w", cur, err)
		}
		_ = resp.Body.Close()

		res.collect(resp.Header)

		if resp.StatusCode >= 300 && resp.StatusCode < 400 {
			loc := resp.Header.Get("Location")
			if loc == "" {
				res.status = resp.StatusCode
				return res, nil
			}
			u, err := req.URL.Parse(loc)
			if err != nil {
				return nil, fmt.Errorf("parse redirect location %q: %w", loc, err)
			}
			cur = u.String()
			continue
		}

		res.status = resp.StatusCode
		if res.size < 0 && resp.ContentLength >= 0 {
			res.size = resp.ContentLength
		}
		break
	}

	if hexSHA256Re.MatchString(res.etag) {
		res.sha256 = res.etag
	}
	res.realCommit = res.commit != ""
	if res.commit == "" {
		// Hub clients (huggingface_hub) refuse to download when the resolve
		// response has no X-Repo-Commit; synthesize one for upstreams (e.g.
		// modelscope.cn) that omit it: the revision itself when it already is
		// a commit hash, otherwise a stable hash of repo identity + revision.
		if m := resolveRe.FindStringSubmatch(key); m != nil {
			if commitRevRe.MatchString(m[2]) {
				res.commit = m[2]
			} else {
				sum := sha256.Sum256([]byte("xet-mirror-pseudo-commit\x00" + m[1] + "\x00" + m[2]))
				res.commit = hex.EncodeToString(sum[:20])
			}
		}
	}
	return res, nil
}

// collect fills empty fields from a response hop; earlier hops win.
func (p *probeResult) collect(header http.Header) {
	if p.etag == "" {
		e := header.Get("X-Linked-Etag")
		if e == "" {
			e = header.Get("ETag")
		}
		p.etag = trimETag(e)
	}
	if p.size < 0 {
		if v := header.Get("X-Linked-Size"); v != "" {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
				p.size = n
			}
		}
	}
	if p.commit == "" {
		p.commit = header.Get("X-Repo-Commit")
	}
	if !p.xet {
		links := hf.ParseLinkHeaders(header.Values("Link"))
		if links["xet-reconstruction-info"] != "" && links["xet-auth"] != "" {
			p.xet = true
		}
	}
}

func trimETag(etag string) string {
	etag = strings.TrimPrefix(strings.TrimSpace(etag), "W/")
	return strings.Trim(etag, `"`)
}
