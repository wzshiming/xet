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

	"github.com/wzshiming/xet/client"
)

// authInjector limits context-carried credentials to the caller's upstream origin.
type authInjector struct {
	inner http.RoundTripper
}

// upstreamAuth is the immutable credential of one upstream origin.
type upstreamAuth struct {
	origin string // scheme://host
	token  string
}

type upstreamAuthKey struct{}

func (t *authInjector) RoundTrip(req *http.Request) (*http.Response, error) {
	a, _ := req.Context().Value(upstreamAuthKey{}).(upstreamAuth)
	if a.token != "" && req.URL.Scheme+"://"+req.URL.Host == a.origin && req.Header.Get("Authorization") == "" {
		req = req.Clone(req.Context())
		req.Header.Set("Authorization", "Bearer "+a.token)
	}
	return t.inner.RoundTrip(req)
}

// withUpstreamAuth marks ctx so the mirror's clients send token to origin only.
func withUpstreamAuth(ctx context.Context, origin, token string) context.Context {
	return context.WithValue(ctx, upstreamAuthKey{}, upstreamAuth{origin: origin, token: token})
}

// probeResult captures upstream metadata for one resolve path. Everything is
// taken from response headers only, so no platform-specific adapters exist.
type probeResult struct {
	status    int
	size      int64 // -1 when unknown
	etag      string
	sha256    string // set when the upstream etag looks like a SHA-256
	commit    string // always a 40-hex: the upstream's own or a synthesized one
	synthetic bool   // commit is the pseudo commit of the requested branch
	xet       bool   // upstream advertised xet link headers on the resolve response
}

var hexSHA256Re = regexp.MustCompile(`^[0-9a-f]{64}$`)

// pseudoCommit is the stable stand-in commit for a branch of an upstream that sends no 40-hex X-Repo-Commit.
func pseudoCommit(repo, rev string) string {
	sum := sha256.Sum256([]byte("xet-mirror-pseudo-commit\x00" + repo + "\x00" + rev))
	return hex.EncodeToString(sum[:20])
}

// probe issues HEAD requests for the resolve key on origin, following
// redirects manually so that metadata headers from the hub hop are retained
// while later hops can still supply the content length; ctx carries the
// origin's credential. Upstreams whose HEAD responses carry no size at all
// (e.g. modelscope.cn) leave size at -1; the ingest download learns it from
// its first response headers and resolve replies wait for that (task.sized).
func (m *Mirror) probe(ctx context.Context, origin string, key resolveKey) (*probeResult, error) {
	res := &probeResult{size: -1}

	cur := origin + key.String()
	for range 8 {
		req, err := http.NewRequestWithContext(ctx, http.MethodHead, cur, nil)
		if err != nil {
			return nil, fmt.Errorf("create probe request: %w", err)
		}
		// Disable transparent gzip so sizes describe the raw bytes.
		req.Header.Set("Accept-Encoding", "identity")

		resp, err := m.probeClient.Do(req)
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
	// Hub clients refuse resolves without a 40-hex X-Repo-Commit; synthesize one when the upstream sends none.
	if commitRevRe.MatchString(key.rev) {
		if !commitRevRe.MatchString(res.commit) {
			res.commit = key.rev
		}
	} else if pseudo := pseudoCommit(key.repo, key.rev); !commitRevRe.MatchString(res.commit) || res.commit == pseudo {
		res.commit, res.synthetic = pseudo, true // an equal header is a chained mirror's own pseudo commit
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
		links := client.ParseLinkHeaders(header.Values("Link"))
		if links["xet-reconstruction-info"] != "" && links["xet-auth"] != "" {
			p.xet = true
		}
	}
}

func trimETag(etag string) string {
	etag = strings.TrimPrefix(strings.TrimSpace(etag), "W/")
	return strings.Trim(etag, `"`)
}
