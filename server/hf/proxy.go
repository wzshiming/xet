package hf

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/wzshiming/xet/mirror"
)

// UpstreamFunc selects the upstream hub and bearer token for an escaped repo; a nil URL means no upstream.
type UpstreamFunc func(ctx context.Context, repo string) (*url.URL, string, error)

// StaticUpstream returns a selector that sends every repo to one hub.
func StaticUpstream(rawURL, token string) (UpstreamFunc, error) {
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("hf: invalid upstream URL %q", rawURL)
	}
	return func(context.Context, string) (*url.URL, string, error) { return u, token, nil }, nil
}

// NewUpstreamProxy forwards to selected upstreams without forwarding downstream credentials.
func NewUpstreamProxy(upstreamFunc UpstreamFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if upstreamFunc == nil {
			serveFetchError(w, false, errors.New("hf: no upstream selector"))
			return
		}
		repo := proxyRepo(r.URL.EscapedPath())
		upstreamURL, upstreamToken, err := upstreamFunc(r.Context(), repo)
		if err != nil {
			serveFetchError(w, errors.Is(err, mirror.ErrUpstreamNotFound), err)
			return
		}
		if upstreamURL == nil {
			serveFetchError(w, false, fmt.Errorf("hf: no upstream URL selected for %q", repo))
			return
		}
		proxy := &httputil.ReverseProxy{
			Rewrite: func(pr *httputil.ProxyRequest) {
				pr.SetURL(upstreamURL)
				pr.Out.Host = upstreamURL.Host
				pr.Out.Header.Del("Authorization")
				if upstreamToken != "" {
					pr.Out.Header.Set("Authorization", "Bearer "+upstreamToken)
				}
			},
			// Keep entity metadata and redirect targets, not upstream control headers.
			ModifyResponse: func(resp *http.Response) error {
				kept := http.Header{}
				for _, k := range []string{"Content-Type", "Content-Length", "Content-Encoding", "Etag", "Date", "Location"} {
					if v := resp.Header.Get(k); v != "" {
						kept.Set(k, v)
					}
				}
				resp.Header = kept
				return nil
			},
		}
		proxy.ServeHTTP(w, r)
	})
}

// Legacy single-segment repos with subpaths are ambiguous: gpt2/refs is treated as a repo.
func proxyRepo(escapedPath string) string {
	if rest, ok := strings.CutPrefix(escapedPath, "/api/"); ok {
		segs := strings.SplitN(rest, "/", 4)
		switch segs[0] {
		case "models", "datasets", "spaces", "kernels":
		default:
			return ""
		}
		if len(segs) < 2 || segs[1] == "" {
			return ""
		}
		repo := segs[1]
		if len(segs) > 2 && segs[2] != "" {
			repo += "/" + segs[2]
		}
		return repoIdentity(segs[0], repo)
	}
	if repo, _, ok := strings.Cut(escapedPath, "/resolve/"); ok {
		return strings.TrimPrefix(repo, "/")
	}
	if repo, _, ok := strings.Cut(escapedPath, ".git/"); ok {
		return strings.TrimPrefix(repo, "/")
	}
	return ""
}
