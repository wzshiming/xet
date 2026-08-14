package mirror

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
)

// NewUpstreamProxy builds a reverse proxy that forwards control-plane
// requests to the upstream hub with the given credential injected, intended
// as the Handler's next (WithNext). Downstream Authorization headers are
// never forwarded upstream.
func NewUpstreamProxy(upstream, upstreamToken string) (http.Handler, error) {
	u, err := url.Parse(upstream)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("mirror: invalid upstream URL %q", upstream)
	}
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(u)
			pr.Out.Host = u.Host
			pr.Out.Header.Del("Authorization")
			if upstreamToken != "" {
				pr.Out.Header.Set("Authorization", "Bearer "+upstreamToken)
			}
		},
		// Upstream response headers are dropped except the entity headers
		// describing the relayed body and the redirect target without which
		// 3xx cannot work.
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
	}, nil
}
