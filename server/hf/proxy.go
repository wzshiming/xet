package hf

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/wzshiming/xet/auth"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/mirror"
)

// upstreamBase parses the provider's hub base URL, which needs an http(s) scheme and a host.
func upstreamBase(base string) (*url.URL, error) {
	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("hf: invalid upstream URL %q", base)
	}
	return u, nil
}

// NewUpstreamProxy forwards to the provider's upstream without forwarding downstream credentials.
func NewUpstreamProxy(upstream client.UpstreamProvider) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if upstream == nil {
			serveFetchError(w, false, errors.New("hf: no upstream provider"))
			return
		}
		perm := auth.Write
		if r.Method == http.MethodGet || r.Method == http.MethodHead {
			perm = auth.Read
		}
		base, upstreamToken, err := upstream.Resolve(r.Context(), perm)
		if err != nil {
			serveFetchError(w, errors.Is(err, mirror.ErrUpstreamNotFound), err)
			return
		}
		upstreamURL, err := upstreamBase(base)
		if err != nil {
			serveFetchError(w, false, err)
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
