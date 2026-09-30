package client

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/wzshiming/xet/download"
	"github.com/wzshiming/xet/upload"
)

// A Cache built with only one manager reports the other as empty.
func TestCacheUsageSkipsNilManagers(t *testing.T) {
	for _, c := range []*Cache{{}, {Download: download.NewCacheManager(t.TempDir(), 0)}, {Upload: upload.NewCacheManager(t.TempDir(), 0)}} {
		if u, err := c.Usage(t.Context()); err != nil || u != (Usage{}) {
			t.Fatalf("Usage(%+v) = %+v, %v; want zero", c, u, err)
		}
	}
}

// authRecorder keeps the Authorization header of every request in arrival order per path.
type authRecorder struct {
	mu   sync.Mutex
	seen map[string][]string
}

func (a *authRecorder) record(r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.seen == nil {
		a.seen = map[string][]string{}
	}
	a.seen[r.URL.Path] = append(a.seen[r.URL.Path], r.Header.Get("Authorization"))
}

func (a *authRecorder) get(path string) []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.seen[path]...)
}

// recordingServer serves handler on a real listener, recording every request's Authorization.
func recordingServer(t *testing.T, handler http.HandlerFunc) (*httptest.Server, *authRecorder) {
	t.Helper()
	rec := &authRecorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.record(r)
		handler(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv, rec
}

// Two clients built on one Cache serve each other's chunks.
func TestSharedCacheAvoidsRefetch(t *testing.T) {
	fx := newDownloadFixture(t, 1)
	cas, rec := recordingServer(t, fx.serveHTTP)
	fx.srv = cas // reconstructions name xorb URLs on fx.srv
	shared := NewCache(t.TempDir(), 0, 0)
	ctx := t.Context()
	fetch := func() *Client {
		c, err := NewClient(WithCache(shared), WithUpstreamProvider(newFakeProvider(cas.URL, "t")))
		if err != nil {
			t.Fatal(err)
		}
		got, err := downloadInto(t, nil, func(w io.WriteSeeker) error { return c.DownloadFile(ctx, fx.hashes[0], w) })
		if err != nil || !bytes.Equal(got, fx.data[0]) {
			t.Fatalf("download: %d bytes, %v; want %d", len(got), err, len(fx.data[0]))
		}
		return c
	}
	c1 := fetch()
	n1 := len(rec.get("/xorbs/0"))
	c2 := fetch()
	if n2 := len(rec.get("/xorbs/0")); n1 == 0 || n2 != n1 {
		t.Fatalf("xorb GETs = %d after the first client, %d after the second; want the second served from the shared cache", n1, n2)
	}
	want, err := shared.Usage(ctx)
	if err != nil || want.Download.Count == 0 {
		t.Fatalf("shared usage = %+v, %v; want entries", want, err)
	}
	for _, c := range []*Client{c1, c2} {
		if got, err := c.Usage(ctx); err != nil || got != want {
			t.Fatalf("client usage = %+v, %v; want %+v", got, err, want)
		}
	}
}
