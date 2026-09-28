package client

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

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

// Two clients built on one Cache serve each other's chunks, whatever WithCacheDir each was given.
func TestSharedCacheAvoidsRefetch(t *testing.T) {
	fx := newDownloadFixture(t, 1)
	cas, rec := recordingServer(t, fx.serveHTTP)
	fx.srv = cas // reconstructions name xorb URLs on fx.srv
	shared := NewCache(t.TempDir(), 0)
	ctx := t.Context()
	fetch := func() *Client {
		c, err := NewClient(WithCacheDir(t.TempDir()), WithCache(shared), WithUpstreamProvider(newFakeProvider(cas.URL, "t")))
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

func TestWithCachePrecedesCacheDir(t *testing.T) {
	fx := newDownloadFixture(t, 1)
	ctx := t.Context()
	for _, cacheFirst := range []bool{false, true} {
		own, dirB := NewCache(t.TempDir(), 0), filepath.Join(t.TempDir(), "unused")
		opts := []Options{WithCacheDir(dirB), WithCache(own), WithUpstreamProvider(StaticUpstreamProvider(fx.srv.URL, ""))}
		if cacheFirst {
			opts[0], opts[1] = opts[1], opts[0]
		}
		c, err := NewClient(opts...)
		if err != nil {
			t.Fatal(err)
		}
		got, err := downloadInto(t, nil, func(w io.WriteSeeker) error { return c.DownloadFile(ctx, fx.hashes[0], w) })
		if err != nil || !bytes.Equal(got, fx.data[0]) {
			t.Fatalf("cacheFirst=%v: download: %d bytes, %v; want %d", cacheFirst, len(got), err, len(fx.data[0]))
		}
		if u, err := own.Usage(ctx); err != nil || u.Download.Count == 0 {
			t.Fatalf("cacheFirst=%v: own cache usage = %+v, %v; want entries", cacheFirst, u, err)
		}
		if _, err := os.Stat(dirB); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("cacheFirst=%v: stat %s: %v; want it never created", cacheFirst, dirB, err)
		}
	}
}
