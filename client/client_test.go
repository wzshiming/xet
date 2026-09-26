package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/auth"
	"github.com/wzshiming/xet/shard"
)

type casCall struct {
	perm auth.Permission
	run  func() error
}

// casCalls runs every client-built CAS operation once against c, tagged with the permission it must resolve.
func casCalls(t *testing.T, ctx context.Context, c *Client) map[string]casCall {
	payload := func() io.ReadSeeker { return bytes.NewReader([]byte("payload")) }
	file := func(download func(context.Context, xet.FileHash, io.WriteSeeker) error) error {
		_, err := downloadInto(t, nil, func(w io.WriteSeeker) error { return download(ctx, xet.FileHash{}, w) })
		return err
	}
	closing := func(r io.Closer, err error) error {
		if err == nil {
			_ = r.Close()
		}
		return err
	}
	return map[string]casCall{
		"GetReconstructionV1":    {auth.Read, func() error { _, err := c.GetReconstructionV1(ctx, xet.FileHash{}, nil); return err }},
		"GetReconstructionV2":    {auth.Read, func() error { _, err := c.GetReconstructionV2(ctx, xet.FileHash{}, nil); return err }},
		"GetBatchReconstruction": {auth.Read, func() error { _, err := c.GetBatchReconstruction(ctx, []xet.FileHash{{}}); return err }},
		"DownloadFile":           {auth.Read, func() error { return file(c.DownloadFile) }},
		"DownloadFileV1":         {auth.Read, func() error { return file(c.DownloadFileV1) }},
		"DownloadFileV2":         {auth.Read, func() error { return file(c.DownloadFileV2) }},
		"DownloadFiles":          {auth.Read, func() error { _, _, err := c.DownloadFiles(ctx, []xet.FileHash{{}}); return err }},
		"DownloadXorb":           {auth.Read, func() error { return closing(c.DownloadXorb(ctx, "default", xet.XorbHash{})) }},
		"FetchXorbRange": {auth.Read, func() error {
			resp, err := c.FetchXorbRange(ctx, "default", xet.XorbHash{}, nil)
			if err == nil {
				_ = resp.Body.Close()
			}
			return err
		}},
		"HasXorb":          {auth.Write, func() error { _, err := c.HasXorb(ctx, xet.XorbHash{}); return err }},
		"UploadXorb":       {auth.Write, func() error { _, err := c.UploadXorb(ctx, xet.XorbHash{}, payload()); return err }},
		"UploadShard":      {auth.Write, func() error { _, err := c.UploadShard(ctx, shard.NewShard()); return err }},
		"UploadShardV2":    {auth.Write, func() error { _, err := c.UploadShardV2(ctx, shard.NewShard()); return err }},
		"QueryDedupShard":  {auth.Write, func() error { _, err := c.QueryDedupShard(ctx, xet.ChunkHash{}); return err }},
		"QueryDedupShards": {auth.Write, func() error { _, err := c.QueryDedupShards(ctx, []xet.ChunkHash{{}}); return err }},
		"UploadFile":       {auth.Write, func() error { _, err := c.UploadFile(ctx, payload()); return err }},
		"UploadFileV1":     {auth.Write, func() error { _, err := c.UploadFileV1(ctx, payload()); return err }},
		"UploadFileV2":     {auth.Write, func() error { _, err := c.UploadFileV2(ctx, payload()); return err }},
		"UploadFiles":      {auth.Write, func() error { _, err := c.UploadFiles(ctx, []io.ReadSeeker{payload()}); return err }},
		"UploadFilesV1":    {auth.Write, func() error { _, err := c.UploadFilesV1(ctx, []io.ReadSeeker{payload()}); return err }},
		"UploadFilesV2":    {auth.Write, func() error { _, err := c.UploadFilesV2(ctx, []io.ReadSeeker{payload()}); return err }},
	}
}

func TestUnboundClientRejectsRequests(t *testing.T) {
	var attempts atomic.Int32
	c, err := NewClient(WithCacheDir(t.TempDir()), WithHTTPClient(countingClient(&attempts)))
	if err != nil {
		t.Fatal(err)
	}
	for name, call := range casCalls(t, t.Context(), c) {
		if err := call.run(); !errors.Is(err, errNoUpstreamProvider) {
			t.Fatalf("%s: err = %v, want errNoUpstreamProvider", name, err)
		}
	}
	if n := attempts.Load(); n != 0 {
		t.Fatalf("attempts = %d, want 0", n)
	}
}

func TestOperationsResolvePermission(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		http.NotFound(w, r)
	}))
	defer srv.Close()
	p := newFakeProvider(srv.URL, "A")
	c, err := NewClient(WithCacheDir(t.TempDir()), WithUpstreamProvider(p))
	if err != nil {
		t.Fatal(err)
	}
	for name, call := range casCalls(t, t.Context(), c) {
		t.Run(name, func(t *testing.T) {
			before := len(p.perms())
			_ = call.run() // every path 404s; only the permission asked for matters
			got := p.perms()[before:]
			if len(got) == 0 {
				t.Fatal("Resolve not called")
			}
			for _, perm := range got {
				if perm != call.perm {
					t.Fatalf("resolved %q, want only %q", got, call.perm)
				}
			}
		})
	}
}

func TestPermissionsUseDistinctUpstreams(t *testing.T) {
	servers := map[auth.Permission]*recordingCAS{
		auth.Read:  {accept: "Bearer tokenR"},
		auth.Write: {accept: "Bearer tokenW"},
	}
	p := &fakeProvider{slots: map[auth.Permission]*fakeSlot{}}
	for perm, cas := range servers {
		srv := httptest.NewServer(cas)
		defer srv.Close()
		p.slots[perm] = &fakeSlot{baseURL: srv.URL, token: strings.TrimPrefix(cas.accept, "Bearer ")}
	}
	c, err := NewClient(WithCacheDir(t.TempDir()), WithUpstreamProvider(p))
	if err != nil {
		t.Fatal(err)
	}
	calls := casCalls(t, t.Context(), c)
	hits := func() map[auth.Permission]int {
		return map[auth.Permission]int{auth.Read: len(servers[auth.Read].snapshot()), auth.Write: len(servers[auth.Write].snapshot())}
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			before, resolves := hits(), len(p.perms())
			_ = call.run() // 200 "{}" answers keep every path short; only routing matters
			after := hits()
			own, other := after[call.perm]-before[call.perm], 0
			for perm := range servers {
				if perm != call.perm {
					other = after[perm] - before[perm]
				}
			}
			if own == 0 || other != 0 || len(p.perms())-resolves != own {
				t.Fatalf("%d requests on the %s endpoint, %d elsewhere, %d resolves; want every request on %s after its own Resolve", own, call.perm, other, len(p.perms())-resolves, call.perm)
			}
		})
	}
	t.Run("concurrent", func(t *testing.T) {
		var wg sync.WaitGroup
		for range 4 {
			for _, call := range calls {
				wg.Go(func() { _ = call.run() })
			}
		}
		wg.Wait()
		for perm, cas := range servers {
			for _, r := range cas.snapshot() {
				if r.auth != cas.accept {
					t.Fatalf("%s endpoint saw %q, want only %q", perm, r.auth, cas.accept)
				}
			}
		}
	})
}

func TestBaseURLValidation(t *testing.T) {
	var attempts atomic.Int32
	for _, baseURL := range []string{"", "cas.example.com", "ftp://cas.example.com", "http://", "http://%zz"} {
		c, err := NewClient(WithHTTPClient(countingClient(&attempts)), WithUpstreamProvider(StaticUpstreamProvider(baseURL, "")))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.HasXorb(t.Context(), xet.XorbHash{}); err == nil || !strings.Contains(err.Error(), fmt.Sprintf("%q", baseURL)) {
			t.Fatalf("base URL %q: err = %v, want an error naming it", baseURL, err)
		}
	}
	if n := attempts.Load(); n != 0 {
		t.Fatalf("attempts = %d, want 0", n)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if want := "/v1/xorbs/default/" + (xet.XorbHash{}).String(); r.URL.Path != want {
			t.Errorf("path = %q, want %q", r.URL.Path, want)
		}
	}))
	defer srv.Close()
	c, err := NewClient(WithHTTPClient(countingClient(&attempts)), WithUpstreamProvider(StaticUpstreamProvider(srv.URL+"/", "")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.HasXorb(t.Context(), xet.XorbHash{}); err != nil {
		t.Fatalf("trailing slash base URL: %v", err)
	}
}

// fakeSlot is one permission's endpoint.
type fakeSlot struct {
	baseURL string
	token   string
}

// fakeProvider serves one slot per permission and records every Resolve.
type fakeProvider struct {
	slots      map[auth.Permission]*fakeSlot
	resolveErr error

	mu       sync.Mutex
	resolved []auth.Permission
	marked   []bool // per Resolve, whether its ctx carried a casAuth mark
}

// newFakeProvider serves baseURL and token for both permissions.
func newFakeProvider(baseURL, token string) *fakeProvider {
	return &fakeProvider{slots: map[auth.Permission]*fakeSlot{
		auth.Read:  {baseURL: baseURL, token: token},
		auth.Write: {baseURL: baseURL, token: token},
	}}
}

func (p *fakeProvider) Resolve(ctx context.Context, perm auth.Permission) (string, string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resolved = append(p.resolved, perm)
	p.marked = append(p.marked, ctx.Value(casAuthKey{}) != nil)
	s := p.slots[perm]
	return s.baseURL, s.token, p.resolveErr
}

// perms returns the permissions passed to Resolve so far.
func (p *fakeProvider) perms() []auth.Permission {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]auth.Permission(nil), p.resolved...)
}

// marks reports, per Resolve so far, whether its ctx carried a casAuth mark.
func (p *fakeProvider) marks() []bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]bool(nil), p.marked...)
}

// A provider's Resolve never inherits the CAS mark of an enclosing operation, so a provider fetching its token through the client's transport cannot send a CAS token to its token endpoint; the batch dedup query falls back to per-chunk queries, which resolve again inside the marked operation.
func TestProviderResolveUnmarked(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		http.NotFound(w, r)
	}))
	defer srv.Close()
	p := newFakeProvider(srv.URL, "A")
	c, err := NewClient(WithCacheDir(t.TempDir()), WithUpstreamProvider(p))
	if err != nil {
		t.Fatal(err)
	}
	for name, call := range casCalls(t, t.Context(), c) {
		before := len(p.perms())
		_ = call.run() // every path 404s
		if name == "QueryDedupShards" && len(p.perms())-before != 2 {
			t.Fatalf("%s resolved %d times, want the batch query and its fallback", name, len(p.perms())-before)
		}
		if slices.Contains(p.marks()[before:], true) {
			t.Fatalf("%s: Resolve saw the CAS mark of an enclosing operation", name)
		}
	}
}

// authTransport authenticates requests to the marked origin only: the same host over another scheme or port, and any other host, stay anonymous.
func TestAuthTransportOrigin(t *testing.T) {
	var seen []string
	rt := &authTransport{base: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		seen = append(seen, r.URL.String()+" "+r.Header.Get("Authorization"))
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Request: r}, nil
	})}
	ctx := context.WithValue(t.Context(), casAuthKey{}, &casAuth{origin: "https://cas.example", token: "secret"})
	for _, target := range []string{"https://cas.example/v1/x", "http://cas.example/v1/x", "https://cas.example:8443/v1/x", "https://other.example/v1/x"} {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := rt.RoundTrip(req); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{"https://cas.example/v1/x Bearer secret", "http://cas.example/v1/x ", "https://cas.example:8443/v1/x ", "https://other.example/v1/x "}
	if !slices.Equal(seen, want) {
		t.Fatalf("authorization by request:\n got %q\nwant %q", seen, want)
	}
}

type recordedRequest struct {
	method, auth string
	body         []byte
}

// recordingCAS answers 200 (JSON "{}") to requests bearing accept, 401 otherwise.
type recordingCAS struct {
	mu       sync.Mutex
	requests []recordedRequest
	accept   string
}

func (s *recordingCAS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	s.mu.Lock()
	s.requests = append(s.requests, recordedRequest{r.Method, r.Header.Get("Authorization"), body})
	s.mu.Unlock()
	if r.Header.Get("Authorization") != s.accept {
		http.Error(w, "expired", http.StatusUnauthorized)
		return
	}
	_, _ = io.WriteString(w, "{}")
}

func (s *recordingCAS) snapshot() []recordedRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]recordedRequest(nil), s.requests...)
}

// A CAS 401 ends the operation after its single attempt: one Resolve, no replacement token, no replay, and no restart of a resumed download.
func TestAuth401IsTerminal(t *testing.T) {
	// resumed runs downloadFile into a destination already holding a prefix and fails t when the prefix is rewound or truncated.
	resumed := func(downloadFile func(*Client, context.Context, xet.FileHash, io.WriteSeeker) error) func(t *testing.T, ctx context.Context, c *Client) error {
		return func(t *testing.T, ctx context.Context, c *Client) error {
			prefix := []byte("partial download")
			var pos int64
			got, err := downloadInto(t, prefix, func(w io.WriteSeeker) error {
				err := downloadFile(c, ctx, xet.FileHash{}, w)
				pos, _ = w.Seek(0, io.SeekCurrent)
				return err
			})
			if pos != int64(len(prefix)) || !bytes.Equal(got, prefix) {
				t.Errorf("destination at offset %d with %d bytes, want untouched at %d", pos, len(got), len(prefix))
			}
			return err
		}
	}
	calls := map[string]func(t *testing.T, ctx context.Context, c *Client) error{
		"GET": func(_ *testing.T, ctx context.Context, c *Client) error {
			_, err := c.GetReconstructionV1(ctx, xet.FileHash{}, nil)
			return err
		},
		"HEAD": func(_ *testing.T, ctx context.Context, c *Client) error {
			_, err := c.HasXorb(ctx, xet.XorbHash{})
			return err
		},
		"GET xorb": func(_ *testing.T, ctx context.Context, c *Client) error {
			_, err := c.DownloadXorb(ctx, "default", xet.XorbHash{})
			return err
		},
		"POST xorb": func(_ *testing.T, ctx context.Context, c *Client) error {
			_, err := c.UploadXorb(ctx, xet.XorbHash{}, bytes.NewReader([]byte("payload")))
			return err
		},
		"resumed DownloadFileV1": resumed((*Client).DownloadFileV1),
		"resumed DownloadFileV2": resumed((*Client).DownloadFileV2),
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			cas := &recordingCAS{accept: "Bearer never"}
			srv := httptest.NewServer(cas)
			defer srv.Close()
			p := newFakeProvider(srv.URL, "A")
			c, err := NewClient(WithRetries(5), WithUpstreamProvider(p))
			if err != nil {
				t.Fatal(err)
			}
			err = call(t, t.Context(), c)
			if err == nil || !strings.Contains(err.Error(), "401") {
				t.Fatalf("err = %v, want 401", err)
			}
			if got := cas.snapshot(); len(got) != 1 || got[0].auth != "Bearer A" {
				t.Fatalf("requests = %+v, want the single rejected attempt", got)
			}
			if n := len(p.perms()); n != 1 {
				t.Fatalf("resolves = %d, want 1", n)
			}
		})
	}
}

// reqError keeps the upstream status text; only a 401 matches errUnauthorized.
func TestReqErrorStatus(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://cas.example/v1/xorbs/default/x", nil)
	for _, tc := range []struct {
		code             int
		status           string
		wantUnauthorized bool
	}{
		{http.StatusUnauthorized, "401 Token expired", true},
		{http.StatusForbidden, "403 Forbidden", false},
	} {
		err := reqError(req, &http.Response{StatusCode: tc.code, Status: tc.status, Body: io.NopCloser(strings.NewReader("body"))})
		if err == nil || !strings.Contains(err.Error(), tc.status) || errors.Is(err, errUnauthorized) != tc.wantUnauthorized {
			t.Fatalf("%s: err = %v, want it named with errUnauthorized match %v", tc.status, err, tc.wantUnauthorized)
		}
	}
}

func TestUpstreamProviderErrorsAreNotRetried(t *testing.T) {
	errBoom := errors.New("boom")
	cas := &recordingCAS{accept: "Bearer never"}
	srv := httptest.NewServer(cas)
	defer srv.Close()
	calls := map[string]func(ctx context.Context, c *Client) error{
		"HasXorb": func(ctx context.Context, c *Client) error { _, err := c.HasXorb(ctx, xet.XorbHash{}); return err },
		"DownloadXorb": func(ctx context.Context, c *Client) error {
			_, err := c.DownloadXorb(ctx, "default", xet.XorbHash{})
			return err
		},
		"FetchXorbRange": func(ctx context.Context, c *Client) error {
			_, err := c.FetchXorbRange(ctx, "default", xet.XorbHash{}, nil)
			return err
		},
		"UploadShardV2": func(ctx context.Context, c *Client) error {
			_, err := c.UploadShardV2(ctx, shard.NewShard())
			return err
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			p := newFakeProvider(srv.URL, "A")
			p.resolveErr = errBoom
			c, err := NewClient(WithRetries(5), WithUpstreamProvider(p))
			if err != nil {
				t.Fatal(err)
			}
			before := len(cas.snapshot())
			err = call(t.Context(), c)
			if !errors.Is(err, errBoom) {
				t.Fatalf("err = %v, want errBoom", err)
			}
			if got := len(cas.snapshot()) - before; got != 0 || len(p.perms()) != 1 {
				t.Fatalf("requests = %d (want 0), resolves = %d (want 1)", got, len(p.perms()))
			}
		})
	}
}

func TestAuthTokenScopedToCASOrigin(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]string{}
	record := func(r *http.Request) {
		mu.Lock()
		seen[r.Host+r.URL.Path] = r.Header.Get("Authorization")
		mu.Unlock()
	}
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		record(r)
		_, _ = io.WriteString(w, "{}")
	}))
	defer other.Close()
	cas := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		record(r)
		if strings.HasPrefix(r.URL.Path, "/v2/reconstructions/") {
			http.Redirect(w, r, other.URL+r.URL.Path, http.StatusFound)
			return
		}
		_, _ = io.WriteString(w, "{}")
	}))
	defer cas.Close()

	bound, err := NewClient(WithHTTPClient(cas.Client()), WithUpstreamProvider(StaticUpstreamProvider(cas.URL, "secret")))
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	hashA, hashB := xet.FileHash{0xa}, xet.FileHash{0xb}
	if _, err := bound.GetReconstructionV1(ctx, hashA, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := bound.GetReconstructionV2(ctx, hashA, nil); err != nil {
		t.Fatalf("redirected reconstruction: %v", err)
	}
	if _, err := bound.GetReconstructionV1(ctx, hashB, http.Header{"Authorization": {"Bearer mine"}}); err != nil {
		t.Fatal(err)
	}
	raw, err := bound.DownloadXorbWithURL(ctx, cas.URL+"/xorbs/raw", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = raw.Close()
	unauth, err := NewClient(WithHTTPClient(cas.Client()), WithUpstreamProvider(StaticUpstreamProvider(cas.URL, "")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := unauth.HasXorb(ctx, xet.XorbHash{}); err != nil {
		t.Fatal(err)
	}

	casHost, otherHost := strings.TrimPrefix(cas.URL, "https://"), strings.TrimPrefix(other.URL, "http://")
	want := map[string]string{
		casHost + "/v1/reconstructions/" + hashA.String():          "Bearer secret",
		casHost + "/v2/reconstructions/" + hashA.String():          "Bearer secret",
		otherHost + "/v2/reconstructions/" + hashA.String():        "",
		casHost + "/v1/reconstructions/" + hashB.String():          "Bearer mine",
		casHost + "/xorbs/raw":                                     "",
		casHost + "/v1/xorbs/default/" + (xet.XorbHash{}).String(): "",
	}
	mu.Lock()
	defer mu.Unlock()
	if !maps.Equal(seen, want) {
		t.Fatalf("authorization by request:\n got %v\nwant %v", seen, want)
	}
}

func TestResumedDownloadKeepsAuthFailureTerminal(t *testing.T) {
	errBoom := errors.New("boom")
	entryPoints := map[string]func(*Client, context.Context, xet.FileHash, io.WriteSeeker) error{
		"v1": (*Client).DownloadFileV1,
		"v2": (*Client).DownloadFileV2,
	}
	for name, downloadFile := range entryPoints {
		t.Run(name, func(t *testing.T) {
			p := newFakeProvider("http://cas.invalid", "A")
			p.resolveErr = errBoom
			c, err := NewClient(WithUpstreamProvider(p))
			if err != nil {
				t.Fatal(err)
			}
			prefix := []byte("partial download")
			var pos int64
			got, err := downloadInto(t, prefix, func(w io.WriteSeeker) error {
				err := downloadFile(c, t.Context(), xet.FileHash{}, w)
				pos, _ = w.Seek(0, io.SeekCurrent)
				return err
			})
			if !errors.Is(err, errBoom) {
				t.Fatalf("err = %v, want errBoom", err)
			}
			if n := len(p.perms()); n != 1 {
				t.Fatalf("resolves = %d, want 1: a provider failure must not restart the download", n)
			}
			if pos != int64(len(prefix)) || !bytes.Equal(got, prefix) {
				t.Fatalf("destination at offset %d with %d bytes, want untouched at %d", pos, len(got), len(prefix))
			}
		})
	}
}

func TestDownloadXorbResumeCarriesToken(t *testing.T) {
	body := bytes.Repeat([]byte("0123456789abcdef"), 256)
	var requests atomic.Int32
	var mu sync.Mutex
	var auths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		auths = append(auths, r.Header.Get("Authorization"))
		mu.Unlock()
		if requests.Add(1) == 1 {
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			_, _ = w.Write(body[:1000])
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			return
		}
		var start int
		if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-", &start); err != nil {
			t.Errorf("resume Range = %q", r.Header.Get("Range"))
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, len(body)-1, len(body)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body[start:])
	}))
	defer srv.Close()

	c, err := NewClient(WithIdleTimeout(100*time.Millisecond), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "secret")))
	if err != nil {
		t.Fatal(err)
	}
	r, err := c.DownloadXorb(t.Context(), "default", xet.XorbHash{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	got, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("got %d bytes, err %v; want %d", len(got), err, len(body))
	}
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(auths, []string{"Bearer secret", "Bearer secret"}) {
		t.Fatalf("authorization per request = %q, want the token on the resume too", auths)
	}
}

func TestStaticUpstreamProvider(t *testing.T) {
	ctx := t.Context()
	p := StaticUpstreamProvider("http://cas.example", "tok")
	for _, perm := range []auth.Permission{auth.Read, auth.Write} {
		if base, token, err := p.Resolve(ctx, perm); err != nil || base != "http://cas.example" || token != "tok" {
			t.Fatalf("Resolve(%s) = %q %q %v, want the static pair", perm, base, token, err)
		}
	}

	var mu sync.Mutex
	withAuth := map[string]bool{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		_, withAuth[r.URL.Path] = r.Header["Authorization"]
		mu.Unlock()
		_, _ = io.WriteString(w, "{}")
	}))
	defer srv.Close()
	anonymous, err := NewClient(WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := anonymous.GetReconstructionV1(ctx, xet.FileHash{}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := anonymous.HasXorb(ctx, xet.XorbHash{}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(withAuth) != 2 {
		t.Fatalf("requests = %v, want a read and a write", withAuth)
	}
	for path, has := range withAuth {
		if has {
			t.Fatalf("%s carried an Authorization header with an empty token", path)
		}
	}
}

func TestWithUpstreamProviderOption(t *testing.T) {
	p1, p2 := StaticUpstreamProvider("http://one", ""), StaticUpstreamProvider("http://two", "")
	c1, err := NewClient(WithUpstreamProvider(p1))
	if err != nil {
		t.Fatal(err)
	}
	c2, err := NewClient(WithUpstreamProvider(p2))
	if err != nil {
		t.Fatal(err)
	}
	if c1.provider != p1 || c2.provider != p2 {
		t.Fatal("option did not bind the provider")
	}
	// Applying the option to a struct copy rebinds only the copy and keeps the shared state.
	copied := *c1
	WithUpstreamProvider(p2)(&copied)
	if c1.provider != p1 || copied.provider != p2 {
		t.Fatal("bindings leaked between copies")
	}
	if copied.httpClient != c1.httpClient || copied.getHttpClient != c1.getHttpClient || copied.cacheManager != c1.cacheManager {
		t.Fatal("copy does not share the HTTP clients and cache")
	}
}
