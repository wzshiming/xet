package mirror

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/hf"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/token"
	"github.com/wzshiming/xet/upload"
)

func TestSpoolTailRead(t *testing.T) {
	sp, err := openSpool(t.TempDir(), "k", "", -1)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 64*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}

	rc := sp.newReader(context.Background(), 0)
	got := make(chan []byte, 1)
	go func() {
		b, _ := io.ReadAll(rc)
		_ = rc.Close()
		got <- b
	}()

	// Write in pieces so the reader has to wait repeatedly.
	for i := 0; i < len(data); i += 8192 {
		if _, err := sp.Write(data[i : i+8192]); err != nil {
			t.Fatal(err)
		}
	}
	sp.finish(nil)

	if b := <-got; !bytes.Equal(b, data) {
		t.Fatalf("tail read mismatch: got %d bytes, want %d", len(b), len(data))
	}

	t.Run("canceled context unblocks reader", func(t *testing.T) {
		sp, err := openSpool(t.TempDir(), "k", "", -1)
		if err != nil {
			t.Fatal(err)
		}
		defer sp.finish(nil)
		ctx, cancel := context.WithCancel(context.Background())
		rc := sp.newReader(ctx, 0)
		defer rc.Close()
		errCh := make(chan error, 1)
		go func() {
			_, err := rc.Read(make([]byte, 1))
			errCh <- err
		}()
		cancel()
		if err := <-errCh; err != context.Canceled {
			t.Fatalf("read err = %v, want context.Canceled", err)
		}
	})

	t.Run("no readers after removal", func(t *testing.T) {
		sp, err := openSpool(t.TempDir(), "k", "", -1)
		if err != nil {
			t.Fatal(err)
		}
		sp.finish(nil) // no refs: the file is removed immediately
		if rc := sp.newReader(context.Background(), 0); rc != nil {
			t.Fatal("expected nil reader after removal")
		}
		if rs := sp.newSeekReader(context.Background(), 0); rs != nil {
			t.Fatal("expected nil seek reader after removal")
		}
	})
}

// mirrorFixture wires a mirror handler on an httptest server whose URL is also
// the storage base URL and external URL.
type mirrorFixture struct {
	srv     *httptest.Server
	handler *Handler
	stor    storage.Storage
	issuer  *token.Issuer
}

func newMirrorFixture(t *testing.T, upstream string, storageDir, cacheDir string, opts ...Option) *mirrorFixture {
	t.Helper()
	var inner atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inner.Load().(http.Handler).ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)

	stor, err := storage.NewFileStorage(
		storage.WithBasePath(storageDir),
		storage.WithBaseURL(srv.URL),
	)
	if err != nil {
		t.Fatal(err)
	}

	issuer, err := token.NewIssuer(nil, 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	h, err := NewHandler(append([]Option{
		WithStorage(stor),
		WithUpstream(upstream),
		WithExternalURL(srv.URL),
		WithCacheDir(cacheDir),
		WithMintToken(issuer.Mint),
	}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	// Same composition as cmd/xetd: the CAS server matches its routes first
	// and falls through to the mirror; the shared issuer ties minting to
	// validation.
	inner.Store(http.Handler(server.NewHandler(
		server.WithStorage(stor),
		server.WithAuthFunc(func(tok string) bool { return issuer.Validate(tok, time.Now()) }),
		server.WithNext(h),
	)))
	return &mirrorFixture{srv: srv, handler: h, stor: stor, issuer: issuer}
}

// noRedirect returns a client that surfaces 3xx responses instead of following.
func noRedirect() *http.Client {
	return &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// waitReady polls a resolve URL until the mirror answers with the ready-state
// redirect, returning that response.
func waitReady(t *testing.T, resolveURL string) *http.Response {
	t.Helper()
	c := noRedirect()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := c.Get(resolveURL)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusFound {
			return resp
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("mirror never reached ready state")
	return nil
}

func downloadViaXet(t *testing.T, resolveURL string) []byte {
	t.Helper()
	ctx := context.Background()
	fileHash, provider, err := hf.ResolveDownload(ctx, nil, resolveURL)
	if err != nil {
		t.Fatalf("resolve download: %v", err)
	}
	c, err := client.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.CreateTemp(t.TempDir(), "dl-*")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := c.DownloadFileWithAuthProvider(ctx, provider, fileHash, f); err != nil {
		t.Fatalf("xet download: %v", err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// plainUpstream is a hub without xet support: resolve requests redirect to a
// CDN path that serves the raw bytes with Range support.
type plainUpstream struct {
	mu       sync.Mutex
	files    map[string][]byte
	commit   string
	dataGETs atomic.Int64
	seenAuth sync.Map // Authorization values observed on any request
	gate     chan struct{}
	gateHit  chan struct{}
}

func newPlainUpstream() *plainUpstream {
	return &plainUpstream{files: map[string][]byte{}, commit: "commit-1"}
}

func (u *plainUpstream) set(path string, data []byte) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.files[path] = data
}

func (u *plainUpstream) get(path string) ([]byte, bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	data, ok := u.files[path]
	return data, ok
}

func (u *plainUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if auth := r.Header.Get("Authorization"); auth != "" {
		u.seenAuth.Store(auth, true)
	}

	if strings.HasPrefix(r.URL.Path, "/cdn") {
		data, ok := u.get(strings.TrimPrefix(r.URL.Path, "/cdn"))
		if !ok {
			http.NotFound(w, r)
			return
		}
		if r.Method == http.MethodGet {
			u.dataGETs.Add(1)
		}
		if u.gate != nil && r.Method == http.MethodGet && r.Header.Get("Range") == "" {
			// Stream in two halves so tests can observe serve-while-caching.
			w.Header().Set("Content-Length", fmt.Sprint(len(data)))
			w.WriteHeader(http.StatusOK)
			half := len(data) / 2
			w.(http.Flusher).Flush()
			if _, err := w.Write(data[:half]); err != nil {
				return
			}
			w.(http.Flusher).Flush()
			close(u.gateHit)
			<-u.gate
			_, _ = w.Write(data[half:])
			return
		}
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(data))
		return
	}

	data, ok := u.get(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	sum := sha256.Sum256(data)
	etag := hex.EncodeToString(sum[:])
	w.Header().Set("ETag", `"`+etag+`"`)
	w.Header().Set("X-Linked-Etag", `"`+etag+`"`)
	w.Header().Set("X-Linked-Size", fmt.Sprint(len(data)))
	w.Header().Set("X-Repo-Commit", u.commit)
	http.Redirect(w, r, "/cdn"+r.URL.Path, http.StatusFound)
}

func TestMirrorPlainUpstream(t *testing.T) {
	upstream := newPlainUpstream()
	upstreamSrv := httptest.NewServer(upstream)
	defer upstreamSrv.Close()

	data := make([]byte, 256*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	const resolvePath = "/org/repo/resolve/main/model.bin"
	upstream.set(resolvePath, data)

	storageDir, cacheDir := t.TempDir(), t.TempDir()
	fx := newMirrorFixture(t, upstreamSrv.URL, storageDir, cacheDir, WithUpstreamToken("up-secret"))
	resolveURL := fx.srv.URL + resolvePath

	t.Run("serve while caching with singleflight", func(t *testing.T) {
		const concurrent = 4
		var wg sync.WaitGroup
		bodies := make([][]byte, concurrent)
		errs := make([]error, concurrent)
		for i := range concurrent {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				req, _ := http.NewRequest(http.MethodGet, resolveURL, nil)
				req.Header.Set("Authorization", "Bearer downstream-junk")
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					errs[i] = err
					return
				}
				defer resp.Body.Close()
				bodies[i], errs[i] = io.ReadAll(resp.Body)
			}(i)
		}
		wg.Wait()
		for i := range concurrent {
			if errs[i] != nil {
				t.Fatalf("request %d: %v", i, errs[i])
			}
			if !bytes.Equal(bodies[i], data) {
				t.Fatalf("request %d: body mismatch (%d bytes, want %d)", i, len(bodies[i]), len(data))
			}
		}
		if got := upstream.dataGETs.Load(); got != 1 {
			t.Fatalf("upstream data GETs = %d, want 1", got)
		}
	})

	t.Run("downstream credentials never forwarded upstream", func(t *testing.T) {
		if _, ok := upstream.seenAuth.Load("Bearer downstream-junk"); ok {
			t.Fatal("downstream Authorization leaked to upstream")
		}
		if _, ok := upstream.seenAuth.Load("Bearer up-secret"); !ok {
			t.Fatal("mirror did not use its upstream credential")
		}
	})

	t.Run("ready state redirects to bridge with xet links", func(t *testing.T) {
		resp := waitReady(t, resolveURL)
		sum := sha256.Sum256(data)
		wantLoc := fx.srv.URL + "/xet-bridge/" + hex.EncodeToString(sum[:])
		if loc := resp.Header.Get("Location"); loc != wantLoc {
			t.Fatalf("Location = %q, want %q", loc, wantLoc)
		}
		links := strings.Join(resp.Header.Values("Link"), ", ")
		if !strings.Contains(links, "xet-reconstruction-info") || !strings.Contains(links, "xet-auth") {
			t.Fatalf("Link headers missing xet rels: %q", links)
		}
		if resp.Header.Get("X-Xet-Hash") == "" {
			t.Fatal("missing X-Xet-Hash header")
		}
		if got := resp.Header.Get("X-Linked-Size"); got != fmt.Sprint(len(data)) {
			t.Fatalf("X-Linked-Size = %q, want %d", got, len(data))
		}

		// Plain client follows the redirect to the sha256 bridge.
		followed, err := http.Get(resolveURL)
		if err != nil {
			t.Fatal(err)
		}
		defer followed.Body.Close()
		body, err := io.ReadAll(followed.Body)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(body, data) {
			t.Fatal("bridge body mismatch")
		}
		if got := upstream.dataGETs.Load(); got != 1 {
			t.Fatalf("upstream data GETs after ready = %d, want 1", got)
		}
	})

	t.Run("HEAD answered from metadata", func(t *testing.T) {
		resp, err := noRedirect().Head(resolveURL)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if got := resp.Header.Get("X-Linked-Size"); got != fmt.Sprint(len(data)) {
			t.Fatalf("X-Linked-Size = %q, want %d", got, len(data))
		}
		if got := resp.Header.Get("X-Repo-Commit"); got != "commit-1" {
			t.Fatalf("X-Repo-Commit = %q, want commit-1", got)
		}
	})

	t.Run("xet downstream downloads through mirror CAS", func(t *testing.T) {
		got := downloadViaXet(t, resolveURL)
		if !bytes.Equal(got, data) {
			t.Fatal("xet downstream body mismatch")
		}
		if got := upstream.dataGETs.Load(); got != 1 {
			t.Fatalf("upstream data GETs = %d, want 1", got)
		}
	})

	t.Run("range on ready file via bridge", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodGet, resolveURL, nil)
		req.Header.Set("Range", "bytes=100-199")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusPartialContent {
			t.Fatalf("status = %d, want 206", resp.StatusCode)
		}
		if !bytes.Equal(body, data[100:200]) {
			t.Fatal("range body mismatch")
		}
	})

	t.Run("ready state survives restart", func(t *testing.T) {
		before := upstream.dataGETs.Load()
		fx2 := newMirrorFixture(t, upstreamSrv.URL, storageDir, cacheDir)
		resp := waitReady(t, fx2.srv.URL+resolvePath)
		if resp.StatusCode != http.StatusFound {
			t.Fatalf("status = %d, want 302", resp.StatusCode)
		}
		if got := upstream.dataGETs.Load(); got != before {
			t.Fatalf("restart refetched upstream: %d -> %d", before, got)
		}
	})
}

func TestMirrorRangeWhileIngesting(t *testing.T) {
	upstream := newPlainUpstream()
	upstream.gate = make(chan struct{})
	upstream.gateHit = make(chan struct{})
	upstreamSrv := httptest.NewServer(upstream)
	defer upstreamSrv.Close()

	data := make([]byte, 128*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	const resolvePath = "/org/repo/resolve/main/slow.bin"
	upstream.set(resolvePath, data)

	fx := newMirrorFixture(t, upstreamSrv.URL, t.TempDir(), t.TempDir())
	resolveURL := fx.srv.URL + resolvePath

	// Open the gate once the upstream has streamed the first half.
	go func() {
		<-upstream.gateHit
		close(upstream.gate)
	}()

	// Request a region in the second half before it has been spooled; the
	// mirror must hold the request until the bytes land.
	start, end := len(data)-4096, len(data)-1
	req, _ := http.NewRequest(http.MethodGet, resolveURL, nil)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", resp.StatusCode)
	}
	if !bytes.Equal(body, data[start:end+1]) {
		t.Fatal("range-while-ingesting body mismatch")
	}
	if got, want := resp.Header.Get("Content-Range"), fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)); got != want {
		t.Fatalf("Content-Range = %q, want %q", got, want)
	}
}

// xetUpstream is a hub with xet support: resolve responses carry the xet link
// headers pointing at a real CAS server backed by the upload pipeline.
type xetUpstream struct {
	hubURL   string
	casURL   string
	stor     storage.Storage
	mu       sync.Mutex
	files    map[string]xetUpstreamFile
	xorbGETs atomic.Int64
	omitSize bool          // advertise no size on resolve responses
	gate     chan struct{} // when set, the first xorb GET blocks until closed
	gateHit  chan struct{}
	gateOnce sync.Once
}

type xetUpstreamFile struct {
	fileHash string
	sha256   string
	size     int
}

func newXetUpstream(t *testing.T) *xetUpstream {
	t.Helper()
	u := &xetUpstream{files: map[string]xetUpstreamFile{}}

	var casInner atomic.Value
	casSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/xorbs/") {
			u.xorbGETs.Add(1)
			if u.gate != nil {
				u.gateOnce.Do(func() { close(u.gateHit) })
				<-u.gate
			}
		}
		casInner.Load().(http.Handler).ServeHTTP(w, r)
	}))
	t.Cleanup(casSrv.Close)

	stor, err := storage.NewFileStorage(
		storage.WithBasePath(t.TempDir()),
		storage.WithBaseURL(casSrv.URL),
	)
	if err != nil {
		t.Fatal(err)
	}
	casInner.Store(http.Handler(server.NewHandler(server.WithStorage(stor))))

	hubSrv := httptest.NewServer(http.HandlerFunc(u.serveHub))
	t.Cleanup(hubSrv.Close)

	u.stor = stor
	u.casURL = casSrv.URL
	u.hubURL = hubSrv.URL
	return u
}

func (u *xetUpstream) add(t *testing.T, path string, data []byte) {
	t.Helper()
	fileHash, err := upload.UploadFile(context.Background(),
		&localCAS{storage: u.stor, namespace: "default"},
		bytes.NewReader(data),
		upload.WithEnableSHA256(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	u.mu.Lock()
	u.files[path] = xetUpstreamFile{
		fileHash: fileHash.String(),
		sha256:   hex.EncodeToString(sum[:]),
		size:     len(data),
	}
	u.mu.Unlock()
}

func (u *xetUpstream) serveHub(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/api/xet-read-token" {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"casUrl":      u.casURL,
			"accessToken": "upstream-cas-token",
			"exp":         time.Now().Add(time.Hour).Unix(),
		})
		return
	}

	u.mu.Lock()
	f, ok := u.files[r.URL.Path]
	u.mu.Unlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("ETag", `"`+f.sha256+`"`)
	w.Header().Set("X-Linked-Etag", `"`+f.sha256+`"`)
	if !u.omitSize {
		w.Header().Set("X-Linked-Size", fmt.Sprint(f.size))
	}
	w.Header().Set("X-Repo-Commit", "xet-commit-1")
	w.Header().Set("X-Xet-Hash", f.fileHash)
	w.Header().Add("Link", fmt.Sprintf("<%s/v1/reconstructions/%s>; rel=\"xet-reconstruction-info\"", u.casURL, f.fileHash))
	w.Header().Add("Link", fmt.Sprintf("<%s/api/xet-read-token>; rel=\"xet-auth\"", u.hubURL))
	w.WriteHeader(http.StatusOK)
}

func TestMirrorXetUpstream(t *testing.T) {
	upstream := newXetUpstream(t)

	data := make([]byte, 256*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	const resolvePath = "/org/repo/resolve/main/weights.bin"
	upstream.add(t, resolvePath, data)

	fx := newMirrorFixture(t, upstream.hubURL, t.TempDir(), t.TempDir())
	resolveURL := fx.srv.URL + resolvePath

	t.Run("serve while caching from xet upstream", func(t *testing.T) {
		resp, err := http.Get(resolveURL)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		if !bytes.Equal(body, data) {
			t.Fatal("body mismatch")
		}
	})

	t.Run("ready serves both plain and xet downstream", func(t *testing.T) {
		waitReady(t, resolveURL)
		ingestGETs := upstream.xorbGETs.Load()
		if ingestGETs == 0 {
			t.Fatal("expected the mirror to fetch xorbs from the upstream CAS")
		}

		followed, err := http.Get(resolveURL)
		if err != nil {
			t.Fatal(err)
		}
		defer followed.Body.Close()
		body, err := io.ReadAll(followed.Body)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(body, data) {
			t.Fatal("plain downstream body mismatch")
		}

		if got := downloadViaXet(t, resolveURL); !bytes.Equal(got, data) {
			t.Fatal("xet downstream body mismatch")
		}

		if got := upstream.xorbGETs.Load(); got != ingestGETs {
			t.Fatalf("upstream CAS hit again after ready: %d -> %d", ingestGETs, got)
		}
	})
}

func TestMirrorXetUpstreamUnknownSize(t *testing.T) {
	upstream := newXetUpstream(t)
	upstream.omitSize = true
	upstream.gate = make(chan struct{})
	upstream.gateHit = make(chan struct{})

	data := make([]byte, 128*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	const resolvePath = "/org/repo/resolve/main/nosize.bin"
	upstream.add(t, resolvePath, data)

	fx := newMirrorFixture(t, upstream.hubURL, t.TempDir(), t.TempDir())
	resolveURL := fx.srv.URL + resolvePath

	// Hold the first xorb fetch until it is observed, so the GET below is
	// served mid-ingest while the total size is still unknown.
	go func() {
		<-upstream.gateHit
		close(upstream.gate)
	}()

	resp, err := http.Get(resolveURL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if !bytes.Equal(body, data) {
		t.Fatalf("body mismatch: got %d bytes, want %d", len(body), len(data))
	}
	waitReady(t, resolveURL)
}

func TestMirrorUpstreamNotFound(t *testing.T) {
	upstream := newPlainUpstream()
	upstreamSrv := httptest.NewServer(upstream)
	defer upstreamSrv.Close()

	fx := newMirrorFixture(t, upstreamSrv.URL, t.TempDir(), t.TempDir())
	resolveURL := fx.srv.URL + "/org/repo/resolve/main/missing.bin"

	for i := range 2 {
		resp, err := http.Get(resolveURL)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("request %d: status = %d, want 404", i, resp.StatusCode)
		}
	}
}

func TestMirrorRevalidateStale(t *testing.T) {
	upstream := newPlainUpstream()
	upstreamSrv := httptest.NewServer(upstream)
	defer upstreamSrv.Close()

	v1 := []byte(strings.Repeat("version-one ", 4096))
	const resolvePath = "/org/repo/resolve/main/data.txt"
	upstream.set(resolvePath, v1)

	// Revalidate on every request.
	fx := newMirrorFixture(t, upstreamSrv.URL, t.TempDir(), t.TempDir(), WithRevalidateInterval(0))
	resolveURL := fx.srv.URL + resolvePath

	resp, err := http.Get(resolveURL)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !bytes.Equal(body, v1) {
		t.Fatal("initial body mismatch")
	}
	waitReady(t, resolveURL)

	// Same etag: revalidation keeps serving the cached copy.
	resp = waitReady(t, resolveURL)
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("status = %d, want 302", resp.StatusCode)
	}

	// Change the content: the next request goes stale and re-ingests.
	v2 := []byte(strings.Repeat("version-two!", 4096))
	upstream.set(resolvePath, v2)
	upstream.commit = "commit-2"

	resp, err = http.Get(resolveURL)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if !bytes.Equal(body, v2) {
		t.Fatalf("stale body not refreshed: got %d bytes, want %d", len(body), len(v2))
	}
	waitReady(t, resolveURL)
}

func TestMirrorControlPlaneProxy(t *testing.T) {
	var upstreamSawAuth atomic.Value
	upstreamSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/models/org/repo" {
			upstreamSawAuth.Store(r.Header.Get("Authorization"))
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"org/repo"}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer upstreamSrv.Close()

	fx := newMirrorFixture(t, upstreamSrv.URL, t.TempDir(), t.TempDir(), WithUpstreamToken("up-secret"))

	req, _ := http.NewRequest(http.MethodGet, fx.srv.URL+"/api/models/org/repo", nil)
	req.Header.Set("Authorization", "Bearer downstream-junk")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if !strings.Contains(string(body), "org/repo") {
		t.Fatalf("unexpected proxy body: %q", body)
	}
	if got := upstreamSawAuth.Load(); got != "Bearer up-secret" {
		t.Fatalf("upstream saw Authorization %q, want injected mirror credential", got)
	}
}

func TestMirrorTokenEndpoint(t *testing.T) {
	upstream := newPlainUpstream()
	upstreamSrv := httptest.NewServer(upstream)
	defer upstreamSrv.Close()

	fx := newMirrorFixture(t, upstreamSrv.URL, t.TempDir(), t.TempDir())

	resp, err := http.Get(fx.srv.URL + "/xet-token")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var tok struct {
		CASURL string `json:"casUrl"`
		Token  string `json:"accessToken"`
		Exp    int64  `json:"exp"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tok); err != nil {
		t.Fatal(err)
	}
	if tok.CASURL != fx.srv.URL {
		t.Fatalf("casUrl = %q, want %q", tok.CASURL, fx.srv.URL)
	}
	if tok.Exp <= time.Now().Unix() {
		t.Fatalf("exp = %d, want in the future", tok.Exp)
	}
	if !fx.issuer.Validate(tok.Token, time.Now()) {
		t.Fatal("minted token does not validate")
	}
}
