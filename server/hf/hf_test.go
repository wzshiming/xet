package hf

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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
	hfclient "github.com/wzshiming/xet/client/hf"
	"github.com/wzshiming/xet/mirror"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/token"
	"github.com/wzshiming/xet/upload"
)

// hubFixture wires a server with the hub front end (mirror engine + upstream
// proxy) on an httptest server whose URL is also the storage base URL and
// external URL.
type hubFixture struct {
	srv    *httptest.Server
	mirror *mirror.Mirror
	stor   storage.Storage
	issuer *token.Issuer
}

func newHubFixture(t *testing.T, upstream string, storageDir, cacheDir string, opts ...mirror.Option) *hubFixture {
	t.Helper()
	return newHubFixtureNext(t, upstream, nil, storageDir, cacheDir, opts...)
}

// newHubFixtureNext is newHubFixture with an explicit next handler; when nil
// an upstream proxy without credential is wired, matching cmd/xetd.
func newHubFixtureNext(t *testing.T, upstream string, next http.Handler, storageDir, cacheDir string, opts ...mirror.Option) *hubFixture {
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

	if next == nil {
		next, err = NewUpstreamProxy(upstream, "")
		if err != nil {
			t.Fatal(err)
		}
	}

	m, err := mirror.NewMirror(append([]mirror.Option{
		mirror.WithStorage(stor),
		mirror.WithUpstream(upstream),
		mirror.WithCacheDir(cacheDir),
	}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	// Same composition as cmd/xetd: the CAS server matches its routes first
	// and falls through to the hub front end; the shared issuer ties minting
	// to validation.
	inner.Store(http.Handler(server.NewHandler(
		server.WithStorage(stor),
		server.WithAuthFunc(func(tok string) bool { return issuer.Validate(tok, time.Now()) }),
		server.WithNext(NewHandler(
			WithMirror(m),
			WithExternalURL(srv.URL),
			WithMintToken(issuer.Mint),
			WithNext(next),
		)),
	)))
	return &hubFixture{srv: srv, mirror: m, stor: stor, issuer: issuer}
}

// noRedirect returns a client that surfaces 3xx responses instead of following.
func noRedirect() *http.Client {
	return &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// waitReady polls a resolve URL until the hub answers with the ready-state
// redirect, returning that response. Reaching it also means the background
// ingest finished writing to storage, so tests end quiesced.
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
	fileHash, provider, err := hfclient.ResolveDownload(ctx, nil, resolveURL)
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
	api      map[string][]byte // raw JSON served under /api/ paths
	commit   string
	dataGETs atomic.Int64
	seenAuth sync.Map // Authorization values observed on any request
	gate     chan struct{}
	gateHit  chan struct{}
}

func newPlainUpstream() *plainUpstream {
	return &plainUpstream{files: map[string][]byte{}, api: map[string][]byte{}, commit: "commit-1"}
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

	if strings.HasPrefix(r.URL.Path, "/api/") {
		u.mu.Lock()
		data, ok := u.api[r.URL.Path]
		u.mu.Unlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
		return
	}

	if after, ok := strings.CutPrefix(r.URL.Path, "/cdn"); ok {
		data, ok := u.get(after)
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
	fx := newHubFixture(t, upstreamSrv.URL, storageDir, cacheDir, mirror.WithUpstreamToken("up-secret"))
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
		fx2 := newHubFixture(t, upstreamSrv.URL, storageDir, cacheDir)
		resp := waitReady(t, fx2.srv.URL+resolvePath)
		if resp.StatusCode != http.StatusFound {
			t.Fatalf("status = %d, want 302", resp.StatusCode)
		}
		if got := upstream.dataGETs.Load(); got != before {
			t.Fatalf("restart refetched upstream: %d -> %d", before, got)
		}
	})
}

// TestMirrorReingestsAfterStorageUnlink covers the self-healing resolve: a
// ready entry whose file was unlinked from storage is dropped and the next
// resolve re-ingests from upstream.
func TestMirrorReingestsAfterStorageUnlink(t *testing.T) {
	upstream := newPlainUpstream()
	upstreamSrv := httptest.NewServer(upstream)
	defer upstreamSrv.Close()

	data := make([]byte, 128*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	const resolvePath = "/org/repo/resolve/main/model.bin"
	upstream.set(resolvePath, data)

	fx := newHubFixture(t, upstreamSrv.URL, t.TempDir(), t.TempDir())
	resolveURL := fx.srv.URL + resolvePath

	resp := waitReady(t, resolveURL)
	fileHash, err := xet.ParseFileHash(resp.Header.Get("X-Xet-Hash"))
	if err != nil {
		t.Fatal(err)
	}
	if got := upstream.dataGETs.Load(); got != 1 {
		t.Fatalf("upstream data GETs = %d, want 1", got)
	}

	removed, err := storage.NewGC(fx.stor.(storage.GCStore)).Unlink(context.Background(), fileHash)
	if err != nil {
		t.Fatal(err)
	}
	if !removed {
		t.Fatal("unlink removed nothing")
	}

	resp = waitReady(t, resolveURL)
	if resp.Header.Get("X-Xet-Hash") == "" {
		t.Fatal("missing X-Xet-Hash after re-ingest")
	}
	if got := upstream.dataGETs.Load(); got != 2 {
		t.Fatalf("upstream data GETs after unlink = %d, want 2", got)
	}

	body, err := http.Get(resolveURL)
	if err != nil {
		t.Fatal(err)
	}
	defer body.Body.Close()
	got, err := io.ReadAll(body.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("re-ingested body mismatch")
	}
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

	fx := newHubFixture(t, upstreamSrv.URL, t.TempDir(), t.TempDir())
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
	waitReady(t, resolveURL)
}

// testCAS adapts storage.Storage to upload.ClientAdapter so tests can seed
// the fake xet upstream without an HTTP hop; dedup always reports new
// chunks, which is fine for one-shot uploads.
type testCAS struct {
	storage storage.Storage
}

func (l testCAS) HasXorb(ctx context.Context, xorbHash xet.XorbHash) (bool, error) {
	return l.storage.HasXorb(ctx, "default", xorbHash)
}

func (l testCAS) UploadXorb(ctx context.Context, xorbHash xet.XorbHash, reader io.ReadSeeker) (*upload.XorbUploadResponse, error) {
	wasInserted, err := l.storage.PutXorb(ctx, "default", xorbHash, reader)
	if err != nil {
		return nil, err
	}
	return &upload.XorbUploadResponse{WasInserted: wasInserted}, nil
}

func (l testCAS) UploadShard(ctx context.Context, shardObj *shard.Shard) (*upload.ShardUploadResponse, error) {
	wasInserted, err := l.storage.PutShard(ctx, shardObj)
	if err != nil {
		return nil, err
	}
	result := 0
	if wasInserted {
		result = 1
	}
	return &upload.ShardUploadResponse{Result: result}, nil
}

func (l testCAS) QueryDedupShards(ctx context.Context, chunkHashes []xet.ChunkHash, _ ...xet.ChunkHash) (map[xet.ChunkHash]*upload.DeduplicationResult, error) {
	results := make(map[xet.ChunkHash]*upload.DeduplicationResult, len(chunkHashes))
	for _, chunkHash := range chunkHashes {
		results[chunkHash] = &upload.DeduplicationResult{ChunkHash: chunkHash, IsNew: true}
	}
	return results, nil
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
		testCAS{storage: u.stor},
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

	fx := newHubFixture(t, upstream.hubURL, t.TempDir(), t.TempDir())
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

	fx := newHubFixture(t, upstream.hubURL, t.TempDir(), t.TempDir())
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

	fx := newHubFixture(t, upstreamSrv.URL, t.TempDir(), t.TempDir())
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
	fx := newHubFixture(t, upstreamSrv.URL, t.TempDir(), t.TempDir(), mirror.WithRevalidateInterval(0))
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

	proxy, err := NewUpstreamProxy(upstreamSrv.URL, "up-secret")
	if err != nil {
		t.Fatal(err)
	}
	fx := newHubFixtureNext(t, upstreamSrv.URL, proxy, t.TempDir(), t.TempDir(), mirror.WithUpstreamToken("up-secret"))

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

	fx := newHubFixture(t, upstreamSrv.URL, t.TempDir(), t.TempDir())

	for _, path := range []string{"/xet-token", "/api/models/org/repo/xet-read-token/main"} {
		resp, err := http.Get(fx.srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		var tok struct {
			CASURL string `json:"casUrl"`
			Token  string `json:"accessToken"`
			Exp    int64  `json:"exp"`
		}
		err = json.NewDecoder(resp.Body).Decode(&tok)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if tok.CASURL != fx.srv.URL {
			t.Fatalf("%s: casUrl = %q, want %q", path, tok.CASURL, fx.srv.URL)
		}
		if tok.Exp <= time.Now().Unix() {
			t.Fatalf("%s: exp = %d, want in the future", path, tok.Exp)
		}
		if !fx.issuer.Validate(tok.Token, time.Now()) {
			t.Fatalf("%s: minted token does not validate", path)
		}
		// huggingface_hub >= 1.29 reads the credential only from these
		// response headers; they must carry the same mint as the JSON body.
		if got := resp.Header.Get("X-Xet-Cas-Url"); got != tok.CASURL {
			t.Fatalf("%s: X-Xet-Cas-Url = %q, want %q", path, got, tok.CASURL)
		}
		if got := resp.Header.Get("X-Xet-Access-Token"); got != tok.Token {
			t.Fatalf("%s: X-Xet-Access-Token = %q, want %q", path, got, tok.Token)
		}
		if got := resp.Header.Get("X-Xet-Token-Expiration"); got != strconv.FormatInt(tok.Exp, 10) {
			t.Fatalf("%s: X-Xet-Token-Expiration = %q, want %d", path, got, tok.Exp)
		}
	}
}

// TestHubRouting pins the routing semantics of the hub front end: escaped
// path segments stay intact through matching (a revision like refs%2Fpr%2F1
// is one segment), and requests matching a hub path with the wrong method
// fall through to next instead of answering 405.
func TestHubRouting(t *testing.T) {
	upstream := newPlainUpstream()
	upstreamSrv := httptest.NewServer(upstream)
	defer upstreamSrv.Close()

	data := []byte("escaped revision bytes")
	// The wire path carries the rev escaped (refs%2Fpr%2F1); the httptest
	// upstream observes it decoded.
	upstream.set("/org/repo/resolve/refs/pr/1/file.bin", data)

	var nextHits atomic.Int64
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nextHits.Add(1)
		w.WriteHeader(http.StatusTeapot)
	})
	fx := newHubFixtureNext(t, upstreamSrv.URL, next, t.TempDir(), t.TempDir())

	t.Run("escaped rev stays one segment", func(t *testing.T) {
		resp, err := http.Get(fx.srv.URL + "/org/repo/resolve/refs%2Fpr%2F1/file.bin")
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		if !bytes.Equal(body, data) {
			t.Fatalf("body = %q, want upstream data", body)
		}
		// Quiesce: the ready redirect means the background ingest finished
		// writing, so temp dir cleanup does not race it.
		waitReady(t, fx.srv.URL+"/org/repo/resolve/refs%2Fpr%2F1/file.bin")
	})

	t.Run("wrong method falls through to next", func(t *testing.T) {
		for _, path := range []string{
			"/xet-token",
			"/api/models/org/repo/xet-read-token/main",
			"/api/models/org/repo/tree/main",
			"/org/repo/resolve/main/file.bin",
		} {
			before := nextHits.Load()
			resp, err := http.Post(fx.srv.URL+path, "application/json", nil)
			if err != nil {
				t.Fatal(err)
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusTeapot {
				t.Fatalf("POST %s: status = %d, want fall-through %d", path, resp.StatusCode, http.StatusTeapot)
			}
			if nextHits.Load() != before+1 {
				t.Fatalf("POST %s did not reach next", path)
			}
		}
	})
}

func TestMirrorTreeRewrite(t *testing.T) {
	upstream := newPlainUpstream()
	upstreamSrv := httptest.NewServer(upstream)
	defer upstreamSrv.Close()

	data := make([]byte, 128*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	const resolvePath = "/org/repo/resolve/main/model.bin"
	upstream.set(resolvePath, data)
	// A real commit revision: branch requests get pinned to it, so the
	// upstream must serve the commit-keyed path the ingest will fetch.
	upstream.commit = strings.Repeat("ab", 20)
	upstream.set("/org/repo/resolve/"+upstream.commit+"/model.bin", data)
	oid := fmt.Sprintf("%x", sha256.Sum256(data))
	treeJSON := []byte(`[
		{"type":"file","path":"model.bin","size":131072,"lfs":{"oid":"` + oid + `","size":131072,"pointerSize":134},"xetHash":"upstream-hash-1"},
		{"type":"file","path":"other.bin","size":7,"lfs":{"oid":"` + strings.Repeat("00", 32) + `","size":7,"pointerSize":133},"xetHash":"upstream-hash-2"},
		{"type":"directory","path":"sub"}
	]`)
	upstream.api["/api/models/org/repo/tree/main"] = treeJSON
	upstream.api["/api/models/org/repo/tree/"+upstream.commit] = treeJSON

	fx := newHubFixture(t, upstreamSrv.URL, t.TempDir(), t.TempDir())

	fetchTree := func(rev string) []map[string]any {
		t.Helper()
		resp, err := http.Get(fx.srv.URL + "/api/models/org/repo/tree/" + rev)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("tree status = %d, want 200", resp.StatusCode)
		}
		var items []map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
			t.Fatal(err)
		}
		if len(items) != 3 {
			t.Fatalf("tree has %d entries, want 3", len(items))
		}
		return items
	}

	t.Run("uncached files lose xetHash", func(t *testing.T) {
		for _, item := range fetchTree("main") {
			if _, ok := item["xetHash"]; ok {
				t.Fatalf("entry %v still carries xetHash", item["path"])
			}
		}
	})

	// Ingest model.bin, then the tree must advertise the mirror's own hash.
	resolveURL := fx.srv.URL + resolvePath
	resp, err := http.Get(resolveURL)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	ready := waitReady(t, resolveURL)
	localHash := ready.Header.Get("X-Xet-Hash")
	if localHash == "" {
		t.Fatal("ready resolve response has no X-Xet-Hash")
	}

	t.Run("cached file advertises local hash on commit rev", func(t *testing.T) {
		for _, item := range fetchTree(upstream.commit) {
			hash, ok := item["xetHash"]
			switch item["path"] {
			case "model.bin":
				if hash != localHash {
					t.Fatalf("model.bin xetHash = %v, want %q", hash, localHash)
				}
			default:
				if ok {
					t.Fatalf("entry %v still carries xetHash", item["path"])
				}
			}
		}
	})

	// Branch trees match by content too; no pinned commit is involved.
	t.Run("branch rev advertises local hash", func(t *testing.T) {
		for _, item := range fetchTree("main") {
			hash, ok := item["xetHash"]
			switch item["path"] {
			case "model.bin":
				if hash != localHash {
					t.Fatalf("model.bin xetHash = %v, want %q", hash, localHash)
				}
			default:
				if ok {
					t.Fatalf("entry %v still carries xetHash", item["path"])
				}
			}
		}
	})

	// Entries without a matching lfs oid (here: none at all) lose the hash.
	t.Run("other repo unaffected", func(t *testing.T) {
		upstream.mu.Lock()
		upstream.api["/api/models/org/other/tree/main"] = []byte(`[{"type":"file","path":"model.bin","size":1,"xetHash":"h"}]`)
		upstream.mu.Unlock()
		resp, err := http.Get(fx.srv.URL + "/api/models/org/other/tree/main")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var items []map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
			t.Fatal(err)
		}
		if _, ok := items[0]["xetHash"]; ok {
			t.Fatal("other repo's entry kept xetHash despite no cache")
		}
	})
}

func TestMirrorBranchPinning(t *testing.T) {
	upstream := newPlainUpstream()
	upstreamSrv := httptest.NewServer(upstream)
	defer upstreamSrv.Close()

	commit1 := strings.Repeat("11", 20)
	commit2 := strings.Repeat("22", 20)
	data1 := []byte("content at commit one")
	data2 := []byte("content at commit two, updated")

	upstream.commit = commit1
	upstream.set("/org/repo/resolve/main/f.bin", data1)
	upstream.set("/org/repo/resolve/"+commit1+"/f.bin", data1)

	// Zero interval: the branch mapping is re-checked on every request.
	fx := newHubFixture(t, upstreamSrv.URL, t.TempDir(), t.TempDir(), mirror.WithRevalidateInterval(0))
	resolveURL := fx.srv.URL + "/org/repo/resolve/main/f.bin"

	fetch := func(url string) []byte {
		t.Helper()
		resp, err := http.Get(url)
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
		return body
	}

	if got := fetch(resolveURL); !bytes.Equal(got, data1) {
		t.Fatal("initial branch fetch mismatch")
	}
	waitReady(t, resolveURL)

	// The entry is keyed by the pinned commit, so a commit-pinned request
	// serves from cache without touching the branch.
	if got := fetch(fx.srv.URL + "/org/repo/resolve/" + commit1 + "/f.bin"); !bytes.Equal(got, data1) {
		t.Fatal("commit-pinned fetch mismatch")
	}

	// Move the branch upstream: the next branch request re-pins and ingests
	// the new content, while the old commit stays served from cache.
	upstream.commit = commit2
	upstream.set("/org/repo/resolve/main/f.bin", data2)
	upstream.set("/org/repo/resolve/"+commit2+"/f.bin", data2)

	if got := fetch(resolveURL); !bytes.Equal(got, data2) {
		t.Fatal("branch fetch after move mismatch")
	}
	waitReady(t, resolveURL)
	if got := fetch(fx.srv.URL + "/org/repo/resolve/" + commit1 + "/f.bin"); !bytes.Equal(got, data1) {
		t.Fatal("old commit no longer served after branch move")
	}
}

// Reproduction for: a client disconnects mid-download, then a new client
// arrives. The new client must immediately receive the already-spooled bytes
// (the ingest keeps running in the background) instead of starting from a
// fresh, empty download.
func TestMirrorClientDisconnectThenNewClient(t *testing.T) {
	upstream := newPlainUpstream()
	upstream.gate = make(chan struct{})
	upstream.gateHit = make(chan struct{})
	upstreamSrv := httptest.NewServer(upstream)
	defer upstreamSrv.Close()

	data := make([]byte, 128*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	const resolvePath = "/org/repo/resolve/main/interrupted.bin"
	upstream.set(resolvePath, data)

	fx := newHubFixture(t, upstreamSrv.URL, t.TempDir(), t.TempDir())
	resolveURL := fx.srv.URL + resolvePath
	half := len(data) / 2

	// Client 1: start the download, read the first half, then disconnect.
	ctx1, cancel1 := context.WithCancel(context.Background())
	req1, _ := http.NewRequestWithContext(ctx1, http.MethodGet, resolveURL, nil)
	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatal(err)
	}
	buf1 := make([]byte, half)
	if _, err := io.ReadFull(resp1.Body, buf1); err != nil {
		t.Fatalf("client1 read first half: %v", err)
	}
	if !bytes.Equal(buf1, data[:half]) {
		t.Fatal("client1 first-half mismatch")
	}
	cancel1() // simulate the client aborting mid-download
	resp1.Body.Close()

	// The upstream is now stalled at the half-way gate; the ingest is still
	// running in the background holding the first half in the spool.
	<-upstream.gateHit

	// Client 2: a fresh request. The first half must arrive promptly from the
	// spool even though the upstream is still stalled.
	req2, _ := http.NewRequest(http.MethodGet, resolveURL, nil)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()

	buf2 := make([]byte, half)
	readDone := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(resp2.Body, buf2)
		readDone <- err
	}()
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("client2 read spooled half: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("client2 did not receive the already-spooled bytes while upstream stalled: progress restarted from scratch")
	}
	if !bytes.Equal(buf2, data[:half]) {
		t.Fatal("client2 first-half mismatch")
	}

	// Unstall the upstream; client 2 must receive the rest.
	close(upstream.gate)
	rest, err := io.ReadAll(resp2.Body)
	if err != nil {
		t.Fatalf("client2 read rest: %v", err)
	}
	if !bytes.Equal(rest, data[half:]) {
		t.Fatal("client2 second-half mismatch")
	}
	waitReady(t, resolveURL)
	if got := upstream.dataGETs.Load(); got != 1 {
		t.Fatalf("upstream data GETs = %d, want 1 (no restart)", got)
	}
}

// Reproduction for the resume-with-Range variant (huggingface_hub style):
// client 2 resumes with a Range request for the not-yet-complete region.
func TestMirrorClientDisconnectThenRangeResume(t *testing.T) {
	upstream := newPlainUpstream()
	upstream.gate = make(chan struct{})
	upstream.gateHit = make(chan struct{})
	upstreamSrv := httptest.NewServer(upstream)
	defer upstreamSrv.Close()

	data := make([]byte, 128*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	const resolvePath = "/org/repo/resolve/main/interrupted2.bin"
	upstream.set(resolvePath, data)

	fx := newHubFixture(t, upstreamSrv.URL, t.TempDir(), t.TempDir())
	resolveURL := fx.srv.URL + resolvePath
	quarter := len(data) / 4

	// Client 1 reads a quarter and disconnects.
	ctx1, cancel1 := context.WithCancel(context.Background())
	req1, _ := http.NewRequestWithContext(ctx1, http.MethodGet, resolveURL, nil)
	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(resp1.Body, make([]byte, quarter)); err != nil {
		t.Fatalf("client1 read: %v", err)
	}
	cancel1()
	resp1.Body.Close()

	<-upstream.gateHit // ingest stalled at half

	// Client 2 resumes from its quarter with a Range request; the region up to
	// the spooled half must arrive promptly.
	req2, _ := http.NewRequest(http.MethodGet, resolveURL, nil)
	req2.Header.Set("Range", fmt.Sprintf("bytes=%d-", quarter))
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusPartialContent {
		t.Fatalf("resume status = %d, want 206", resp2.StatusCode)
	}

	upToHalf := make([]byte, len(data)/2-quarter)
	readDone := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(resp2.Body, upToHalf)
		readDone <- err
	}()
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("client2 read spooled region: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("client2 Range resume did not receive already-spooled bytes")
	}
	if !bytes.Equal(upToHalf, data[quarter:len(data)/2]) {
		t.Fatal("client2 resumed region mismatch")
	}

	close(upstream.gate)
	rest, err := io.ReadAll(resp2.Body)
	if err != nil {
		t.Fatalf("client2 read rest: %v", err)
	}
	if !bytes.Equal(rest, data[len(data)/2:]) {
		t.Fatal("client2 tail mismatch")
	}
	waitReady(t, resolveURL)
}
