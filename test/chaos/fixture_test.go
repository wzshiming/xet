package chaos_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/auth"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/client/hf"
	"github.com/wzshiming/xet/client/hf/hftest"
	"github.com/wzshiming/xet/download"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/local"
)

const (
	testTimeout    = 15 * time.Second
	defaultIdle    = 2 * time.Second
	idleTimeout    = 200 * time.Millisecond // stall and trickle tests opt in with shortIdle
	defaultRetries = 2
	hubToken       = "hub-token"
	knownGapsEnv   = "XET_CHAOS_KNOWN_GAPS"
	// multiXorbSize is the smallest scenario file that spans two xorbs.
	multiXorbSize = xet.MaxXorbSize + 256<<10

	hubRepoID    = "chaos/fixture"
	issuerSecret = "chaos-fixture-issuer-secret"
	// clockStartUnix is 2100-01-01T00:00:00Z, far enough ahead that the hub
	// client's real-time expiry check keeps the fake clock's tokens cached.
	clockStartUnix = 4102444800
)

var (
	shortIdle = client.WithIdleTimeout(idleTimeout)
	noRetries = client.WithRetries(0)

	// denyXorbs answers every xorb GET with 403 and passes the rest through.
	denyXorbs rule = func(r record) fault {
		if r.xorbGet() {
			return fault{kind: injectStatus, status: http.StatusForbidden}
		}
		return fault{}
	}
)

// fixture is a real CAS backend reached through the TCP proxy and the fault
// proxy: client -> tcp -> fp -> backend, with presigned xorb URLs carrying
// casURL so xorb GETs traverse both. file2 shares its first half with file1,
// so its layout spans two xorbs; small rides along in batch downloads.
type fixture struct {
	tcp      *tcpProxy
	fp       *faultProxy
	casURL   string
	backend  *httptest.Server
	storage  *local.Storage
	issuer   *auth.Issuer
	clock    *fakeClock
	hub      *hftest.Hub
	hubProxy *faultProxy
	repo     hf.Repo

	file1, file2, small, empty *blob
	first, big                 task // file2's term 0 (file1's xorb) and term 1 (several chunks of its own xorb)

	wantHub  bool
	wantAuth bool
	authTTL  time.Duration
}

type fixtureOption func(*fixture)

// withHub serves the files from a fake hub behind hubProxy and binds fx.repo to it.
func withHub() fixtureOption {
	return func(fx *fixture) { fx.wantHub = true }
}

// withAuth gates the backend (and the hub's tokens) with an issuer on a fake
// clock; ttl <= 0 means one hour.
func withAuth(ttl time.Duration) fixtureOption {
	return func(fx *fixture) {
		fx.wantAuth = true
		fx.authTTL = ttl
	}
}

// fakeClock is the issuer's notion of now.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// issuerProvider mints a fresh token for every requested permission.
type issuerProvider struct {
	baseURL string
	issuer  *auth.Issuer
}

func (p issuerProvider) Resolve(_ context.Context, perm auth.Permission) (string, string, error) {
	token, _, err := p.issuer.Sign(auth.Grant{Permission: perm})
	if err != nil {
		return "", "", err
	}
	return p.baseURL, token, nil
}

// blob is one seeded file.
type blob struct {
	name   string
	data   []byte
	hash   xet.FileHash
	sha256 [32]byte
	layout *download.ReconstructionResponseV1 // nil for empty
}

// pseudoRandom is an LCG byte stream; seeds >= 16 are reserved for scenario files.
func pseudoRandom(size int, seed uint32) []byte {
	data := make([]byte, size)
	s := seed
	for i := range data {
		s = s*1664525 + 1013904223
		data[i] = byte(s >> 24)
	}
	return data
}

// task is one xorb byte range the client fetches for a term.
type task struct {
	hash       string // xorb hash hex
	chunks     download.ChunkRange
	start, end int64 // the term's URL byte range, inclusive
}

func (tk task) rangeHeader() string { return fmt.Sprintf("bytes=%d-%d", tk.start, tk.end) }

// match reports whether r is a GET of this xorb for exactly this range.
func (tk task) match(r record) bool {
	return r.xorbGet() && strings.Contains(r.Path, tk.hash) && r.Range == tk.rangeHeader()
}

// within keeps the GETs of this xorb whose Range ends at the task's end and starts in [start, start+prefix]: the original request and the resumes that begin at most prefix bytes in.
func (tk task) within(prefix int64) func(record) bool {
	return func(r record) bool {
		var start, end int64
		if !r.xorbGet() || !strings.Contains(r.Path, tk.hash) {
			return false
		}
		if _, err := fmt.Sscanf(r.Range, "bytes=%d-%d", &start, &end); err != nil {
			return false
		}
		return end == tk.end && start >= tk.start && start <= tk.start+prefix
	}
}

func newFixture(t *testing.T, opts ...fixtureOption) *fixture {
	t.Helper()
	fx := &fixture{}
	for _, opt := range opts {
		opt(fx)
	}
	if fx.wantAuth {
		ttl := fx.authTTL
		if ttl <= 0 {
			ttl = time.Hour
		}
		fx.clock = &fakeClock{t: time.Unix(clockStartUnix, 0).UTC()}
		issuer, err := auth.NewIssuer([]byte(issuerSecret), ttl, fx.clock.now)
		if err != nil {
			t.Fatal(err)
		}
		fx.issuer = issuer
	}

	fx.fp = newFaultProxy()
	fx.tcp = newTCPProxy(t, fx.fp.Addr())
	fx.casURL = fx.tcp.URL()
	st, err := local.NewStorage(local.WithBasePath(t.TempDir()), local.WithBaseURL(fx.casURL))
	if err != nil {
		fx.fp.Close()
		t.Fatal(err)
	}
	fx.storage = st
	handlerOpts := []server.Option{server.WithStorage(st)}
	if fx.issuer != nil {
		handlerOpts = append(handlerOpts, server.WithAuthorizer(fx.issuer))
	}
	fx.backend = httptest.NewServer(server.NewHandler(handlerOpts...))
	// Blackholed connections keep proxy handlers blocked on writes, and
	// stalled handlers hold backend responses open: reset, then drain in order.
	t.Cleanup(func() {
		fx.tcp.closeExisting()
		fx.fp.Close()
		fx.backend.Close()
	})
	fx.fp.setTarget(fx.backend.URL)

	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	defer cancel()
	fx.file1 = fx.seed(t, ctx, "file1", pseudoRandom(1<<20, 1))
	fx.file2 = fx.seed(t, ctx, "file2", slices.Concat(fx.file1.data[:512<<10], pseudoRandom(512<<10, 2)))
	fx.small = fx.seed(t, ctx, "small", bytes.Repeat([]byte("chaos"), 4096))
	fx.empty = fx.seed(t, ctx, "empty", []byte{})

	layout := fx.file2.layout
	if len(layout.Terms) != 2 || layout.Terms[0].Hash == layout.Terms[1].Hash {
		t.Fatalf("fixture: file2 terms %+v, want two terms from different xorbs", layout.Terms)
	}
	if layout.Terms[0].Hash != fx.file1.layout.Terms[0].Hash {
		t.Fatalf("fixture: file2 term 0 is in xorb %s, want file1's %s", layout.Terms[0].Hash, fx.file1.layout.Terms[0].Hash)
	}
	if n := layout.Terms[1].Range.End - layout.Terms[1].Range.Start; n < 2 {
		t.Fatalf("fixture: file2 term 1 spans %d chunk, want several: %+v", n, layout.Terms)
	}
	for _, term := range layout.Terms {
		if n := len(layout.FetchInfo[term.Hash]); n != 1 {
			t.Fatalf("fixture: xorb %s has %d fetch entries, want 1: %+v", term.Hash, n, layout.FetchInfo)
		}
	}
	fx.first, fx.big = taskFor(t, fx.file2, 0), taskFor(t, fx.file2, 1)

	if fx.wantHub {
		fx.hub = hftest.NewHub(t, hftest.Options{
			Repo:     hubRepoID,
			RepoType: "model",
			Revision: "main",
			Token:    hubToken,
			CAS:      fx.casURL,
			Issuer:   fx.issuer,
			Storage:  st,
			// Every committed file is a xet file, text or not.
			UploadMode: func(string, []byte, int64) string { return "lfs" },
		})
		fx.hubProxy = newFaultProxy()
		t.Cleanup(fx.hubProxy.Close)
		fx.hubProxy.setTarget(fx.hub.URL)
		fx.repo = hf.Repo{Endpoint: fx.hubProxy.URL(), RepoType: "model", RepoID: hubRepoID, Revision: "main"}
		fx.seedHub(t, ctx, fx.file1, fx.file2, fx.small)
	}
	return fx
}

// seed stores data straight into storage and records its V1 layout.
func (fx *fixture) seed(t *testing.T, ctx context.Context, name string, data []byte) *blob {
	t.Helper()
	hash, err := storage.PutFile(ctx, fx.storage, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("fixture: seed %s: %v", name, err)
	}
	b := &blob{name: name, data: data, hash: hash, sha256: sha256.Sum256(data)}
	if len(data) == 0 {
		return b
	}
	sh, err := fx.storage.GetShard(ctx, hash)
	if err != nil {
		t.Fatalf("fixture: shard of %s: %v", name, err)
	}
	if b.layout, err = download.BuildReconstructionResponseV1(ctx, fx.storage, sh, hash, ""); err != nil {
		t.Fatalf("fixture: layout of %s: %v", name, err)
	}
	return b
}

// seedHub commits files to the hub as xet files in one direct, unproxied
// commit; the hub resolves their SHA-256 to the hashes storage already holds.
func (fx *fixture) seedHub(t *testing.T, ctx context.Context, files ...*blob) {
	t.Helper()
	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	_ = enc.Encode(map[string]any{"key": "header", "value": map[string]string{"summary": "seed"}})
	for _, f := range files {
		_ = enc.Encode(map[string]any{"key": "lfsFile", "value": map[string]any{
			"algo": "sha256", "oid": hex.EncodeToString(f.sha256[:]), "path": f.name, "size": len(f.data),
		}})
	}
	direct := hf.Repo{Endpoint: fx.hub.URL, RepoType: "model", RepoID: hubRepoID, Revision: "main"}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, direct.CommitURL(), &body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+hubToken)
	req.Header.Set("Content-Type", "application/x-ndjson")
	transport := freshTransport()
	defer transport.CloseIdleConnections()
	resp, err := (&http.Client{Transport: transport}).Do(req)
	if err != nil {
		t.Fatalf("fixture: seed hub: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(resp.Body)
		t.Fatalf("fixture: seed hub: %s: %s", resp.Status, msg)
	}
}

// token mints a perm token on the fixture's issuer.
func (fx *fixture) token(t *testing.T, perm auth.Permission) string {
	t.Helper()
	if fx.issuer == nil {
		t.Fatal("fixture: token needs withAuth")
	}
	token, _, err := fx.issuer.Sign(auth.Grant{Permission: perm})
	if err != nil {
		t.Fatal(err)
	}
	return token
}

// taskFor returns the byte range the layout advertises for f's term.
func taskFor(t *testing.T, f *blob, term int) task {
	t.Helper()
	tm := f.layout.Terms[term]
	for _, entry := range f.layout.FetchInfo[tm.Hash] {
		if entry.Range == tm.Range {
			return task{hash: tm.Hash, chunks: entry.Range, start: entry.URLRange.Start, end: entry.URLRange.End}
		}
	}
	t.Fatalf("fixture: no fetch info covers %s term %d %+v", f.name, term, tm)
	return task{}
}

// chunkBounds returns the absolute end offset of every packed chunk in the task; the last is task.end+1.
func (fx *fixture) chunkBounds(t *testing.T, tk task) []int64 {
	t.Helper()
	hash, err := xet.ParseXorbHash(tk.hash)
	if err != nil {
		t.Fatal(err)
	}
	offsets, err := fx.storage.GetXorbChunkOffsets(context.Background(), hash)
	if err != nil {
		t.Fatal(err)
	}
	var bounds []int64
	for idx := tk.chunks.Start; idx < tk.chunks.End; idx++ {
		bounds = append(bounds, int64(offsets[idx]))
	}
	if len(bounds) == 0 || bounds[len(bounds)-1] != tk.end+1 {
		t.Fatalf("fixture: task %+v ends at %d, chunk bounds %v", tk, tk.end, bounds)
	}
	return bounds
}

// midChunkPrefix returns an offset from the task's start that lands strictly
// inside the task's chunk index 2, the third chunk, between bounds[1] and bounds[2].
func (fx *fixture) midChunkPrefix(t *testing.T, tk task) int64 {
	t.Helper()
	bounds := fx.chunkBounds(t, tk)
	if len(bounds) < 3 || bounds[2]-bounds[1] < 2 {
		t.Fatalf("fixture: chunk bounds %v leave no room inside chunk 2 of %+v", bounds, tk)
	}
	mid := (bounds[1] + bounds[2]) / 2
	if mid <= bounds[1] || mid >= bounds[2] {
		t.Fatalf("fixture: midpoint %d of chunk 2 is not strictly inside %v", mid, bounds[1:3])
	}
	return mid - tk.start
}

// maxChunk returns the largest packed chunk size in the task.
func (fx *fixture) maxChunk(t *testing.T, tk task) int64 {
	t.Helper()
	var largest int64
	prev := tk.start
	for _, bound := range fx.chunkBounds(t, tk) {
		largest = max(largest, bound-prev)
		prev = bound
	}
	return largest
}

// freshTransport is a private HTTP/1.1 transport with a bounded dial and no response timeouts.
func freshTransport() *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{Timeout: 2 * time.Second}).DialContext
	transport.ForceAttemptHTTP2 = false
	return transport
}

// clientDefaults are the CAS client options every chaos client starts from.
func clientDefaults(cacheDir string) []client.Options {
	return []client.Options{
		client.WithCache(client.NewCache(cacheDir, 0, 0)),
		client.WithIdleTimeout(defaultIdle),
		client.WithRetries(defaultRetries),
		client.WithConcurrency(4),
	}
}

// provider binds baseURL anonymously, or through the issuer under withAuth.
func (fx *fixture) provider(baseURL string) client.UpstreamProvider {
	if fx.issuer != nil {
		return issuerProvider{baseURL: baseURL, issuer: fx.issuer}
	}
	return client.StaticUpstreamProvider(baseURL, "")
}

// buildClient assembles a client on its own transport; caller opts come last so they override.
func (fx *fixture) buildClient(baseURL, cacheDir string, opts ...client.Options) (*client.Client, *http.Transport, error) {
	transport := freshTransport()
	all := []client.Options{
		client.WithHTTPClient(&http.Client{Transport: transport}),
		client.WithUpstreamProvider(fx.provider(baseURL)),
	}
	all = append(all, clientDefaults(cacheDir)...)
	all = append(all, opts...)
	c, err := client.NewClient(all...)
	return c, transport, err
}

func (fx *fixture) clientFor(t *testing.T, baseURL, cacheDir string, opts ...client.Options) *client.Client {
	t.Helper()
	c, transport, err := fx.buildClient(baseURL, cacheDir, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(transport.CloseIdleConnections)
	return c
}

// newClient talks to the CAS through both proxies, using cacheDir.
func (fx *fixture) newClient(t *testing.T, cacheDir string, opts ...client.Options) *client.Client {
	t.Helper()
	return fx.clientFor(t, fx.casURL, cacheDir, opts...)
}

// directClient sends API requests straight to the backend; xorb URLs still carry casURL.
func (fx *fixture) directClient(t *testing.T, cacheDir string, opts ...client.Options) *client.Client {
	t.Helper()
	return fx.clientFor(t, fx.backend.URL, cacheDir, opts...)
}

// newHFClient binds a hub client to fx.repo; the hub hands it the CAS behind both proxies.
func (fx *fixture) newHFClient(t *testing.T, cacheDir string, opts ...hf.Options) *hf.Client {
	t.Helper()
	if fx.hub == nil {
		t.Fatal("fixture: newHFClient needs withHub")
	}
	transport := freshTransport()
	t.Cleanup(transport.CloseIdleConnections)
	all := []hf.Options{
		hf.WithToken(hubToken),
		hf.WithHTTPClient(&http.Client{Transport: transport}),
		hf.WithClientOptions(clientDefaults(cacheDir)...),
	}
	all = append(all, opts...)
	c, err := hf.NewClient(fx.repo, all...)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

type apiVersion int

const (
	apiAuto apiVersion = iota
	apiV1
	apiV2
	apiBatch
)

func (v apiVersion) String() string {
	switch v {
	case apiAuto:
		return "auto"
	case apiV1:
		return "v1"
	case apiV2:
		return "v2"
	case apiBatch:
		return "batch"
	}
	return fmt.Sprintf("api(%d)", int(v))
}

// output is a file that hides os.File's ReadFrom so every byte passes through
// Write and every rewind is visible.
type output struct {
	f       *os.File
	pos     int64 // current file offset
	written int64 // bytes written so far
	rewinds int   // Seeks that landed before the current offset
}

func openOutput(dir string, seed []byte) (*output, error) {
	f, err := os.CreateTemp(dir, "out-*")
	if err != nil {
		return nil, err
	}
	if _, err := f.Write(seed); err != nil {
		f.Close()
		os.Remove(f.Name())
		return nil, err
	}
	return &output{f: f, pos: int64(len(seed))}, nil
}

// newOutput returns an output pre-filled with seed (nil for a fresh one).
func newOutput(t *testing.T, seed []byte) *output {
	t.Helper()
	out, err := openOutput(t.TempDir(), seed)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(out.close)
	return out
}

func (o *output) close() {
	_ = o.f.Close()
	_ = os.Remove(o.f.Name())
}

func (o *output) Write(p []byte) (int, error) {
	n, err := o.f.Write(p)
	o.pos += int64(n)
	o.written += int64(n)
	return n, err
}

// Seek counts a rewind when it lands before the current offset; the resume probe Seek(0, io.SeekEnd) never counts.
func (o *output) Seek(offset int64, whence int) (int64, error) {
	pos, err := o.f.Seek(offset, whence)
	if err != nil {
		return pos, err
	}
	if pos < o.pos && (offset != 0 || whence != io.SeekEnd) {
		o.rewinds++
	}
	o.pos = pos
	return pos, nil
}

func (o *output) readAll() ([]byte, error) {
	return os.ReadFile(o.f.Name())
}

func (o *output) bytes(t *testing.T) []byte {
	t.Helper()
	got, err := o.readAll()
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// download fetches f through api into out; batch mode also drains small and checks it.
func (fx *fixture) download(ctx context.Context, c *client.Client, api apiVersion, f *blob, out *output) error {
	switch api {
	case apiAuto:
		return c.DownloadFile(ctx, f.hash, out)
	case apiV1:
		return c.DownloadFileV1(ctx, f.hash, out)
	case apiV2:
		return c.DownloadFileV2(ctx, f.hash, out)
	}
	if f == fx.small {
		return errors.New("batch: small is the companion file and cannot be the subject")
	}
	readers, sizes, err := c.DownloadFiles(ctx, []xet.FileHash{f.hash, fx.small.hash})
	if err != nil {
		return err
	}
	for _, r := range readers {
		if r != nil {
			defer r.Close()
		}
	}
	if readers[0] == nil || readers[1] == nil || sizes[0] != int64(len(f.data)) || sizes[1] != int64(len(fx.small.data)) {
		return fmt.Errorf("batch: readers %v sizes %v", readers, sizes)
	}
	if _, err := io.Copy(out, readers[0]); err != nil {
		return err
	}
	small, err := io.ReadAll(readers[1])
	if err != nil {
		return err
	}
	if err := diffBytes(small, fx.small.data); err != nil {
		return fmt.Errorf("batch: small: %w", err)
	}
	return nil
}

// mustDownload downloads f into a fresh output and requires exact bytes.
func (fx *fixture) mustDownload(t *testing.T, ctx context.Context, c *client.Client, api apiVersion, f *blob) *output {
	t.Helper()
	out := newOutput(t, nil)
	if err := fx.download(ctx, c, api, f, out); err != nil {
		t.Fatalf("%s %s download: %v", f.name, api, err)
	}
	requireBytes(t, out, f.data)
	return out
}

// diffBytes is nil when got equals want, else names the lengths and the first differing offset.
func diffBytes(got, want []byte) error {
	if bytes.Equal(got, want) {
		return nil
	}
	n := min(len(got), len(want))
	for i := range n {
		if got[i] != want[i] {
			return fmt.Errorf("got %d bytes, want %d; first difference at offset %d (0x%02x vs 0x%02x)", len(got), len(want), i, got[i], want[i])
		}
	}
	return fmt.Errorf("got %d bytes, want %d; first difference at offset %d", len(got), len(want), n)
}

func requireBytes(t *testing.T, out *output, want []byte) {
	t.Helper()
	if err := diffBytes(out.bytes(t), want); err != nil {
		t.Fatalf("output: %v", err)
	}
}

// cachedErr proves cacheDir holds all of f: a fresh client succeeds while every xorb GET is denied.
func (fx *fixture) cachedErr(ctx context.Context, f *blob, cacheDir string) error {
	fx.fp.arm(denyXorbs)
	defer fx.fp.arm(nil)
	c, transport, err := fx.buildClient(fx.casURL, cacheDir)
	if err != nil {
		return err
	}
	defer transport.CloseIdleConnections()
	out, err := openOutput("", nil)
	if err != nil {
		return err
	}
	defer out.close()
	if err := fx.download(ctx, c, apiV1, f, out); err != nil {
		return fmt.Errorf("%s from cache %s: %w", f.name, cacheDir, err)
	}
	got, err := out.readAll()
	if err != nil {
		return err
	}
	if err := diffBytes(got, f.data); err != nil {
		return fmt.Errorf("%s from cache %s: %w", f.name, cacheDir, err)
	}
	if gets := filter(fx.fp.records(), record.xorbGet); len(gets) != 0 {
		return fmt.Errorf("%s from cache %s: %d xorb GETs: %+v", f.name, cacheDir, len(gets), gets)
	}
	return nil
}

func (fx *fixture) assertCached(t *testing.T, ctx context.Context, f *blob, cacheDir string) {
	t.Helper()
	if err := fx.cachedErr(ctx, f, cacheDir); err != nil {
		t.Fatal(err)
	}
}

// heal completes f on cacheDir with no faults, proves the cache is whole,
// and returns the xorb GETs the completion needed.
func (fx *fixture) heal(t *testing.T, ctx context.Context, f *blob, cacheDir string) []record {
	t.Helper()
	fx.fp.arm(nil)
	fx.tcp.clear()
	fx.mustDownload(t, ctx, fx.newClient(t, cacheDir), apiAuto, f)
	gets := filter(fx.fp.settle(t), record.xorbGet)
	fx.assertCached(t, ctx, f, cacheDir)
	return gets
}

// accountingErr checks that gets fetched every xorb of f, each byte range
// once, and exactly extra ranges outside f's layout.
func accountingErr(gets []record, f *blob, extra int) error {
	var terms []download.Term
	if f.layout != nil {
		terms = f.layout.Terms
	}
	seen := make(map[string]bool)
	foreign := 0
	for _, g := range gets {
		key := g.Path + " " + g.Range
		if seen[key] {
			return fmt.Errorf("range fetched twice: %+v", g)
		}
		seen[key] = true
		known := false
		for _, term := range terms {
			known = known || strings.Contains(g.Path, term.Hash)
		}
		if !known {
			foreign++
		}
	}
	for _, term := range terms {
		if len(filter(gets, xorbOf(term.Hash))) == 0 {
			return fmt.Errorf("xorb %s of %s never fetched: %+v", term.Hash, f.name, gets)
		}
	}
	if foreign != extra {
		return fmt.Errorf("%d GETs outside %s's layout, want %d: %+v", foreign, f.name, extra, gets)
	}
	return nil
}

func (fx *fixture) requireAccounting(t *testing.T, gets []record, f *blob, extra int) {
	t.Helper()
	if err := accountingErr(gets, f, extra); err != nil {
		t.Fatal(err)
	}
}

// stagingFiles lists the xorb-* names under <cacheDir>/upload/staging.
func stagingFiles(t *testing.T, cacheDir string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(cacheDir, "upload", "staging"))
	if errors.Is(err, fs.ErrNotExist) {
		return []string{}
	}
	if err != nil {
		t.Fatal(err)
	}
	names := []string{}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "xorb-") {
			names = append(names, entry.Name())
		}
	}
	return names
}

// serverHas reports whether storage holds a shard for the file.
func (fx *fixture) serverHas(t *testing.T, hash xet.FileHash) bool {
	t.Helper()
	_, err := fx.storage.GetShard(context.Background(), hash)
	if err == nil {
		return true
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false
	}
	t.Fatalf("storage shard lookup for %s: %v", hash, err)
	return false
}

// storedErr reads data back from storage by its SHA-256, independent of proxies and clients.
func (fx *fixture) storedErr(data []byte) error {
	if len(data) == 0 {
		return errors.New("storedErr: data must be non-empty")
	}
	rc, err := fx.storage.GetReconstructedFile(context.Background(), sha256.Sum256(data))
	if err != nil {
		return fmt.Errorf("stored file by sha256: %w", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("read stored file: %w", err)
	}
	return diffBytes(got, data)
}

func (fx *fixture) requireStored(t *testing.T, data []byte) {
	t.Helper()
	if err := fx.storedErr(data); err != nil {
		t.Fatal(err)
	}
}

// workerSet is the ids of the goroutines alive at a snapshot.
type workerSet map[string]struct{}

// goroutineStacks returns one block per live goroutine, "goroutine N [state]:" first.
func goroutineStacks() []string {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	return strings.Split(strings.TrimSpace(string(buf)), "\n\n")
}

func goroutineID(stack string) string {
	head, _, _ := strings.Cut(stack, "\n")
	fields := strings.Fields(head)
	if len(fields) < 2 {
		return head
	}
	return fields[1]
}

// workerFrame reports whether a frame of stack is in the client, hub client, download or upload package.
func workerFrame(stack string) bool {
	for line := range strings.SplitSeq(stack, "\n") {
		line = strings.TrimPrefix(line, "created by ")
		for _, pkg := range [...]string{"github.com/wzshiming/xet/client.", "github.com/wzshiming/xet/client/hf.", "github.com/wzshiming/xet/download.", "github.com/wzshiming/xet/upload."} {
			if strings.HasPrefix(line, pkg) {
				return true
			}
		}
	}
	return false
}

func snapshotWorkers() workerSet {
	set := workerSet{}
	for _, stack := range goroutineStacks() {
		set[goroutineID(stack)] = struct{}{}
	}
	return set
}

// leakedWorkers returns the stacks of the worker goroutines started since before.
func leakedWorkers(before workerSet) []string {
	var leaked []string
	for _, stack := range goroutineStacks() {
		if _, ok := before[goroutineID(stack)]; !ok && workerFrame(stack) {
			leaked = append(leaked, stack)
		}
	}
	return leaked
}

// noLeakedWorkers waits for every worker goroutine started since before to exit.
func (fx *fixture) noLeakedWorkers(t *testing.T, before workerSet) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for {
		leaked := leakedWorkers(before)
		if len(leaked) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d worker goroutines still running after %v:\n%s", len(leaked), waitTimeout, strings.Join(leaked, "\n\n"))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func gapsEnabled() bool { return os.Getenv(knownGapsEnv) == "1" }

// knownGap skips the rest of the test unless the known gaps are opted in; callable after setup.
func knownGap(t *testing.T, reason string) {
	t.Helper()
	if !gapsEnabled() {
		t.Skipf("known gap: %s (%s=1 runs it)", reason, knownGapsEnv)
	}
}

func testCtx(t *testing.T, d time.Duration) (context.Context, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), d)
	t.Cleanup(cancel)
	return ctx, cancel
}

func xorbOf(hash string) func(record) bool {
	return func(r record) bool { return r.xorbGet() && strings.Contains(r.Path, hash) }
}

// parseRange returns the inclusive bounds of a bytes=start-end header.
func parseRange(t *testing.T, header string) (start, end int64) {
	t.Helper()
	if _, err := fmt.Sscanf(header, "bytes=%d-%d", &start, &end); err != nil {
		t.Fatalf("Range %q: %v", header, err)
	}
	return start, end
}
