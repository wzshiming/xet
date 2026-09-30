package hf

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/auth"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/client/hf/hftest"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage/local"
)

const (
	hubSecret  = "hub-secret"
	wrongToken = "hf_wrong_token_000000000000"
	repoID     = "wzshiming/test"
	lfsPath    = "regression/go-lfs.bin"
	textPath   = "regression/go-regular.txt"
	textData   = "xet private-repo regression fixture — go-regular.txt\ncommitted by the Go client\n"
	sampleLen  = 512 // leading bytes a preupload entry samples, like the reference's
)

// deterministic returns n pseudo-random bytes that are the same on every run.
func deterministic(n int) []byte {
	data := make([]byte, n)
	_, _ = rand.NewChaCha8([32]byte{1: 0x67, 2: 0x6f}).Read(data)
	return data
}

// casLog records every CAS request's method, path and raw Authorization.
type casLog struct {
	mu   sync.Mutex
	reqs []hftest.Request
}

func (l *casLog) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		l.mu.Lock()
		l.reqs = append(l.reqs, hftest.Request{Method: r.Method, Path: r.URL.RequestURI(), Authorization: r.Header.Get("Authorization")})
		l.mu.Unlock()
		next.ServeHTTP(w, r)
	})
}

func (l *casLog) requests() []hftest.Request {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.reqs)
}

// xorbFetches counts the CAS requests that reached a xorb, uploads and data fetches alike.
func (l *casLog) xorbFetches() int {
	n := 0
	for _, req := range l.requests() {
		if strings.Contains(req.Path, "/xorbs/") {
			n++
		}
	}
	return n
}

// spyProvider counts the resolutions asked of the provider it wraps.
type spyProvider struct {
	inner client.UpstreamProvider
	calls atomic.Int32
}

func (p *spyProvider) Resolve(ctx context.Context, perm auth.Permission) (string, string, error) {
	p.calls.Add(1)
	return p.inner.Resolve(ctx, perm)
}

// privateRepo is a private hub guarding repoID with hubSecret in front of a real CAS whose tokens the hub's issuer mints.
type privateRepo struct {
	hub    *hftest.Hub
	cas    *casLog
	opts   hftest.Options // the hub's; a second hub on the same CAS varies them
	target Repo
	hc     *http.Client
}

func newPrivateRepo(t *testing.T) *privateRepo {
	t.Helper()
	issuer, err := auth.NewIssuer(nil, time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	stor, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	log := &casLog{}
	cas := httptest.NewServer(log.wrap(server.NewHandler(server.WithStorage(stor), server.WithAuthorizer(issuer))))
	t.Cleanup(cas.Close)
	opts := hftest.Options{Token: hubSecret, CAS: cas.URL, Issuer: issuer, Storage: stor}
	hub := hftest.NewHub(t, opts)
	return &privateRepo{hub: hub, cas: log, opts: opts, target: Repo{Endpoint: hub.URL, RepoID: repoID}, hc: &http.Client{}}
}

// committer returns a client bound to the repository with the hub token.
func (r *privateRepo) committer(t *testing.T) *Client {
	t.Helper()
	return r.bound(t, r.target, hubSecret)
}

// bound returns a client bound to target with hfToken.
func (r *privateRepo) bound(t *testing.T, target Repo, hfToken string) *Client {
	t.Helper()
	c, err := NewClient(target, WithHTTPClient(r.hc), WithToken(hfToken), WithClientOptions(client.WithCache(client.NewCache(t.TempDir(), 0, 0))))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func (r *privateRepo) resolveURL(path string) string {
	return r.hub.URL + "/" + repoID + "/resolve/main/" + path
}

// get fetches url from the hub with the token, never following redirects.
func (r *privateRepo) get(t *testing.T, method, url, hfToken string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if hfToken != "" {
		req.Header.Set("Authorization", "Bearer "+hfToken)
	}
	hc := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, body
}

func routes(reqs []hftest.Request) []string {
	out := make([]string, 0, len(reqs))
	for _, req := range reqs {
		out = append(out, req.Route())
	}
	return out
}

// requestOf returns the first request with the route.
func requestOf(t *testing.T, reqs []hftest.Request, route string) hftest.Request {
	t.Helper()
	i := slices.IndexFunc(reqs, func(r hftest.Request) bool { return r.Route() == route })
	if i < 0 {
		t.Fatalf("no %s request in %q", route, routes(reqs))
	}
	return reqs[i]
}

// exchangeOf returns the first hub exchange of tr with the route.
func exchangeOf(t *testing.T, tr *hftest.Trace, route string) hftest.Exchange {
	t.Helper()
	i := slices.IndexFunc(tr.Hub(), func(ex hftest.Exchange) bool { return hftest.Route(ex) == route })
	if i < 0 {
		t.Fatalf("%s: no %s exchange", tr.Scenario, route)
	}
	return tr.Hub()[i]
}

// download runs fn against a fresh file and returns its content.
func download(t *testing.T, fn func(io.WriteSeeker) error) ([]byte, error) {
	t.Helper()
	f, err := os.Create(filepath.Join(t.TempDir(), "out"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := fn(f); err != nil {
		return nil, err
	}
	return os.ReadFile(f.Name())
}

func files(lfsData []byte) []CommitFile {
	return []CommitFile{{Path: lfsPath, Content: bytes.NewReader(lfsData)}, {Path: textPath, Content: strings.NewReader(textData)}}
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// A commit through the private hub stores the lfs file in the CAS and the text file in the hub; both come back through Resolve, the bound client and a plain GET, each credential staying on its own origin.
func TestCommitRoundTrip(t *testing.T) {
	repo := newPrivateRepo(t)
	ctx := t.Context()
	lfsData := deterministic(1 << 20)

	c := repo.committer(t)
	commit, err := c.Commit(ctx, "test: round trip", files(lfsData)...)
	if err != nil {
		t.Fatal(err)
	}
	if commit.OID == "" || commit.OID != repo.hub.Head() || commit.URL != "https://huggingface.co/"+repoID+"/commit/"+commit.OID {
		t.Fatalf("commit = %+v, hub head %s", commit, repo.hub.Head())
	}
	if got := routes(repo.hub.Requests()); !slices.Equal(got, []string{"POST preupload", "GET xet-write-token", "POST commit"}) {
		t.Fatalf("hub routes = %q", got)
	}
	uploaded := len(repo.hub.Requests())

	f, err := c.Resolve(ctx, lfsPath)
	if err != nil {
		t.Fatal(err)
	}
	got, err := download(t, func(w io.WriteSeeker) error { return c.DownloadFile(ctx, f.Hash, w) })
	if err != nil || !bytes.Equal(got, lfsData) {
		t.Fatalf("DownloadFile: %d bytes, %v; want %d", len(got), err, len(lfsData))
	}
	// Resolve only HEADs; the bound client fetches its read token once for the download.
	if got := routes(repo.hub.Requests()[uploaded:]); !slices.Equal(got, []string{"HEAD resolve", "GET xet-read-token"}) {
		t.Fatalf("resolve and download hub routes = %q", got)
	}

	resp, _ := repo.get(t, http.MethodHead, repo.resolveURL(lfsPath), hubSecret)
	hash, err := xet.ParseFileHash(resp.Header.Get("X-Xet-Hash"))
	if err != nil || hash != f.Hash {
		t.Fatalf("X-Xet-Hash = %q (%v), resolved %s", resp.Header.Get("X-Xet-Hash"), err, f.Hash)
	}
	bound := repo.bound(t, repo.target, hubSecret)
	got, err = download(t, func(w io.WriteSeeker) error { return bound.DownloadFile(ctx, hash, w) })
	if err != nil || !bytes.Equal(got, lfsData) {
		t.Fatalf("DownloadFile: %d bytes, %v; want %d", len(got), err, len(lfsData))
	}

	resp, body := repo.get(t, http.MethodGet, repo.resolveURL(textPath), hubSecret)
	if resp.StatusCode != http.StatusOK || string(body) != textData {
		t.Fatalf("GET regular file: %d %q", resp.StatusCode, body)
	}

	// The hub token authenticates hub requests only; the CAS API sees the tokens the hub minted, and xorb data fetches stay anonymous like presigned URLs.
	for _, req := range repo.hub.Requests() {
		if req.Authorization != "Bearer <redacted>" {
			t.Fatalf("hub request %s %s carried %q, want the hub token", req.Method, req.Path, req.Authorization)
		}
	}
	casReqs := repo.cas.requests()
	if len(casReqs) == 0 {
		t.Fatal("the CAS saw no request")
	}
	for _, req := range casReqs {
		if req.Authorization == "Bearer "+hubSecret {
			t.Fatalf("CAS request %s %s carried the hub token", req.Method, req.Path)
		}
		dataFetch := req.Method == http.MethodGet && req.Route() == "GET xorbs"
		if req.Authorization == "" && !dataFetch {
			t.Fatalf("CAS request %s %s carried no token", req.Method, req.Path)
		}
		if token, ok := strings.CutPrefix(req.Authorization, "Bearer "); ok {
			if _, valid := repo.opts.Issuer.Validate(token); !valid {
				t.Fatalf("CAS request %s %s carried a token the hub did not mint", req.Method, req.Path)
			}
		}
	}
}

// NewClient binds the CAS operations to the repository whatever provider opts name: neither Commit nor a download afterwards consults the user's provider.
func TestNewClientOverridesUserProvider(t *testing.T) {
	repo := newPrivateRepo(t)
	ctx := t.Context()
	lfsData := deterministic(1 << 20)
	wrong := &spyProvider{inner: NewTokenProvider(repo.hc, repo.target, wrongToken)}
	c, err := NewClient(repo.target, WithHTTPClient(repo.hc), WithToken(hubSecret), WithClientOptions(client.WithCache(client.NewCache(t.TempDir(), 0, 0)), client.WithUpstreamProvider(wrong)))
	if err != nil {
		t.Fatal(err)
	}

	commit, err := c.Commit(ctx, "test: user provider", files(lfsData)...)
	if err != nil {
		t.Fatal(err)
	}
	if commit.OID == "" || commit.OID != repo.hub.Head() {
		t.Fatalf("commit = %+v, hub head %s", commit, repo.hub.Head())
	}
	if n := wrong.calls.Load(); n != 0 {
		t.Fatalf("Commit resolved through the user's provider %d times", n)
	}
	reqs := repo.hub.Requests()
	if got := routes(reqs); !slices.Equal(got, []string{"POST preupload", "GET xet-write-token", "POST commit"}) {
		t.Fatalf("hub routes = %q", got)
	}
	for _, req := range reqs {
		if req.Authorization != "Bearer <redacted>" {
			t.Fatalf("hub request %s %s carried %q, want the commit's token", req.Method, req.Path, req.Authorization)
		}
	}

	f, err := c.Resolve(ctx, lfsPath)
	if err != nil {
		t.Fatal(err)
	}
	got, err := download(t, func(w io.WriteSeeker) error { return c.DownloadFile(ctx, f.Hash, w) })
	if err != nil || !bytes.Equal(got, lfsData) {
		t.Fatalf("DownloadFile: %d bytes, %v; want %d", len(got), err, len(lfsData))
	}
	if n := wrong.calls.Load(); n != 0 {
		t.Fatalf("DownloadFile resolved through the user's provider %d times", n)
	}
}

// Two clients bound to the repository sharing one Cache download a resolved file, the second from the shared chunks alone.
func TestBoundClientsShareCache(t *testing.T) {
	repo := newPrivateRepo(t)
	ctx := t.Context()
	lfsData := deterministic(1 << 20)
	if _, err := repo.committer(t).Commit(ctx, "test: shared cache", files(lfsData)...); err != nil {
		t.Fatal(err)
	}

	shared := client.NewCache(t.TempDir(), 0, 0)
	clients := make([]*Client, 2)
	for i := range clients {
		c, err := NewClient(repo.target, WithHTTPClient(repo.hc), WithToken(hubSecret), WithClientOptions(client.WithCache(shared)))
		if err != nil {
			t.Fatal(err)
		}
		clients[i] = c
	}
	f, err := clients[0].Resolve(ctx, lfsPath)
	if err != nil {
		t.Fatal(err)
	}
	fetch := func(c *Client) int {
		t.Helper()
		got, err := download(t, func(w io.WriteSeeker) error { return c.DownloadFile(ctx, f.Hash, w) })
		if err != nil || !bytes.Equal(got, lfsData) {
			t.Fatalf("DownloadFile: %d bytes, %v; want %d", len(got), err, len(lfsData))
		}
		return repo.cas.xorbFetches()
	}
	uploaded := repo.cas.xorbFetches()
	n1 := fetch(clients[0])
	if n2 := fetch(clients[1]); n1 == uploaded || n2 != n1 {
		t.Fatalf("xorb requests = %d after the first client, %d after the second (%d from the upload); want the second served from the shared cache", n1, n2, uploaded)
	}
	if u, err := shared.Usage(ctx); err != nil || u.Download.Count == 0 {
		t.Fatalf("shared usage = %+v, %v; want entries", u, err)
	}
}

// The preupload, write-token and commit endpoints derive from the bound repository, its revision escaped.
func TestCommitEscapedRevision(t *testing.T) {
	repo := newPrivateRepo(t)
	opts := repo.opts
	opts.Revision = "refs/pr/1"
	hub := hftest.NewHub(t, opts)
	target := Repo{Endpoint: hub.URL, RepoID: repoID, Revision: opts.Revision}
	if got, want := target.CommitURL(), hub.URL+"/api/models/"+repoID+"/commit/refs%2Fpr%2F1"; got != want {
		t.Fatalf("CommitURL = %q, want %q", got, want)
	}

	c := repo.bound(t, target, hubSecret)
	if _, err := c.Commit(t.Context(), "test: escaped revision", files(deterministic(1<<16))...); err != nil {
		t.Fatal(err)
	}
	var paths []string
	for _, req := range hub.Requests() {
		path, _, _ := strings.Cut(req.Path, "?")
		paths = append(paths, req.Method+" "+path)
	}
	base := "/api/models/" + repoID
	if want := []string{"POST " + base + "/preupload/refs%2Fpr%2F1", "GET " + base + "/xet-write-token/refs%2Fpr%2F1", "POST " + base + "/commit/refs%2Fpr%2F1"}; !slices.Equal(paths, want) {
		t.Fatalf("hub paths = %q, want %q", paths, want)
	}
	if n := len(hub.Commits()); n != 1 {
		t.Fatalf("hub commits = %d, want one", n)
	}
}

// The Go client's hub traffic has the reference's request routes and JSON shapes once the CLI's repository-creation conveniences are removed.
func TestCommitWireShape(t *testing.T) {
	repo := newPrivateRepo(t)
	lfsData := deterministic(1 << 20)
	if _, err := repo.committer(t).Commit(t.Context(), "test: wire shape", files(lfsData)...); err != nil {
		t.Fatal(err)
	}
	ref := hftest.Fixture(t, "ref-upload")

	var want []string
	for _, ex := range ref.Hub() {
		if route := hftest.Route(ex); route != "GET agent-harnesses" && route != "POST repos/create" {
			want = append(want, route)
		}
	}
	reqs := repo.hub.Requests()
	if got := routes(reqs); !slices.Equal(got, want) {
		t.Fatalf("hub routes = %q, want the reference's %q", got, want)
	}

	pre, refPre := requestOf(t, reqs, "POST preupload"), exchangeOf(t, ref, "POST preupload")
	if got, want := hftest.JSONKeys(string(pre.Body)), hftest.JSONKeys(refPre.RequestBody.Text); !slices.Equal(got, want) || !slices.Equal(want, []string{"files"}) {
		t.Fatalf("preupload keys = %q, want %q", got, want)
	}
	if got, want := preuploadEntryKeys(t, pre.Body), preuploadEntryKeys(t, []byte(refPre.RequestBody.Text)); !slices.Equal(got, want) {
		t.Fatalf("preupload files[0] keys = %q, want %q", got, want)
	}
	if ct := pre.Header.Get("Content-Type"); ct != refPre.RequestHeaders.Get("Content-Type") {
		t.Fatalf("preupload Content-Type = %q, want %q", ct, refPre.RequestHeaders.Get("Content-Type"))
	}

	commit, refCommit := requestOf(t, reqs, "POST commit"), exchangeOf(t, ref, "POST commit")
	if ct := commit.Header.Get("Content-Type"); ct != refCommit.RequestHeaders.Get("Content-Type") {
		t.Fatalf("commit Content-Type = %q, want %q", ct, refCommit.RequestHeaders.Get("Content-Type"))
	}
	if !bytes.HasSuffix(commit.Body, []byte("\n")) {
		t.Fatal("commit payload lacks the trailing newline")
	}
	lines, refLines := ndjsonLines(t, string(commit.Body)), ndjsonLines(t, refCommit.RequestBody.Text)
	if len(lines) != len(refLines) {
		t.Fatalf("commit has %d lines, the reference %d", len(lines), len(refLines))
	}
	for i := range lines {
		if lines[i].Key != refLines[i].Key {
			t.Fatalf("commit line %d key = %q, want %q", i, lines[i].Key, refLines[i].Key)
		}
		if got, want := hftest.JSONKeys(string(lines[i].Value)), hftest.JSONKeys(string(refLines[i].Value)); !slices.Equal(got, want) {
			t.Fatalf("commit %s value keys = %q, want %q", lines[i].Key, got, want)
		}
	}
	var header struct{ Summary, Description string }
	var lfs struct {
		Algo, OID, Path string
		Size            int64
	}
	var regular struct{ Content, Encoding, Path string }
	for _, pair := range []struct {
		key string
		out any
	}{{"header", &header}, {"lfsFile", &lfs}, {"file", &regular}} {
		i := slices.IndexFunc(lines, func(l ndjsonLine) bool { return l.Key == pair.key })
		if err := json.Unmarshal(lines[i].Value, pair.out); err != nil {
			t.Fatal(err)
		}
	}
	if header.Summary != "test: wire shape" || header.Description != "" {
		t.Fatalf("header = %+v", header)
	}
	if lfs.Algo != "sha256" || lfs.OID != sha256Hex(lfsData) || lfs.Path != lfsPath || lfs.Size != int64(len(lfsData)) {
		t.Fatalf("lfsFile = %+v", lfs)
	}
	if content, err := base64.StdEncoding.DecodeString(regular.Content); err != nil || string(content) != textData || regular.Encoding != "base64" || regular.Path != textPath {
		t.Fatalf("file = %+v (%v)", regular, err)
	}

	// The reference's shard upload went to /v2/shards; so does the client's, after the xorb.
	cas := routes(repo.cas.requests())
	shards, xorbs := slices.Index(cas, "POST shards"), slices.Index(cas, "POST xorbs")
	if shards < 0 || xorbs < 0 || xorbs > shards || requestOf(t, repo.cas.requests(), "POST shards").Path != "/v2/shards" {
		t.Fatalf("CAS routes = %q, want a xorb POST followed by POST /v2/shards", cas)
	}
	if refCAS := routes(exchangesToRequests(ref.CAS())); !slices.Contains(refCAS, "POST shards") || !slices.Contains(refCAS, "POST xorbs") {
		t.Fatalf("reference CAS routes = %q", refCAS)
	}
}

func exchangesToRequests(exs []hftest.Exchange) []hftest.Request {
	out := make([]hftest.Request, 0, len(exs))
	for _, ex := range exs {
		out = append(out, hftest.Request{Method: ex.Method, Path: ex.Path})
	}
	return out
}

func preuploadEntryKeys(t *testing.T, body []byte) []string {
	t.Helper()
	var req struct {
		Files []json.RawMessage `json:"files"`
	}
	if err := json.Unmarshal(body, &req); err != nil || len(req.Files) == 0 {
		t.Fatalf("preupload body %q: %v", body, err)
	}
	return hftest.JSONKeys(string(req.Files[0]))
}

type ndjsonLine struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

func ndjsonLines(t *testing.T, text string) []ndjsonLine {
	t.Helper()
	var lines []ndjsonLine
	for line := range strings.SplitSeq(strings.TrimSpace(text), "\n") {
		var l ndjsonLine
		if err := json.Unmarshal([]byte(line), &l); err != nil {
			t.Fatalf("line %q: %v", line, err)
		}
		lines = append(lines, l)
	}
	return lines
}

// Committing content the revision already holds, lfs or regular, asks the hub once and stops, like the recorded re-upload never reached a commit; a changed regular file alone commits alone.
func TestCommitUnchanged(t *testing.T) {
	repo := newPrivateRepo(t)
	ctx := t.Context()
	lfsData := deterministic(1 << 20)
	c := repo.committer(t)
	if _, err := c.Commit(ctx, "test: first", files(lfsData)...); err != nil {
		t.Fatal(err)
	}
	before := len(repo.hub.Requests())

	commit, err := c.Commit(ctx, "test: again", files(lfsData)...)
	if !errors.Is(err, ErrNoChanges) || commit != nil {
		t.Fatalf("Commit of unchanged content = %v, %v; want ErrNoChanges", commit, err)
	}
	if got := routes(repo.hub.Requests()[before:]); !slices.Equal(got, []string{"POST preupload"}) {
		t.Fatalf("hub routes = %q, want one preupload and no commit", got)
	}
	if n := len(repo.hub.Commits()); n != 1 {
		t.Fatalf("hub commits = %d, want the first only", n)
	}

	before = len(repo.hub.Requests())
	changed := []CommitFile{{Path: lfsPath, Content: bytes.NewReader(lfsData)}, {Path: textPath, Content: strings.NewReader(textData + "changed\n")}}
	if _, err := c.Commit(ctx, "test: text", changed...); err != nil {
		t.Fatal(err)
	}
	reqs := repo.hub.Requests()[before:]
	if got := routes(reqs); !slices.Equal(got, []string{"POST preupload", "POST commit"}) {
		t.Fatalf("hub routes = %q, want a preupload and a commit without a CAS token", got)
	}
	lines := ndjsonLines(t, string(requestOf(t, reqs, "POST commit").Body))
	if len(lines) != 2 || lines[1].Key != "file" {
		t.Fatalf("commit lines = %d starting %q, want a header and the one file line", len(lines), lines[0].Key)
	}

	ref := hftest.Fixture(t, "ref-reupload")
	refRoutes := routes(exchangesToRequests(ref.Hub()))
	if slices.Contains(refRoutes, "POST commit") || !slices.Contains(refRoutes, "POST preupload") {
		t.Fatalf("reference re-upload routes = %q", refRoutes)
	}
	var pre struct {
		Files []struct {
			OID string `json:"oid"`
		} `json:"files"`
	}
	if err := json.Unmarshal([]byte(exchangeOf(t, ref, "POST preupload").ResponseBody.Text), &pre); err != nil || len(pre.Files) != 1 || len(pre.Files[0].OID) != 64 {
		t.Fatalf("reference preupload = %+v (%v), want the known file's oid", pre, err)
	}
}

// Without the hub token, or with a wrong one, every hub entry point answers 401 once and the client stops there.
func TestPrivateRepoRejectsBadCredentials(t *testing.T) {
	for _, tc := range []struct{ name, token, want string }{
		{"anonymous", "", ""},
		{"wrong token", wrongToken, "Bearer <wrong>"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := newPrivateRepo(t)
			ctx := t.Context()
			c := repo.bound(t, repo.target, tc.token)

			_, err := c.Commit(ctx, "test: denied", files(deterministic(4096))...)
			if err == nil || !strings.Contains(err.Error(), "401") || !strings.Contains(err.Error(), "Invalid username or password.") {
				t.Fatalf("Commit = %v, want the hub's 401", err)
			}
			reqs := repo.hub.Requests()
			if got := routes(reqs); !slices.Equal(got, []string{"POST preupload"}) || reqs[0].Authorization != tc.want {
				t.Fatalf("hub saw %q with %q, want one preupload with %q", got, reqs[0].Authorization, tc.want)
			}
			if len(repo.cas.requests()) != 0 {
				t.Fatal("the CAS saw a request without a hub credential")
			}

			if _, err := c.Resolve(ctx, lfsPath); err == nil || !strings.Contains(err.Error(), "401") {
				t.Fatalf("Resolve = %v, want 401", err)
			}
			if got := routes(repo.hub.Requests()[1:]); !slices.Equal(got, []string{"HEAD resolve"}) {
				t.Fatalf("resolve hub routes = %q, want one HEAD", got)
			}

			if _, err := download(t, func(w io.WriteSeeker) error { return c.DownloadFile(ctx, xet.FileHash{}, w) }); err == nil || !strings.Contains(err.Error(), "401") {
				t.Fatalf("DownloadFile = %v, want 401", err)
			}
			if got := routes(repo.hub.Requests()[2:]); !slices.Equal(got, []string{"GET xet-read-token"}) {
				t.Fatalf("download hub routes = %q, want one token request and no retry", got)
			}
		})
	}
}

// A commit whose lfs object the hub cannot find in its CAS is rejected with the recorded 400 and creates nothing.
func TestCommitDanglingLFS(t *testing.T) {
	repo := newPrivateRepo(t)
	empty, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	opts := repo.opts
	opts.Storage = empty
	hub := hftest.NewHub(t, opts)
	c := repo.bound(t, Repo{Endpoint: hub.URL, RepoID: repoID}, hubSecret)

	_, err = c.Commit(t.Context(), "test: dangling", CommitFile{Path: lfsPath, Content: bytes.NewReader(deterministic(1 << 20))})
	if err == nil || !strings.Contains(err.Error(), "400") || !strings.Contains(err.Error(), "LFS pointer pointed to a file that does not exist") || !strings.Contains(err.Error(), lfsPath) {
		t.Fatalf("Commit = %v, want the hub's dangling lfs rejection", err)
	}
	if got := routes(hub.Requests()); !slices.Equal(got, []string{"POST preupload", "GET xet-write-token", "POST commit"}) {
		t.Fatalf("hub routes = %q", got)
	}
	if len(hub.Commits()) != 0 {
		t.Fatalf("hub commits = %v, want none", hub.Commits())
	}
}

// lfsByExtension picks the upload mode by path alone, so one reader can be committed as an lfs file and as a regular one.
func lfsByExtension(path string, _ []byte, _ int64) string {
	if strings.HasSuffix(path, ".bin") {
		return "lfs"
	}
	return "regular"
}

// Files sharing one reader, handed over mid-way, are committed in full: every pass over a file starts from its beginning.
func TestCommitSharedReader(t *testing.T) {
	data := deterministic(1 << 20)
	for _, tc := range []struct {
		name  string
		paths []string
	}{
		{"two regular files", []string{"regression/a.txt", "regression/b.txt"}},
		{"lfs then regular", []string{"regression/shared.bin", "regression/shared.txt"}},
		{"regular then lfs", []string{"regression/shared.txt", "regression/shared.bin"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			repo := newPrivateRepo(t)
			opts := repo.opts
			opts.UploadMode = lfsByExtension
			hub := hftest.NewHub(t, opts)
			c := repo.bound(t, Repo{Endpoint: hub.URL, RepoID: repoID}, hubSecret)

			shared := bytes.NewReader(data)
			if _, err := shared.Seek(int64(len(data)/2), io.SeekStart); err != nil {
				t.Fatal(err)
			}
			files := make([]CommitFile, 0, len(tc.paths))
			for _, path := range tc.paths {
				files = append(files, CommitFile{Path: path, Content: shared})
			}
			if _, err := c.Commit(ctx, "test: shared reader", files...); err != nil {
				t.Fatal(err)
			}

			for _, path := range tc.paths {
				url := hub.URL + "/" + repoID + "/resolve/main/" + path
				var got []byte
				if lfsByExtension(path, nil, 0) == "lfs" {
					f, err := c.Resolve(ctx, path)
					if err != nil {
						t.Fatalf("resolve %s: %v", path, err)
					}
					if got, err = download(t, func(w io.WriteSeeker) error { return c.DownloadFile(ctx, f.Hash, w) }); err != nil {
						t.Fatalf("download %s: %v", path, err)
					}
				} else {
					resp, body := repo.get(t, http.MethodGet, url, hubSecret)
					if resp.StatusCode != http.StatusOK {
						t.Fatalf("GET %s: %d", path, resp.StatusCode)
					}
					got = body
				}
				if !bytes.Equal(got, data) {
					t.Errorf("%s holds %d bytes, want the full %d", path, len(got), len(data))
				}
			}
		})
	}
}

// Paths are validated before the hub is asked, and preupload samples are the first 512 bytes like the reference's.
func TestCommitPathsAndSamples(t *testing.T) {
	repo := newPrivateRepo(t)
	ctx := t.Context()
	c := repo.committer(t)

	if _, err := c.Commit(ctx, "test: empty"); err == nil {
		t.Fatal("Commit without files succeeded")
	}
	for _, path := range []string{"", "/abs.bin", "a/../b", "./a", "a//b", "a/", ".", ".."} {
		if _, err := c.Commit(ctx, "test: path", CommitFile{Path: path, Content: strings.NewReader("x")}); err == nil || !strings.Contains(err.Error(), "invalid path") {
			t.Fatalf("Commit(%q) = %v, want invalid path", path, err)
		}
	}
	if n := len(repo.hub.Requests()); n != 0 {
		t.Fatalf("hub saw %d requests before validation passed", n)
	}

	ref := exchangeOf(t, hftest.Fixture(t, "ref-upload"), "POST preupload")
	refFiles := preuploadSamples(t, []byte(ref.RequestBody.Text))
	if len(refFiles) != 2 || len(refFiles[0].Sample) != sampleLen || refFiles[0].Size <= sampleLen || len(refFiles[1].Sample) != int(refFiles[1].Size) {
		t.Fatalf("reference samples: %d files, %d and %d bytes for sizes %d and %d", len(refFiles), len(refFiles[0].Sample), len(refFiles[1].Sample), refFiles[0].Size, refFiles[1].Size)
	}

	long, short := deterministic(600), []byte(textData)
	if _, err := c.Commit(ctx, "test: samples", CommitFile{Path: "a/long.bin", Content: bytes.NewReader(long)}, CommitFile{Path: "a/short.txt", Content: bytes.NewReader(short)}); err != nil {
		t.Fatal(err)
	}
	got := preuploadSamples(t, requestOf(t, repo.hub.Requests(), "POST preupload").Body)
	if len(got) != 2 || got[0].Path != "a/long.bin" || got[0].Size != 600 || !bytes.Equal(got[0].Sample, long[:sampleLen]) || got[1].Path != "a/short.txt" || got[1].Size != int64(len(short)) || !bytes.Equal(got[1].Sample, short) {
		t.Fatalf("preupload files = %+v", got)
	}
}

type preuploadSample struct {
	Path   string `json:"path"`
	Sample []byte `json:"sample"`
	Size   int64  `json:"size"`
}

func preuploadSamples(t *testing.T, body []byte) []preuploadSample {
	t.Helper()
	var req struct {
		Files []preuploadSample `json:"files"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		t.Fatalf("preupload body %q: %v", body, err)
	}
	return req.Files
}
