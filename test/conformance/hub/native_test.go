package hub_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/auth"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/client/hftest"
	"github.com/wzshiming/xet/mirror"
	"github.com/wzshiming/xet/server"
	hfserver "github.com/wzshiming/xet/server/hf"
	"github.com/wzshiming/xet/storage/local"
	"github.com/wzshiming/xet/test/conformance/hubtrace"
)

const (
	nativeLFSName     = "go-lfs.bin"
	nativeRegularName = "go-regular.txt"
	// go-lfs.bin is PCG(lfsSeed, nativeLFSStream) output salted like ref-lfs.bin, so the two never share content.
	nativeLFSStream = 2
	bearer          = "Bearer <redacted>"
)

// native holds what the Go client scenarios share: the token, the fixtures, the traced tool's name and, when on PATH, the hf CLI that cross-checks them.
type native struct {
	token, tool, outDir, repo, upstream string
	lfs, regular                        []byte
	lfsSHA256                           string
	ref                                 *reference // nil without the hf CLI
	refFx                               *fixtures  // what TestReferencePrivateRepo committed, up to its timestamp line
	uploaded                            bool
	refHash                             xet.FileHash // ref-lfs.bin as resolved by go-download
}

// TestNativePrivateRepo runs the Go client, and a mirror built on it, against the private repository TestReferencePrivateRepo populated, recording every hub and CAS exchange the way the reference scenarios are recorded.
func TestNativePrivateRepo(t *testing.T) {
	if *tokenFile == "" {
		t.Skip("no -hf.token-file")
	}
	if testing.Short() {
		t.Skip("network test; skipped in -short mode")
	}
	n := &native{
		token:    readToken(t, *tokenFile),
		tool:     "github.com/wzshiming/xet/client (" + runtime.Version() + ")",
		outDir:   traceDir(t),
		repo:     *repoID,
		upstream: strings.TrimRight(*upstream, "/"),
		lfs:      nativeLFSContent(*salt),
		refFx:    newFixtures(t, *salt, time.Now()),
	}
	sum := sha256.Sum256(n.lfs)
	n.lfsSHA256 = hex.EncodeToString(sum[:])
	n.regular = []byte("xet private-repo regression fixture — go-regular.txt\n" +
		"Committed next to go-lfs.bin by the Go client (github.com/wzshiming/xet/client) from test/conformance/hub; the line below makes every commit distinct.\n" +
		"recorded: " + time.Now().UTC().Format(time.RFC3339) + "\n")
	if hfBin, err := exec.LookPath("hf"); err == nil {
		n.ref = &reference{hfBin: hfBin, token: n.token, outDir: n.outDir, repo: n.repo, upstream: n.upstream}
		n.ref.tool, n.ref.hasXet = toolInfo(t, n.ref)
		t.Logf("tool: %s", n.ref.tool)
	}

	for _, step := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"go-upload", n.upload},
		{"go-upload-verify", n.uploadVerify},
		{"go-download", n.download},
		{"go-anonymous", n.anonymous},
		{"go-mirror", n.mirror},
		{"go-mirror-anonymous", n.mirrorAnonymous},
	} {
		t.Run(step.name, step.run)
	}
}

// upload commits go-lfs.bin and go-regular.txt with the Go client's Commit and checks the hub and CAS exchanges it took.
func (n *native) upload(t *testing.T) {
	rec := n.record(t, "go-upload", n.tool)
	ctx, cancel := context.WithTimeout(t.Context(), hfTimeout)
	defer cancel()
	hc := &http.Client{}
	target := client.HubRepo{Endpoint: rec.HubURL(), RepoID: n.repo}
	c := newClient(t, hc, nil)
	commit, err := c.Commit(ctx, target.CommitURL(), n.token, "test: go client private repo regression",
		client.CommitFile{Path: repoDir + "/" + nativeLFSName, Content: bytes.NewReader(n.lfs)},
		client.CommitFile{Path: repoDir + "/" + nativeRegularName, Content: bytes.NewReader(n.regular)},
	)
	if err != nil {
		t.Fatalf("commit: %s", hubtrace.Redact(err.Error()))
	}
	if commit.OID == "" {
		t.Fatal("commit returned an empty oid")
	}
	n.uploaded = true
	t.Logf("committed %s", hubtrace.Redact(commit.OID+" ("+commit.URL+")"))

	tr := rec.trace()
	hub, cas := tr.Hub(), tr.CAS()
	fresh := !lfsKnownToHub(hub, n.lfsSHA256)
	want := []string{"POST preupload", "POST commit"}
	if fresh {
		want = []string{"POST preupload", "GET xet-write-token", "POST commit"}
	} else {
		t.Logf("%s already on the hub at this salt; the client commits without a CAS upload", nativeLFSName)
	}
	if got := routes(hub); !slices.Equal(got, want) {
		t.Errorf("hub routes = %v, want %v", describe(hub), want)
	}
	checkAuthorized(t, hub)
	checkStatuses(t, hub, http.StatusOK)
	switch {
	case fresh:
		for _, w := range []struct{ method, prefix string }{{http.MethodPost, "/v1/xorbs/"}, {http.MethodPost, "/v2/shards"}} {
			if !hasExchange(cas, w.method, w.prefix, http.StatusOK) {
				t.Errorf("CAS exchanges %v lack a 200 %s %s", describe(cas), w.method, w.prefix)
			}
		}
	case len(cas) != 0:
		t.Errorf("CAS exchanges = %v, want none for an unchanged %s", describe(cas), nativeLFSName)
	}
}

// uploadVerify downloads what go-upload committed with the official CLI and compares the bytes.
func (n *native) uploadVerify(t *testing.T) {
	if n.ref == nil {
		t.Skip(`hf CLI not found on PATH; install with: pip install -U "huggingface_hub[cli,hf_xet]"`)
	}
	if !n.uploaded {
		t.Skip("go-upload committed nothing to verify")
	}
	rec := n.record(t, "go-upload-verify", n.ref.tool)
	localDir := t.TempDir()
	n.ref.mustHF(t, rec.HubURL(), n.token, !n.ref.hasXet, "download", n.repo, repoDir+"/"+nativeLFSName, repoDir+"/"+nativeRegularName, "--local-dir", localDir)
	if got := sha256File(t, filepath.Join(localDir, repoDir, nativeLFSName)); got != n.lfsSHA256 {
		t.Errorf("%s sha256 = %s, want %s", nativeLFSName, got, n.lfsSHA256)
	}
	got, err := os.ReadFile(filepath.Join(localDir, repoDir, nativeRegularName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, n.regular) {
		t.Errorf("%s = %s, want %s", nativeRegularName, digest(got), digest(n.regular))
	}
}

// download fetches the reference fixtures with the Go client: ref-lfs.bin through Resolve and through a bound token provider, ref-regular.txt over plain HTTP.
func (n *native) download(t *testing.T) {
	rec := n.record(t, "go-download", n.tool)
	ctx, cancel := context.WithTimeout(t.Context(), hfTimeout)
	defer cancel()
	hc := &http.Client{}
	c := newClient(t, hc, nil)
	resolved, err := c.Resolve(ctx, rec.HubURL()+n.resolvePath(lfsName), n.token)
	if err != nil {
		t.Fatalf("resolve %s: %s", lfsName, hubtrace.Redact(err.Error()))
	}
	n.refHash = resolved.Hash
	t.Logf("%s resolves to %s", lfsName, resolved.Hash)
	got, err := downloadInto(t, func(w io.WriteSeeker) error { return c.DownloadResolved(ctx, resolved, w) })
	if err != nil {
		t.Errorf("DownloadResolved: %s", hubtrace.Redact(err.Error()))
	} else if got != n.refFx.lfsSHA256 {
		t.Errorf("DownloadResolved sha256 = %s, want %s", got, n.refFx.lfsSHA256)
	}

	target := client.HubRepo{Endpoint: rec.HubURL(), RepoID: n.repo}
	bound := newClient(t, hc, client.NewHubTokenProvider(hc, target, n.token))
	got, err = downloadInto(t, func(w io.WriteSeeker) error { return bound.DownloadFile(ctx, resolved.Hash, w) })
	if err != nil {
		t.Errorf("DownloadFile: %s", hubtrace.Redact(err.Error()))
	} else if got != n.refFx.lfsSHA256 {
		t.Errorf("DownloadFile sha256 = %s, want %s", got, n.refFx.lfsSHA256)
	}

	status, body := get(t, rec.HubURL()+n.resolvePath(regularName), n.token)
	first, _, _ := bytes.Cut(body, []byte("\n"))
	wantFirst, _, _ := bytes.Cut(n.refFx.regular, []byte("\n"))
	if status != http.StatusOK || !bytes.Equal(first, wantFirst) {
		t.Errorf("GET %s -> %d, first line %s; want 200 and %s", regularName, status, digest(first), digest(wantFirst))
	}

	tr := rec.trace()
	hub := tr.Hub()
	if len(hub) < 2 || hftest.Route(hub[0]) != "HEAD resolve" || hub[0].Status != http.StatusFound ||
		hftest.Route(hub[1]) != "GET xet-read-token" || hub[1].Status != http.StatusOK {
		t.Errorf("hub exchanges = %v, want HEAD resolve (302), GET xet-read-token (200) first", describe(hub))
	}
	checkAuthorized(t, hub)
	checkStatuses(t, hub, http.StatusOK, http.StatusFound)
	if cas := tr.CAS(); !hasExchange(cas, http.MethodGet, "/v2/reconstructions/", http.StatusOK) {
		t.Errorf("CAS exchanges %v lack a 200 GET /v2/reconstructions/", describe(cas))
	}
}

// anonymous checks that every Go entry point fails on the hub's 401 with a single request, without or with a wrong token.
func (n *native) anonymous(t *testing.T) {
	rec := n.record(t, "go-anonymous", n.tool)
	ctx, cancel := context.WithTimeout(t.Context(), hfTimeout)
	defer cancel()
	hc := &http.Client{}
	c := newClient(t, hc, nil)
	for _, cred := range []struct{ name, token string }{{"anonymous", ""}, {"wrong-token", wrongToken}} {
		_, err := c.Resolve(ctx, rec.HubURL()+n.resolvePath(lfsName), cred.token)
		want401(t, cred.name+" Resolve", err)
	}
	target := client.HubRepo{Endpoint: rec.HubURL(), RepoID: n.repo}
	wrong := newClient(t, hc, client.NewHubTokenProvider(hc, target, wrongToken))
	_, err := downloadInto(t, func(w io.WriteSeeker) error { return wrong.DownloadFile(ctx, n.fileHash(), w) })
	want401(t, "wrong-token DownloadFile", err)
	_, err = c.Commit(ctx, target.CommitURL(), "", "test: anonymous commit",
		client.CommitFile{Path: repoDir + "/" + nativeRegularName, Content: bytes.NewReader(n.regular)})
	want401(t, "anonymous Commit", err)

	tr := rec.trace()
	hub := tr.Hub()
	got := map[string]int{}
	for _, ex := range hub {
		got[hftest.Route(ex)]++
		if ex.Status != http.StatusUnauthorized {
			t.Errorf("%s %s -> %d, want 401", ex.Method, ex.Path, ex.Status)
		}
	}
	if want := map[string]int{"HEAD resolve": 2, "GET xet-read-token": 1, "POST preupload": 1}; !maps.Equal(got, want) {
		t.Errorf("hub exchanges = %v, want exactly %v", describe(hub), want)
	}
	if cas := tr.CAS(); len(cas) != 0 {
		t.Errorf("CAS exchanges = %v, want none", describe(cas))
	}
}

// mirror puts a mirror holding the token in front of the recorder and downloads ref-lfs.bin through it with the anonymous CLI: cold while ingesting, then over xet and plain HTTP once ready.
func (n *native) mirror(t *testing.T) {
	if n.ref == nil {
		t.Skip(`hf CLI not found on PATH; install with: pip install -U "huggingface_hub[cli,hf_xet]"`)
	}
	rec := n.record(t, "go-mirror", n.mirrorTool("downstream: "+n.ref.tool))
	mirrorURL := startMirror(t, rec.HubURL(), n.token)
	n.downloadViaMirror(t, mirrorURL, "cold", !n.ref.hasXet)
	waitReady(t, mirrorURL+n.resolvePath(lfsName))
	if n.ref.hasXet {
		n.downloadViaMirror(t, mirrorURL, "xet warm", false)
	}
	n.downloadViaMirror(t, mirrorURL, "plain warm", true)

	tr := rec.trace()
	hub := tr.Hub()
	checkAuthorized(t, hub)
	// Control-plane requests the CLI sends through the proxy may 404 upstream; the mirror answers only for its ingest exchanges.
	var ingest []hftest.Exchange
	for _, ex := range hub {
		if route := hftest.Route(ex); route == "HEAD resolve" || route == "GET xet-read-token" {
			ingest = append(ingest, ex)
		}
	}
	checkStatuses(t, ingest, http.StatusOK, http.StatusFound)
	if !hasExchange(ingest, http.MethodHead, "/", http.StatusFound) || !hasExchange(ingest, http.MethodGet, "/api/", http.StatusOK) {
		t.Errorf("upstream exchanges = %v, want a 302 HEAD resolve and a 200 GET xet-read-token from the mirror", describe(hub))
	}
	if cas := tr.CAS(); !hasExchange(cas, http.MethodGet, "/v2/reconstructions/", http.StatusOK) {
		t.Errorf("CAS exchanges %v lack a 200 GET /v2/reconstructions/", describe(cas))
	}
}

// downloadViaMirror fetches ref-lfs.bin from the mirror with the CLI and no token, checking the bytes against the reference fixture.
func (n *native) downloadViaMirror(t *testing.T, mirrorURL, phase string, disableXet bool) {
	t.Helper()
	localDir := t.TempDir()
	n.ref.mustHF(t, mirrorURL, "", disableXet, "download", n.repo, repoDir+"/"+lfsName, "--local-dir", localDir)
	if got := sha256File(t, filepath.Join(localDir, repoDir, lfsName)); got != n.refFx.lfsSHA256 {
		t.Errorf("%s download: %s sha256 = %s, want %s", phase, lfsName, got, n.refFx.lfsSHA256)
	}
}

// mirrorAnonymous fronts the recorder with a mirror holding no token: the downstream gets the upstream's 401 surfaced as a 502 and the mirror gives up on it at once.
func (n *native) mirrorAnonymous(t *testing.T) {
	rec := n.record(t, "go-mirror-anonymous", n.mirrorTool("downstream: net/http"))
	mirrorURL := startMirror(t, rec.HubURL(), "")
	status, body := get(t, mirrorURL+n.resolvePath(lfsName), "")
	t.Logf("anonymous mirror GET %s -> %d: %s", lfsName, status, hubtrace.Redact(strings.TrimSpace(string(body[:min(len(body), 200)]))))
	if status != http.StatusBadGateway || !bytes.Contains(body, []byte("upstream status 401")) {
		t.Errorf("anonymous mirror GET %s -> %d %q, want 502 naming upstream status 401", lfsName, status, hubtrace.Redact(string(body)))
	}

	hub := rec.trace().Hub()
	unauthorized := 0
	for _, ex := range hub {
		if ex.Status == http.StatusUnauthorized {
			unauthorized++
		}
	}
	if unauthorized == 0 {
		t.Errorf("upstream exchanges = %v, want at least one 401", describe(hub))
	}
	if len(hub) > 3 {
		t.Errorf("mirror made %d upstream requests for one downstream request, want at most 3: %v", len(hub), describe(hub))
	}
}

// mirrorTool names the traced client of a mirror scenario: the mirror itself, and what pulled through it.
func (n *native) mirrorTool(downstream string) string {
	return "github.com/wzshiming/xet mirror (" + runtime.Version() + "); " + downstream
}

func (n *native) resolvePath(file string) string {
	return "/" + n.repo + "/resolve/main/" + repoDir + "/" + file
}

// fileHash is ref-lfs.bin's hash from go-download, or a stand-in when that step did not run: the credential check comes before any use of it.
func (n *native) fileHash() xet.FileHash {
	if n.refHash != (xet.FileHash{}) {
		return n.refHash
	}
	return xet.FileHash(sha256.Sum256([]byte("xet private-repo regression: unresolved file")))
}

// recording is one scenario's recorder with the labels its trace is saved under.
type recording struct {
	*hubtrace.Recorder
	scenario, repo, tool string
}

// trace snapshots the exchanges recorded so far.
func (r *recording) trace() *hftest.Trace {
	return r.Trace(r.scenario, r.repo, r.tool)
}

// record starts a recorder for scenario and, when the subtest ends, logs its exchanges and saves its trace as tool's, scoped to the regression folder.
func (n *native) record(t *testing.T, scenario, tool string) *recording {
	t.Helper()
	rec, err := hubtrace.Start(n.upstream)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rec.Close)
	r := &recording{Recorder: rec, scenario: scenario, repo: n.repo, tool: tool}
	t.Cleanup(func() {
		tr := r.trace()
		for _, ex := range tr.Exchanges {
			t.Logf("%3d %s %s %s -> %d", ex.Seq, ex.Origin, ex.Method, ex.Path, ex.Status)
		}
		hubtrace.Scope(tr, repoDir+"/")
		path := filepath.Join(n.outDir, scenario+".json")
		if err := tr.Save(path); err != nil {
			t.Errorf("save trace: %v", err)
			return
		}
		t.Logf("trace saved to %s", path)
	})
	return r
}

// newClient returns a Go client sending through hc, bound to provider when one is given.
func newClient(t *testing.T, hc *http.Client, provider client.UpstreamProvider) *client.Client {
	t.Helper()
	c, err := client.NewClient(client.WithHTTPClient(hc), client.WithCacheDir(t.TempDir()), client.WithUpstreamProvider(provider))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// downloadInto runs download against a fresh file and returns the sha256 of what it wrote.
func downloadInto(t *testing.T, download func(io.WriteSeeker) error) (string, error) {
	t.Helper()
	f, err := os.Create(filepath.Join(t.TempDir(), "out"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := download(f); err != nil {
		return "", err
	}
	return sha256File(t, f.Name()), nil
}

// get fetches url with token, never following redirects, and returns the status and body.
func get(t *testing.T, url, token string) (int, []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := probeClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, body
}

// want401 fails t unless err names status 401.
func want401(t *testing.T, op string, err error) {
	t.Helper()
	switch {
	case err == nil:
		t.Errorf("%s succeeded, want a 401 error", op)
	case !strings.Contains(err.Error(), "401"):
		t.Errorf("%s: %s, want an error naming status 401", op, hubtrace.Redact(err.Error()))
	default:
		t.Logf("%s failed as expected: %s", op, hubtrace.Redact(err.Error()))
	}
}

// lfsKnownToHub reports whether the preupload answer in hub already carried go-lfs.bin's digest as its oid, the case in which the client commits without uploading it.
func lfsKnownToHub(hub []hftest.Exchange, digest string) bool {
	for _, ex := range hub {
		if hftest.Route(ex) != "POST preupload" || ex.ResponseBody == nil {
			continue
		}
		var resp struct {
			Files []struct {
				Path string `json:"path"`
				OID  string `json:"oid"`
			} `json:"files"`
		}
		if json.Unmarshal([]byte(ex.ResponseBody.Text), &resp) != nil {
			return false
		}
		for _, f := range resp.Files {
			if f.Path == repoDir+"/"+nativeLFSName && f.OID == digest {
				return true
			}
		}
	}
	return false
}

// routes reduces exchanges to their hftest routes.
func routes(exchanges []hftest.Exchange) []string {
	out := make([]string, 0, len(exchanges))
	for _, ex := range exchanges {
		out = append(out, hftest.Route(ex))
	}
	return out
}

// describe renders exchanges as "route (status)" for failure messages.
func describe(exchanges []hftest.Exchange) []string {
	out := make([]string, 0, len(exchanges))
	for _, ex := range exchanges {
		out = append(out, fmt.Sprintf("%s (%d)", hftest.Route(ex), ex.Status))
	}
	return out
}

// hasExchange reports whether exchanges holds a method request whose path starts with prefix and was answered with status.
func hasExchange(exchanges []hftest.Exchange, method, prefix string, status int) bool {
	return slices.ContainsFunc(exchanges, func(ex hftest.Exchange) bool {
		return ex.Method == method && strings.HasPrefix(ex.Path, prefix) && ex.Status == status
	})
}

// checkAuthorized fails t for every exchange sent without the bearer credential.
func checkAuthorized(t *testing.T, exchanges []hftest.Exchange) {
	t.Helper()
	for _, ex := range exchanges {
		if got := ex.RequestHeaders.Get("Authorization"); got != bearer {
			t.Errorf("%s %s sent Authorization %q, want %q", ex.Method, ex.Path, got, bearer)
		}
	}
}

// checkStatuses fails t for every exchange answered outside statuses.
func checkStatuses(t *testing.T, exchanges []hftest.Exchange, statuses ...int) {
	t.Helper()
	for _, ex := range exchanges {
		if !slices.Contains(statuses, ex.Status) {
			t.Errorf("%s %s -> %d, want one of %v", ex.Method, ex.Path, ex.Status, statuses)
		}
	}
}

// nativeLFSContent is go-lfs.bin: lfsContent on nativeLFSStream.
func nativeLFSContent(salt string) []byte {
	return lfsContent(nativeLFSStream, salt)
}

// startMirror wires the cmd/xetd composition in front of upstream on fresh storage and cache dirs, token being the credential every upstream request carries.
func startMirror(t *testing.T, upstream, token string) string {
	t.Helper()
	var inner atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inner.Load().(http.Handler).ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)

	stor, err := local.NewStorage(local.WithBasePath(t.TempDir()), local.WithBaseURL(srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	issuer, err := auth.NewIssuer(nil, 15*time.Minute, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	upstreamFunc, err := hfserver.StaticUpstream(upstream, token)
	if err != nil {
		t.Fatal(err)
	}
	m, err := mirror.NewMirror(mirror.WithStorage(stor), mirror.WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	hfh := hfserver.NewHandler(
		hfserver.WithMirror(m),
		hfserver.WithUpstream(upstreamFunc),
		hfserver.WithExternalURL(srv.URL),
		hfserver.WithMinter(hfserver.MinterFunc(func(r *http.Request, req hfserver.TokenRequest) (string, int64, error) {
			if req.Permission != auth.Read {
				return "", 0, hfserver.ErrNotHandled
			}
			return issuer.Sign(auth.Grant{Permission: auth.Read, File: req.File})
		})),
		hfserver.WithNext(hfserver.NewUpstreamProxy(upstreamFunc)),
	)
	inner.Store(http.Handler(server.NewHandler(
		server.WithStorage(stor),
		server.WithAuthorizer(issuer),
		server.WithNext(hfh),
	)))
	return srv.URL
}

// waitReady HEADs resolveURL until the mirror answers 200 with a local xet hash, the ingest having finished.
func waitReady(t *testing.T, resolveURL string) {
	t.Helper()
	deadline := time.Now().Add(hfTimeout)
	last := "no request made"
	for time.Now().Before(deadline) {
		resp, err := probeClient.Head(resolveURL)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK && resp.Header.Get("X-Xet-Hash") != "" {
			return
		}
		last = resp.Status
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("mirror never reached ready state; last status: %s", last)
}
