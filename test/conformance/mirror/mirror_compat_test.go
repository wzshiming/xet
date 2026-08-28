// Package mirror_test exercises the mirror as a middle layer between real
// public hubs and the official `hf` CLI acting as the downstream client:
//
//   - xet-capable upstream:  https://huggingface.co  (openai/gpt-oss-20b)
//   - plain HTTP upstream:   https://modelscope.cn   (openai-mirror/gpt-oss-20b)
//
// Prerequisites, each skipping the tests when missing:
//   - the `hf` CLI on PATH (pip install -U "huggingface_hub[cli,hf_xet]")
//   - network reachability of the upstream; huggingface.co may need an HTTPS
//     proxy, e.g.: https_proxy=http://127.0.0.1:1087 go test ./mirror/...
package mirror_test

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet/mirror"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/server/hf"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/token"
)

// testFile is an LFS-backed file (~27 MiB) present in both test repos.
const testFile = "tokenizer.json"

func TestHuggingFaceXetUpstream(t *testing.T) {
	runMirrorCompat(t, "https://huggingface.co", "openai/gpt-oss-20b")
}

func TestModelScopePlainUpstream(t *testing.T) {
	runMirrorCompat(t, "https://modelscope.cn", "openai-mirror/gpt-oss-20b")
}

// runMirrorCompat starts a mirror in front of upstream and downloads testFile
// through it with the hf CLI in three states: cold (streamed while the mirror
// ingests), warm via the xet protocol, and warm via the plain sha256 bridge.
// It then restarts the mirror on the persisted cache with an unreachable
// upstream and revalidation due on every request: a hub outage must not
// break a warm mirror.
func runMirrorCompat(t *testing.T, upstream, repo string) {
	if testing.Short() {
		t.Skip("network test; skipped in -short mode")
	}
	hfBin, err := exec.LookPath("hf")
	if err != nil {
		t.Skip(`hf CLI not found on PATH; install with: pip install -U "huggingface_hub[cli,hf_xet]"`)
	}

	resolvePath := "/" + repo + "/resolve/main/" + testFile
	wantSHA256 := probeUpstreamSHA256(t, upstream+resolvePath)

	storageDir, cacheDir := t.TempDir(), t.TempDir()
	mirrorURL := startMirror(t, upstream, storageDir, cacheDir)
	resolveURL := mirrorURL + resolvePath

	t.Run("hf cli cold", func(t *testing.T) {
		verifyDownload(t, hfBin, mirrorURL, repo, wantSHA256, false)
	})

	resp := waitReady(t, resolveURL)
	if resp.Header.Get("X-Xet-Hash") == "" {
		t.Error("ready resolve response is missing X-Xet-Hash")
	}
	if loc := resp.Header.Get("Location"); !strings.HasSuffix(loc, "/xet-bridge/"+wantSHA256) {
		t.Errorf("ready resolve redirect = %q, want suffix %q", loc, "/xet-bridge/"+wantSHA256)
	}
	// Metadata hub clients refuse to download without, mirrored from the
	// upstream or synthesized (modelscope omits size on HEAD and may omit
	// the commit; the mirror must fill both).
	if got := trimETag(resp.Header.Get("ETag")); got != wantSHA256 {
		t.Errorf("ready ETag = %q, want upstream sha256 %q", got, wantSHA256)
	}
	if size, err := strconv.ParseInt(resp.Header.Get("X-Linked-Size"), 10, 64); err != nil || size <= 0 {
		t.Errorf("ready X-Linked-Size = %q, want a positive size", resp.Header.Get("X-Linked-Size"))
	}
	if commit := resp.Header.Get("X-Repo-Commit"); !commitRe.MatchString(commit) {
		t.Errorf("ready X-Repo-Commit = %q, want a 40-hex commit", commit)
	}

	t.Run("hf cli xet warm", func(t *testing.T) {
		if !hasHFXet(hfBin) {
			t.Skip("hf CLI has no hf_xet; xet downstream path unavailable")
		}
		verifyDownload(t, hfBin, mirrorURL, repo, wantSHA256, false)
	})

	t.Run("hf cli plain warm", func(t *testing.T) {
		verifyDownload(t, hfBin, mirrorURL, repo, wantSHA256, true)
	})

	restartedURL := startMirror(t, unreachableUpstream(t), storageDir, cacheDir,
		mirror.WithRevalidateInterval(0))
	waitReady(t, restartedURL+resolvePath)

	t.Run("hf cli xet warm after restart, upstream down", func(t *testing.T) {
		if !hasHFXet(hfBin) {
			t.Skip("hf CLI has no hf_xet; xet downstream path unavailable")
		}
		verifyDownload(t, hfBin, restartedURL, repo, wantSHA256, false)
	})

	t.Run("hf cli plain warm after restart, upstream down", func(t *testing.T) {
		verifyDownload(t, hfBin, restartedURL, repo, wantSHA256, true)
	})
}

// unreachableUpstream returns a loopback URL that refuses connections.
func unreachableUpstream(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.NotFoundHandler())
	srv.Close()
	return srv.URL
}

// startMirror wires the same composition as cmd/xetd: a CAS server that
// matches its routes first, serves the hub front end over the mirror engine,
// and falls through to the upstream proxy, with a shared issuer tying token
// minting to validation. storageDir and cacheDir are taken by the caller so
// a restarted mirror can reuse them.
func startMirror(t *testing.T, upstream, storageDir, cacheDir string, opts ...mirror.Option) string {
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
	proxy, err := hf.NewUpstreamProxy(upstream, "")
	if err != nil {
		t.Fatal(err)
	}
	m, err := mirror.NewMirror(
		append([]mirror.Option{
			mirror.WithStorage(stor),
			mirror.WithUpstream(upstream),
			mirror.WithCacheDir(cacheDir),
		}, opts...)...,
	)
	if err != nil {
		t.Fatal(err)
	}
	hfh := hf.NewHandler(
		hf.WithMirror(m),
		hf.WithExternalURL(srv.URL),
		hf.WithMintToken(issuer.Mint),
		hf.WithNext(proxy),
	)
	inner.Store(http.Handler(server.NewHandler(
		server.WithStorage(stor),
		server.WithAuthFunc(func(tok string) bool { return issuer.Validate(tok, time.Now()) }),
		server.WithNext(hfh),
	)))
	return srv.URL
}

// verifyDownload runs `hf download` against the mirror into a fresh local dir
// with a fresh HF_HOME, then checks the bytes against the upstream sha256.
func verifyDownload(t *testing.T, hfBin, endpoint, repo, wantSHA256 string, disableXet bool) {
	t.Helper()
	localDir := t.TempDir()
	cmd := exec.CommandContext(t.Context(), hfBin, "download", repo, testFile, "--local-dir", localDir)
	cmd.Env = hfEnv(t, endpoint, disableXet)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("hf download failed: %v\n%s", err, out)
	}
	if got := sha256File(t, filepath.Join(localDir, testFile)); got != wantSHA256 {
		t.Fatalf("downloaded sha256 = %s, want %s", got, wantSHA256)
	}
}

// hfEnv builds the CLI environment: hub traffic pinned to the mirror, caches
// and credentials isolated per invocation, and proxy variables stripped so
// loopback traffic to the mirror never routes through a proxy.
func hfEnv(t *testing.T, endpoint string, disableXet bool) []string {
	t.Helper()
	var env []string
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		switch strings.ToUpper(name) {
		case "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY":
			continue
		}
		if strings.HasPrefix(name, "HF_") || strings.HasPrefix(name, "HUGGING") {
			continue
		}
		env = append(env, kv)
	}
	env = append(env,
		"HF_ENDPOINT="+endpoint,
		"HF_HOME="+t.TempDir(),
		"HF_HUB_DISABLE_TELEMETRY=1",
		"HF_HUB_DISABLE_PROGRESS_BARS=1",
		"HF_HUB_ETAG_TIMEOUT=60",
		"NO_PROXY=127.0.0.1,localhost",
		"no_proxy=127.0.0.1,localhost",
	)
	if disableXet {
		env = append(env, "HF_HUB_DISABLE_XET=1")
	}
	return env
}

var (
	sha256Re = regexp.MustCompile(`^[0-9a-f]{64}$`)
	commitRe = regexp.MustCompile(`^[0-9a-f]{40}$`)
)

// probeUpstreamSHA256 checks the upstream is reachable and returns the sha256
// the hub hop advertises for the file, skipping the test when unreachable.
func probeUpstreamSHA256(t *testing.T, resolveURL string) string {
	t.Helper()
	c := &http.Client{
		Timeout: 30 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	req, err := http.NewRequest(http.MethodHead, resolveURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := c.Do(req)
	if err != nil {
		t.Skipf("upstream unreachable (huggingface.co may need https_proxy, e.g. https_proxy=http://127.0.0.1:1087): %v", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		t.Skipf("upstream %s returned status %d", resolveURL, resp.StatusCode)
	}
	etag := trimETag(resp.Header.Get("X-Linked-Etag"))
	if etag == "" {
		etag = trimETag(resp.Header.Get("ETag"))
	}
	if !sha256Re.MatchString(etag) {
		t.Fatalf("upstream etag %q is not a sha256; pick an LFS-backed test file", etag)
	}
	return etag
}

func trimETag(etag string) string {
	return strings.Trim(strings.TrimPrefix(strings.TrimSpace(etag), "W/"), `"`)
}

// waitReady polls the resolve URL with HEAD until the mirror answers with the
// ready-state redirect, meaning ingestion completed into local storage.
func waitReady(t *testing.T, resolveURL string) *http.Response {
	t.Helper()
	c := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	deadline := time.Now().Add(5 * time.Minute)
	last := "no request made"
	for time.Now().Before(deadline) {
		resp, err := c.Head(resolveURL)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusFound {
			return resp
		}
		last = resp.Status
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("mirror never reached ready state; last status: %s", last)
	return nil
}

func sha256File(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// hasHFXet reports whether the CLI's Python environment ships hf_xet.
func hasHFXet(hfBin string) bool {
	out, err := exec.Command(hfBin, "env").Output()
	if err != nil {
		return false
	}
	for _, line := range strings.Split(string(out), "\n") {
		if k, v, ok := strings.Cut(line, ":"); ok &&
			strings.Contains(k, "hf_xet") && !strings.Contains(v, "not installed") {
			return true
		}
	}
	return false
}
