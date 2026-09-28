// Package hub_test records the official hf CLI's behaviour against a private
// Hugging Face repository as redacted traces in the client/hftest format:
// a folder upload, downloads over xet and over plain HTTP, a re-upload of
// identical content, and the hub's answers to anonymous and wrong-token
// requests.
//
// The scenarios run only when a token file is given (CI skips them) and need
// the hf CLI on PATH (pip install -U "huggingface_hub[cli,hf_xet]") with write
// access to the repository:
//
//	go -C test/conformance test ./hub/ -run TestReferencePrivateRepo -v \
//	    -hf.token-file=/path/to/token -hf.record-dir=/path/out
//
// huggingface.co may need an HTTPS proxy, e.g. https_proxy=http://127.0.0.1:1087;
// the proxy variables are passed on to the CLI while its traffic to the
// loopback recorder bypasses them.
package hub_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet/test/conformance/hubtrace"
)

var (
	tokenFile = flag.String("hf.token-file", "", "file holding the hub token; empty skips the live scenarios")
	repoID    = flag.String("hf.repo", "wzshiming/test", "private repository the scenarios run against")
	upstream  = flag.String("hf.upstream", "https://huggingface.co", "hub endpoint")
	recordDir = flag.String("hf.record-dir", "", "directory receiving one <scenario>.json trace each (default: a new temp dir)")
	salt      = flag.String("hf.salt", "", "mixed into the seed of ref-lfs.bin to force a fresh upload")
)

const (
	repoDir     = "regression"
	lfsName     = "ref-lfs.bin"
	regularName = "ref-regular.txt"
	lfsSize     = 1 << 20
	// ref-lfs.bin is PCG(lfsSeed, lfsStream) output; -hf.salt xors sha256(salt)[:8] into the stream.
	lfsSeed, lfsStream = 20260925, 1
	hfTimeout          = 5 * time.Minute
	wrongToken         = "hf_invalidinvalidinvalidinvalid00"
)

// reference holds what every scenario shares: the CLI, the token, the fixtures and where the traces go.
type reference struct {
	hfBin, token, tool, outDir, repo, upstream string
	hasXet                                     bool
	fixtures                                   *fixtures
}

// fixtures is the local folder uploaded as regression/ and the bytes it holds.
type fixtures struct {
	dir       string
	lfs       []byte
	lfsSHA256 string
	regular   []byte
}

func TestReferencePrivateRepo(t *testing.T) {
	if *tokenFile == "" {
		t.Skip("no -hf.token-file")
	}
	if testing.Short() {
		t.Skip("network test; skipped in -short mode")
	}
	hfBin, err := exec.LookPath("hf")
	if err != nil {
		t.Skip(`hf CLI not found on PATH; install with: pip install -U "huggingface_hub[cli,hf_xet]"`)
	}
	ref := &reference{
		hfBin:    hfBin,
		token:    readToken(t, *tokenFile),
		outDir:   traceDir(t),
		repo:     *repoID,
		upstream: strings.TrimRight(*upstream, "/"),
	}
	ref.tool, ref.hasXet = toolInfo(t, ref)
	ref.fixtures = newFixtures(t, *salt, time.Now())
	t.Logf("tool: %s", ref.tool)

	steps := []struct {
		name         string
		run          func(*testing.T)
		prerequisite bool // a failure here leaves nothing for the later scenarios to fetch
	}{
		{"ref-upload", ref.upload, true},
		{"ref-download-xet", ref.downloadXet, false},
		{"ref-download-plain", ref.downloadPlain, false},
		{"ref-reupload", ref.reupload, false},
		{"ref-anonymous", ref.anonymous, false},
	}
	for _, step := range steps {
		if !t.Run(step.name, step.run) && step.prerequisite {
			return
		}
	}
}

func (r *reference) upload(t *testing.T) {
	rec := r.record(t, "ref-upload")
	out := r.mustHF(t, rec.HubURL(), r.token, false, "upload", r.repo, r.fixtures.dir, repoDir, "--commit-message", "test: private repo regression fixtures")
	t.Logf("hf upload:\n%s", out)
}

func (r *reference) downloadXet(t *testing.T) {
	if !r.hasXet {
		t.Skip("hf CLI has no hf_xet; xet download path unavailable")
	}
	r.download(t, "ref-download-xet", false)
}

func (r *reference) downloadPlain(t *testing.T) {
	r.download(t, "ref-download-plain", true)
}

// download fetches both fixtures with the CLI into a fresh dir and checks them byte for byte.
func (r *reference) download(t *testing.T, scenario string, disableXet bool) {
	rec := r.record(t, scenario)
	localDir := t.TempDir()
	r.mustHF(t, rec.HubURL(), r.token, disableXet, "download", r.repo, repoDir+"/"+lfsName, repoDir+"/"+regularName, "--local-dir", localDir)
	if got := sha256File(t, filepath.Join(localDir, repoDir, lfsName)); got != r.fixtures.lfsSHA256 {
		t.Errorf("%s sha256 = %s, want %s", lfsName, got, r.fixtures.lfsSHA256)
	}
	got, err := os.ReadFile(filepath.Join(localDir, repoDir, regularName))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, r.fixtures.regular) {
		t.Errorf("%s = %s, want %s", regularName, digest(got), digest(r.fixtures.regular))
	}
}

var noChangesRe = regexp.MustCompile(`(?i)no files have been modified|nothing to commit|no changes|empty commit`)

// reupload sends ref-lfs.bin again unchanged; the hub or the CLI may accept it or refuse an empty commit.
func (r *reference) reupload(t *testing.T) {
	rec := r.record(t, "ref-reupload")
	out, err := r.hf(t, rec.HubURL(), r.token, false, "upload", r.repo, filepath.Join(r.fixtures.dir, lfsName), repoDir+"/"+lfsName)
	switch {
	case err == nil:
		t.Logf("re-upload of identical content accepted:\n%s", out)
	case noChangesRe.Match(out):
		t.Logf("re-upload of identical content reported no changes (%v):\n%s", err, out)
	default:
		t.Fatalf("hf upload (re-upload): %v\n%s", err, out)
	}
}

// anonymous records the hub refusing the CLI and direct requests without or with a wrong token, then two informational probes with the valid one.
func (r *reference) anonymous(t *testing.T) {
	rec := r.record(t, "ref-anonymous")
	out, err := r.hf(t, rec.HubURL(), "", false, "download", r.repo, repoDir+"/"+lfsName, "--local-dir", t.TempDir())
	if err == nil {
		t.Errorf("anonymous hf download succeeded; %s is not private", r.repo)
	} else {
		t.Logf("anonymous hf download failed as expected (%v):\n%s", err, out)
	}

	probes := r.probes()
	for _, cred := range []struct{ name, token string }{{"anonymous", ""}, {"wrong-token", wrongToken}} {
		for _, p := range probes {
			status := r.probe(t, rec.HubURL(), cred.token, p)
			t.Logf("%s %s %s -> %d", cred.name, p.method, p.path, status)
			if status != http.StatusUnauthorized && status != http.StatusForbidden {
				t.Errorf("%s %s %s: status %d, want 401 or 403", cred.name, p.method, p.path, status)
			}
		}
	}
	for _, p := range []probe{
		{method: http.MethodHead, path: r.resolvePath(repoDir + "/does-not-exist.bin")},
		r.commitProbe(),
	} {
		status := r.probe(t, rec.HubURL(), r.token, p)
		t.Logf("valid-token %s %s -> %d", p.method, p.path, status)
	}
}

// probe is one direct hub request.
type probe struct {
	method, path, contentType string
	body                      []byte
}

// probes covers the endpoints a private download and upload touch.
func (r *reference) probes() []probe {
	sample := base64.StdEncoding.EncodeToString([]byte("xet probe sample"))
	preupload := fmt.Sprintf(`{"files":[{"path":%q,"sample":%q,"size":16}]}`, repoDir+"/probe.bin", sample)
	api := "/api/models/" + r.repo
	return []probe{
		{method: http.MethodHead, path: r.resolvePath(repoDir + "/" + lfsName)},
		{method: http.MethodGet, path: api + "/xet-read-token/main"},
		{method: http.MethodGet, path: api + "/xet-write-token/main"},
		{method: http.MethodPost, path: api + "/preupload/main", contentType: "application/json", body: []byte(preupload)},
		r.commitProbe(),
	}
}

// commitProbe commits an lfsFile whose object nobody uploaded.
func (r *reference) commitProbe() probe {
	oid := sha256.Sum256([]byte("xet private-repo probe: missing object"))
	body := `{"key":"header","value":{"summary":"probe"}}` + "\n" +
		fmt.Sprintf(`{"key":"lfsFile","value":{"path":%q,"algo":"sha256","oid":%q,"size":16}}`, repoDir+"/probe-missing-object.bin", hex.EncodeToString(oid[:])) + "\n"
	return probe{method: http.MethodPost, path: "/api/models/" + r.repo + "/commit/main", contentType: "application/x-ndjson", body: []byte(body)}
}

func (r *reference) resolvePath(file string) string {
	return "/" + r.repo + "/resolve/main/" + file
}

var probeClient = &http.Client{
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
}

// probe sends p through the recorder at hubURL, never following redirects, and returns the status.
func (r *reference) probe(t *testing.T, hubURL, token string, p probe) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, p.method, hubURL+p.path, bytes.NewReader(p.body))
	if err != nil {
		t.Fatal(err)
	}
	if p.contentType != "" {
		req.Header.Set("Content-Type", p.contentType)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := probeClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", p.method, p.path, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// record starts a recorder for scenario and, when the subtest ends, logs its exchanges and saves its trace scoped to the regression folder.
func (r *reference) record(t *testing.T, scenario string) *hubtrace.Recorder {
	t.Helper()
	rec, err := hubtrace.Start(r.upstream)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rec.Close)
	t.Cleanup(func() {
		tr := rec.Trace(scenario, r.repo, r.tool)
		for _, ex := range tr.Exchanges {
			t.Logf("%3d %s %s %s -> %d", ex.Seq, ex.Origin, ex.Method, ex.Path, ex.Status)
		}
		hubtrace.Scope(tr, repoDir+"/")
		path := filepath.Join(r.outDir, scenario+".json")
		if err := tr.Save(path); err != nil {
			t.Errorf("save trace: %v", err)
			return
		}
		t.Logf("trace saved to %s", path)
	})
	return rec
}

// hf runs the CLI against endpoint with a fresh HF_HOME holding token when set; the output comes back redacted.
func (r *reference) hf(t *testing.T, endpoint, token string, disableXet bool, args ...string) ([]byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), hfTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, r.hfBin, args...)
	cmd.Env = hfEnv(t, endpoint, token, disableXet)
	out, err := cmd.CombinedOutput()
	return []byte(hubtrace.Redact(string(out))), err
}

func (r *reference) mustHF(t *testing.T, endpoint, token string, disableXet bool, args ...string) []byte {
	t.Helper()
	out, err := r.hf(t, endpoint, token, disableXet, args...)
	if err != nil {
		t.Fatalf("hf %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

// hfEnv builds the CLI environment: hub traffic pinned to endpoint, cache and credentials isolated in a fresh HF_HOME (the token saved where the CLI looks for it), the inherited proxies kept for CDN and CAS traffic but bypassed for the loopback recorder.
func hfEnv(t *testing.T, endpoint, token string, disableXet bool) []string {
	t.Helper()
	var env []string
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		name = strings.ToUpper(name)
		if name == "NO_PROXY" || strings.HasPrefix(name, "HF_") || strings.HasPrefix(name, "HUGGING") {
			continue
		}
		env = append(env, kv)
	}
	home := t.TempDir()
	if token != "" {
		if err := os.WriteFile(filepath.Join(home, "token"), []byte(token), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	env = append(env,
		"HF_ENDPOINT="+endpoint,
		"HF_HOME="+home,
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

// toolInfo describes the CLI from the version line of `hf version` and the huggingface_hub and hf_xet lines of `hf env`, reporting whether hf_xet is installed.
func toolInfo(t *testing.T, r *reference) (tool string, hasXet bool) {
	t.Helper()
	var parts []string
	if out, err := r.hf(t, r.upstream, "", false, "version"); err == nil {
		for line := range strings.SplitSeq(string(out), "\n") {
			if line = strings.TrimSpace(line); strings.HasPrefix(line, "version=") {
				parts = append(parts, "hf "+line)
			}
		}
	}
	out, err := r.hf(t, r.upstream, "", false, "env")
	if err != nil {
		t.Fatalf("hf env: %v\n%s", err, out)
	}
	for line := range strings.SplitSeq(string(out), "\n") {
		line = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "-"))
		k, v, ok := strings.Cut(line, ":")
		if !ok || !(strings.Contains(k, "huggingface_hub") || strings.Contains(k, "hf_xet")) {
			continue
		}
		parts = append(parts, line)
		if strings.Contains(k, "hf_xet") && !strings.Contains(v, "not installed") {
			hasXet = true
		}
	}
	return strings.Join(parts, "; "), hasXet
}

// readToken loads the hub token into memory; it is never logged.
func readToken(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	token := strings.TrimSpace(string(data))
	if token == "" {
		t.Fatalf("token file %s is empty", path)
	}
	return token
}

// traceDir is -hf.record-dir, or a new temp dir that outlives the test.
func traceDir(t *testing.T) string {
	t.Helper()
	if *recordDir != "" {
		if err := os.MkdirAll(*recordDir, 0o755); err != nil {
			t.Fatal(err)
		}
		return *recordDir
	}
	dir, err := os.MkdirTemp("", "xet-hub-trace-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("recording traces to %s", dir)
	return dir
}

// newFixtures writes the upload folder: ref-lfs.bin and ref-regular.txt, a fixed sentence plus the run's timestamp so no commit is empty.
func newFixtures(t *testing.T, salt string, now time.Time) *fixtures {
	t.Helper()
	fx := &fixtures{dir: t.TempDir(), lfs: refLFSContent(salt)}
	sum := sha256.Sum256(fx.lfs)
	fx.lfsSHA256 = hex.EncodeToString(sum[:])
	fx.regular = []byte("xet private-repo regression fixture — ref-regular.txt\n" +
		"Committed next to ref-lfs.bin by test/conformance/hub; the line below makes every commit distinct.\n" +
		"recorded: " + now.UTC().Format(time.RFC3339) + "\n")
	for name, data := range map[string][]byte{lfsName: fx.lfs, regularName: fx.regular} {
		if err := os.WriteFile(filepath.Join(fx.dir, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return fx
}

// refLFSContent is ref-lfs.bin: lfsContent on lfsStream.
func refLFSContent(salt string) []byte {
	return lfsContent(lfsStream, salt)
}

// lfsContent is lfsSize bytes of PCG(lfsSeed, stream) output, the stream xor'd with sha256(salt)[:8] when salt is set.
func lfsContent(stream uint64, salt string) []byte {
	if salt != "" {
		sum := sha256.Sum256([]byte(salt))
		stream ^= binary.LittleEndian.Uint64(sum[:8])
	}
	rng := rand.New(rand.NewPCG(lfsSeed, stream))
	data := make([]byte, lfsSize)
	for chunk := range slices.Chunk(data, 8) {
		binary.LittleEndian.PutUint64(chunk, rng.Uint64())
	}
	return data
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

// digest describes fetched content for a failure message without printing any of its bytes.
func digest(b []byte) string {
	sum := sha256.Sum256(b)
	return fmt.Sprintf("%d bytes, sha256 %s", len(b), hex.EncodeToString(sum[:]))
}
