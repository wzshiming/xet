package hubtrace_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/client/hftest"
	"github.com/wzshiming/xet/test/conformance/hubtrace"
)

const (
	hubToken    = "hf_abcdefghijklmnopqrstuvwxyz0123"
	casToken    = "eyJaaaaaaaaaaa.bbbbbbbbbbbb.cccccccccccc"
	fileHash    = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	resolvePath = "/o/r/resolve/main/f.bin"
	tokenPath   = "/api/models/o/r/xet-read-token/main"
	reconPath   = "/reconstruction/" + fileHash
	xorbPath    = "/v1/xorbs/default/" + fileHash
	location    = "https://cdn.example/f?X-Amz-Signature=abc"
	reconLink   = "https://cas.example" + reconPath
)

// authLog keeps the Authorization header seen per path.
type authLog struct {
	mu   sync.Mutex
	seen map[string]string
}

func (l *authLog) record(r *http.Request) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.seen == nil {
		l.seen = map[string]string{}
	}
	l.seen[r.URL.Path] = r.Header.Get("Authorization")
}

func (l *authLog) get(path string) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.seen[path]
}

// fakeCAS echoes the request path and swallows one binary upload.
func fakeCAS(t *testing.T, log *authLog) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.record(r)
		if r.Method == http.MethodPost && r.URL.Path == xorbPath {
			n, _ := io.Copy(io.Discard, r.Body)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"was_inserted":true,"received":%d}`, n)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprint(w, r.URL.Path)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// fakeHub answers a private repository: 401 without credentials, a redirecting resolve with xet links on its own origin, and a token pointing at casURL.
func fakeHub(t *testing.T, casURL string) *httptest.Server {
	t.Helper()
	var hub *httptest.Server
	hub = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Error-Code", "RepoNotFound")
			w.Header().Set("Set-Cookie", "session=SECRET; Path=/")
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, `{"error":"Invalid credentials in Authorization header","cookie":"session=SECRET"}`)
			return
		}
		switch {
		case (r.Method == http.MethodHead || r.Method == http.MethodGet) && r.URL.Path == resolvePath:
			w.Header().Set("Location", location)
			w.Header().Set("X-Amz-Cf-Id", "YZi6Lj4RhVdmyus9ssU4gOMNWXSjrwwc8RnzUl7RoK4m5Uo1GSw33w==")
			w.Header().Set("X-Amz-Cf-Pop", "HKG61-P1")
			w.Header().Set("X-Xet-Hash", fileHash)
			w.Header().Add("Link", "<"+hub.URL+tokenPath+`>; rel="xet-auth"`)
			w.Header().Add("Link", "<"+reconLink+`>; rel="xet-reconstruction-info"`)
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusFound)
			if r.Method == http.MethodGet {
				fmt.Fprint(w, "Found. Redirecting to "+location)
			}
		case r.Method == http.MethodGet && r.URL.Path == tokenPath:
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Xet-Cas-Url", casURL)
			w.Header().Set("X-Xet-Access-Token", casToken)
			fmt.Fprintf(w, `{"casUrl":%q,"accessToken":%q,"exp":123}`, casURL, casToken)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(hub.Close)
	return hub
}

// do sends one request without following redirects and returns the response with its body read.
func do(t *testing.T, method, url, auth, contentType string, body []byte) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	c := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, data
}

func TestRecorderRewritesAndRedacts(t *testing.T) {
	var casLog authLog
	cas := fakeCAS(t, &casLog)
	hub := fakeHub(t, cas.URL)

	rec, err := hubtrace.Start(hub.URL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rec.Close)
	h := rec.HubURL()
	if h == hub.URL || !strings.HasPrefix(h, "http://127.0.0.1:") {
		t.Fatalf("HubURL = %q, want a loopback origin distinct from the upstream %q", h, hub.URL)
	}

	resp, _ := do(t, http.MethodHead, h+resolvePath, "Bearer "+hubToken, "", nil)
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("resolve status = %d, want 302", resp.StatusCode)
	}
	if got := resp.Header.Get("Location"); got != location {
		t.Errorf("Location = %q, want the CDN redirect untouched %q", got, location)
	}
	if got := resp.Header.Get("X-Xet-Hash"); got != fileHash {
		t.Errorf("X-Xet-Hash = %q, want %q", got, fileHash)
	}
	links := client.ParseLinkHeaders(resp.Header.Values("Link"))
	if got := links["xet-auth"]; got != h+tokenPath {
		t.Fatalf("xet-auth link = %q, want %q", got, h+tokenPath)
	}
	if got := links["xet-reconstruction-info"]; got != reconLink {
		t.Errorf("xet-reconstruction-info link = %q, want %q", got, reconLink)
	}

	resp, body := do(t, http.MethodGet, links["xet-auth"], "Bearer "+hubToken, "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("token status = %d, want 200: %s", resp.StatusCode, body)
	}
	var tok struct {
		CASURL string `json:"casUrl"`
		Token  string `json:"accessToken"`
		Exp    int64  `json:"exp"`
	}
	if err := json.Unmarshal(body, &tok); err != nil {
		t.Fatalf("decode token body %q: %v", body, err)
	}
	if tok.CASURL == cas.URL || tok.CASURL == h || !strings.HasPrefix(tok.CASURL, "http://127.0.0.1:") {
		t.Fatalf("casUrl = %q, want a CAS proxy distinct from the CAS %q and the hub %q", tok.CASURL, cas.URL, h)
	}
	if got := resp.Header.Get("X-Xet-Cas-Url"); got != tok.CASURL {
		t.Errorf("X-Xet-Cas-Url = %q, want the casUrl %q", got, tok.CASURL)
	}
	if tok.Token != casToken || resp.Header.Get("X-Xet-Access-Token") != casToken || tok.Exp != 123 {
		t.Errorf("token = %+v, header %q; want the upstream token and exp on the wire", tok, resp.Header.Get("X-Xet-Access-Token"))
	}

	resp, body = do(t, http.MethodGet, tok.CASURL+reconPath, "Bearer "+casToken, "", nil)
	if resp.StatusCode != http.StatusOK || string(body) != reconPath {
		t.Fatalf("CAS GET = %d %q, want 200 %q", resp.StatusCode, body, reconPath)
	}
	if got := casLog.get(reconPath); got != "Bearer "+casToken {
		t.Errorf("CAS saw Authorization %q, want %q", got, "Bearer "+casToken)
	}

	xorb := make([]byte, 4096)
	for i := range xorb {
		xorb[i] = byte(i * 7)
	}
	resp, _ = do(t, http.MethodPost, tok.CASURL+xorbPath, "Bearer "+casToken, "application/octet-stream", xorb)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CAS POST status = %d, want 200", resp.StatusCode)
	}

	resp, _ = do(t, http.MethodGet, h+tokenPath, "", "", nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("anonymous token status = %d, want 401", resp.StatusCode)
	}

	resp, body = do(t, http.MethodGet, h+resolvePath, "Bearer "+hubToken, "", nil)
	if resp.StatusCode != http.StatusFound || string(body) != "Found. Redirecting to "+location {
		t.Fatalf("GET resolve = %d %q, want 302 naming the redirect", resp.StatusCode, body)
	}

	tr := rec.Trace("unit", "o/r", "hubtrace_test "+hubToken)
	want := []struct {
		origin, method, path string
		status               int
	}{
		{"hub", http.MethodHead, resolvePath, http.StatusFound},
		{"hub", http.MethodGet, tokenPath, http.StatusOK},
		{"cas", http.MethodGet, reconPath, http.StatusOK},
		{"cas", http.MethodPost, xorbPath, http.StatusOK},
		{"hub", http.MethodGet, tokenPath, http.StatusUnauthorized},
		{"hub", http.MethodGet, resolvePath, http.StatusFound},
	}
	if len(tr.Exchanges) != len(want) {
		t.Fatalf("recorded %d exchanges, want %d: %+v", len(tr.Exchanges), len(want), tr.Exchanges)
	}
	for i, w := range want {
		ex := tr.Exchanges[i]
		if ex.Seq != i+1 || ex.Origin != w.origin || ex.Method != w.method || ex.Path != w.path || ex.Status != w.status {
			t.Errorf("exchange %d = %d %s %s %s %d, want %d %s %s %s %d", i, ex.Seq, ex.Origin, ex.Method, ex.Path, ex.Status, i+1, w.origin, w.method, w.path, w.status)
		}
	}
	if tr.Upstream != hub.URL || tr.Repo != "o/r" || tr.Tool != "hubtrace_test <redacted>" || tr.Recorded == "" {
		t.Errorf("trace header = %q %q %q %q, want upstream %q, repo o/r, redacted tool, a timestamp", tr.Upstream, tr.Repo, tr.Tool, tr.Recorded, hub.URL)
	}

	resolve, token, recon, upload, anon := tr.Exchanges[0], tr.Exchanges[1], tr.Exchanges[2], tr.Exchanges[3], tr.Exchanges[4]
	if got := resolve.RequestHeaders.Get("Authorization"); got != "Bearer <redacted>" {
		t.Errorf("resolve Authorization = %q, want %q", got, "Bearer <redacted>")
	}
	if got := resolve.ResponseHeaders.Get("Location"); got != "https://cdn.example/f?<redacted>" {
		t.Errorf("resolve Location = %q, want the query redacted", got)
	}
	if got := resolve.ResponseHeaders.Get("X-Xet-Hash"); got != fileHash {
		t.Errorf("resolve X-Xet-Hash = %q, want %q", got, fileHash)
	}
	for _, name := range []string{"X-Amz-Cf-Id", "X-Amz-Cf-Pop"} {
		if got := resolve.ResponseHeaders.Values(name); len(got) != 0 {
			t.Errorf("resolve %s = %q, want the CloudFront trace header dropped", name, got)
		}
	}
	links = client.ParseLinkHeaders(resolve.ResponseHeaders.Values("Link"))
	if links["xet-auth"] != h+tokenPath || links["xet-reconstruction-info"] != reconLink {
		t.Errorf("recorded links = %v, want xet-auth on %s and the reconstruction link untouched", links, h)
	}
	if resolve.RequestBody != nil || resolve.ResponseBody != nil {
		t.Errorf("HEAD exchange recorded bodies %+v %+v, want none", resolve.RequestBody, resolve.ResponseBody)
	}

	if got := token.ResponseHeaders.Get("X-Xet-Access-Token"); got != "<redacted>" {
		t.Errorf("X-Xet-Access-Token = %q, want <redacted>", got)
	}
	if got := token.ResponseHeaders.Get("X-Xet-Cas-Url"); got != tok.CASURL {
		t.Errorf("recorded X-Xet-Cas-Url = %q, want %q", got, tok.CASURL)
	}
	if token.ResponseBody == nil || token.ResponseBody.Text != fmt.Sprintf(`{"accessToken":"<redacted>","casUrl":%q,"exp":123}`, tok.CASURL) {
		t.Errorf("token body = %+v, want the redacted token JSON pointing at %s", token.ResponseBody, tok.CASURL)
	}

	if got := recon.RequestHeaders.Get("Authorization"); got != "Bearer <redacted>" {
		t.Errorf("CAS Authorization = %q, want %q", got, "Bearer <redacted>")
	}
	if recon.ResponseBody == nil || recon.ResponseBody.Text != reconPath {
		t.Errorf("CAS response body = %+v, want text %q", recon.ResponseBody, reconPath)
	}

	sum := sha256.Sum256(xorb)
	if b := upload.RequestBody; b == nil || b.Size != int64(len(xorb)) || b.SHA256 != hex.EncodeToString(sum[:]) || b.Text != "" {
		t.Errorf("binary request body = %+v, want size %d, sha256 %x and no text", b, len(xorb), sum)
	}

	if anon.RequestHeaders.Get("Authorization") != "" || anon.ResponseHeaders.Get("X-Error-Code") != "RepoNotFound" || anon.ResponseBody == nil || anon.ResponseBody.Text != `{"cookie":"<redacted>","error":"Invalid credentials in Authorization header"}` {
		t.Errorf("anonymous exchange = %+v, want no Authorization, X-Error-Code and the error body with its cookie redacted", anon)
	}
	if got := anon.ResponseHeaders.Values("Set-Cookie"); len(got) != 0 {
		t.Errorf("anonymous Set-Cookie = %q, want the header dropped", got)
	}
	if b := tr.Exchanges[5].ResponseBody; b == nil || b.Text != "Found. Redirecting to https://cdn.example/f?<redacted>" {
		t.Errorf("redirect body = %+v, want the presigned query redacted from the text", b)
	}

	path := filepath.Join(t.TempDir(), "traces", "unit.json")
	if err := tr.Save(path); err != nil {
		t.Fatal(err)
	}
	saved, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"hf_abc", "eyJ", "X-Amz-Signature=abc", "X-Amz-Cf-", "HKG61", "Set-Cookie", "session=SECRET"} {
		if bytes.Contains(saved, []byte(secret)) {
			t.Errorf("saved trace contains %q", secret)
		}
	}
	for _, keep := range []string{"X-Xet-Hash", "Bearer <redacted>", `\"exp\":123`} {
		if !bytes.Contains(saved, []byte(keep)) {
			t.Errorf("saved trace lacks %q", keep)
		}
	}
	if !bytes.HasSuffix(saved, []byte("}\n")) {
		t.Errorf("saved trace does not end in a newline: %q", saved[max(0, len(saved)-8):])
	}

	loaded, err := hftest.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(loaded, tr) {
		t.Errorf("Load(Save(trace)) differs:\n got %+v\nwant %+v", loaded, tr)
	}

	rec.Reset()
	if n := len(rec.Trace("unit", "o/r", "").Exchanges); n != 0 {
		t.Errorf("exchanges after Reset = %d, want 0", n)
	}
}

func TestRedact(t *testing.T) {
	for _, tc := range []struct{ name, in, want string }{
		{"absolute presigned URL in prose", "fetching https://cdn.example/f.bin?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=abc now", "fetching https://cdn.example/f.bin?<redacted> now"},
		{"absolute URL query in a link header", `<https://cas.example/v1/reconstructions/abc?token=x>; rel="xet-reconstruction-info"`, `<https://cas.example/v1/reconstructions/abc?<redacted>>; rel="xet-reconstruction-info"`},
		{"relative signed URL", "/x/y?X-Amz-Signature=abc&Expires=1", "/x/y?<redacted>"},
		{"relative signed URL in JSON text", `{"url":"/x/y?Policy=p&Key-Pair-Id=k"}`, `{"url":"/x/y?<redacted>"}`},
		{"hub path with a cache-buster", "/api/models/o/r/xet-read-token/main?1790329460", "/api/models/o/r/xet-read-token/main?1790329460"},
		{"unsigned relative query", "/o/r/resolve/main/f.bin?download=true", "/o/r/resolve/main/f.bin?download=true"},
		{"question mark in prose", "continue? [y/N] token=none", "continue? [y/N] token=none"},
		{"user token", "Authorization: Bearer " + hubToken, "Authorization: Bearer <redacted>"},
		{"jwt", "token " + casToken + " expires", "token <redacted> expires"},
		{"already redacted absolute URL", "https://cdn.example/f?<redacted>", "https://cdn.example/f?<redacted>"},
		{"already redacted relative URL", "/x/y?<redacted>", "/x/y?<redacted>"},
		{"upper-case scheme", "HTTPS://cdn.example/f?opaque=SECRET", "HTTPS://cdn.example/f?<redacted>"},
		{"percent-encoded signed key", "/f?%53ignature=SECRET", "/f?<redacted>"},
		{"percent-encoded unsigned key", "/f?%64ownload=true", "/f?%64ownload=true"},
		{"set-cookie header line", "Set-Cookie: session=SECRET", "Set-Cookie: <redacted>"},
		{"cookie pair with attributes", "cookie=abc; other", "cookie=<redacted>"},
		{"cookie header among others", "Cookie: a=b; c=d\nX-Keep: yes", "Cookie: <redacted>\nX-Keep: yes"},
		{"cdn repository id in a presigned location", "https://us.aws.cdn.hf.co/xet-bridge-us/69704be01cef19c99a1ba098/f857c20483a87a6737b06f718d340429fc080f73c00f6d46b2030b58734a61e4?X-Xet-Cas-Uid=64e9ed3b233101ed99cd9030&Expires=1", "https://us.aws.cdn.hf.co/xet-bridge-us/<redacted>/f857c20483a87a6737b06f718d340429fc080f73c00f6d46b2030b58734a61e4?<redacted>"},
		{"cdn repository id in CLI output", "Error: 403 for url https://us.aws.cdn.hf.co/xet-bridge-us/69704be01cef19c99a1ba098/f857c20483a87a6737b06f718d340429fc080f73c00f6d46b2030b58734a61e4 (Request ID: x)", "Error: 403 for url https://us.aws.cdn.hf.co/xet-bridge-us/<redacted>/f857c20483a87a6737b06f718d340429fc080f73c00f6d46b2030b58734a61e4 (Request ID: x)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := hubtrace.Redact(tc.in); got != tc.want {
				t.Errorf("Redact(%q)\n got %q\nwant %q", tc.in, got, tc.want)
			}
			if again := hubtrace.Redact(tc.want); again != tc.want {
				t.Errorf("Redact(%q) = %q, want it unchanged", tc.want, again)
			}
		})
	}
}

func TestScope(t *testing.T) {
	const (
		repoID   = "69704be01cef19c99a1ba098"
		cdn      = "https://us.aws.cdn.hf.co/xet-bridge-us/" + repoID + "/f857c204?<redacted>"
		wantCDN  = "https://us.aws.cdn.hf.co/xet-bridge-us/<redacted>/f857c204?<redacted>"
		revision = `{"_id":"` + repoID + `","author":"o","cardData":{"license":"mit","language":["en"]},"config":{"architectures":["X"]},"id":"o/r","library_name":"transformers","pipeline_tag":"text-generation","private":true,"sha":"bcf51b2e","siblings":[{"rfilename":"LICENSE"},{"rfilename":"regression/ref-lfs.bin"},{"rfilename":"regression/ref-regular.txt"}],"spaces":["o/demo"],"tags":["transformers","license:mit"],"transformersInfo":{"auto_model":"AutoModel"},"usedStorage":443891859,"widgetData":[{"text":"Hi"}]}`
		wantRev  = `{"_id":"<redacted>","author":"o","cardData":{},"config":{},"id":"o/r","library_name":"<redacted>","pipeline_tag":"<redacted>","private":true,"sha":"bcf51b2e","siblings":[{"rfilename":"regression/ref-lfs.bin"},{"rfilename":"regression/ref-regular.txt"}],"spaces":[],"tags":[],"transformersInfo":{},"usedStorage":0,"widgetData":{}}`
		tree     = `[{"oid":"57fddbfa","path":"regression","size":0,"type":"directory"},{"lfs":{"oid":"cd449473","pointerSize":132,"size":1048576},"oid":"c9176dbf","path":"regression/a.bin","size":1048576,"type":"file","xetHash":"f857c204"},{"oid":"1a2b","path":"LICENSE","size":11357,"type":"file"},{"oid":"3c4d","path":"vocab.json","size":798293,"type":"file"}]`
		wantTree = `[{"oid":"57fddbfa","path":"regression","size":0,"type":"directory"},{"lfs":{"oid":"cd449473","pointerSize":132,"size":1048576},"oid":"c9176dbf","path":"regression/a.bin","size":1048576,"type":"file","xetHash":"f857c204"}]`
		text     = "LICENSE vocab.json {\"_id\":\"keep\"} regression/other"
		redirect = "Found. Redirecting to " + cdn
	)
	body := func(text, contentType string) (*hftest.Body, http.Header) {
		sum := sha256.Sum256([]byte("raw wire bytes of " + text))
		return &hftest.Body{Size: int64(len(text)) + 7, SHA256: hex.EncodeToString(sum[:]), Text: text}, http.Header{"Content-Type": {contentType}}
	}
	tr := &hftest.Trace{Scenario: "unit", Repo: "o/r"}
	for _, in := range []struct{ text, contentType string }{
		{revision, "application/json; charset=utf-8"},
		{tree, "application/json; charset=utf-8"},
		{text, "text/plain; charset=utf-8"},
		{`{"error":"Invalid credentials in Authorization header"}`, "application/json"},
	} {
		b, h := body(in.text, in.contentType)
		tr.Exchanges = append(tr.Exchanges, hftest.Exchange{Seq: len(tr.Exchanges) + 1, Origin: "hub", Method: http.MethodGet, Path: "/api/models/o/r/x", Status: http.StatusOK, ResponseHeaders: h, ResponseBody: b, RequestBody: &hftest.Body{Size: 1, SHA256: "ab", Text: `{"_id":"request bodies are not listings"}`}})
	}
	// The repository id the revision listing names recurs in the CDN paths of a redirect, before Scope ever saw the listing.
	b, h := body(redirect, "text/plain; charset=utf-8")
	h.Set("Location", cdn)
	tr.Exchanges = append(tr.Exchanges, hftest.Exchange{Seq: len(tr.Exchanges) + 1, Origin: "hub", Method: http.MethodGet, Path: "/xet-bridge-us/" + repoID + "/f857c204", RequestHeaders: http.Header{"Referer": {cdn}}, Status: http.StatusFound, ResponseHeaders: h, ResponseBody: b})
	before := make([]hftest.Body, 0, len(tr.Exchanges))
	for _, ex := range tr.Exchanges {
		before = append(before, *ex.ResponseBody)
	}

	hubtrace.Scope(tr, "regression/")

	for i, want := range []string{wantRev, wantTree, text, `{"error":"Invalid credentials in Authorization header"}`, "Found. Redirecting to " + wantCDN} {
		got := tr.Exchanges[i].ResponseBody
		if got.Text != want {
			t.Errorf("exchange %d text\n got %s\nwant %s", i+1, got.Text, want)
		}
		if got.Size != before[i].Size || got.SHA256 != before[i].SHA256 {
			t.Errorf("exchange %d size/sha256 = %d %s, want the raw body's %d %s untouched", i+1, got.Size, got.SHA256, before[i].Size, before[i].SHA256)
		}
		if i < 4 {
			if got := tr.Exchanges[i].RequestBody.Text; got != `{"_id":"request bodies are not listings"}` {
				t.Errorf("exchange %d request body = %q, want it untouched", i+1, got)
			}
		}
	}
	found := tr.Exchanges[4]
	if found.Path != "/xet-bridge-us/<redacted>/f857c204" || found.RequestHeaders.Get("Referer") != wantCDN || found.ResponseHeaders.Get("Location") != wantCDN {
		t.Errorf("redirect exchange path %q, Referer %q, Location %q; want the repository id replaced with <redacted> in each", found.Path, found.RequestHeaders.Get("Referer"), found.ResponseHeaders.Get("Location"))
	}
	if got := found.ResponseHeaders.Get("Content-Type"); got != "text/plain; charset=utf-8" {
		t.Errorf("redirect Content-Type = %q, want it untouched", got)
	}
}
