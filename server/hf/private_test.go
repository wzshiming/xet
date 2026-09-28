package hf

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/auth"
	"github.com/wzshiming/xet/client/hftest"
	"github.com/wzshiming/xet/mirror"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage/local"
	"github.com/wzshiming/xet/upload"
)

const (
	privateRepo  = "wzshiming/test"
	privateToken = "hub-secret"
	weightsPath  = "regression/weights.bin"
	readmePath   = "regression/README.txt"
)

// privateHub is a token-gated hub replaying huggingface.co's private
// repository behaviour in front of its own CAS, holding one committed lfs
// file and one regular file.
type privateHub struct {
	hub     *hftest.Hub
	casURL  string
	issuer  *auth.Issuer
	weights []byte
	readme  []byte
	seeded  int // hub requests the seeding commit made
}

func newPrivateHub(t *testing.T) *privateHub {
	t.Helper()
	ctx := t.Context()
	issuer, err := auth.NewIssuer(nil, time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	stor, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	cas := httptest.NewServer(server.NewHandler(server.WithStorage(stor), server.WithAuthorizer(issuer)))
	t.Cleanup(cas.Close)
	hub := hftest.NewHub(t, hftest.Options{Repo: privateRepo, Token: privateToken, CAS: cas.URL, Issuer: issuer, Storage: stor})

	weights := make([]byte, 256*1024)
	if _, err := rand.Read(weights); err != nil {
		t.Fatal(err)
	}
	readme := []byte("private repository regression fixture: a regular file the hub serves itself\n")
	if _, err := upload.UploadFile(ctx, testCAS{storage: stor}, bytes.NewReader(weights), upload.WithEnableSHA256(true)); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(weights)
	var payload bytes.Buffer
	enc := json.NewEncoder(&payload)
	for _, line := range []map[string]any{
		{"key": "header", "value": map[string]any{"summary": "seed the private repository", "description": ""}},
		{"key": "lfsFile", "value": map[string]any{"algo": "sha256", "oid": hex.EncodeToString(sum[:]), "path": weightsPath, "size": len(weights)}},
		{"key": "file", "value": map[string]any{"content": readme, "encoding": "base64", "path": readmePath}},
	} {
		if err := enc.Encode(line); err != nil {
			t.Fatal(err)
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, hub.URL+"/api/models/"+privateRepo+"/commit/main", &payload)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("Authorization", "Bearer "+privateToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("seed commit: status %d, body %q, %v", resp.StatusCode, body, err)
	}
	if len(hub.Commits()) != 1 {
		t.Fatalf("commits after seeding = %v, want one", hub.Commits())
	}
	return &privateHub{hub: hub, casURL: cas.URL, issuer: issuer, weights: weights, readme: readme, seeded: len(hub.Requests())}
}

// upstreamRequests returns what the hub received after seeding: the mirror's traffic.
func (p *privateHub) upstreamRequests() []hftest.Request {
	return p.hub.Requests()[p.seeded:]
}

func (p *privateHub) resolveURL(base, path string) string {
	return base + "/" + privateRepo + "/resolve/main/" + path
}

// roundTrip sends one downstream request through hc, with authorization as
// the Authorization header when set, and returns the response with its body.
func roundTrip(t *testing.T, hc *http.Client, method, url, authorization string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, body
}

func routesOf(reqs []hftest.Request) []string {
	routes := make([]string, 0, len(reqs))
	for _, req := range reqs {
		routes = append(routes, req.Route())
	}
	return routes
}

// assertAuthorization fails when the upstream saw no request or one whose Authorization differs from want.
func assertAuthorization(t *testing.T, reqs []hftest.Request, want string) {
	t.Helper()
	if len(reqs) == 0 {
		t.Fatal("the upstream saw no request")
	}
	for _, req := range reqs {
		if req.Authorization != want {
			t.Fatalf("upstream request %s %s carried Authorization %q, want %q", req.Method, req.Path, req.Authorization, want)
		}
	}
}

type casToken struct {
	CASURL string `json:"casUrl"`
	Token  string `json:"accessToken"`
}

// A private upstream is served to anonymous downstream clients on the
// configured token alone: resolve, HEAD metadata, regular files, misses, the
// tree rewrite and the write-token fall-through reach the hub as the mirror,
// never as the downstream, while read tokens are minted locally.
func TestMirrorPrivateUpstream(t *testing.T) {
	p := newPrivateHub(t)
	fx := newHubFixtureSelector(t, staticUpstream(t, p.hub.URL, privateToken), nil, t.TempDir(), t.TempDir())
	weightsURL := p.resolveURL(fx.srv.URL, weightsPath)
	readmeURL := p.resolveURL(fx.srv.URL, readmePath)
	sum := sha256.Sum256(p.weights)
	var localHash string

	t.Run("anonymous GET served while ingesting", func(t *testing.T) {
		resp, body := roundTrip(t, http.DefaultClient, http.MethodGet, weightsURL, "")
		if resp.StatusCode != http.StatusOK || !bytes.Equal(body, p.weights) {
			t.Fatalf("status = %d, %d bytes; want 200 and %d bytes", resp.StatusCode, len(body), len(p.weights))
		}
	})

	t.Run("HEAD answered from the pinned metadata", func(t *testing.T) {
		waitReady(t, weightsURL)
		resp, _ := roundTrip(t, http.DefaultClient, http.MethodHead, weightsURL, "")
		if resp.StatusCode != http.StatusOK || resp.Request.URL.String() != weightsURL || resp.Header.Get("Location") != "" {
			t.Fatalf("HEAD status %d at %q, Location %q; want 200 at %q", resp.StatusCode, resp.Request.URL, resp.Header.Get("Location"), weightsURL)
		}
		hash, err := xet.ParseFileHash(resp.Header.Get("X-Xet-Hash"))
		if err != nil || hash == (xet.FileHash{}) {
			t.Fatalf("X-Xet-Hash = %q: %v", resp.Header.Get("X-Xet-Hash"), err)
		}
		localHash = hash.String()
		if got, want := resp.Header.Get("ETag"), `"`+hex.EncodeToString(sum[:])+`"`; got != want {
			t.Fatalf("ETag = %s, want %s", got, want)
		}
		if got := resp.Header.Get("X-Repo-Commit"); got != p.hub.Head() {
			t.Fatalf("X-Repo-Commit = %q, want the upstream head %s", got, p.hub.Head())
		}
		if got := resp.Header.Get("X-Linked-Size"); got != strconv.Itoa(len(p.weights)) {
			t.Fatalf("X-Linked-Size = %q, want %d", got, len(p.weights))
		}
	})

	t.Run("cached GET needs no upstream", func(t *testing.T) {
		before := len(p.hub.Requests())
		resp, body := roundTrip(t, http.DefaultClient, http.MethodGet, weightsURL, "")
		if resp.StatusCode != http.StatusOK || !bytes.Equal(body, p.weights) {
			t.Fatalf("status = %d, %d bytes; want 200 and %d bytes", resp.StatusCode, len(body), len(p.weights))
		}
		if n := len(p.hub.Requests()) - before; n != 0 {
			t.Fatalf("cached GET made %d upstream requests, want 0", n)
		}
	})

	t.Run("regular file with a downstream credential", func(t *testing.T) {
		resp, body := roundTrip(t, http.DefaultClient, http.MethodGet, readmeURL, "Bearer downstream-junk")
		if resp.StatusCode != http.StatusOK || !bytes.Equal(body, p.readme) {
			t.Fatalf("status = %d, body %q; want 200 and the committed content", resp.StatusCode, body)
		}
		waitReady(t, readmeURL)
	})

	t.Run("missing file", func(t *testing.T) {
		resp, _ := roundTrip(t, noRedirect(), http.MethodGet, p.resolveURL(fx.srv.URL, "regression/missing.bin"), "")
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", resp.StatusCode)
		}
	})

	t.Run("tree rewrite", func(t *testing.T) {
		resp, body := roundTrip(t, http.DefaultClient, http.MethodGet, fx.srv.URL+"/api/models/"+privateRepo+"/tree/main", "Bearer downstream-junk")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, body %q; want 200", resp.StatusCode, body)
		}
		var items []map[string]any
		if err := json.Unmarshal(body, &items); err != nil {
			t.Fatal(err)
		}
		if len(items) != 2 {
			t.Fatalf("tree lists %d entries, want the two committed files", len(items))
		}
		for _, item := range items {
			hash, ok := item["xetHash"]
			switch item["path"] {
			case weightsPath:
				if hash != localHash {
					t.Fatalf("%s xetHash = %v, want the mirror's %s", weightsPath, hash, localHash)
				}
			case readmePath:
				if ok {
					t.Fatalf("%s carries xetHash %v", readmePath, hash)
				}
			default:
				t.Fatalf("unexpected tree entry %v", item["path"])
			}
		}
		reqs := p.upstreamRequests()
		if last := reqs[len(reqs)-1]; last.Route() != "GET tree" || last.Authorization != "Bearer <redacted>" {
			t.Fatalf("last upstream request = %s %s with Authorization %q", last.Method, last.Path, last.Authorization)
		}
	})

	t.Run("write token falls through to the upstream", func(t *testing.T) {
		resp, body := roundTrip(t, http.DefaultClient, http.MethodGet, fx.srv.URL+"/api/models/"+privateRepo+"/xet-write-token/main", "Bearer downstream-junk")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, body %q; want 200", resp.StatusCode, body)
		}
		var tok casToken
		if err := json.Unmarshal(body, &tok); err != nil {
			t.Fatal(err)
		}
		if tok.CASURL != p.casURL {
			t.Fatalf("casUrl = %q, want the upstream CAS %s", tok.CASURL, p.casURL)
		}
		if grant, ok := p.issuer.Validate(tok.Token); !ok || grant.Permission != auth.Write {
			t.Fatalf("accessToken is not an upstream write token: %+v, %v", grant, ok)
		}
		reqs := p.upstreamRequests()
		if last := reqs[len(reqs)-1]; last.Route() != "GET xet-write-token" || last.Authorization != "Bearer <redacted>" {
			t.Fatalf("last upstream request = %s %s with Authorization %q", last.Method, last.Path, last.Authorization)
		}
	})

	t.Run("read token minted locally", func(t *testing.T) {
		before := len(p.hub.Requests())
		resp, body := roundTrip(t, http.DefaultClient, http.MethodGet, fx.srv.URL+"/api/models/"+privateRepo+"/xet-read-token/main", "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, body %q; want 200", resp.StatusCode, body)
		}
		var tok casToken
		if err := json.Unmarshal(body, &tok); err != nil {
			t.Fatal(err)
		}
		if tok.CASURL != fx.srv.URL {
			t.Fatalf("casUrl = %q, want the mirror %s", tok.CASURL, fx.srv.URL)
		}
		if grant, ok := fx.issuer.Validate(tok.Token); !ok || grant.Permission != auth.Read {
			t.Fatalf("accessToken is not a mirror read token: %+v, %v", grant, ok)
		}
		if n := len(p.hub.Requests()) - before; n != 0 {
			t.Fatalf("local read token made %d upstream requests, want 0", n)
		}
	})

	t.Run("upstream saw only the configured token", func(t *testing.T) {
		reqs := p.upstreamRequests()
		assertAuthorization(t, reqs, "Bearer <redacted>")
		routes := routesOf(reqs)
		for _, want := range []string{"HEAD resolve", "GET xet-read-token", "GET resolve", "GET tree", "GET xet-write-token"} {
			if !slices.Contains(routes, want) {
				t.Fatalf("upstream routes %q lack %s", routes, want)
			}
		}
	})
}

// assertUpstreamDenied pins the downstream answer to a credential the private
// upstream rejects: 502 naming the upstream 401 on GET and HEAD, at most three
// upstream attempts for the two requests, all carrying wantAuth.
func assertUpstreamDenied(t *testing.T, p *privateHub, fx *hubFixture, wantAuth string) {
	t.Helper()
	weightsURL := p.resolveURL(fx.srv.URL, weightsPath)
	resp, body := roundTrip(t, noRedirect(), http.MethodGet, weightsURL, "")
	if resp.StatusCode != http.StatusBadGateway || !strings.Contains(string(body), "upstream status 401") {
		t.Fatalf("GET status = %d, body %q; want 502 naming upstream status 401", resp.StatusCode, body)
	}
	resp, _ = roundTrip(t, noRedirect(), http.MethodHead, weightsURL, "")
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("HEAD status = %d, want 502", resp.StatusCode)
	}
	reqs := p.upstreamRequests()
	if len(reqs) > 3 {
		t.Fatalf("upstream saw %d requests for two downstream requests, want at most 3: %q", len(reqs), routesOf(reqs))
	}
	assertAuthorization(t, reqs, wantAuth)
	if routes := routesOf(reqs); !slices.Contains(routes, "HEAD resolve") {
		t.Fatalf("upstream routes = %q, want a HEAD resolve probe", routes)
	}
}

func TestMirrorPrivateUpstreamWithoutToken(t *testing.T) {
	p := newPrivateHub(t)
	fx := newHubFixtureSelector(t, staticUpstream(t, p.hub.URL, ""), nil, t.TempDir(), t.TempDir())
	assertUpstreamDenied(t, p, fx, "")
}

func TestMirrorPrivateUpstreamWrongToken(t *testing.T) {
	p := newPrivateHub(t)
	fx := newHubFixtureSelector(t, staticUpstream(t, p.hub.URL, "not-the-secret"), nil, t.TempDir(), t.TempDir())
	assertUpstreamDenied(t, p, fx, "Bearer <wrong>")
}

// Revalidating a ready file re-probes the private upstream with the configured token.
func TestMirrorPrivateUpstreamRevalidation(t *testing.T) {
	p := newPrivateHub(t)
	fx := newHubFixtureSelector(t, staticUpstream(t, p.hub.URL, privateToken), nil, t.TempDir(), t.TempDir(), mirror.WithRevalidateInterval(0))
	weightsURL := p.resolveURL(fx.srv.URL, weightsPath)

	resp, body := roundTrip(t, http.DefaultClient, http.MethodGet, weightsURL, "")
	if resp.StatusCode != http.StatusOK || !bytes.Equal(body, p.weights) {
		t.Fatalf("status = %d, %d bytes; want 200 and %d bytes", resp.StatusCode, len(body), len(p.weights))
	}
	waitReady(t, weightsURL)

	before := len(p.hub.Requests())
	resp, body = roundTrip(t, http.DefaultClient, http.MethodGet, weightsURL, "")
	if resp.StatusCode != http.StatusOK || !bytes.Equal(body, p.weights) {
		t.Fatalf("revalidated status = %d, %d bytes; want 200 and %d bytes", resp.StatusCode, len(body), len(p.weights))
	}
	if routes := routesOf(p.hub.Requests()[before:]); !slices.Contains(routes, "HEAD resolve") {
		t.Fatalf("revalidating GET made upstream requests %q, want a HEAD resolve probe", routes)
	}
	assertAuthorization(t, p.upstreamRequests(), "Bearer <redacted>")
}
