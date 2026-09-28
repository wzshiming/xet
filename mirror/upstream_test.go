package mirror

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage/local"
	"github.com/wzshiming/xet/upload"
)

// xetUpstream is a hub+CAS pair serving one xet file: the hub hands out casTokens in turn (repeating the last, issuing every earlier one inside the client's renewal margin) and both record the Authorization headers they see.
type xetUpstream struct {
	hubURL   string
	sha256   string
	seenAuth sync.Map // CAS: Authorization header -> true

	mu        sync.Mutex
	tokenAuth []string // Authorization of each xet-read-token request, in order
	hubAuth   string   // Authorization of the last resolve request
}

func (u *xetUpstream) tokenRequests() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return slices.Clone(u.tokenAuth)
}

func newXetUpstream(t *testing.T, resolvePath string, data []byte, casTokens ...string) *xetUpstream {
	t.Helper()
	u := &xetUpstream{}
	var cas http.Handler
	casSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.seenAuth.Store(r.Header.Get("Authorization"), true)
		cas.ServeHTTP(w, r)
	}))
	t.Cleanup(casSrv.Close)
	stor, err := local.NewStorage(local.WithBasePath(t.TempDir()), local.WithBaseURL(casSrv.URL))
	if err != nil {
		t.Fatal(err)
	}
	cas = server.NewHandler(server.WithStorage(stor))
	seed := &localCAS{storage: stor, namespace: "default"}
	fileHash, err := upload.UploadFile(context.Background(), seed, bytes.NewReader(data), upload.WithEnableSHA256(true))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	u.sha256 = hex.EncodeToString(sum[:])

	var fetches atomic.Int32
	hubSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/xet-read-token":
			u.mu.Lock()
			u.tokenAuth = append(u.tokenAuth, r.Header.Get("Authorization"))
			u.mu.Unlock()
			n := min(int(fetches.Add(1)), len(casTokens))
			ttl := time.Hour
			if n < len(casTokens) {
				ttl = 30 * time.Second // inside the client's one-minute renewal margin
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"casUrl": casSrv.URL, "accessToken": casTokens[n-1], "exp": time.Now().Add(ttl).Unix()})
		case resolvePath:
			u.mu.Lock()
			u.hubAuth = r.Header.Get("Authorization")
			u.mu.Unlock()
			w.Header().Set("ETag", `"`+u.sha256+`"`)
			w.Header().Set("X-Linked-Size", fmt.Sprint(len(data)))
			w.Header().Set("X-Repo-Commit", "xet-commit-1")
			w.Header().Add("Link", fmt.Sprintf("<%s/v1/reconstructions/%s>; rel=\"xet-reconstruction-info\"", casSrv.URL, fileHash))
			w.Header().Add("Link", fmt.Sprintf("<%s/api/xet-read-token>; rel=\"xet-auth\"", u.hubURL))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(hubSrv.Close)
	u.hubURL = hubSrv.URL
	return u
}

// Two files on different xet upstreams share one mirror client: each hub and CAS sees only the tokens of its own URL.
func TestMirrorXetUpstreamPerURL(t *testing.T) {
	dataA, dataB := []byte("xet bytes from upstream A"), []byte("xet bytes from upstream B")
	upA := newXetUpstream(t, "/org/a/resolve/main/f.bin", dataA, "cas-token-a")
	upB := newXetUpstream(t, "/org/b/resolve/main/f.bin", dataB, "cas-token-b")
	m, stor := newTestMirror(t, "http://unused.invalid", t.TempDir(), t.TempDir())

	inA, err := m.Mirror.Ingest(upA.hubURL+"/org/a/resolve/main/f.bin", "hub-token-a")
	if err != nil {
		t.Fatal(err)
	}
	inB, err := m.Mirror.Ingest(upB.hubURL+"/org/b/resolve/main/f.bin", "hub-token-b")
	if err != nil {
		t.Fatal(err)
	}
	awaitClosed(t, inA.Done(), "upstream A ingest")
	awaitClosed(t, inB.Done(), "upstream B ingest")
	for _, tc := range []struct {
		name         string
		in           *Ingestion
		up           *xetUpstream
		data         []byte
		token, other string
		hubToken     string
	}{
		{"A", inA, upA, dataA, "Bearer cas-token-a", "Bearer cas-token-b", "Bearer hub-token-a"},
		{"B", inB, upB, dataB, "Bearer cas-token-b", "Bearer cas-token-a", "Bearer hub-token-b"},
	} {
		entry, err := tc.in.Entry()
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if got := readStored(t, stor, entry.SHA256); !bytes.Equal(got, tc.data) {
			t.Fatalf("%s: stored %q, want %q", tc.name, got, tc.data)
		}
		if _, ok := tc.up.seenAuth.Load(tc.token); !ok {
			t.Fatalf("%s: CAS did not receive its own token", tc.name)
		}
		if _, ok := tc.up.seenAuth.Load(tc.other); ok {
			t.Fatalf("%s: CAS received the other upstream's token", tc.name)
		}
		if got := tc.up.tokenRequests(); !slices.Equal(got, []string{tc.hubToken}) {
			t.Fatalf("%s: xet-read-token Authorization = %q, want [%q]", tc.name, got, tc.hubToken)
		}
	}
}

// Files are identified by their hub path alone: a second origin serving the same path is answered from the first one's ingest without being contacted, which is why a mirror must route each repository to one upstream.
func TestMirrorIdentityIgnoresOrigin(t *testing.T) {
	const path = "/org/repo/resolve/main/f.bin"
	dataA := []byte("bytes on upstream A")
	upA, upB := newPlainUpstream(), newPlainUpstream()
	upA.set(path, dataA)
	upB.set(path, []byte("bytes on upstream B"))
	srvA, srvB := httptest.NewServer(upA), httptest.NewServer(upB)
	t.Cleanup(srvA.Close)
	t.Cleanup(srvB.Close)
	m, stor := newTestMirror(t, srvA.URL, t.TempDir(), t.TempDir())

	for _, tc := range []struct{ url, token string }{{srvA.URL + path, "tok-a"}, {srvB.URL + path, "tok-b"}} {
		in, err := m.Mirror.Ingest(tc.url, tc.token)
		if err != nil {
			t.Fatal(err)
		}
		awaitClosed(t, in.Done(), tc.url)
		entry, err := in.Entry()
		if err != nil {
			t.Fatal(err)
		}
		if got := readStored(t, stor, entry.SHA256); !bytes.Equal(got, dataA) {
			t.Fatalf("%s: stored %q, want the first ingest's %q", tc.url, got, dataA)
		}
	}
	if _, ok := upB.seenAuth.Load("Bearer tok-b"); ok || upB.dataGETs.Load() != 0 {
		t.Fatal("upstream B was contacted for a path the mirror already holds")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// authInjector sends the token to the marked origin only: the same host over another scheme or port, and any other host, stay anonymous.
func TestAuthInjectorOrigin(t *testing.T) {
	var seen []string
	rt := &authInjector{inner: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		seen = append(seen, r.URL.String()+" "+r.Header.Get("Authorization"))
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Request: r}, nil
	})}
	ctx := withUpstreamAuth(t.Context(), "https://hub.example", "secret")
	for _, target := range []string{"https://hub.example/org/repo/resolve/main/f", "http://hub.example/org/repo/resolve/main/f", "https://hub.example:8443/org/repo/resolve/main/f", "https://cdn.example/org/repo/resolve/main/f"} {
		req, err := http.NewRequestWithContext(ctx, http.MethodHead, target, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := rt.RoundTrip(req); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{"https://hub.example/org/repo/resolve/main/f Bearer secret", "http://hub.example/org/repo/resolve/main/f ", "https://hub.example:8443/org/repo/resolve/main/f ", "https://cdn.example/org/repo/resolve/main/f "}
	if !slices.Equal(seen, want) {
		t.Fatalf("authorization by request:\n got %q\nwant %q", seen, want)
	}
}

// The hub token of the resolver that started the ingest authenticates the resolve, the xet-read-token fetch, and the renewal of a CAS token issued inside the expiry margin; the CAS only ever sees the renewed token.
func TestMirrorXetHubTokenRenewal(t *testing.T) {
	data := []byte("xet bytes behind a rotating CAS token")
	up := newXetUpstream(t, "/org/repo/resolve/main/f.bin", data, "cas-1", "cas-2")
	m, stor := newTestMirror(t, up.hubURL, t.TempDir(), t.TempDir())
	m.token = "hub-secret"

	entry, err := ingestWait(t, m, "org/repo", "main", "f.bin")
	if err != nil {
		t.Fatal(err)
	}
	if got := readStored(t, stor, entry.SHA256); !bytes.Equal(got, data) {
		t.Fatalf("stored %q, want %q", got, data)
	}
	if got := up.tokenRequests(); !slices.Equal(got, []string{"Bearer hub-secret", "Bearer hub-secret"}) {
		t.Fatalf("xet-read-token Authorization = %q, want the hub token on the fetch and the renewal", got)
	}
	if _, ok := up.seenAuth.Load("Bearer cas-2"); !ok {
		t.Fatal("CAS never saw the renewed token")
	}
	if _, ok := up.seenAuth.Load("Bearer cas-1"); ok {
		t.Fatal("CAS saw the expiring token")
	}
	up.mu.Lock()
	defer up.mu.Unlock()
	if up.hubAuth != "Bearer hub-secret" {
		t.Fatalf("resolve Authorization = %q, want the hub token", up.hubAuth)
	}
}

// The first resolver's origin and token stay pinned on the ingest: a resolver joining with another token never reaches the upstream.
func TestMirrorPinsStarterToken(t *testing.T) {
	upstream := newPlainUpstream()
	upstream.gate = make(chan struct{})
	upstream.gateHit = make(chan struct{})
	data := make([]byte, 128*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	upstream.set("/org/repo/resolve/main/f.bin", data)
	srv := httptest.NewServer(upstream)
	t.Cleanup(srv.Close)
	var release sync.Once
	defer release.Do(func() { close(upstream.gate) }) // Close blocks on the gated handler

	m, stor := newTestMirror(t, srv.URL, t.TempDir(), t.TempDir())
	ctx := context.Background()
	target := resolveURL(srv.URL, "org/repo", "main", "f.bin")

	first, err := m.Mirror.Resolve(ctx, target, "tok-a")
	if err != nil || first.Stream == nil {
		t.Fatalf("first Resolve = %+v, %v; want an in-flight stream", first, err)
	}
	awaitClosed(t, upstream.gateHit, "upstream transfer")
	second, err := m.Mirror.Resolve(ctx, target, "tok-b")
	if err != nil || second.Stream == nil || second.Stream.t != first.Stream.t {
		t.Fatalf("second Resolve = %+v, %v; want to join the first task", second, err)
	}
	release.Do(func() { close(upstream.gate) })
	awaitClosed(t, first.Stream.t.done, "ingest")

	res, err := m.Mirror.Resolve(ctx, target, "tok-b")
	if err != nil || res.Entry == nil {
		t.Fatalf("Resolve after ingest = %+v, %v; want the ready entry", res, err)
	}
	if got := readStored(t, stor, res.Entry.SHA256); !bytes.Equal(got, data) {
		t.Fatal("stored bytes differ from upstream data")
	}
	if _, ok := upstream.seenAuth.Load("Bearer tok-a"); !ok {
		t.Fatal("upstream never saw the starter's token")
	}
	if _, ok := upstream.seenAuth.Load("Bearer tok-b"); ok {
		t.Fatal("upstream saw the joiner's token")
	}
}

// URLs that are not hub download URLs are rejected before any upstream request.
func TestMirrorRejectsNonResolveURL(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)
	m, _ := newTestMirror(t, srv.URL, t.TempDir(), t.TempDir())
	ctx := context.Background()

	for _, raw := range []string{srv.URL + "/org/repo/tree/main", srv.URL + "/org/repo/resolve/main/", "hub.example/org/repo/resolve/main/f.bin", "://x"} {
		if _, err := m.Mirror.Resolve(ctx, raw, ""); err == nil {
			t.Errorf("Resolve(%q) succeeded", raw)
		}
		if _, err := m.Mirror.Ingest(raw, ""); err == nil {
			t.Errorf("Ingest(%q) succeeded", raw)
		}
	}
	if _, err := m.FetchUpstream(ctx, "hub.example/api/models/org/repo", ""); err == nil {
		t.Error("FetchUpstream without a scheme succeeded")
	}
	if n := requests.Load(); n != 0 {
		t.Fatalf("upstream requests = %d, want none", n)
	}
}
