package hftest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"maps"
	"math/rand/v2"
	"mime"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/auth"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/local"
	"github.com/wzshiming/xet/upload"
)

// storeCAS adapts a store to upload.ClientAdapter so the test seeds the CAS without an HTTP hop.
type storeCAS struct{ storage.Storage }

func (s storeCAS) HasXorb(ctx context.Context, h xet.XorbHash) (bool, error) {
	return s.Storage.HasXorb(ctx, "default", h)
}

func (s storeCAS) UploadXorb(ctx context.Context, h xet.XorbHash, r io.ReadSeeker) (*upload.XorbUploadResponse, error) {
	inserted, err := s.Storage.PutXorb(ctx, "default", h, r)
	return &upload.XorbUploadResponse{WasInserted: inserted}, err
}

func (s storeCAS) UploadShard(ctx context.Context, sh *shard.Shard) (*upload.ShardUploadResponse, error) {
	if _, err := s.Storage.PutShard(ctx, sh); err != nil {
		return nil, err
	}
	return &upload.ShardUploadResponse{Result: 1}, nil
}

func (s storeCAS) QueryDedupShards(_ context.Context, hashes []xet.ChunkHash, _ ...xet.ChunkHash) (map[xet.ChunkHash]*upload.DeduplicationResult, error) {
	out := make(map[xet.ChunkHash]*upload.DeduplicationResult, len(hashes))
	for _, h := range hashes {
		out[h] = &upload.DeduplicationResult{ChunkHash: h, IsNew: true}
	}
	return out, nil
}

// deterministic returns n pseudo-random bytes that are the same on every run.
func deterministic(n int) []byte {
	data := make([]byte, n)
	_, _ = rand.NewChaCha8([32]byte{1: 0x58, 2: 0x45, 3: 0x54}).Read(data)
	return data
}

// noise lists the recorded response headers that come from the hub's infrastructure rather than its API.
var noise = map[string]bool{"X-Amz-Cf-Id": true, "X-Amz-Cf-Pop": true, "X-Cache": true, "X-Hub-Cache": true, "X-Powered-By": true, "X-Request-Id": true}

var commitSHA = regexp.MustCompile(`\b[0-9a-f]{40}\b`)

// exchange returns the first hub exchange of tr with the route and status whose request carried a bearer token or not, as asked.
func exchange(t *testing.T, tr *Trace, route string, status int, bearer bool) Exchange {
	t.Helper()
	for _, ex := range tr.Hub() {
		if Route(ex) == route && ex.Status == status && (ex.RequestHeaders.Get("Authorization") != "") == bearer {
			return ex
		}
	}
	t.Fatalf("%s: no %s exchange with status %d (bearer=%v)", tr.Scenario, route, status, bearer)
	return Exchange{}
}

// replay sends ex's request to hub with cred as Authorization, recorded commit oids swapped for hub's head and body (nil: the recorded one) as payload.
func replay(t *testing.T, hub *Hub, ex Exchange, cred string, body []byte) (*http.Response, []byte) {
	t.Helper()
	if body == nil && ex.RequestBody != nil {
		body = []byte(ex.RequestBody.Text)
	}
	req, err := http.NewRequestWithContext(t.Context(), ex.Method, hub.URL+commitSHA.ReplaceAllString(ex.Path, hub.Head()), bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if ct := ex.RequestHeaders.Get("Content-Type"); ct != "" {
		req.Header.Set("Content-Type", ct)
	}
	if cred != "" {
		req.Header.Set("Authorization", cred)
	}
	hc := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, data
}

func mediaType(contentType string) string {
	mt, _, _ := mime.ParseMediaType(contentType)
	return mt
}

func linkRels(values []string) []string {
	rels := make([]string, 0, 2)
	for rel := range client.ParseLinkHeaders(values) {
		rels = append(rels, rel)
	}
	slices.Sort(rels)
	return rels
}

// assertShape checks resp against ex: same status, every significant recorded header present, same media type and Link rels.
func assertShape(t *testing.T, ex Exchange, resp *http.Response) {
	t.Helper()
	what := ex.Method + " " + ex.Path
	if resp.StatusCode != ex.Status {
		t.Fatalf("%s: status = %d, want %d", what, resp.StatusCode, ex.Status)
	}
	for name := range ex.ResponseHeaders {
		name = http.CanonicalHeaderKey(name)
		significant := strings.HasPrefix(name, "X-") || name == "Link" || name == "Location" || name == "Www-Authenticate" || name == "Etag"
		if !significant || noise[name] {
			continue
		}
		if resp.Header.Get(name) == "" {
			t.Errorf("%s: response lacks the recorded header %s", what, name)
		}
	}
	if want := mediaType(ex.ResponseHeaders.Get("Content-Type")); want != "" {
		if got := mediaType(resp.Header.Get("Content-Type")); got != want {
			t.Errorf("%s: media type = %q, want %q", what, got, want)
		}
	}
	if want := linkRels(ex.ResponseHeaders.Values("Link")); len(want) > 0 {
		if got := linkRels(resp.Header.Values("Link")); !slices.Equal(got, want) {
			t.Errorf("%s: Link rels = %q, want %q", what, got, want)
		}
	}
}

// assertKeys checks body has the top-level keys of ex's recorded JSON body.
func assertKeys(t *testing.T, ex Exchange, body []byte) {
	t.Helper()
	if got, want := JSONKeys(string(body)), JSONKeys(ex.ResponseBody.Text); !slices.Equal(got, want) {
		t.Errorf("%s %s: body keys = %q, want %q", ex.Method, ex.Path, got, want)
	}
}

type preuploadEntry struct {
	Path       string `json:"path"`
	UploadMode string `json:"uploadMode"`
	OID        string `json:"oid"`
}

func preuploadEntries(t *testing.T, body string) map[string]preuploadEntry {
	t.Helper()
	var resp struct {
		Files []preuploadEntry `json:"files"`
	}
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		t.Fatal(err)
	}
	out := make(map[string]preuploadEntry, len(resp.Files))
	for _, f := range resp.Files {
		out[f.Path] = f
	}
	return out
}

// entryKeys returns the key set of each files[] entry of a preupload response.
func entryKeys(t *testing.T, body string) [][]string {
	t.Helper()
	var resp struct {
		Files []json.RawMessage `json:"files"`
	}
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		t.Fatal(err)
	}
	keys := make([][]string, 0, len(resp.Files))
	for _, f := range resp.Files {
		keys = append(keys, JSONKeys(string(f)))
	}
	return keys
}

// commitPayload rewrites the recorded commit's lfsFile line to oid and size so it names the seeded file.
func commitPayload(t *testing.T, ex Exchange, oid string, size int) []byte {
	t.Helper()
	var out bytes.Buffer
	for line := range strings.SplitSeq(strings.TrimSpace(ex.RequestBody.Text), "\n") {
		var l struct {
			Key   string         `json:"key"`
			Value map[string]any `json:"value"`
		}
		if err := json.Unmarshal([]byte(line), &l); err != nil {
			t.Fatal(err)
		}
		if l.Key == "lfsFile" {
			l.Value["oid"], l.Value["size"] = oid, size
		}
		b, err := json.Marshal(l)
		if err != nil {
			t.Fatal(err)
		}
		out.Write(b)
		out.WriteByte('\n')
	}
	return out.Bytes()
}

// The fake hub answers every kind of exchange the hf CLI recorded with the recorded status, headers, media types, JSON key sets and Link rels, with the repository's real values filled in.
func TestHubMatchesRecordedShapes(t *testing.T) {
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
	const token = "Bearer hub-secret"
	hub := NewHub(t, Options{Token: "hub-secret", CAS: cas.URL, Issuer: issuer, Storage: stor})

	refUpload, refReupload, refPlain, refXet, refAnon := Fixture(t, "ref-upload"), Fixture(t, "ref-reupload"), Fixture(t, "ref-download-plain"), Fixture(t, "ref-download-xet"), Fixture(t, "ref-anonymous")

	lfsData := deterministic(1 << 20)
	hash, err := upload.UploadFile(ctx, storeCAS{stor}, bytes.NewReader(lfsData), upload.WithEnableSHA256(true))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(lfsData)
	lfsSHA := hex.EncodeToString(sum[:])

	// Every recorded 401, replayed anonymously and with a wrong token.
	for _, ex := range refAnon.Hub() {
		if ex.Status != http.StatusUnauthorized {
			continue
		}
		cred := ""
		if ex.RequestHeaders.Get("Authorization") != "" {
			cred = "Bearer hf_wrong_token"
		}
		resp, body := replay(t, hub, ex, cred, nil)
		assertShape(t, ex, resp)
		if ex.Method != http.MethodHead {
			assertKeys(t, ex, body)
		}
		if got := resp.Header.Get("X-Error-Message"); got != ex.ResponseHeaders.Get("X-Error-Message") {
			t.Errorf("%s: X-Error-Message = %q, want %q", ex.Path, got, ex.ResponseHeaders.Get("X-Error-Message"))
		}
	}

	ex := exchange(t, refUpload, "POST repos/create", http.StatusConflict, true)
	resp, body := replay(t, hub, ex, token, nil)
	assertShape(t, ex, resp)
	assertKeys(t, ex, body)

	ex = exchange(t, refUpload, "POST preupload", http.StatusOK, true)
	resp, body = replay(t, hub, ex, token, nil)
	assertShape(t, ex, resp)
	assertKeys(t, ex, body)
	if got, want := entryKeys(t, string(body)), entryKeys(t, ex.ResponseBody.Text); !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("preupload entry keys = %q, want %q", got, want)
	}
	for path, want := range preuploadEntries(t, ex.ResponseBody.Text) {
		if got := preuploadEntries(t, string(body))[path].UploadMode; got != want.UploadMode {
			t.Errorf("preupload mode of %s = %q, want %q", path, got, want.UploadMode)
		}
	}

	for _, tc := range []struct {
		tr    *Trace
		route string
	}{{refUpload, "GET xet-write-token"}, {refXet, "GET xet-read-token"}} {
		ex = exchange(t, tc.tr, tc.route, http.StatusOK, true)
		resp, body = replay(t, hub, ex, token, nil)
		assertShape(t, ex, resp)
		assertKeys(t, ex, body)
		if resp.Header.Get("X-Xet-Cas-Url") != cas.URL {
			t.Errorf("%s: X-Xet-Cas-Url = %q, want the CAS", tc.route, resp.Header.Get("X-Xet-Cas-Url"))
		}
		if _, ok := issuer.Validate(resp.Header.Get("X-Xet-Access-Token")); !ok {
			t.Errorf("%s: X-Xet-Access-Token is not a token the issuer signed", tc.route)
		}
	}

	// A commit naming an lfs object nobody uploaded is rejected as recorded.
	ex = exchange(t, refAnon, "POST commit", http.StatusBadRequest, true)
	resp, body = replay(t, hub, ex, token, nil)
	assertShape(t, ex, resp)
	assertKeys(t, ex, body)
	if got := resp.Header.Get("X-Error-Message"); got != ex.ResponseHeaders.Get("X-Error-Message") {
		t.Errorf("dangling lfs X-Error-Message = %q, want %q", got, ex.ResponseHeaders.Get("X-Error-Message"))
	}
	if len(hub.Commits()) != 0 {
		t.Fatalf("commits after the rejected commit = %v, want none", hub.Commits())
	}

	ex = exchange(t, refUpload, "POST commit", http.StatusOK, true)
	resp, body = replay(t, hub, ex, token, commitPayload(t, ex, lfsSHA, len(lfsData)))
	assertShape(t, ex, resp)
	assertKeys(t, ex, body)
	var created struct {
		CommitOID string `json:"commitOid"`
		CommitURL string `json:"commitUrl"`
	}
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	if created.CommitOID == "" || created.CommitOID != hub.Head() || !slices.Equal(hub.Commits(), []string{created.CommitOID}) {
		t.Fatalf("commit %q; head %q, commits %v", created.CommitOID, hub.Head(), hub.Commits())
	}
	if want := "https://huggingface.co/wzshiming/test/commit/" + created.CommitOID; created.CommitURL != want {
		t.Fatalf("commitUrl = %q, want %q", created.CommitURL, want)
	}

	// Preuploading the committed lfs file again reports its oid like the recorded re-upload.
	ex = exchange(t, refReupload, "POST preupload", http.StatusOK, true)
	resp, body = replay(t, hub, ex, token, nil)
	assertShape(t, ex, resp)
	if got, want := entryKeys(t, string(body)), entryKeys(t, ex.ResponseBody.Text); !slices.EqualFunc(got, want, slices.Equal) {
		t.Errorf("re-upload preupload entry keys = %q, want %q", got, want)
	}
	if got := preuploadEntries(t, string(body))["regression/ref-lfs.bin"].OID; got != lfsSHA {
		t.Errorf("re-upload preupload oid = %q, want the committed sha256 %s", got, lfsSHA)
	}

	// Resolving the lfs file redirects with both xet links; the regular file is served with its git blob ETag; a missing entry is a recorded 404.
	for _, method := range []string{http.MethodHead, http.MethodGet} {
		ex = exchange(t, refPlain, method+" resolve", http.StatusFound, true)
		resp, body = replay(t, hub, ex, token, nil)
		assertShape(t, ex, resp)
		links := client.ParseLinkHeaders(resp.Header.Values("Link"))
		if got, want := links["xet-reconstruction-info"], cas.URL+"/v1/reconstructions/"+hash.String(); got != want {
			t.Errorf("%s resolve reconstruction link = %q, want %q", method, got, want)
		}
		if got, want := links["xet-auth"], hub.URL+"/api/models/wzshiming/test/xet-read-token/"+hub.Head(); got != want {
			t.Errorf("%s resolve xet-auth link = %q, want %q", method, got, want)
		}
		if resp.Header.Get("X-Xet-Hash") != hash.String() || resp.Header.Get("X-Linked-Etag") != `"`+lfsSHA+`"` || resp.Header.Get("X-Linked-Size") != "1048576" || resp.Header.Get("X-Repo-Commit") != hub.Head() {
			t.Errorf("%s resolve headers = %v", method, resp.Header)
		}
		if method == http.MethodGet && !strings.HasPrefix(string(body), "Found. Redirecting to "+resp.Header.Get("Location")) {
			t.Errorf("GET resolve body = %q", body)
		}

		ex = exchange(t, refPlain, method+" resolve", http.StatusOK, true)
		resp, body = replay(t, hub, ex, token, nil)
		assertShape(t, ex, resp)
		if got, want := resp.Header.Get("Etag"), ex.ResponseHeaders.Get("Etag"); got != want {
			t.Errorf("%s regular resolve ETag = %s, want the recorded git blob id %s", method, got, want)
		}
		if method == http.MethodGet && string(body) != ex.ResponseBody.Text {
			t.Errorf("GET regular resolve body = %q, want the committed content %q", body, ex.ResponseBody.Text)
		}
	}
	ex = exchange(t, refAnon, "HEAD resolve", http.StatusNotFound, true)
	resp, _ = replay(t, hub, ex, token, nil)
	assertShape(t, ex, resp)
	if resp.Header.Get("X-Error-Code") != "EntryNotFound" || resp.Header.Get("X-Repo-Commit") != hub.Head() {
		t.Errorf("missing entry headers = %v", resp.Header)
	}

	// The redirect target serves the reconstructed bytes without a credential.
	resp, body = replay(t, hub, Exchange{Method: http.MethodGet, Path: "/cdn/" + lfsSHA}, "", nil)
	if resp.StatusCode != http.StatusOK || !bytes.Equal(body, lfsData) {
		t.Fatalf("GET /cdn: status %d, %d bytes; want 200 and %d", resp.StatusCode, len(body), len(lfsData))
	}

	// revision and tree carry the recorded keys the CLI reads.
	ex = exchange(t, refPlain, "GET revision", http.StatusOK, true)
	resp, body = replay(t, hub, ex, token, nil)
	assertShape(t, ex, resp)
	recorded := JSONKeys(ex.ResponseBody.Text)
	for _, key := range JSONKeys(string(body)) {
		if !slices.Contains(recorded, key) {
			t.Errorf("revision key %q is not in the recording", key)
		}
	}
	var revision struct {
		SHA     string `json:"sha"`
		Private bool   `json:"private"`
		ID      string `json:"id"`
	}
	if err := json.Unmarshal(body, &revision); err != nil || revision.SHA != hub.Head() || !revision.Private || revision.ID != "wzshiming/test" {
		t.Errorf("revision = %+v, %v", revision, err)
	}

	ex = exchange(t, refPlain, "GET tree", http.StatusOK, true)
	resp, body = replay(t, hub, ex, token, nil)
	assertShape(t, ex, resp)
	var got, want []map[string]json.RawMessage
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(ex.ResponseBody.Text), &want); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("tree lists %d entries, want the two committed files", len(got))
	}
	for _, entry := range got {
		path := string(entry["path"])
		i := slices.IndexFunc(want, func(e map[string]json.RawMessage) bool { return string(e["path"]) == path })
		if i < 0 {
			t.Fatalf("tree entry %s is not in the recording", path)
		}
		if gotKeys, wantKeys := slices.Sorted(maps.Keys(entry)), slices.Sorted(maps.Keys(want[i])); !slices.Equal(gotKeys, wantKeys) {
			t.Errorf("tree entry %s keys = %q, want %q", path, gotKeys, wantKeys)
		}
		if lfs, ok := entry["lfs"]; ok {
			if gotKeys, wantKeys := JSONKeys(string(lfs)), JSONKeys(string(want[i]["lfs"])); !slices.Equal(gotKeys, wantKeys) || !bytes.Contains(lfs, []byte(`"pointerSize":132`)) {
				t.Errorf("tree lfs of %s = %s, want keys %q and the recorded pointer size", path, lfs, wantKeys)
			}
		}
	}

	// The request log reduces every credential and never keeps the raw token.
	for _, req := range hub.Requests() {
		if req.Authorization != "" && req.Authorization != "Bearer <redacted>" && req.Authorization != "Bearer <wrong>" || req.Header.Get("Authorization") != req.Authorization {
			t.Fatalf("request %s %s logged Authorization %q / %q", req.Method, req.Path, req.Authorization, req.Header.Get("Authorization"))
		}
	}
}

// Route and JSONKeys reduce the recorded reference to the sequences and shapes the client tests compare against.
func TestRouteReducesRecordedExchanges(t *testing.T) {
	tr := Fixture(t, "ref-upload")
	var hub, cas []string
	for _, ex := range tr.Hub() {
		hub = append(hub, Route(ex))
	}
	for _, ex := range tr.CAS() {
		cas = append(cas, Route(ex))
	}
	if want := []string{"GET agent-harnesses", "POST repos/create", "POST preupload", "GET xet-write-token", "POST commit"}; !slices.Equal(hub, want) {
		t.Fatalf("hub routes = %q, want %q", hub, want)
	}
	if want := []string{"GET chunks", "POST xorbs", "POST shards"}; !slices.Equal(cas, want) {
		t.Fatalf("CAS routes = %q, want %q", cas, want)
	}
	if got := Route(Exchange{Method: http.MethodHead, Path: "/wzshiming/test/resolve/main/regression/ref-lfs.bin"}); got != "HEAD resolve" {
		t.Fatalf("resolve route = %q", got)
	}
	if got := (Request{Method: http.MethodGet, Path: "/api/models/wzshiming/test/xet-read-token/main?1790327053"}).Route(); got != "GET xet-read-token" {
		t.Fatalf("request route = %q", got)
	}
	commit := exchange(t, tr, "POST commit", http.StatusOK, true)
	if got := JSONKeys(commit.RequestBody.Text); !slices.Equal(got, []string{"key", "value"}) {
		t.Fatalf("NDJSON first line keys = %q", got)
	}
	if got := JSONKeys("[1,2]"); got != nil {
		t.Fatalf("JSONKeys of an array = %q, want nil", got)
	}
}
