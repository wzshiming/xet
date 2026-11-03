package client

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/auth"
	"github.com/wzshiming/xet/upload"
)

// scriptedPayload is CRC-64/NVME's check input, so its checksum is the check value 0xAE8B14860A799888.
const scriptedPayload = "123456789"

var scriptedHash = xet.XorbHash{1, 2, 3}

// scriptedGrantBody is the grant request body for scriptedPayload as scriptedRequest.decoded returns it.
var scriptedGrantBody = map[string]any{
	"length":   json.Number(strconv.Itoa(len(scriptedPayload))),
	"checksum": map[string]any{"algo": "crc64nvme", "value": strconv.FormatUint(0xAE8B14860A799888, 10)},
}

// scriptedReply is one scripted answer; serve, when set, answers in place of status and body; unread answers before the request body is read, leaving it to serve.
type scriptedReply struct {
	status int
	body   string
	serve  func(http.ResponseWriter, *http.Request)
	unread bool
}

// scriptedRequest is one request a scriptedCAS received; length is its Content-Length, -1 when chunked.
type scriptedRequest struct {
	route  string
	host   string
	method string
	uri    string
	header http.Header
	body   []byte
	length int64
}

// decoded is r's JSON body with numbers kept as json.Number, so its exact shape can be compared.
func (r scriptedRequest) decoded(t *testing.T) any {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(r.body))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		t.Fatalf("%s body %q: %v", r.route, r.body, err)
	}
	return v
}

// scriptedCAS answers hash's grant, upload, commit and legacy routes from per-route scripts, else the happy path, recording every request.
type scriptedCAS struct {
	*httptest.Server
	hash xet.XorbHash

	mu      sync.Mutex
	scripts map[string][]scriptedReply
	seen    []scriptedRequest
	grants  int
}

func newScriptedCAS(t *testing.T) *scriptedCAS {
	return newScriptedCASFor(t, scriptedHash)
}

// newScriptedCASFor is a scriptedCAS for xorbHash's routes.
func newScriptedCASFor(t *testing.T, xorbHash xet.XorbHash) *scriptedCAS {
	s := &scriptedCAS{hash: xorbHash, scripts: map[string][]scriptedReply{}}
	s.Server = httptest.NewServer(s)
	t.Cleanup(s.Close)
	return s
}

// scriptedGrant is a 201 granting id: method on url with headers.
func scriptedGrant(id, method, url string, headers map[string]string) scriptedReply {
	if headers == nil {
		headers = map[string]string{}
	}
	body, _ := json.Marshal(map[string]any{"id": id, "method": method, "url": url, "headers": headers})
	return scriptedReply{status: http.StatusCreated, body: string(body)}
}

// scriptedRoute is scriptedRouteFor scriptedHash.
func scriptedRoute(r *http.Request) string {
	return scriptedRouteFor(r, scriptedHash)
}

// scriptedRouteFor names r: "grant", "upload <path after /upload/>", "commit <escaped grant id>", "legacy" or "unexpected <method> <target>", grant, commit and legacy being xorbHash's.
func scriptedRouteFor(r *http.Request, xorbHash xet.XorbHash) string {
	path, xorb := r.URL.EscapedPath(), xorbHash.String()
	if rest, ok := strings.CutPrefix(path, "/upload/"); ok {
		return "upload " + rest
	}
	if r.Method == http.MethodPost {
		if id, ok := strings.CutPrefix(path, "/v1/xorb-commits/default/"+xorb+"/grant/"); ok {
			return "commit " + id
		}
		switch path {
		case "/v1/xorb-grants/default/" + xorb:
			return "grant"
		case "/v1/xorbs/default/" + xorb:
			return "legacy"
		}
	}
	return "unexpected " + r.Method + " " + r.RequestURI
}

func (s *scriptedCAS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	route := scriptedRouteFor(r, s.hash)
	kind, _, _ := strings.Cut(route, " ")
	s.mu.Lock()
	reply := s.next(kind)
	s.mu.Unlock()
	var body []byte
	if !reply.unread {
		body, _ = io.ReadAll(r.Body)
	}
	s.mu.Lock()
	s.seen = append(s.seen, scriptedRequest{route, r.Host, r.Method, r.RequestURI, r.Header.Clone(), body, r.ContentLength})
	s.mu.Unlock()
	if reply.serve != nil {
		reply.serve(w, r)
		return
	}
	w.WriteHeader(reply.status)
	_, _ = io.WriteString(w, reply.body)
}

// next pops kind's script or falls back to the happy path; s.mu is held.
func (s *scriptedCAS) next(kind string) scriptedReply {
	if kind == "grant" {
		s.grants++
	}
	if script := s.scripts[kind]; len(script) > 0 {
		s.scripts[kind] = script[1:]
		return script[0]
	}
	switch kind {
	case "grant":
		id := "g" + strconv.Itoa(s.grants)
		return scriptedGrant(id, http.MethodPut, s.URL+"/upload/"+id, nil)
	case "upload":
		return scriptedReply{status: http.StatusOK}
	case "commit", "legacy":
		return scriptedReply{status: http.StatusOK, body: `{"was_inserted":true}`}
	}
	return scriptedReply{status: http.StatusTeapot}
}

// script queues replies for the next requests of kind.
func (s *scriptedCAS) script(kind string, replies ...scriptedReply) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scripts[kind] = append(s.scripts[kind], replies...)
}

// requests returns the requests of kind so far.
func (s *scriptedCAS) requests(kind string) []scriptedRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []scriptedRequest
	for _, r := range s.seen {
		if k, _, _ := strings.Cut(r.route, " "); k == kind {
			out = append(out, r)
		}
	}
	return out
}

// routes lists the route of every request so far, in arrival order.
func (s *scriptedCAS) routes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.seen))
	for _, r := range s.seen {
		out = append(out, r.route)
	}
	return out
}

func (s *scriptedCAS) expectRoutes(t *testing.T, want ...string) {
	t.Helper()
	if got := s.routes(); !slices.Equal(got, want) {
		t.Fatalf("requests = %q, want %q", got, want)
	}
}

// client binds a client retrying once to s under the token "secret".
func (s *scriptedCAS) client(t *testing.T, opts ...Options) (*Client, *fakeProvider) {
	t.Helper()
	p := newFakeProvider(s.URL, "secret")
	c, err := NewClient(append([]Options{WithRetries(1), WithUpstreamProvider(p)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	return c, p
}

func (s *scriptedCAS) uploadPayload(t *testing.T, opts ...Options) (*upload.XorbUploadResponse, error) {
	t.Helper()
	c, _ := s.client(t, opts...)
	return c.UploadXorb(t.Context(), scriptedHash, strings.NewReader(scriptedPayload))
}

func TestUploadXorbThroughGrant(t *testing.T) {
	cas := newScriptedCAS(t)
	c, p := cas.client(t)
	resp, err := c.UploadXorb(t.Context(), scriptedHash, strings.NewReader(scriptedPayload))
	cas.expectRoutes(t, "grant", "upload g1", "commit g1")
	if err != nil || resp == nil || !resp.WasInserted {
		t.Fatalf("UploadXorb = %+v, %v; want the commit's was_inserted true", resp, err)
	}
	if got := p.perms(); !slices.Equal(got, []auth.Permission{auth.Write}) {
		t.Fatalf("resolves = %q, want one write Resolve for every step", got)
	}
}

func TestUploadXorbGrantRequest(t *testing.T) {
	cas := newScriptedCAS(t)
	_, err := cas.uploadPayload(t)
	cas.expectRoutes(t, "grant", "upload g1", "commit g1")
	if err != nil {
		t.Fatal(err)
	}
	g := cas.requests("grant")[0]
	if want := "/v1/xorb-grants/default/" + scriptedHash.String(); g.uri != want {
		t.Errorf("grant target = %q, want %q", g.uri, want)
	}
	if got := g.header.Get("Authorization"); got != "Bearer secret" {
		t.Errorf("grant Authorization = %q, want the CAS bearer", got)
	}
	if mt, _, err := mime.ParseMediaType(g.header.Get("Content-Type")); err != nil || mt != "application/json" {
		t.Errorf("grant Content-Type = %q, want application/json", g.header.Get("Content-Type"))
	}
	if got := g.decoded(t); !reflect.DeepEqual(got, scriptedGrantBody) {
		t.Errorf("grant body = %s, want %v", g.body, scriptedGrantBody)
	}
}

func TestUploadXorbGrantedUploadIsVerbatim(t *testing.T) {
	cas := newScriptedCAS(t)
	storage := httptest.NewServer(cas)
	t.Cleanup(storage.Close)
	// A method other than PUT and an unsorted, escaped query show nothing is rebuilt.
	const target = "/upload/g1?X-Amz-Signature=ab%2Fcd&X-Amz-Date=20261102T000000Z"
	granted := map[string]string{"x-amz-checksum-crc64nvme": "rosUhgp5mIg=", "x-amz-meta-grant": "g1; a=b, c"}
	cas.script("grant", scriptedGrant("g1", http.MethodPost, storage.URL+target, granted))
	_, err := cas.uploadPayload(t)
	cas.expectRoutes(t, "grant", "upload g1", "commit g1")
	if err != nil {
		t.Fatal(err)
	}
	up := cas.requests("upload")[0]
	if up.method != http.MethodPost || "http://"+up.host != storage.URL || up.uri != target {
		t.Errorf("upload = %s http://%s%s, want %s %s", up.method, up.host, up.uri, http.MethodPost, storage.URL+target)
	}
	for name, value := range granted {
		if got := up.header.Values(name); !slices.Equal(got, []string{value}) {
			t.Errorf("upload header %s = %q, want %q", name, got, value)
		}
	}
	// Go's transport adds User-Agent and Accept-Encoding to every request itself.
	allowed := map[string]bool{"Content-Length": true, "User-Agent": true, "Accept-Encoding": true}
	for name := range granted {
		allowed[http.CanonicalHeaderKey(name)] = true
	}
	for name, values := range up.header {
		if !allowed[name] {
			t.Errorf("upload carried %s: %q, want only the granted headers and Content-Length", name, values)
		}
	}
	if up.length != int64(len(scriptedPayload)) || string(up.body) != scriptedPayload {
		t.Errorf("upload Content-Length %d, body %q; want %d, %q", up.length, up.body, len(scriptedPayload), scriptedPayload)
	}
}

func TestUploadXorbGrantedUploadRetryResendsPayload(t *testing.T) {
	cas := newScriptedCAS(t)
	cas.script("upload", scriptedReply{status: http.StatusServiceUnavailable})
	resp, err := cas.uploadPayload(t)
	cas.expectRoutes(t, "grant", "upload g1", "upload g1", "commit g1")
	if err != nil || resp == nil || !resp.WasInserted {
		t.Fatalf("UploadXorb = %+v, %v; want the commit's was_inserted true", resp, err)
	}
	for i, up := range cas.requests("upload") {
		if up.length != int64(len(scriptedPayload)) || string(up.body) != scriptedPayload {
			t.Errorf("upload %d: Content-Length %d, body %q; want %d, %q", i+1, up.length, up.body, len(scriptedPayload), scriptedPayload)
		}
	}
}

// The CAS bearer rides on the grant and the commit only, even when the granted URL is on the CAS origin.
func TestUploadXorbGrantedUploadIsAnonymous(t *testing.T) {
	for _, tc := range []struct {
		name  string
		onCAS bool
	}{{"other host", false}, {"CAS host", true}} {
		t.Run(tc.name, func(t *testing.T) {
			cas := newScriptedCAS(t)
			base := cas.URL
			if !tc.onCAS {
				other := httptest.NewServer(cas)
				t.Cleanup(other.Close)
				base = other.URL
			}
			cas.script("grant", scriptedGrant("g1", http.MethodPut, base+"/upload/g1", nil))
			_, err := cas.uploadPayload(t)
			cas.expectRoutes(t, "grant", "upload g1", "commit g1")
			if err != nil {
				t.Fatal(err)
			}
			for _, kind := range []string{"grant", "commit"} {
				if got := cas.requests(kind)[0].header.Get("Authorization"); got != "Bearer secret" {
					t.Errorf("%s Authorization = %q, want the CAS bearer", kind, got)
				}
			}
			up := cas.requests("upload")[0]
			if "http://"+up.host != base {
				t.Errorf("upload went to %s, want %s", up.host, base)
			}
			if got, ok := up.header["Authorization"]; ok {
				t.Errorf("upload Authorization = %q, want none", got)
			}
		})
	}
}

func TestUploadXorbGrantExistingXorb(t *testing.T) {
	cas := newScriptedCAS(t)
	cas.script("grant", scriptedReply{status: http.StatusOK})
	resp, err := cas.uploadPayload(t)
	cas.expectRoutes(t, "grant")
	if err != nil || resp == nil || resp.WasInserted {
		t.Fatalf("UploadXorb = %+v, %v; want was_inserted false", resp, err)
	}
}

func TestUploadXorbGrantNotFoundFallsBack(t *testing.T) {
	cas := newScriptedCAS(t)
	cas.script("grant", scriptedReply{status: http.StatusNotFound})
	resp, err := cas.uploadPayload(t)
	cas.expectRoutes(t, "grant", "legacy")
	if err != nil || resp == nil || !resp.WasInserted {
		t.Fatalf("UploadXorb = %+v, %v; want the legacy POST's was_inserted true", resp, err)
	}
	if legacy := cas.requests("legacy")[0]; legacy.length != int64(len(scriptedPayload)) || string(legacy.body) != scriptedPayload {
		t.Fatalf("legacy POST Content-Length %d, body %q; want %d, %q", legacy.length, legacy.body, len(scriptedPayload), scriptedPayload)
	}
}

func TestUploadXorbGrantsOption(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []Options
		want []string
	}{
		{"default", nil, []string{"grant", "upload g1", "commit g1"}},
		{"WithXorbUploadGrants(true)", []Options{WithXorbUploadGrants(true)}, []string{"grant", "upload g1", "commit g1"}},
		{"WithXorbUploadGrants(false)", []Options{WithXorbUploadGrants(false)}, []string{"legacy"}},
	} {
		cas := newScriptedCAS(t)
		if _, err := cas.uploadPayload(t, tc.opts...); err != nil {
			t.Errorf("%s: %v", tc.name, err)
		}
		if got := cas.routes(); !slices.Equal(got, tc.want) {
			t.Errorf("%s: requests = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestUploadXorbGrantedUploadAcceptedCommits(t *testing.T) {
	for _, status := range []int{http.StatusNoContent, http.StatusPreconditionFailed} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			cas := newScriptedCAS(t)
			cas.script("upload", scriptedReply{status: status})
			resp, err := cas.uploadPayload(t)
			cas.expectRoutes(t, "grant", "upload g1", "commit g1")
			if err != nil || resp == nil || !resp.WasInserted {
				t.Fatalf("UploadXorb = %+v, %v; want the commit's was_inserted true", resp, err)
			}
		})
	}
}

func TestUploadXorbCommitNotFoundRegrants(t *testing.T) {
	cas := newScriptedCAS(t)
	cas.script("commit", scriptedReply{status: http.StatusNotFound})
	resp, err := cas.uploadPayload(t)
	cas.expectRoutes(t, "grant", "upload g1", "commit g1", "grant", "upload g2", "commit g2")
	if err != nil || resp == nil || !resp.WasInserted {
		t.Fatalf("UploadXorb = %+v, %v; want the second commit's was_inserted true", resp, err)
	}
}

func TestUploadXorbGrantedUploadRejectedRegrants(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusForbidden, http.StatusConflict} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			cas := newScriptedCAS(t)
			cas.script("upload", scriptedReply{status: status})
			resp, err := cas.uploadPayload(t)
			cas.expectRoutes(t, "grant", "upload g1", "grant", "upload g2", "commit g2")
			if err != nil || resp == nil || !resp.WasInserted {
				t.Fatalf("UploadXorb = %+v, %v; want the second grant's commit", resp, err)
			}
		})
	}
}

func TestUploadXorbGrantedUploadFailureFallsBack(t *testing.T) {
	closed := httptest.NewServer(http.NotFoundHandler())
	closed.Close()
	for _, tc := range []struct {
		name   string
		method string // granted instead of PUT when set
		url    string // granted instead of the CAS's own /upload/g1 when set
		upload []scriptedReply
		want   []string
	}{
		{"connection refused", "", closed.URL + "/upload/g1", nil, []string{"grant", "legacy"}},
		{"unbuildable request", "BAD METHOD", "", nil, []string{"grant", "legacy"}},
		{"401", "", "", []scriptedReply{{status: http.StatusUnauthorized}}, []string{"grant", "upload g1", "legacy"}},
		{"404", "", "", []scriptedReply{{status: http.StatusNotFound}}, []string{"grant", "upload g1", "legacy"}},
		{"500 after retries", "", "", []scriptedReply{{status: http.StatusInternalServerError}, {status: http.StatusInternalServerError}}, []string{"grant", "upload g1", "upload g1", "legacy"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cas := newScriptedCAS(t)
			if tc.method != "" || tc.url != "" {
				cas.script("grant", scriptedGrant("g1", cmp.Or(tc.method, http.MethodPut), cmp.Or(tc.url, cas.URL+"/upload/g1"), nil))
			}
			cas.script("upload", tc.upload...)
			resp, err := cas.uploadPayload(t)
			cas.expectRoutes(t, tc.want...)
			if err != nil || resp == nil || !resp.WasInserted {
				t.Fatalf("UploadXorb = %+v, %v; want the legacy POST's was_inserted true", resp, err)
			}
		})
	}
}

func TestUploadXorbGrantAttemptsExhaustedFallBack(t *testing.T) {
	for _, tc := range []struct {
		name, kind string
		status     int
		want       []string
	}{
		{"upload 403", "upload", http.StatusForbidden, []string{"grant", "upload g1", "grant", "upload g2", "grant", "upload g3", "legacy"}},
		{"commit 404", "commit", http.StatusNotFound, []string{"grant", "upload g1", "commit g1", "grant", "upload g2", "commit g2", "grant", "upload g3", "commit g3", "legacy"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cas := newScriptedCAS(t)
			reply := scriptedReply{status: tc.status}
			cas.script(tc.kind, reply, reply, reply, reply)
			resp, err := cas.uploadPayload(t)
			cas.expectRoutes(t, tc.want...)
			if err != nil || resp == nil || !resp.WasInserted {
				t.Fatalf("UploadXorb = %+v, %v; want the legacy POST's was_inserted true", resp, err)
			}
		})
	}
}

func TestUploadXorbGrantFatalResponses(t *testing.T) {
	failed := scriptedReply{status: http.StatusInternalServerError}
	for _, tc := range []struct {
		name    string
		kind    string
		replies []scriptedReply
		want    []string
	}{
		{"grant 400", "grant", []scriptedReply{{status: http.StatusBadRequest}}, []string{"grant"}},
		{"grant 401", "grant", []scriptedReply{{status: http.StatusUnauthorized}}, []string{"grant"}},
		{"grant 403", "grant", []scriptedReply{{status: http.StatusForbidden}}, []string{"grant"}},
		{"grant 500 after retries", "grant", []scriptedReply{failed, failed}, []string{"grant", "grant"}},
		{"grant 202", "grant", []scriptedReply{{status: http.StatusAccepted}}, []string{"grant"}},
		{"grant 429", "grant", []scriptedReply{{status: http.StatusTooManyRequests}}, []string{"grant"}},
		{"undecodable 201", "grant", []scriptedReply{{status: http.StatusCreated, body: "not json"}}, []string{"grant"}},
		{"commit 400", "commit", []scriptedReply{{status: http.StatusBadRequest}}, []string{"grant", "upload g1", "commit g1"}},
		{"commit 403", "commit", []scriptedReply{{status: http.StatusForbidden}}, []string{"grant", "upload g1", "commit g1"}},
		{"commit 409", "commit", []scriptedReply{{status: http.StatusConflict}}, []string{"grant", "upload g1", "commit g1"}},
		{"commit 500 after retries", "commit", []scriptedReply{failed, failed}, []string{"grant", "upload g1", "commit g1", "commit g1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cas := newScriptedCAS(t)
			cas.script(tc.kind, tc.replies...)
			resp, err := cas.uploadPayload(t)
			cas.expectRoutes(t, tc.want...)
			if err == nil || resp != nil {
				t.Fatalf("UploadXorb = %+v, %v; want nil and an error", resp, err)
			}
		})
	}
}

// A grant is unusable when its id cannot be a commit path segment, its method is empty, or its url is not absolute: fatal, with no PUT, regrant or legacy POST.
func TestUploadXorbUnusableGrantFails(t *testing.T) {
	for _, tc := range []struct {
		name, id, method string
		path             string // granted on the CAS origin when set
		url              string // granted verbatim when set
	}{
		{"empty id", "", http.MethodPut, "/upload/x", ""},
		{"id .", ".", http.MethodPut, "/upload/x", ""},
		{"id ..", "..", http.MethodPut, "/upload/x", ""},
		{"empty method", "g1", "", "/upload/g1", ""},
		{"empty url", "g1", http.MethodPut, "", ""},
		{"path-only url", "g1", http.MethodPut, "", "/upload/g1"},
		{"url without scheme", "g1", http.MethodPut, "", "upload.example/x"},
		{"unparseable url", "g1", http.MethodPut, "", "http://%zz/x"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cas := newScriptedCAS(t)
			url := tc.url
			if tc.path != "" {
				url = cas.URL + tc.path
			}
			cas.script("grant", scriptedGrant(tc.id, tc.method, url, nil))
			resp, err := cas.uploadPayload(t)
			cas.expectRoutes(t, "grant")
			if err == nil || resp != nil {
				t.Fatalf("UploadXorb = %+v, %v; want nil and an error", resp, err)
			}
		})
	}
}

func TestUploadXorbCommitResult(t *testing.T) {
	for _, tc := range []struct {
		name  string
		reply scriptedReply
		want  bool
	}{
		{"was_inserted false", scriptedReply{status: http.StatusOK, body: `{"was_inserted":false}`}, false},
		{"empty object 200", scriptedReply{status: http.StatusOK, body: `{}`}, true},
		{"empty 200", scriptedReply{status: http.StatusOK}, true},
		{"204", scriptedReply{status: http.StatusNoContent}, true},
		{"non-JSON 200", scriptedReply{status: http.StatusOK, body: "committed"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cas := newScriptedCAS(t)
			cas.script("commit", tc.reply)
			resp, err := cas.uploadPayload(t)
			cas.expectRoutes(t, "grant", "upload g1", "commit g1")
			if err != nil || resp == nil || resp.WasInserted != tc.want {
				t.Fatalf("UploadXorb = %+v, %v; want was_inserted %v", resp, err, tc.want)
			}
		})
	}
}

// The grant id is escaped like Rust's urlencoding::encode: everything but alphanumerics and -_.~, space as %20.
func TestUploadXorbCommitEscapesGrantID(t *testing.T) {
	cas := newScriptedCAS(t)
	cas.script("grant", scriptedGrant("g 1/2+=", http.MethodPut, cas.URL+"/upload/g1", nil))
	_, err := cas.uploadPayload(t)
	cas.expectRoutes(t, "grant", "upload g1", "commit g%201%2F2%2B%3D")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := cas.requests("commit")[0].uri, "/v1/xorb-commits/default/"+scriptedHash.String()+"/grant/g%201%2F2%2B%3D"; got != want {
		t.Fatalf("commit target = %q, want %q", got, want)
	}
}

func TestUploadXorbGrantFromReaderOffset(t *testing.T) {
	cas := newScriptedCAS(t)
	cas.script("upload", scriptedReply{status: http.StatusNotFound})
	c, _ := cas.client(t)
	const skipped = "skipped:"
	reader := strings.NewReader(skipped + scriptedPayload)
	if _, err := reader.Seek(int64(len(skipped)), io.SeekStart); err != nil {
		t.Fatal(err)
	}
	_, err := c.UploadXorb(t.Context(), scriptedHash, reader)
	cas.expectRoutes(t, "grant", "upload g1", "legacy")
	if err != nil {
		t.Fatal(err)
	}
	if got := cas.requests("grant")[0].decoded(t); !reflect.DeepEqual(got, scriptedGrantBody) {
		t.Errorf("grant body = %v, want %v", got, scriptedGrantBody)
	}
	for _, r := range append(cas.requests("upload"), cas.requests("legacy")...) {
		if r.length != int64(len(scriptedPayload)) || string(r.body) != scriptedPayload {
			t.Errorf("%s: Content-Length %d, body %q; want %d, %q", r.route, r.length, r.body, len(scriptedPayload), scriptedPayload)
		}
	}
}

func TestUploadXorbGrantCancelDuringUpload(t *testing.T) {
	cas := newScriptedCAS(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cas.script("upload", scriptedReply{serve: func(_ http.ResponseWriter, r *http.Request) {
		cancel()
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second): // a client deaf to ctx gets a 200 instead of hanging the test
		}
	}})
	var sent []string
	hc := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		sent = append(sent, r.Method+" "+r.URL.Path)
		return http.DefaultTransport.RoundTrip(r)
	})}
	c, _ := cas.client(t, WithHTTPClient(hc))
	resp, err := c.UploadXorb(ctx, scriptedHash, strings.NewReader(scriptedPayload))
	if !errors.Is(err, context.Canceled) || resp != nil {
		t.Fatalf("UploadXorb = %+v, %v; want nil and context.Canceled", resp, err)
	}
	if want := []string{"POST /v1/xorb-grants/default/" + scriptedHash.String(), "PUT /upload/g1"}; !slices.Equal(sent, want) {
		t.Fatalf("requests sent = %q, want %q", sent, want)
	}
}

// handoffPayload is far bigger than the socket buffers, so a reply sent before the body is read reaches the client while the transport is still writing it; every 4-byte word differs, so a body stitched from wrong offsets cannot match.
func handoffPayload() []byte {
	p := make([]byte, 8<<20)
	for i := 0; i < len(p); i += 4 {
		binary.LittleEndian.PutUint32(p[i:], uint32(i))
	}
	return p
}

// A 403 sent before the body is read reaches the client while the transport is still writing the first PUT, so re-seeking the shared reader for the regrant's PUT would race it; that PUT must carry the whole payload. The race is timing-dependent, hence the repetitions.
func TestUploadXorbGrantedUploadEarlyRejectionRegrantsIntact(t *testing.T) {
	payload := handoffPayload()
	want := []string{"grant", "upload g1", "grant", "upload g2", "commit g2"}
	for rep := range 8 {
		cas := newScriptedCAS(t)
		cas.script("upload", scriptedReply{status: http.StatusForbidden, unread: true})
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		c, _ := cas.client(t)
		resp, err := c.UploadXorb(ctx, scriptedHash, bytes.NewReader(payload))
		cancel()
		if err != nil || resp == nil || !resp.WasInserted {
			t.Errorf("repetition %d: UploadXorb = %+v, %v; want the second grant's commit", rep, resp, err)
		}
		if got := cas.routes(); !slices.Equal(got, want) {
			t.Errorf("repetition %d: requests = %q, want %q", rep, got, want)
		}
		for i, up := range cas.requests("upload") {
			if i == 0 {
				continue // rejected before its body was read
			}
			if up.length != int64(len(payload)) || !bytes.Equal(up.body, payload) {
				t.Errorf("repetition %d: %s Content-Length %d, body %d bytes, intact %t; want the %d-byte payload", rep, up.route, up.length, len(up.body), bytes.Equal(up.body, payload), len(payload))
			}
		}
	}
}

// Three early 403s exhaust the grants while their PUTs' bodies may still be in flight; the legacy POST must carry the whole payload.
func TestUploadXorbGrantedUploadEarlyRejectionsFallBackIntact(t *testing.T) {
	payload := handoffPayload()
	cas := newScriptedCAS(t)
	early := scriptedReply{status: http.StatusForbidden, unread: true}
	cas.script("upload", early, early, early)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	c, _ := cas.client(t)
	resp, err := c.UploadXorb(ctx, scriptedHash, bytes.NewReader(payload))
	cas.expectRoutes(t, "grant", "upload g1", "grant", "upload g2", "grant", "upload g3", "legacy")
	if err != nil || resp == nil || !resp.WasInserted {
		t.Fatalf("UploadXorb = %+v, %v; want the legacy POST's was_inserted true", resp, err)
	}
	if legacy := cas.requests("legacy")[0]; legacy.length != int64(len(payload)) || !bytes.Equal(legacy.body, payload) {
		t.Fatalf("legacy POST Content-Length %d, body %d bytes, intact %t; want the %d-byte payload", legacy.length, len(legacy.body), bytes.Equal(legacy.body, payload), len(payload))
	}
}

// A PUT whose server takes part of the body and then stalls without replying leaves the body in flight; cancelling ctx must be what ends the call, with nothing sent after that PUT.
func TestUploadXorbGrantCancelWhileBodyInFlight(t *testing.T) {
	payload := handoffPayload()
	cas := newScriptedCAS(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	released := make(chan struct{})
	defer close(released)
	cas.script("upload", scriptedReply{unread: true, serve: func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadFull(r.Body, make([]byte, 64<<10))
		cancel()
		select {
		case <-released:
		case <-time.After(10 * time.Second): // a client deaf to ctx gets a 200 instead of hanging the test
		}
	}})
	var sent []string
	hc := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		sent = append(sent, r.Method+" "+r.URL.Path)
		return http.DefaultTransport.RoundTrip(r)
	})}
	c, _ := cas.client(t, WithHTTPClient(hc))
	resp, err := c.UploadXorb(ctx, scriptedHash, bytes.NewReader(payload))
	if !errors.Is(err, context.Canceled) || resp != nil {
		t.Fatalf("UploadXorb = %+v, %v; want nil and context.Canceled", resp, err)
	}
	if want := []string{"POST /v1/xorb-grants/default/" + scriptedHash.String(), "PUT /upload/g1"}; !slices.Equal(sent, want) {
		t.Fatalf("requests sent = %q, want %q", sent, want)
	}
}
