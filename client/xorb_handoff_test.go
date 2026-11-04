package client

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// handoffTimeout replaces xorbBodyHandoffTimeout's default so a stale body times out within a test.
const handoffTimeout = 100 * time.Millisecond

// handoffCAS is a transport faking the CAS and the granted storage on the uploading goroutine: grant n grants a PUT to /upload/g<n>, PUT n gets put's status, commits and the legacy POST get 200; it closes no request body.
type handoffCAS struct {
	put          func(n int, body io.ReadCloser) int
	routes       []string // scriptedRoute of every request, in order
	legacy       []byte   // the legacy POST's body, read to EOF
	legacyLength int64
	grants, puts int
}

// newHandoffCAS shortens xorbBodyHandoffTimeout to handoffTimeout for t.
func newHandoffCAS(t *testing.T, put func(n int, body io.ReadCloser) int) *handoffCAS {
	saved := xorbBodyHandoffTimeout
	xorbBodyHandoffTimeout = handoffTimeout
	t.Cleanup(func() { xorbBodyHandoffTimeout = saved })
	return &handoffCAS{put: put}
}

func (f *handoffCAS) RoundTrip(r *http.Request) (*http.Response, error) {
	route := scriptedRoute(r)
	f.routes = append(f.routes, route)
	reply := scriptedReply{status: http.StatusOK, body: `{"was_inserted":true}`}
	switch kind, _, _ := strings.Cut(route, " "); kind {
	case "grant":
		f.grants++
		id := "g" + strconv.Itoa(f.grants)
		reply = scriptedGrant(id, http.MethodPut, "http://storage.example/upload/"+id, nil)
	case "upload":
		f.puts++
		reply = scriptedReply{status: f.put(f.puts, r.Body)}
	case "legacy":
		f.legacyLength = r.ContentLength
		f.legacy, _ = io.ReadAll(r.Body)
	case "unexpected":
		reply = scriptedReply{status: http.StatusTeapot}
	}
	return &http.Response{StatusCode: reply.status, Body: io.NopCloser(strings.NewReader(reply.body))}, nil
}

func (f *handoffCAS) client(t *testing.T) *Client {
	t.Helper()
	c, err := NewClient(WithUpstreamProvider(StaticUpstreamProvider("http://cas.example", "secret")), WithHTTPClient(&http.Client{Transport: f}))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// readPrefix reads the first KiB of body, as a transport that has started sending it.
func readPrefix(t *testing.T, body io.Reader) {
	t.Helper()
	if _, err := io.ReadFull(body, make([]byte, 1<<10)); err != nil {
		t.Errorf("read the PUT body's first KiB: %v", err)
	}
}

// A body the transport read but never closes holds the shared reader for xorbBodyHandoffTimeout only: then the client closes it itself and the regrant's PUT carries the whole payload.
func TestUploadXorbHandoffClosesStaleBodyAfterTimeout(t *testing.T) {
	payload := handoffPayload()
	var stale io.ReadCloser
	var rejected, resent time.Time
	var resentBody []byte
	var staleErr error
	cas := newHandoffCAS(t, func(n int, body io.ReadCloser) int {
		if n == 1 {
			readPrefix(t, body)
			stale, rejected = body, time.Now()
			return http.StatusForbidden
		}
		resent = time.Now()
		resentBody, _ = io.ReadAll(body)
		_, staleErr = stale.Read(make([]byte, 1))
		return http.StatusOK
	})
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	resp, err := cas.client(t).UploadXorb(ctx, scriptedHash, bytes.NewReader(payload))
	if err != nil || resp == nil || !resp.WasInserted {
		t.Fatalf("UploadXorb = %+v, %v after requests %q; want the second grant's commit", resp, err, cas.routes)
	}
	if want := []string{"grant", "upload g1", "grant", "upload g2", "commit g2"}; !slices.Equal(cas.routes, want) {
		t.Errorf("requests = %q, want %q", cas.routes, want)
	}
	if !bytes.Equal(resentBody, payload) {
		t.Errorf("second PUT body: %d bytes, intact %t; want the %d-byte payload", len(resentBody), bytes.Equal(resentBody, payload), len(payload))
	}
	if !errors.Is(staleErr, http.ErrBodyReadAfterClose) {
		t.Errorf("stale first PUT body Read once the second PUT is sent = %v, want http.ErrBodyReadAfterClose", staleErr)
	}
	if waited := resent.Sub(rejected); waited < handoffTimeout || waited >= time.Second {
		t.Errorf("second PUT sent %v after the 403, want at least xorbBodyHandoffTimeout (%v) and under 1s", waited, handoffTimeout)
	}
}

// Cancelling ctx while the client waits out a read stale body ends the upload with ctx's error, sending nothing more.
func TestUploadXorbHandoffWaitEndsOnCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	cas := newHandoffCAS(t, func(n int, body io.ReadCloser) int {
		if n == 1 {
			readPrefix(t, body)
			time.AfterFunc(20*time.Millisecond, cancel)
		}
		return http.StatusForbidden
	})
	resp, err := cas.client(t).UploadXorb(ctx, scriptedHash, bytes.NewReader(handoffPayload()))
	if !errors.Is(err, context.Canceled) || resp != nil {
		t.Fatalf("UploadXorb = %+v, %v after requests %q; want nil and context.Canceled", resp, err, cas.routes)
	}
	// The regrant is requested before the wait starts; only what would follow the wait must not go out.
	if want := []string{"grant", "upload g1", "grant"}; !slices.Equal(cas.routes, want) && !slices.Equal(cas.routes, want[:2]) {
		t.Errorf("requests = %q, want %q and nothing after the cancel", cas.routes, want)
	}
}

// A body the transport never read is closed at once rather than waited for: the handoff timeout is only for a body the transport may still be sending.
func TestUploadXorbHandoffSkipsWaitForUnreadBody(t *testing.T) {
	var rejected, resent time.Time
	cas := newHandoffCAS(t, func(n int, body io.ReadCloser) int {
		if n == 1 {
			rejected = time.Now()
			return http.StatusForbidden
		}
		resent = time.Now()
		_, _ = io.Copy(io.Discard, body)
		return http.StatusOK
	})
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	resp, err := cas.client(t).UploadXorb(ctx, scriptedHash, strings.NewReader(scriptedPayload))
	if err != nil || resp == nil || !resp.WasInserted {
		t.Fatalf("UploadXorb = %+v, %v after requests %q; want the second grant's commit", resp, err, cas.routes)
	}
	if waited := resent.Sub(rejected); waited >= 50*time.Millisecond {
		t.Errorf("second PUT sent %v after the 403 to an unread body, want under 50ms, well before xorbBodyHandoffTimeout (%v)", waited, handoffTimeout)
	}
}

// Three 403s exhaust the grants, the last after its PUT's body was partly read and left open; the legacy POST waits that body out and must carry the whole payload.
func TestUploadXorbHandoffTimeoutFallbackIntact(t *testing.T) {
	payload := handoffPayload()
	cas := newHandoffCAS(t, func(n int, body io.ReadCloser) int {
		if n == 3 {
			readPrefix(t, body)
		}
		return http.StatusForbidden
	})
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	resp, err := cas.client(t).UploadXorb(ctx, scriptedHash, bytes.NewReader(payload))
	if err != nil || resp == nil || !resp.WasInserted {
		t.Fatalf("UploadXorb = %+v, %v after requests %q; want the legacy POST's was_inserted true", resp, err, cas.routes)
	}
	if want := []string{"grant", "upload g1", "grant", "upload g2", "grant", "upload g3", "legacy"}; !slices.Equal(cas.routes, want) {
		t.Errorf("requests = %q, want %q", cas.routes, want)
	}
	if cas.legacyLength != int64(len(payload)) || !bytes.Equal(cas.legacy, payload) {
		t.Errorf("legacy POST Content-Length %d, body %d bytes, intact %t; want the %d-byte payload", cas.legacyLength, len(cas.legacy), bytes.Equal(cas.legacy, payload), len(payload))
	}
}
