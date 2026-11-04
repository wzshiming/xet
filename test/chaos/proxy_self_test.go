package chaos_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"
)

// selfClient dials a fresh connection per request, so each request is exactly one proxied connection.
var selfClient = &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}

const selfPingMsg = "ping"

// selfRead is what a backend handler read from a request body, and when that read returned.
type selfRead struct {
	data []byte
	err  error
	at   time.Time
}

// selfResponse is how a request made in a goroutine ended; status 0 means no response arrived.
type selfResponse struct {
	status int
	err    error
}

// TestSelfPassThrough pins arm(nil) as the pass-through phase.
func TestSelfPassThrough(t *testing.T) {
	t.Parallel()
	srv, _ := selfBlob(t, []byte("ok"))
	p := newFaultProxy()
	t.Cleanup(p.Close)
	p.setTarget(srv.URL)
	p.arm(nil)
	selfOK(t, p.URL()+"/x")
}

// TestSelfSeq pins Seq counting the phase's earlier requests with the same method, path and Range, and arm restarting it.
func TestSelfSeq(t *testing.T) {
	t.Parallel()
	srv, _ := selfBlob(t, selfData(64))
	p := newFaultProxy()
	t.Cleanup(p.Close)
	p.setTarget(srv.URL)
	firstOnly := func(r record) fault {
		if r.Seq == 0 {
			return fault{kind: injectStatus, status: http.StatusServiceUnavailable}
		}
		return fault{}
	}
	for phase := range 2 {
		p.arm(firstOnly)
		var statuses []int
		for _, rng := range []string{"", "", "bytes=0-9"} {
			resp, _, _ := selfGet(t, p.URL()+"/blob", rng)
			statuses = append(statuses, resp.StatusCode)
		}
		if want := []int{http.StatusServiceUnavailable, http.StatusOK, http.StatusServiceUnavailable}; !slices.Equal(statuses, want) {
			t.Fatalf("phase %d: statuses %v for two whole GETs then a ranged one; want %v from a rule failing Seq 0", phase, statuses, want)
		}
		var seqs []int
		for _, rec := range p.settle(t) {
			seqs = append(seqs, rec.Seq)
		}
		if want := []int{0, 1, 0}; !slices.Equal(seqs, want) {
			t.Fatalf("phase %d: record Seq %v, want %v", phase, seqs, want)
		}
	}
}

// TestSelfRuleRecords pins a rule reading records() without deadlocking and seeing the phase's earlier requests.
func TestSelfRuleRecords(t *testing.T) {
	t.Parallel()
	srv, _ := selfBlob(t, []byte("ok"))
	p := newFaultProxy()
	t.Cleanup(p.Close)
	p.setTarget(srv.URL)
	var mu sync.Mutex
	var seen []int
	p.arm(func(record) fault {
		n := len(p.records())
		mu.Lock()
		seen = append(seen, n)
		mu.Unlock()
		return fault{}
	})
	for range 2 {
		selfOK(t, p.URL()+"/")
	}
	mu.Lock()
	defer mu.Unlock()
	if want := []int{0, 1}; !slices.Equal(seen, want) {
		t.Fatalf("the rule saw %v records, want %v: the phase's earlier requests", seen, want)
	}
}

// TestSelfCloseMidRequestBody pins Close returning within 1s while a client stalls mid-request-body under every fault that reads the body.
func TestSelfCloseMidRequestBody(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		f    fault
	}{
		{"injectStatus", fault{kind: injectStatus, status: http.StatusServiceUnavailable}},
		{"injectBody", fault{kind: injectBody, status: http.StatusOK, contentType: "text/plain", body: []byte("ok")}},
		{"stallHeaders", fault{kind: stallHeaders}},
		{"passThrough", fault{kind: passThrough}},
		{"cutRequestBody", fault{kind: cutRequestBody, prefix: 5000}},
		{"stallRequestBody", fault{kind: stallRequestBody, prefix: 5000}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, _ := selfSink(t)
			p := newFaultProxy()
			var once sync.Once
			t.Cleanup(func() { once.Do(p.Close) })
			p.setTarget(srv.URL)
			p.arm(func(record) fault { return tc.f })
			c := selfDial(t, p.Addr())
			if _, err := io.WriteString(c, selfPostHead(p.Addr(), 1<<20)+string(selfData(1000))); err != nil {
				t.Fatal(err)
			}
			p.wait(t, func(recs []record, _ int) bool { return len(recs) == 1 && recs[0].ReqBytes == 1000 })
			start := time.Now()
			closed := make(chan struct{}, 1)
			go func() {
				once.Do(p.Close)
				closed <- struct{}{}
			}()
			// Cleanup closes c before p, so a Close stuck on the client fails the test instead of hanging it.
			selfRecv(t, closed, "Close to return while the client stalls mid-body")
			if d := time.Since(start); d > time.Second {
				t.Fatalf("Close took %v while the client stalls mid-body, want under 1s", d)
			}
			selfDeadline(t, c, time.Second)
			if _, err := io.ReadAll(c); errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatal("client connection still open 1s after Close")
			}
		})
	}
}

// TestSelfFaultKinds pins each response fault kind against a 64 KiB backend body.
func TestSelfFaultKinds(t *testing.T) {
	t.Parallel()
	blob := selfData(64 << 10)

	t.Run("passThrough", func(t *testing.T) {
		t.Parallel()
		p, _ := selfBlobFault(t, blob, fault{kind: passThrough})
		resp, got, err := selfGet(t, p.URL()+"/blob", "")
		if resp.StatusCode != http.StatusOK || err != nil || !bytes.Equal(got, blob) {
			t.Fatalf("status %d, %d bytes, err %v; want 200 with the whole body", resp.StatusCode, len(got), err)
		}
		if rec := selfOnly(t, p); rec.Bytes != int64(len(blob)) {
			t.Fatalf("record Bytes %d, want %d", rec.Bytes, len(blob))
		}
	})
	t.Run("shortBody", func(t *testing.T) {
		t.Parallel()
		p, _ := selfBlobFault(t, blob, fault{kind: shortBody, prefix: 1000})
		if _, got, err := selfGet(t, p.URL()+"/blob", ""); !bytes.Equal(got, blob[:1000]) || !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("read %d bytes, err %v; want the first 1000 then io.ErrUnexpectedEOF", len(got), err)
		}
	})
	t.Run("shortChunked", func(t *testing.T) {
		t.Parallel()
		p, _ := selfBlobFault(t, blob, fault{kind: shortChunked, prefix: 1000})
		if _, got, err := selfGet(t, p.URL()+"/blob", ""); !bytes.Equal(got, blob[:1000]) || err != nil {
			t.Fatalf("read %d bytes, err %v; want the first 1000 then a clean EOF", len(got), err)
		}
	})
	for _, tc := range []struct {
		name string
		kind faultKind
	}{{name: "abortBody", kind: abortBody}, {name: "resetBody", kind: resetBody}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, _ := selfBlobFault(t, blob, fault{kind: tc.kind, prefix: 1000})
			if _, got, err := selfGet(t, p.URL()+"/blob", ""); !bytes.Equal(got, blob[:1000]) || err == nil {
				t.Fatalf("read %d bytes, err %v; want the first 1000 then an error", len(got), err)
			}
		})
	}
	t.Run("stallBody", func(t *testing.T) {
		t.Parallel()
		p, _ := selfBlobFault(t, blob, fault{kind: stallBody, prefix: 1000})
		c, br := selfRawGet(t, p.Addr())
		selfDeadline(t, c, waitTimeout)
		resp, err := http.ReadResponse(br, nil)
		if err != nil {
			t.Fatal(err)
		}
		got := make([]byte, 1000)
		if _, err := io.ReadFull(resp.Body, got); err != nil || !bytes.Equal(got, blob[:1000]) {
			t.Fatalf("prefix read err %v; want the first 1000 bytes", err)
		}
		selfDeadline(t, c, 200*time.Millisecond)
		if n, err := resp.Body.Read(got); !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("read %d bytes past the prefix, err %v; want the 200ms read deadline to hit", n, err)
		}
		if recs := p.records(); len(recs) != 1 || !recs[0].End.IsZero() {
			t.Fatalf("records %+v while the client holds the response; want one with End zero", recs)
		}
		c.Close()
		if rec := selfOnly(t, p); rec.End.IsZero() {
			t.Fatal("record End still zero after the client closed")
		}
	})
	t.Run("stallHeaders", func(t *testing.T) {
		t.Parallel()
		p, _ := selfBlobFault(t, blob, fault{kind: stallHeaders})
		c, br := selfRawGet(t, p.Addr())
		selfDeadline(t, c, 200*time.Millisecond)
		resp, err := http.ReadResponse(br, nil)
		if err == nil {
			t.Fatalf("got %s; want no response before the 200ms read deadline", resp.Status)
		}
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("read err %v; want the 200ms read deadline to hit", err)
		}
		c.Close()
		if rec := selfOnly(t, p); rec.Status != 0 {
			t.Fatalf("record Status %d, want 0", rec.Status)
		}
	})
	t.Run("trickle", func(t *testing.T) {
		t.Parallel()
		p, _ := selfBlobFault(t, blob[:8<<10], fault{kind: trickle, piece: 1024, gap: 20 * time.Millisecond})
		start := time.Now()
		_, got, err := selfGet(t, p.URL()+"/blob", "")
		if elapsed := time.Since(start); err != nil || !bytes.Equal(got, blob[:8<<10]) || elapsed < 140*time.Millisecond {
			t.Fatalf("read %d bytes in %v, err %v; want all 8 KiB taking at least 140ms", len(got), elapsed, err)
		}
	})
	t.Run("injectStatus", func(t *testing.T) {
		t.Parallel()
		p, hits := selfBlobFault(t, blob, fault{kind: injectStatus, status: http.StatusServiceUnavailable})
		resp, _, _ := selfGet(t, p.URL()+"/blob", "")
		rec := selfOnly(t, p)
		if resp.StatusCode != http.StatusServiceUnavailable || len(hits) != 0 || rec.Status != http.StatusServiceUnavailable {
			t.Fatalf("status %d, record Status %d after %d backend calls; want 503 recorded without calling the backend", resp.StatusCode, rec.Status, len(hits))
		}
	})
	t.Run("shiftRange", func(t *testing.T) {
		t.Parallel()
		p, ranges := selfBlobFault(t, blob, fault{kind: shiftRange})
		selfGet(t, p.URL()+"/blob", "bytes=10-19")
		if got := selfRecv(t, ranges, "the backend request"); got != "bytes=11-19" {
			t.Fatalf("backend saw Range %q, want %q", got, "bytes=11-19")
		}
		if rec := selfOnly(t, p); rec.Range != "bytes=10-19" {
			t.Fatalf("record Range %q, want the original %q", rec.Range, "bytes=10-19")
		}
	})
	t.Run("ignoreRange", func(t *testing.T) {
		t.Parallel()
		p, ranges := selfBlobFault(t, blob, fault{kind: ignoreRange})
		selfGet(t, p.URL()+"/blob", "bytes=10-19")
		if got := selfRecv(t, ranges, "the backend request"); got != "" {
			t.Fatalf("backend saw Range %q, want none", got)
		}
	})
	t.Run("noContentRange", func(t *testing.T) {
		t.Parallel()
		p, _ := selfBlobFault(t, blob, fault{kind: noContentRange})
		resp, _, _ := selfGet(t, p.URL()+"/blob", "bytes=10-19")
		if cr := resp.Header.Get("Content-Range"); resp.StatusCode != http.StatusPartialContent || cr != "" {
			t.Fatalf("status %d, Content-Range %q; want 206 without Content-Range", resp.StatusCode, cr)
		}
	})
	t.Run("tagURLs", func(t *testing.T) {
		t.Parallel()
		srv := selfServe(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"fetch_info":{"a":[{"url":"http://127.0.0.1:9/v1/xorbs/a"}]}}`)
		})
		p := selfFault(t, srv.URL, fault{kind: tagURLs, tag: 3})
		_, got, err := selfGet(t, p.URL()+"/v1/reconstructions/a", "")
		var doc struct {
			FetchInfo map[string][]struct {
				URL string `json:"url"`
			} `json:"fetch_info"`
		}
		const want = "http://127.0.0.1:9/v1/xorbs/a?gen=3"
		if err != nil || json.Unmarshal(got, &doc) != nil || len(doc.FetchInfo["a"]) != 1 || doc.FetchInfo["a"][0].URL != want {
			t.Fatalf("body %s, err %v; want the xorb URL rewritten to %s", got, err, want)
		}
	})
}

// TestSelfRequestBody pins how faults treat a 1 MiB request body.
func TestSelfRequestBody(t *testing.T) {
	t.Parallel()
	payload := selfData(1 << 20)

	t.Run("injectStatus", func(t *testing.T) {
		t.Parallel()
		srv, _ := selfSink(t)
		p := selfFault(t, srv.URL, fault{kind: injectStatus, status: http.StatusServiceUnavailable})
		if status, _, err := selfRequest(selfCtx(t, waitTimeout), http.MethodPost, p.URL()+"/upload", payload); status != http.StatusServiceUnavailable || err != nil {
			t.Fatalf("status %d, err %v; want 503", status, err)
		}
		if rec := selfOnly(t, p); rec.ReqBytes != 1<<20 {
			t.Fatalf("record ReqBytes %d, want %d", rec.ReqBytes, 1<<20)
		}
	})
	t.Run("passThrough", func(t *testing.T) {
		t.Parallel()
		srv, reads := selfSink(t)
		p := selfFault(t, srv.URL, fault{kind: passThrough})
		if status, _, err := selfRequest(selfCtx(t, waitTimeout), http.MethodPost, p.URL()+"/upload", payload); status != http.StatusOK || err != nil {
			t.Fatalf("status %d, err %v; want 200", status, err)
		}
		if r := selfRecv(t, reads, "the backend read"); r.err != nil || !bytes.Equal(r.data, payload) {
			t.Fatalf("backend read %d bytes, err %v; want exactly the %d sent", len(r.data), r.err, len(payload))
		}
	})
	t.Run("stallHeaders", func(t *testing.T) {
		t.Parallel()
		srv, _ := selfSink(t)
		p := selfFault(t, srv.URL, fault{kind: stallHeaders})
		ctx, cancel := context.WithCancel(selfCtx(t, waitTimeout))
		defer cancel()
		done := selfRequestAsync(ctx, http.MethodPost, p.URL()+"/upload", payload)
		p.wait(t, func(recs []record, _ int) bool { return len(recs) == 1 && recs[0].ReqBytes == 1<<20 })
		selfNoResponse(t, done, 300*time.Millisecond)
		cancel()
		if r := selfRecv(t, done, "the upload to end"); !errors.Is(r.err, context.Canceled) {
			t.Fatalf("status %d, err %v after the client left; want context.Canceled", r.status, r.err)
		}
		if rec := selfOnly(t, p); rec.Status != 0 {
			t.Fatalf("record Status %d, want 0 while the headers are withheld", rec.Status)
		}
	})
}

// TestSelfInjectBody pins injectBody answering with its own status, type and body without the backend.
func TestSelfInjectBody(t *testing.T) {
	t.Parallel()
	body := []byte("{\"a\":1}\n{\"b\":2}\n")
	p, hits := selfBlobFault(t, selfData(16), fault{kind: injectBody, status: http.StatusOK, contentType: "application/x-ndjson", body: body})
	resp, got, err := selfGet(t, p.URL()+"/blob", "")
	if ct := resp.Header.Get("Content-Type"); resp.StatusCode != http.StatusOK || ct != "application/x-ndjson" ||
		resp.ContentLength != int64(len(body)) || err != nil || !bytes.Equal(got, body) {
		t.Fatalf("status %d, Content-Type %q, Content-Length %d, body %q, err %v; want 200, application/x-ndjson, %d, %q",
			resp.StatusCode, ct, resp.ContentLength, got, err, len(body), body)
	}
	if rec := selfOnly(t, p); rec.Bytes != int64(len(body)) || len(hits) != 0 {
		t.Fatalf("record Bytes %d after %d backend calls; want %d and none", rec.Bytes, len(hits), len(body))
	}
}

// TestSelfCutRequestBody pins cutRequestBody truncating an upload at the backend and resetting the client.
func TestSelfCutRequestBody(t *testing.T) {
	t.Parallel()
	payload := selfData(1 << 20)
	srv, reads := selfSink(t)
	p := selfFault(t, srv.URL, fault{kind: cutRequestBody, prefix: 1000})
	if status, _, err := selfRequest(selfCtx(t, waitTimeout), http.MethodPost, p.URL()+"/upload", payload); !selfConnErr(err) {
		t.Fatalf("status %d, err %v; want a *url.Error from the reset connection", status, err)
	}
	if rec := selfOnly(t, p); rec.Status != 0 || rec.Bytes != 0 || rec.ReqBytes != 1000 {
		t.Fatalf("record Status %d, Bytes %d, ReqBytes %d; want 0, 0, 1000", rec.Status, rec.Bytes, rec.ReqBytes)
	}
	if r := selfRecv(t, reads, "the backend read"); !bytes.Equal(r.data, payload[:1000]) || !errors.Is(r.err, io.ErrUnexpectedEOF) {
		t.Fatalf("backend read %d bytes, err %v; want the first 1000 then io.ErrUnexpectedEOF", len(r.data), r.err)
	}
}

// TestSelfStallRequestBody pins stallRequestBody holding an upload after its prefix until the client or Close ends it.
func TestSelfStallRequestBody(t *testing.T) {
	t.Parallel()
	payload := selfData(1 << 20)
	stall := fault{kind: stallRequestBody, prefix: 1000}
	// backend reports the 1000-byte prefix it read, then when and how its next Read returned.
	backend := func(t *testing.T) (*httptest.Server, chan selfRead, chan selfRead) {
		prefix, next := make(chan selfRead, 1), make(chan selfRead, 1)
		srv := selfServe(t, func(w http.ResponseWriter, r *http.Request) {
			buf := make([]byte, 1000)
			_, err := io.ReadFull(r.Body, buf)
			selfOffer(prefix, selfRead{data: buf, err: err, at: time.Now()})
			if err != nil {
				return
			}
			n, err := r.Body.Read(buf)
			selfOffer(next, selfRead{data: buf[:n], err: err, at: time.Now()})
		})
		return srv, prefix, next
	}

	t.Run("clientLeaves", func(t *testing.T) {
		t.Parallel()
		srv, prefix, next := backend(t)
		p := selfFault(t, srv.URL, stall)
		ctx, cancel := context.WithCancel(selfCtx(t, waitTimeout))
		defer cancel()
		done := selfRequestAsync(ctx, http.MethodPost, p.URL()+"/upload", payload)
		p.wait(t, func(recs []record, _ int) bool { return len(recs) == 1 && recs[0].ReqBytes == 1000 })
		if r := selfRecv(t, prefix, "the backend prefix"); r.err != nil || !bytes.Equal(r.data, payload[:1000]) {
			t.Fatalf("backend prefix read err %v; want the first 1000 bytes", r.err)
		}
		selfNoResponse(t, done, 300*time.Millisecond)
		left := time.Now()
		cancel()
		if r := selfRecv(t, done, "the upload to end"); !errors.Is(r.err, context.Canceled) {
			t.Fatalf("status %d, err %v after the client left; want context.Canceled", r.status, r.err)
		}
		if r := selfRecv(t, next, "the backend's next Read"); r.at.Before(left) || r.err == nil {
			t.Fatalf("backend's next Read returned %d bytes, err %v, %v after the client left; want an error no earlier", len(r.data), r.err, r.at.Sub(left))
		}
	})
	t.Run("proxyClose", func(t *testing.T) {
		t.Parallel()
		srv, prefix, next := backend(t)
		p := newFaultProxy()
		var once sync.Once
		t.Cleanup(func() { once.Do(p.Close) })
		p.setTarget(srv.URL)
		p.arm(func(record) fault { return stall })
		ctx := selfCtx(t, waitTimeout)
		done := make(chan error, 1)
		go func() {
			_, _, err := selfRequest(ctx, http.MethodPost, p.URL()+"/upload", payload)
			done <- err
		}()
		select {
		case <-prefix:
		case err := <-done:
			t.Fatalf("upload ended with %v before the backend read its prefix", err)
		case <-time.After(waitTimeout):
			t.Fatal("timed out waiting for the backend prefix")
		}
		start := time.Now()
		once.Do(p.Close)
		if d := time.Since(start); d > time.Second {
			t.Fatalf("Close took %v during a stalled upload, want under 1s", d)
		}
		if r := selfRecv(t, next, "the backend's next Read"); r.at.Before(start) || r.err == nil {
			t.Fatalf("backend's next Read returned %d bytes, err %v; want an error once Close released it", len(r.data), r.err)
		}
	})
}

// TestSelfFullDuplex pins the proxy relaying and flushing a response while the request body is still streaming.
func TestSelfFullDuplex(t *testing.T) {
	t.Parallel()
	payload := selfData(512 << 10)
	reads := make(chan selfRead, 1)
	srv := selfServe(t, func(w http.ResponseWriter, r *http.Request) {
		rc := http.NewResponseController(w)
		_ = rc.EnableFullDuplex()
		_, _ = io.WriteString(w, "first\n")
		_ = rc.Flush()
		data, err := io.ReadAll(r.Body)
		selfOffer(reads, selfRead{data: data, err: err, at: time.Now()})
		_, _ = io.WriteString(w, "second\n")
	})
	p := selfFault(t, srv.URL, fault{kind: passThrough})
	c := selfDial(t, p.Addr())
	half := len(payload) / 2
	if _, err := io.WriteString(c, selfPostHead(p.Addr(), len(payload))+string(payload[:half])); err != nil {
		t.Fatal(err)
	}
	selfDeadline(t, c, 2*time.Second)
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatalf("response head with half the body sent: %v; want it within 2s while the body still streams", err)
	}
	line := make([]byte, len("first\n"))
	if _, err := io.ReadFull(resp.Body, line); resp.StatusCode != http.StatusOK || err != nil || string(line) != "first\n" {
		t.Fatalf("status %d, first read %q, err %v with half the body sent; want 200 and %q", resp.StatusCode, line, err, "first\n")
	}
	if _, err := c.Write(payload[half:]); err != nil {
		t.Fatal(err)
	}
	selfDeadline(t, c, waitTimeout)
	if rest, err := io.ReadAll(resp.Body); err != nil || string(rest) != "second\n" {
		t.Fatalf("rest of the body %q, err %v; want %q", rest, err, "second\n")
	}
	if r := selfRecv(t, reads, "the backend read"); r.err != nil || !bytes.Equal(r.data, payload) {
		t.Fatalf("backend read %d bytes, err %v; want all %d", len(r.data), r.err, len(payload))
	}
}

// TestSelfTCPPassThrough pins tcpProxy relaying a 1 MiB echo byte-exact and counting it.
func TestSelfTCPPassThrough(t *testing.T) {
	t.Parallel()
	p := newTCPProxy(t, selfEcho(t))
	data := selfData(1 << 20)
	if got, err := selfExchange(t, p.Addr(), data); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("echoed %d bytes, err %v; want the %d sent", len(got), err, len(data))
	}
	selfPoll(t, "open() == 0", func() bool { return p.open() == 0 })
	if p.conns() != 1 || p.bytes(toServer) != 1<<20 || p.bytes(toClient) != 1<<20 || p.cuts() != 0 {
		t.Fatalf("conns %d, bytes toServer %d, toClient %d, cuts %d; want 1, %d, %d, 0",
			p.conns(), p.bytes(toServer), p.bytes(toClient), p.cuts(), 1<<20, 1<<20)
	}
}

// TestSelfTCPDelay pins delay holding every chunk in both directions.
func TestSelfTCPDelay(t *testing.T) {
	t.Parallel()
	echo := selfEcho(t)

	t.Run("fixed", func(t *testing.T) {
		t.Parallel()
		p := newTCPProxy(t, echo)
		p.delay(toServer, 100*time.Millisecond, 0)
		p.delay(toClient, 100*time.Millisecond, 0)
		c := selfDial(t, p.Addr())
		for i := range 3 {
			if rtt := selfPing(t, c); rtt < 200*time.Millisecond {
				t.Fatalf("round trip %d took %v, want at least 200ms", i+1, rtt)
			}
		}
	})
	t.Run("jitter", func(t *testing.T) {
		t.Parallel()
		p := newTCPProxy(t, echo)
		p.delay(toServer, 100*time.Millisecond, 50*time.Millisecond)
		p.delay(toClient, 100*time.Millisecond, 50*time.Millisecond)
		if rtt := selfPing(t, selfDial(t, p.Addr())); rtt < 100*time.Millisecond {
			t.Fatalf("round trip took %v, want at least 100ms", rtt)
		}
	})
}

// TestSelfTCPRate pins rate pacing a 24 KiB echo at 32 KiB/s in 8 KiB pieces.
func TestSelfTCPRate(t *testing.T) {
	t.Parallel()
	p := newTCPProxy(t, selfEcho(t))
	p.rate(toClient, 32<<10, 8<<10)
	c := selfDial(t, p.Addr())
	data := selfData(24 << 10)
	got := make([]byte, len(data))
	start := time.Now()
	if _, err := c.Write(data); err != nil {
		t.Fatal(err)
	}
	selfDeadline(t, c, waitTimeout)
	if _, err := io.ReadFull(c, got[:8<<10]); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 1250*time.Millisecond {
		t.Fatalf("first 8 KiB took %v, want at most 1.25s", d)
	}
	if _, err := io.ReadFull(c, got[8<<10:8<<10+1]); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 250*time.Millisecond {
		t.Fatalf("byte 8193 arrived %v after the send, want at least 250ms", d)
	}
	if _, err := io.ReadFull(c, got[8<<10+1:]); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 500*time.Millisecond || !bytes.Equal(got, data) {
		t.Fatalf("24 KiB took %v (intact %t), want at least 500ms", d, bytes.Equal(got, data))
	}
}

// TestSelfTCPCut pins cutOnce and cutAfter firing at a byte count in one direction.
func TestSelfTCPCut(t *testing.T) {
	t.Parallel()
	echo := selfEcho(t)
	data := selfData(8 << 10)

	t.Run("onceCloseToClient", func(t *testing.T) {
		t.Parallel()
		p := newTCPProxy(t, echo)
		p.cutOnce(toClient, 4096, cutClose)
		if got, err := selfExchange(t, p.Addr(), data); err != nil || !bytes.Equal(got, data[:4096]) {
			t.Fatalf("first connection read %d bytes, err %v; want exactly 4096 then EOF", len(got), err)
		}
		selfPoll(t, "cuts() == 1", func() bool { return p.cuts() == 1 })
		if got, err := selfExchange(t, p.Addr(), data); err != nil || !bytes.Equal(got, data) {
			t.Fatalf("next connection read %d bytes, err %v; want all %d", len(got), err, len(data))
		}
		if n := p.cuts(); n != 1 {
			t.Fatalf("cuts() = %d after the intact connection, want 1", n)
		}
	})
	t.Run("afterRstToClient", func(t *testing.T) {
		t.Parallel()
		p := newTCPProxy(t, echo)
		p.cutAfter(toClient, 4096, cutRst)
		for i := range 2 {
			if got, err := selfExchange(t, p.Addr(), data); len(got) > 4096 || (err != nil && !errors.Is(err, syscall.ECONNRESET)) {
				t.Fatalf("connection %d read %d bytes, err %v; want at most 4096 then ECONNRESET or EOF", i+1, len(got), err)
			}
		}
		selfPoll(t, "cuts() == 2", func() bool { return p.cuts() == 2 })
	})
	t.Run("afterCloseToServer", func(t *testing.T) {
		t.Parallel()
		p := newTCPProxy(t, echo)
		p.cutAfter(toServer, 4096, cutClose)
		if got, err := selfExchange(t, p.Addr(), data); err != nil || !bytes.Equal(got, data[:4096]) {
			t.Fatalf("echo returned %d bytes, err %v; want exactly 4096 then EOF", len(got), err)
		}
	})
}

// TestSelfTCPBlackhole pins cutBlackhole silencing a connection until Close resets it.
func TestSelfTCPBlackhole(t *testing.T) {
	t.Parallel()
	p := newTCPProxy(t, selfEcho(t))
	p.cutAfter(toClient, 4096, cutBlackhole)
	c := selfDial(t, p.Addr())
	data := selfData(8 << 10)
	if _, err := c.Write(data); err != nil {
		t.Fatal(err)
	}
	selfDeadline(t, c, waitTimeout)
	got := make([]byte, 4096)
	if _, err := io.ReadFull(c, got); err != nil || !bytes.Equal(got, data[:4096]) {
		t.Fatalf("read err %v; want the first 4096 bytes", err)
	}
	selfDeadline(t, c, 300*time.Millisecond)
	if n, err := c.Read(got); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("read %d bytes past the cut, err %v; want the 300ms read deadline to hit", n, err)
	}
	if n := p.open(); n != 1 {
		t.Fatalf("open() = %d with the connection blackholed, want 1", n)
	}
	start := time.Now()
	p.Close()
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Close took %v with a blackholed connection, want under 1s", d)
	}
	selfDeadline(t, c, time.Second)
	if n, err := c.Read(got); err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("read %d bytes, err %v after Close; want a connection error", n, err)
	}
}

// TestSelfTCPRefuse pins refuse resetting accepted connections and clear restoring unimpaired service.
func TestSelfTCPRefuse(t *testing.T) {
	t.Parallel()
	web, _ := selfBlob(t, []byte("ok"))
	addr := web.Listener.Addr().String()

	t.Run("once", func(t *testing.T) {
		t.Parallel()
		p := newTCPProxy(t, addr)
		p.refuse(1)
		c := selfDial(t, p.Addr())
		selfDeadline(t, c, time.Second)
		if n, err := c.Read(make([]byte, 1)); err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("refused connection read %d bytes, err %v; want a connection error within 1s", n, err)
		}
		selfPoll(t, "refused() == 1", func() bool { return p.refused() == 1 })
		selfOK(t, p.URL()+"/")
	})
	t.Run("always", func(t *testing.T) {
		t.Parallel()
		p := newTCPProxy(t, addr)
		p.refuse(-1)
		p.delay(toClient, time.Second, 0)
		p.cutAfter(toClient, 1, cutRst)
		for i := range 2 {
			if status, _, err := selfRequest(selfCtx(t, waitTimeout), http.MethodGet, p.URL()+"/", nil); !selfConnErr(err) {
				t.Fatalf("GET %d: status %d, err %v; want a *url.Error from the refused connection", i+1, status, err)
			}
		}
		selfPoll(t, "refused() == 2", func() bool { return p.refused() == 2 })
		p.clear()
		start := time.Now()
		if status, got, err := selfRequest(selfCtx(t, waitTimeout), http.MethodGet, p.URL()+"/", nil); status != http.StatusOK || err != nil || string(got) != "ok" {
			t.Fatalf("GET after clear: status %d, body %q, err %v; want 200 ok over a connection the cleared cut leaves intact", status, got, err)
		}
		if d := time.Since(start); d >= time.Second {
			t.Fatalf("GET after clear took %v, want under the cleared 1s delay", d)
		}
	})
}

// TestSelfTCPMaxConns pins maxConns holding excess connections un-accepted until a slot frees.
func TestSelfTCPMaxConns(t *testing.T) {
	t.Parallel()
	p := newTCPProxy(t, selfEcho(t))
	p.maxConns(1)
	a := selfDial(t, p.Addr())
	selfPing(t, a)
	b := selfDial(t, p.Addr())
	if _, err := io.WriteString(b, selfPingMsg); err != nil {
		t.Fatal(err)
	}
	selfDeadline(t, b, 300*time.Millisecond)
	got := make([]byte, len(selfPingMsg))
	if n, err := b.Read(got); n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("held connection read %d bytes, err %v; want the 300ms read deadline to hit with nothing", n, err)
	}
	if n := p.refused(); n != 0 {
		t.Fatalf("refused() = %d while holding a connection, want 0", n)
	}
	a.Close()
	selfDeadline(t, b, waitTimeout)
	if _, err := io.ReadFull(b, got); err != nil || string(got) != selfPingMsg {
		t.Fatalf("after A closed, B read %q, err %v; want its echo %q", got, err, selfPingMsg)
	}
	b.Close()
	selfPing(t, selfDial(t, p.Addr()))
}

// TestSelfTCPCloseExisting pins closeExisting resetting live connections while the proxy keeps serving.
func TestSelfTCPCloseExisting(t *testing.T) {
	t.Parallel()
	p := newTCPProxy(t, selfEcho(t))
	c := selfDial(t, p.Addr())
	selfPing(t, c)
	p.closeExisting()
	if n := p.open(); n != 0 {
		t.Fatalf("open() = %d after closeExisting, want 0", n)
	}
	selfDeadline(t, c, time.Second)
	if n, err := c.Read(make([]byte, 1)); err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("live connection read %d bytes, err %v after closeExisting; want a connection error", n, err)
	}
	selfPing(t, selfDial(t, p.Addr()))
}

// TestSelfTCPClose pins Close as bounded, idempotent and final.
func TestSelfTCPClose(t *testing.T) {
	t.Parallel()
	p := newTCPProxy(t, selfEcho(t))
	p.cutOnce(toServer, 0, cutBlackhole)
	held := selfDial(t, p.Addr())
	if _, err := io.WriteString(held, selfPingMsg); err != nil {
		t.Fatal(err)
	}
	selfDeadline(t, held, 100*time.Millisecond)
	if n, err := held.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("blackholed connection read %d bytes, err %v; want silence", n, err)
	}
	selfPing(t, selfDial(t, p.Addr()))
	for i := range 2 {
		start := time.Now()
		p.Close()
		if d := time.Since(start); d > time.Second {
			t.Fatalf("Close %d took %v, want under 1s", i+1, d)
		}
	}
	if c, err := net.DialTimeout("tcp", p.Addr(), time.Second); !errors.Is(err, syscall.ECONNREFUSED) {
		if err == nil {
			c.Close()
		}
		t.Fatalf("dial after Close: err %v; want ECONNREFUSED", err)
	}
}

// selfData returns n deterministic bytes whose pattern does not line up with power-of-two offsets.
func selfData(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

// selfOffer sends v on ch unless ch is full, so backend handlers never block on the test.
func selfOffer[T any](ch chan T, v T) {
	select {
	case ch <- v:
	default:
	}
}

// selfRecv returns the next value on ch, failing t after waitTimeout.
func selfRecv[T any](t *testing.T, ch chan T, what string) T {
	t.Helper()
	var v T
	select {
	case v = <-ch:
	case <-time.After(waitTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
	return v
}

// selfPoll fails t unless cond holds within waitTimeout.
func selfPoll(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// selfCtx returns a context that expires after d.
func selfCtx(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(t.Context(), d)
	t.Cleanup(cancel)
	return ctx
}

// selfServe starts a backend whose cleanup cuts live connections first, so stuck handlers return.
func selfServe(t *testing.T, h http.HandlerFunc) *httptest.Server {
	srv := httptest.NewServer(h)
	t.Cleanup(func() {
		srv.CloseClientConnections()
		srv.Close()
	})
	return srv
}

// selfBlob serves body with Range support and reports each request's Range header.
func selfBlob(t *testing.T, body []byte) (*httptest.Server, chan string) {
	ranges := make(chan string, 8)
	srv := selfServe(t, func(w http.ResponseWriter, r *http.Request) {
		selfOffer(ranges, r.Header.Get("Range"))
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(body))
	})
	return srv, ranges
}

// selfSink reads each request body to its end and reports it.
func selfSink(t *testing.T) (*httptest.Server, chan selfRead) {
	reads := make(chan selfRead, 8)
	srv := selfServe(t, func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		selfOffer(reads, selfRead{data: data, err: err, at: time.Now()})
	})
	return srv, reads
}

// selfFault starts a faultProxy in front of target that applies f to every request.
func selfFault(t *testing.T, target string, f fault) *faultProxy {
	p := newFaultProxy()
	t.Cleanup(p.Close)
	p.setTarget(target)
	p.arm(func(record) fault { return f })
	return p
}

// selfBlobFault serves body from a selfBlob backend through a faultProxy applying f.
func selfBlobFault(t *testing.T, body []byte, f fault) (*faultProxy, chan string) {
	srv, ranges := selfBlob(t, body)
	return selfFault(t, srv.URL, f), ranges
}

// selfOnly settles p and returns its single record.
func selfOnly(t *testing.T, p *faultProxy) record {
	t.Helper()
	p.settle(t)
	recs := p.records()
	if len(recs) != 1 {
		t.Fatalf("got %d records, want 1: %+v", len(recs), recs)
	}
	return recs[0]
}

// selfGet fetches u, with a Range header unless rng is empty, and reads the body until it ends or fails.
func selfGet(t *testing.T, u, rng string) (*http.Response, []byte, error) {
	t.Helper()
	req, err := http.NewRequestWithContext(selfCtx(t, waitTimeout), http.MethodGet, u, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rng != "" {
		req.Header.Set("Range", rng)
	}
	resp, err := selfClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	return resp, body, err
}

// selfRequest sends body to u and returns the status and response body; status 0 means no response arrived.
func selfRequest(ctx context.Context, method, u string, body []byte) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, u, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	resp, err := selfClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	return resp.StatusCode, got, err
}

// selfRequestAsync runs selfRequest in a goroutine and reports how it ended on the returned channel.
func selfRequestAsync(ctx context.Context, method, u string, body []byte) chan selfResponse {
	done := make(chan selfResponse, 1)
	go func() {
		status, _, err := selfRequest(ctx, method, u, body)
		done <- selfResponse{status: status, err: err}
	}()
	return done
}

// selfNoResponse fails t if the request reporting on done ends within d.
func selfNoResponse(t *testing.T, done <-chan selfResponse, d time.Duration) {
	t.Helper()
	select {
	case r := <-done:
		t.Fatalf("status %d, err %v; want no response within %v", r.status, r.err, d)
	case <-time.After(d):
	}
}

// selfPostHead is the head of a POST /upload to addr declaring a body of n bytes.
func selfPostHead(addr string, n int) string {
	return fmt.Sprintf("POST /upload HTTP/1.1\r\nHost: %s\r\nContent-Length: %d\r\n\r\n", addr, n)
}

// selfOK fails t unless GET u answers 200 "ok".
func selfOK(t *testing.T, u string) {
	t.Helper()
	resp, got, err := selfGet(t, u, "")
	if resp.StatusCode != http.StatusOK || err != nil || string(got) != "ok" {
		t.Fatalf("GET %s: status %d, body %q, err %v; want 200 ok", u, resp.StatusCode, got, err)
	}
}

// selfConnErr reports whether err is a *url.Error caused by the connection rather than the client's deadline.
func selfConnErr(err error) bool {
	var uerr *url.Error
	return errors.As(err, &uerr) && !errors.Is(err, context.DeadlineExceeded)
}

// selfRawGet sends a GET over its own connection, so a read deadline leaves the request open.
func selfRawGet(t *testing.T, addr string) (*net.TCPConn, *bufio.Reader) {
	t.Helper()
	c := selfDial(t, addr)
	if _, err := io.WriteString(c, "GET /blob HTTP/1.1\r\nHost: "+addr+"\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	return c, bufio.NewReader(c)
}

// selfEcho starts a TCP echo server; each connection echoes until its peer half-closes.
func selfEcho(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	conns := map[net.Conn]bool{}
	wg.Go(func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			if conns == nil {
				mu.Unlock()
				c.Close()
				return
			}
			conns[c] = true
			mu.Unlock()
			wg.Go(func() {
				defer c.Close()
				_, _ = io.Copy(c, c)
			})
		}
	})
	t.Cleanup(func() {
		ln.Close()
		mu.Lock()
		for c := range conns {
			c.Close()
		}
		conns = nil
		mu.Unlock()
		wg.Wait()
	})
	return ln.Addr().String()
}

// selfDial connects to addr and closes the connection at cleanup.
func selfDial(t *testing.T, addr string) *net.TCPConn {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, waitTimeout)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c.(*net.TCPConn)
}

// selfDeadline sets c's read deadline d from now.
func selfDeadline(t *testing.T, c net.Conn, d time.Duration) {
	t.Helper()
	if err := c.SetReadDeadline(time.Now().Add(d)); err != nil {
		t.Fatal(err)
	}
}

// selfPing writes selfPingMsg to c and returns how long its echo took to come back.
func selfPing(t *testing.T, c net.Conn) time.Duration {
	t.Helper()
	start := time.Now()
	if _, err := io.WriteString(c, selfPingMsg); err != nil {
		t.Fatal(err)
	}
	selfDeadline(t, c, waitTimeout)
	got := make([]byte, len(selfPingMsg))
	if _, err := io.ReadFull(c, got); err != nil || string(got) != selfPingMsg {
		t.Fatalf("echo %q, err %v; want %q", got, err, selfPingMsg)
	}
	return time.Since(start)
}

// selfExchange sends data over a new connection to addr, half-closes it and returns what came back before EOF or an error.
func selfExchange(t *testing.T, addr string, data []byte) ([]byte, error) {
	t.Helper()
	c := selfDial(t, addr)
	defer c.Close()
	go func() {
		// A cut may reset the connection mid-write; the reads tell the outcome.
		if _, err := c.Write(data); err == nil {
			_ = c.CloseWrite()
		}
	}()
	selfDeadline(t, c, waitTimeout)
	return io.ReadAll(c)
}
