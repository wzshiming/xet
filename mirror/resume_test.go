package mirror

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// The spool file name must embed the content identity (etag + size), so a
// reopen with the same validators resumes from the file length and different
// validators land in a different, swept-clean file.
func TestSpoolNamedByValidatorsResume(t *testing.T) {
	dir := t.TempDir()
	const key = "/org/repo/resolve/main/a.bin"
	etag := strings.Repeat("ab", 32) // hex sha256-style etag

	sp, err := openSpool(dir, key, etag, 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sp.Write(make([]byte, 40)); err != nil {
		t.Fatal(err)
	}
	name := filepath.Base(sp.f.Name())
	if want := spoolKeyPrefix(key) + etag + "-100.spool"; name != want {
		t.Fatalf("spool name = %q, want %q", name, want)
	}
	sp.finish(fmt.Errorf("interrupted"))

	// Same validators: resume from the partial length.
	sp2, err := openSpool(dir, key, etag, 100)
	if err != nil {
		t.Fatal(err)
	}
	if got := sp2.size(); got != 40 {
		t.Fatalf("resumed size = %d, want 40", got)
	}
	sp2.finish(fmt.Errorf("interrupted again"))

	// Changed etag: new file from zero, stale sibling swept.
	etag2 := strings.Repeat("cd", 32)
	sp3, err := openSpool(dir, key, etag2, 100)
	if err != nil {
		t.Fatal(err)
	}
	if got := sp3.size(); got != 0 {
		t.Fatalf("size after etag change = %d, want 0", got)
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var spools []string
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".spool") {
			spools = append(spools, e.Name())
		}
	}
	if len(spools) != 1 || spools[0] != spoolKeyPrefix(key)+etag2+"-100.spool" {
		t.Fatalf("stale spool not swept: %v", spools)
	}
	sp3.finish(nil)

	// No etag: nothing trustworthy in the name, always start fresh.
	sp4, err := openSpool(dir, key, "", -1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sp4.Write(make([]byte, 8)); err != nil {
		t.Fatal(err)
	}
	sp4.finish(fmt.Errorf("interrupted"))
	sp5, err := openSpool(dir, key, "", -1)
	if err != nil {
		t.Fatal(err)
	}
	if got := sp5.size(); got != 0 {
		t.Fatalf("etag-less spool resumed (%d bytes), want truncation", got)
	}
	sp5.finish(nil)
}

// Reproduction for: a client disconnects mid-download, then a new client
// arrives. The new client must immediately receive the already-spooled bytes
// (the ingest keeps running in the background) instead of starting from a
// fresh, empty download.
func TestMirrorClientDisconnectThenNewClient(t *testing.T) {
	upstream := newPlainUpstream()
	upstream.gate = make(chan struct{})
	upstream.gateHit = make(chan struct{})
	upstreamSrv := httptest.NewServer(upstream)
	defer upstreamSrv.Close()

	data := make([]byte, 128*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	const resolvePath = "/org/repo/resolve/main/interrupted.bin"
	upstream.set(resolvePath, data)

	fx := newMirrorFixture(t, upstreamSrv.URL, t.TempDir(), t.TempDir())
	resolveURL := fx.srv.URL + resolvePath
	half := len(data) / 2

	// Client 1: start the download, read the first half, then disconnect.
	ctx1, cancel1 := context.WithCancel(context.Background())
	req1, _ := http.NewRequestWithContext(ctx1, http.MethodGet, resolveURL, nil)
	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatal(err)
	}
	buf1 := make([]byte, half)
	if _, err := io.ReadFull(resp1.Body, buf1); err != nil {
		t.Fatalf("client1 read first half: %v", err)
	}
	if !bytes.Equal(buf1, data[:half]) {
		t.Fatal("client1 first-half mismatch")
	}
	cancel1() // simulate the client aborting mid-download
	resp1.Body.Close()

	// The upstream is now stalled at the half-way gate; the ingest is still
	// running in the background holding the first half in the spool.
	<-upstream.gateHit

	// Client 2: a fresh request. The first half must arrive promptly from the
	// spool even though the upstream is still stalled.
	req2, _ := http.NewRequest(http.MethodGet, resolveURL, nil)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()

	buf2 := make([]byte, half)
	readDone := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(resp2.Body, buf2)
		readDone <- err
	}()
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("client2 read spooled half: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("client2 did not receive the already-spooled bytes while upstream stalled: progress restarted from scratch")
	}
	if !bytes.Equal(buf2, data[:half]) {
		t.Fatal("client2 first-half mismatch")
	}

	// Unstall the upstream; client 2 must receive the rest.
	close(upstream.gate)
	rest, err := io.ReadAll(resp2.Body)
	if err != nil {
		t.Fatalf("client2 read rest: %v", err)
	}
	if !bytes.Equal(rest, data[half:]) {
		t.Fatal("client2 second-half mismatch")
	}
	waitReady(t, resolveURL)
	if got := upstream.dataGETs.Load(); got != 1 {
		t.Fatalf("upstream data GETs = %d, want 1 (no restart)", got)
	}
}

// Reproduction for the resume-with-Range variant (huggingface_hub style):
// client 2 resumes with a Range request for the not-yet-complete region.
func TestMirrorClientDisconnectThenRangeResume(t *testing.T) {
	upstream := newPlainUpstream()
	upstream.gate = make(chan struct{})
	upstream.gateHit = make(chan struct{})
	upstreamSrv := httptest.NewServer(upstream)
	defer upstreamSrv.Close()

	data := make([]byte, 128*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	const resolvePath = "/org/repo/resolve/main/interrupted2.bin"
	upstream.set(resolvePath, data)

	fx := newMirrorFixture(t, upstreamSrv.URL, t.TempDir(), t.TempDir())
	resolveURL := fx.srv.URL + resolvePath
	quarter := len(data) / 4

	// Client 1 reads a quarter and disconnects.
	ctx1, cancel1 := context.WithCancel(context.Background())
	req1, _ := http.NewRequestWithContext(ctx1, http.MethodGet, resolveURL, nil)
	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(resp1.Body, make([]byte, quarter)); err != nil {
		t.Fatalf("client1 read: %v", err)
	}
	cancel1()
	resp1.Body.Close()

	<-upstream.gateHit // ingest stalled at half

	// Client 2 resumes from its quarter with a Range request; the region up to
	// the spooled half must arrive promptly.
	req2, _ := http.NewRequest(http.MethodGet, resolveURL, nil)
	req2.Header.Set("Range", fmt.Sprintf("bytes=%d-", quarter))
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusPartialContent {
		t.Fatalf("resume status = %d, want 206", resp2.StatusCode)
	}

	upToHalf := make([]byte, len(data)/2-quarter)
	readDone := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(resp2.Body, upToHalf)
		readDone <- err
	}()
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("client2 read spooled region: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("client2 Range resume did not receive already-spooled bytes")
	}
	if !bytes.Equal(upToHalf, data[quarter:len(data)/2]) {
		t.Fatal("client2 resumed region mismatch")
	}

	close(upstream.gate)
	rest, err := io.ReadAll(resp2.Body)
	if err != nil {
		t.Fatalf("client2 read rest: %v", err)
	}
	if !bytes.Equal(rest, data[len(data)/2:]) {
		t.Fatal("client2 tail mismatch")
	}
}

// flakyUpstream serves one partial body (sendMax bytes, then a dropped
// connection) and answers every following data request with 500 while
// failLeft > 0, so the whole ingest task fails with partial progress. It
// records the Range offsets of incoming data requests.
type flakyUpstream struct {
	mu          sync.Mutex
	data        []byte
	commit      string
	etag        string
	failLeft    int   // remaining forced failures
	sendMax     int   // bytes to send on the first failing request
	partialSent bool  // the one partial body has been served
	offsets     []int // Range start of each data GET
}

func (u *flakyUpstream) heal() {
	u.mu.Lock()
	u.failLeft = 0
	u.mu.Unlock()
}

func (u *flakyUpstream) rangeOffsets() []int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]int(nil), u.offsets...)
}

func (u *flakyUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/cdn") {
		u.mu.Lock()
		data := u.data
		offset := 0
		if rg := r.Header.Get("Range"); strings.HasPrefix(rg, "bytes=") {
			fmt.Sscanf(rg, "bytes=%d-", &offset)
		}
		mode := "serve"
		if r.Method == http.MethodGet {
			u.offsets = append(u.offsets, offset)
			if u.failLeft > 0 {
				u.failLeft--
				if u.partialSent {
					mode = "error"
				} else {
					u.partialSent = true
					mode = "partial"
				}
			}
		}
		sendMax := u.sendMax
		u.mu.Unlock()

		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", fmt.Sprint(len(data)))
			return
		}
		if mode == "error" {
			http.Error(w, "upstream flake", http.StatusInternalServerError)
			return
		}
		if offset > 0 {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, len(data)-1, len(data)))
			w.Header().Set("Content-Length", fmt.Sprint(len(data)-offset))
			w.WriteHeader(http.StatusPartialContent)
		} else {
			w.Header().Set("Content-Length", fmt.Sprint(len(data)))
			w.WriteHeader(http.StatusOK)
		}
		body := data[offset:]
		if mode == "partial" && len(body) > sendMax {
			_, _ = w.Write(body[:sendMax])
			w.(http.Flusher).Flush()
			// Drop the connection mid-body.
			if hj, ok := w.(http.Hijacker); ok {
				if conn, _, err := hj.Hijack(); err == nil {
					conn.Close()
					return
				}
			}
			panic(http.ErrAbortHandler)
		}
		_, _ = w.Write(body)
		return
	}

	u.mu.Lock()
	etag, commit, size := u.etag, u.commit, len(u.data)
	u.mu.Unlock()
	w.Header().Set("ETag", `"`+etag+`"`)
	w.Header().Set("X-Linked-Etag", `"`+etag+`"`)
	w.Header().Set("X-Linked-Size", fmt.Sprint(size))
	w.Header().Set("X-Repo-Commit", commit)
	http.Redirect(w, r, "/cdn"+r.URL.Path, http.StatusFound)
}

// clearBackoff lets the next request start a fresh task immediately.
func clearBackoff(h *Handler, key string) {
	k, _ := parseResolveKey(key)
	h.mu.Lock()
	if e := h.entries[k]; e != nil && e.State == stateFailed {
		e.nextRetry = time.Time{}
	}
	h.mu.Unlock()
}

// A task that dies mid-download must leave its partial spool behind, and the
// next task must resume from that offset instead of refetching from zero.
func TestMirrorResumeAfterTaskFailure(t *testing.T) {
	data := make([]byte, 96*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	up := &flakyUpstream{data: data, commit: "commit-1", etag: "etag-1", failLeft: 1000, sendMax: 32 * 1024}
	upstreamSrv := httptest.NewServer(up)
	defer upstreamSrv.Close()

	const resolvePath = "/org/repo/resolve/main/flaky.bin"
	fx := newMirrorFixture(t, upstreamSrv.URL, t.TempDir(), t.TempDir())
	resolveURL := fx.srv.URL + resolvePath

	// First request: the upstream serves one partial body then fails hard, so
	// the task fails with partial progress, and each retry must have resumed
	// from the previous offset.
	resp, err := http.Get(resolveURL)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	waitFailed(t, fx.handler, resolvePath)
	offsets := up.rangeOffsets()
	if len(offsets) < 2 {
		t.Fatalf("expected multiple fetch attempts, got offsets %v", offsets)
	}
	for _, off := range offsets[1:] {
		if off == 0 {
			t.Fatalf("a retry restarted from 0 instead of resuming: offsets %v", offsets)
		}
	}

	// Second request (upstream healed, backoff cleared): the new task must
	// resume from the partial spool, not from zero.
	up.heal()
	clearBackoff(fx.handler, resolvePath)
	before := len(offsets)
	resp, err = http.Get(resolveURL)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body, data) {
		t.Fatalf("body mismatch after resume: got %d bytes, want %d", len(body), len(data))
	}
	offsets = up.rangeOffsets()
	if len(offsets) <= before {
		t.Fatal("second task issued no upstream fetch")
	}
	if first := offsets[before]; first == 0 {
		t.Fatalf("second task restarted from 0 instead of resuming: offsets %v", offsets)
	}
	waitReady(t, resolveURL)
}

// A restart of the mirror process must also resume from the partial spool.
func TestMirrorResumeAcrossRestart(t *testing.T) {
	data := make([]byte, 96*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	up := &flakyUpstream{data: data, commit: "commit-1", etag: "etag-1", failLeft: 1000, sendMax: 24 * 1024}
	upstreamSrv := httptest.NewServer(up)
	defer upstreamSrv.Close()

	const resolvePath = "/org/repo/resolve/main/restart.bin"
	storageDir, cacheDir := t.TempDir(), t.TempDir()
	fx := newMirrorFixture(t, upstreamSrv.URL, storageDir, cacheDir)

	resp, err := http.Get(fx.srv.URL + resolvePath)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	waitFailed(t, fx.handler, resolvePath)
	up.heal()
	before := len(up.rangeOffsets())

	// "Restart": a fresh handler over the same cache dir.
	fx2 := newMirrorFixture(t, upstreamSrv.URL, storageDir, cacheDir)
	resp, err = http.Get(fx2.srv.URL + resolvePath)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body, data) {
		t.Fatalf("body mismatch after restart: got %d bytes, want %d", len(body), len(data))
	}
	offsets := up.rangeOffsets()
	if len(offsets) <= before {
		t.Fatal("restarted mirror issued no upstream fetch")
	}
	if first := offsets[before]; first == 0 {
		t.Fatalf("restarted mirror refetched from 0 instead of resuming: offsets %v", offsets)
	}
	waitReady(t, fx2.srv.URL+resolvePath)
}

// A changed upstream etag must invalidate the partial spool instead of
// resuming into mismatched content.
func TestMirrorStalePartialDiscarded(t *testing.T) {
	data := make([]byte, 64*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	up := &flakyUpstream{data: data, commit: "commit-1", etag: "etag-1", failLeft: 1000, sendMax: 16 * 1024}
	upstreamSrv := httptest.NewServer(up)
	defer upstreamSrv.Close()

	const resolvePath = "/org/repo/resolve/main/stale.bin"
	fx := newMirrorFixture(t, upstreamSrv.URL, t.TempDir(), t.TempDir())
	resolveURL := fx.srv.URL + resolvePath

	resp, err := http.Get(resolveURL)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	waitFailed(t, fx.handler, resolvePath)
	before := len(up.rangeOffsets())

	// Upstream content changed: new etag, new bytes.
	data2 := make([]byte, 64*1024)
	if _, err := rand.Read(data2); err != nil {
		t.Fatal(err)
	}
	up.mu.Lock()
	up.data = data2
	up.etag = "etag-2"
	up.commit = "commit-2"
	up.failLeft = 0
	up.mu.Unlock()

	clearBackoff(fx.handler, resolvePath)
	resp, err = http.Get(resolveURL)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body, data2) {
		t.Fatal("body mismatch after etag change")
	}
	offsets := up.rangeOffsets()
	if len(offsets) <= before {
		t.Fatal("no upstream fetch after etag change")
	}
	if first := offsets[before]; first != 0 {
		t.Fatalf("stale partial was resumed (offset %d) instead of discarded", first)
	}
	waitReady(t, resolveURL)
}

// waitFailed polls until the entry for key is in the failed state.
func waitFailed(t *testing.T, h *Handler, key string) {
	t.Helper()
	k, _ := parseResolveKey(key)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		h.mu.Lock()
		e := h.entries[k]
		_, running := h.tasks[k]
		h.mu.Unlock()
		if !running && e != nil && e.State == stateFailed {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("task never reached failed state")
}
