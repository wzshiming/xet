package mirror

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage/local"
	"github.com/wzshiming/xet/upload"
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
	hangOnce    bool
	canceled    int
	abort       chan struct{}
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
			if u.hangOnce && !u.partialSent {
				u.partialSent = true
				mode = "hang"
			} else if u.failLeft > 0 {
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
		if mode == "hang" {
			_, _ = w.Write(body[:sendMax])
			w.(http.Flusher).Flush()
			select {
			case <-r.Context().Done():
				u.mu.Lock()
				u.canceled++
				u.mu.Unlock()
			case <-u.abort:
			}
			return
		}
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

// clearBackoff lets the next ingest start a fresh task immediately.
func clearBackoff(m *Mirror, key string) {
	k, _ := parseResolveKey(key)
	m.mu.Lock()
	if e := m.entries[k]; e != nil && e.State == stateFailed {
		e.nextRetry = time.Time{}
	}
	m.mu.Unlock()
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
	m, stor := newTestMirror(t, upstreamSrv.URL, t.TempDir(), t.TempDir())

	// First ingest: the upstream serves one partial body then fails hard, so
	// the task fails with partial progress, and each retry must have resumed
	// from the previous offset.
	in, err := m.Ingest("org/repo", "main", "flaky.bin")
	if err != nil {
		t.Fatal(err)
	}
	<-in.Done()
	if _, err := in.Entry(); err == nil {
		t.Fatal("ingest against a failing upstream unexpectedly succeeded")
	}

	offsets := up.rangeOffsets()
	if len(offsets) < 2 {
		t.Fatalf("expected multiple fetch attempts, got offsets %v", offsets)
	}
	for _, off := range offsets[1:] {
		if off == 0 {
			t.Fatalf("a retry restarted from 0 instead of resuming: offsets %v", offsets)
		}
	}

	// Second ingest (upstream healed, backoff cleared): the new task must
	// resume from the partial spool, not from zero.
	up.heal()
	clearBackoff(m, resolvePath)
	before := len(offsets)
	in, err = m.Ingest("org/repo", "main", "flaky.bin")
	if err != nil {
		t.Fatal(err)
	}
	<-in.Done()
	entry, err := in.Entry()
	if err != nil {
		t.Fatal(err)
	}
	if got := readStored(t, stor, entry.SHA256); !bytes.Equal(got, data) {
		t.Fatalf("stored bytes mismatch after resume: got %d bytes, want %d", len(got), len(data))
	}
	offsets = up.rangeOffsets()
	if len(offsets) <= before {
		t.Fatal("second task issued no upstream fetch")
	}
	if first := offsets[before]; first == 0 {
		t.Fatalf("second task restarted from 0 instead of resuming: offsets %v", offsets)
	}
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

	storageDir, cacheDir := t.TempDir(), t.TempDir()
	m, _ := newTestMirror(t, upstreamSrv.URL, storageDir, cacheDir)

	in, err := m.Ingest("org/repo", "main", "restart.bin")
	if err != nil {
		t.Fatal(err)
	}
	<-in.Done()
	if _, err := in.Entry(); err == nil {
		t.Fatal("ingest against a failing upstream unexpectedly succeeded")
	}
	up.heal()
	before := len(up.rangeOffsets())

	// "Restart": a fresh engine over the same cache dir.
	m2, stor2 := newTestMirror(t, upstreamSrv.URL, storageDir, cacheDir)
	in, err = m2.Ingest("org/repo", "main", "restart.bin")
	if err != nil {
		t.Fatal(err)
	}
	<-in.Done()
	entry, err := in.Entry()
	if err != nil {
		t.Fatal(err)
	}
	if got := readStored(t, stor2, entry.SHA256); !bytes.Equal(got, data) {
		t.Fatalf("stored bytes mismatch after restart: got %d bytes, want %d", len(got), len(data))
	}
	offsets := up.rangeOffsets()
	if len(offsets) <= before {
		t.Fatal("restarted mirror issued no upstream fetch")
	}
	if first := offsets[before]; first == 0 {
		t.Fatalf("restarted mirror refetched from 0 instead of resuming: offsets %v", offsets)
	}
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
	m, stor := newTestMirror(t, upstreamSrv.URL, t.TempDir(), t.TempDir())

	in, err := m.Ingest("org/repo", "main", "stale.bin")
	if err != nil {
		t.Fatal(err)
	}
	<-in.Done()
	if _, err := in.Entry(); err == nil {
		t.Fatal("ingest against a failing upstream unexpectedly succeeded")
	}
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

	clearBackoff(m, resolvePath)
	in, err = m.Ingest("org/repo", "main", "stale.bin")
	if err != nil {
		t.Fatal(err)
	}
	<-in.Done()
	entry, err := in.Entry()
	if err != nil {
		t.Fatal(err)
	}
	if got := readStored(t, stor, entry.SHA256); !bytes.Equal(got, data2) {
		t.Fatal("stored bytes mismatch after etag change")
	}
	offsets := up.rangeOffsets()
	if len(offsets) <= before {
		t.Fatal("no upstream fetch after etag change")
	}
	if first := offsets[before]; first != 0 {
		t.Fatalf("stale partial was resumed (offset %d) instead of discarded", first)
	}
}

func TestMirrorStalledPlainFetchResumes(t *testing.T) {
	data := make([]byte, 96*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	up := &flakyUpstream{data: data, commit: "commit-1", etag: "etag-1", hangOnce: true, sendMax: 32 * 1024, abort: make(chan struct{})}
	srv := httptest.NewServer(up)
	t.Cleanup(srv.Close)

	m, stor := newTestMirror(t, srv.URL, t.TempDir(), t.TempDir())
	t.Cleanup(func() { close(up.abort) })
	// Shorten the upstream read-idle guard below the shared auth injector.
	m.probeClient.Transport.(*authInjector).inner = client.NewIdleTimeoutTransport(http.DefaultTransport.(*http.Transport).Clone(), 200*time.Millisecond)

	in, err := m.Ingest("org/repo", "main", "stall.bin")
	if err != nil {
		t.Fatal(err)
	}
	awaitClosed(t, in.Done(), "stalled ingest")
	entry, err := in.Entry()
	if err != nil {
		t.Fatal(err)
	}
	up.mu.Lock()
	canceled := up.canceled
	up.mu.Unlock()
	if canceled != 1 {
		t.Fatalf("upstream saw %d canceled requests, want the stalled one", canceled)
	}
	if offsets := up.rangeOffsets(); !slices.Equal(offsets, []int{0, 32 * 1024}) {
		t.Fatalf("data GET offsets = %v, want [0 32768]: resume from the exact prefix", offsets)
	}
	if got := readStored(t, stor, entry.SHA256); !bytes.Equal(got, data) {
		t.Fatal("stored bytes mismatch after stall resume")
	}
}

// The second xorb is paced or stalled before its first chunk completes.
type xetStallUpstream struct {
	hubURL, casURL string
	fileHash       string
	sha256         string
	size           int
	xorbB          string // hash of the stalled xorb
	slowFeed       bool
	stalls         atomic.Int32
	canceled       atomic.Int32
	xorbGETs       atomic.Int32
	mu             sync.Mutex
	reconRanges    []string // Range header of each reconstruction request
	xorbBRanges    []string // Range header of each GET for the stalled xorb
	abort          chan struct{}
}

type stallWriter struct {
	http.ResponseWriter
	r *http.Request
	u *xetStallUpstream
}

func (w *stallWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p[:min(len(p), 4096)])
	w.ResponseWriter.(http.Flusher).Flush()
	select {
	case <-w.r.Context().Done():
		w.u.canceled.Add(1)
	case <-w.u.abort:
	}
	return n, errors.Join(err, errors.New("stalled"))
}

// Partial chunks must count as progress before they can reach the spool.
type slowWriter struct {
	http.ResponseWriter
	r             *http.Request
	u             *xetStallUpstream
	sent, slowEnd int
}

func (w *slowWriter) Write(p []byte) (int, error) {
	if w.slowEnd == 0 {
		w.slowEnd = 8 + (int(p[1]) | int(p[2])<<8 | int(p[3])<<16) - 1 // chunk header + compressed size
	}
	written := 0
	for len(p) > 0 {
		n, paced := len(p), w.sent < w.slowEnd
		if paced {
			n = min(n, (w.slowEnd+29)/30, w.slowEnd-w.sent)
		}
		k, err := w.ResponseWriter.Write(p[:n])
		w.sent, written, p = w.sent+k, written+k, p[n:]
		if err != nil {
			return written, err
		}
		if !paced {
			continue
		}
		w.ResponseWriter.(http.Flusher).Flush()
		select {
		case <-time.After(20 * time.Millisecond):
		case <-w.r.Context().Done():
			w.u.canceled.Add(1)
			return written, w.r.Context().Err()
		case <-w.u.abort:
			return written, errors.New("aborted")
		}
	}
	return written, nil
}

func newXetStallUpstream(t *testing.T, resolvePath string, head, tail []byte) *xetStallUpstream {
	t.Helper()
	u := &xetStallUpstream{abort: make(chan struct{})}
	data := append(append([]byte(nil), head...), tail...)

	var cas http.Handler
	casSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/reconstructions/"):
			u.mu.Lock()
			u.reconRanges = append(u.reconRanges, r.Header.Get("Range"))
			u.mu.Unlock()
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/xorbs/"):
			u.xorbGETs.Add(1)
			if strings.Contains(r.URL.Path, u.xorbB) {
				u.mu.Lock()
				u.xorbBRanges = append(u.xorbBRanges, r.Header.Get("Range"))
				u.mu.Unlock()
				if u.slowFeed {
					w = &slowWriter{ResponseWriter: w, r: r, u: u}
				} else if u.stalls.Add(1) == 1 {
					w = &stallWriter{ResponseWriter: w, r: r, u: u}
				}
			}
		}
		cas.ServeHTTP(w, r)
	}))
	t.Cleanup(casSrv.Close)
	stor, err := local.NewStorage(local.WithBasePath(t.TempDir()), local.WithBaseURL(casSrv.URL))
	if err != nil {
		t.Fatal(err)
	}
	cas = server.NewHandler(server.WithStorage(stor))
	ctx := context.Background()
	seed := &localCAS{storage: stor, namespace: "default"}
	// Seeding the prefix makes the combined upload span two xorbs.
	if _, err := upload.UploadFile(ctx, seed, bytes.NewReader(head), upload.WithEnableSHA256(true)); err != nil {
		t.Fatal(err)
	}
	fileHash, err := upload.UploadFile(ctx, seed, bytes.NewReader(data), upload.WithEnableSHA256(true))
	if err != nil {
		t.Fatal(err)
	}
	sh, err := stor.GetShard(ctx, fileHash)
	if err != nil {
		t.Fatal(err)
	}
	entries := sh.Files[0].Entries
	if len(entries) != 2 || entries[0].CASHash == entries[1].CASHash {
		t.Fatalf("fixture file spans %d terms, want two xorbs", len(entries))
	}
	u.xorbB = entries[1].CASHash.String()
	sum := sha256.Sum256(data)
	u.fileHash, u.sha256, u.size = fileHash.String(), hex.EncodeToString(sum[:]), len(data)

	hubSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/xet-read-token" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"casUrl": u.casURL, "accessToken": "upstream-cas-token", "exp": time.Now().Add(time.Hour).Unix()})
			return
		}
		if r.URL.Path != resolvePath {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("ETag", `"`+u.sha256+`"`)
		w.Header().Set("X-Linked-Size", fmt.Sprint(u.size))
		w.Header().Set("X-Repo-Commit", "xet-commit-1")
		w.Header().Set("X-Xet-Hash", u.fileHash)
		w.Header().Add("Link", fmt.Sprintf("<%s/v1/reconstructions/%s>; rel=\"xet-reconstruction-info\"", u.casURL, u.fileHash))
		w.Header().Add("Link", fmt.Sprintf("<%s/api/xet-read-token>; rel=\"xet-auth\"", u.hubURL))
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(hubSrv.Close)
	u.casURL, u.hubURL = casSrv.URL, hubSrv.URL
	return u
}

// shortIdleClient is a xet client whose GET/HEAD read-idle guard fires after 200ms.
func shortIdleClient(t *testing.T) *client.Client {
	t.Helper()
	c, err := client.NewClient(client.WithIdleTimeout(200*time.Millisecond), client.WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// A stalled term body is resumed by the client at the compressed offset: no new attempt, no second reconstruction query.
func TestMirrorStalledXetFetchResumes(t *testing.T) {
	head, tail := make([]byte, 256*1024), make([]byte, 256*1024)
	if _, err := rand.Read(head); err != nil {
		t.Fatal(err)
	}
	if _, err := rand.Read(tail); err != nil {
		t.Fatal(err)
	}
	const resolvePath = "/org/repo/resolve/main/stall.bin"
	up := newXetStallUpstream(t, resolvePath, head, tail)
	m, stor := newTestMirror(t, up.hubURL, t.TempDir(), t.TempDir(), WithClient(shortIdleClient(t)))
	t.Cleanup(func() { close(up.abort) }) // runs first: unblocks a still-stalled handler before servers close

	in, err := m.Ingest("org/repo", "main", "stall.bin")
	if err != nil {
		t.Fatal(err)
	}
	awaitClosed(t, in.Done(), "stalled xet ingest")
	entry, err := in.Entry()
	if err != nil {
		t.Fatal(err)
	}
	if got := up.canceled.Load(); got != 1 {
		t.Fatalf("upstream CAS saw %d canceled xorb requests, want the stalled one", got)
	}
	if got := up.xorbGETs.Load(); got != 3 {
		t.Fatalf("upstream xorb GETs = %d, want 3 (first xorb, stalled second, resumed second)", got)
	}
	up.mu.Lock()
	reconRanges := append([]string(nil), up.reconRanges...)
	xorbBRanges := append([]string(nil), up.xorbBRanges...)
	up.mu.Unlock()
	if !slices.Equal(reconRanges, []string{""}) {
		t.Fatalf("reconstruction Range headers = %q, want one unranged query", reconRanges)
	}
	var start, end int64
	if len(xorbBRanges) != 2 {
		t.Fatalf("stalled xorb Range headers = %q, want the stalled and the resumed request", xorbBRanges)
	}
	if _, err := fmt.Sscanf(xorbBRanges[0], "bytes=%d-%d", &start, &end); err != nil {
		t.Fatalf("stalled xorb Range %q: %v", xorbBRanges[0], err)
	}
	if want := fmt.Sprintf("bytes=%d-%d", start+4096, end); xorbBRanges[1] != want {
		t.Fatalf("resumed xorb Range = %q, want %q (the bytes the stalled response delivered)", xorbBRanges[1], want)
	}
	if entry.SHA256 != up.sha256 {
		t.Fatalf("entry sha256 = %s, want %s", entry.SHA256, up.sha256)
	}
	if got := readStored(t, stor, entry.SHA256); !bytes.Equal(got, append(head, tail...)) {
		t.Fatal("stored bytes mismatch after xet stall resume")
	}
}

// A term body that keeps moving without completing a chunk for longer than the idle timeout is live: one attempt, no cancel, exact bytes.
func TestMirrorSlowXetFetchNotStalled(t *testing.T) {
	head, tail := make([]byte, 256*1024), make([]byte, 256*1024)
	if _, err := rand.Read(head); err != nil {
		t.Fatal(err)
	}
	if _, err := rand.Read(tail); err != nil {
		t.Fatal(err)
	}
	const resolvePath = "/org/repo/resolve/main/slow.bin"
	up := newXetStallUpstream(t, resolvePath, head, tail)
	up.slowFeed = true
	xc := shortIdleClient(t)
	cacheDir := t.TempDir()
	m, stor := newTestMirror(t, up.hubURL, t.TempDir(), cacheDir, WithClient(xc))
	t.Cleanup(func() { close(up.abort) })
	if m.xetClient != xc {
		t.Fatal("WithClient not used")
	}

	in, err := m.Ingest("org/repo", "main", "slow.bin")
	if err != nil {
		t.Fatal(err)
	}
	awaitClosed(t, in.Done(), "slow xet ingest")
	entry, err := in.Entry()
	if err != nil {
		t.Fatalf("continuously progressing transfer failed: %v", err)
	}
	if got := up.canceled.Load(); got != 0 {
		t.Fatalf("upstream CAS saw %d canceled xorb requests, want 0", got)
	}
	if got := up.xorbGETs.Load(); got != 2 {
		t.Fatalf("upstream xorb GETs = %d, want 2 (one per xorb, single attempt)", got)
	}
	if got := readStored(t, stor, entry.SHA256); !bytes.Equal(got, append(head, tail...)) {
		t.Fatal("stored bytes mismatch")
	}
	usage, err := xc.Usage(context.Background())
	if err != nil || usage.Download.Count == 0 {
		t.Fatalf("supplied client chunk cache: %+v, %v; want entries", usage.Download, err)
	}
	if _, err := os.Stat(filepath.Join(cacheDir, "chunks")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("mirror created its own chunk cache despite WithClient: %v", err)
	}
}
